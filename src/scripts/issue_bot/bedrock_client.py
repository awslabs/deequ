import json
import logging

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger("issue_bot")

_CIRCUIT_BREAKER_THRESHOLD = 3


class BedrockClient:
    def __init__(self, cfg):
        self._model_id = cfg.bedrock_model_id
        self._client = boto3.client(
            "bedrock-runtime",
            config=BotoConfig(
                read_timeout=cfg.bedrock_timeout,
                connect_timeout=cfg.bedrock_timeout,
                retries={"max_attempts": 1, "mode": "standard"},
            ),
        )
        self._guardrail_id = cfg.guardrail_id
        self._failures = 0
        self._circuit_open = False

    @property
    def available(self):
        return not self._circuit_open

    def invoke(self, prompt, max_tokens=4096, temperature=0.3, json_schema=None):
        """Invoke Bedrock using the Converse API.

        Splits the prompt at <knowledge_base> to separate trusted instructions
        (system prompt, cached) from untrusted user content (guarded).
        """
        if self._circuit_open:
            logger.warning("Circuit breaker open, skipping Bedrock call")
            return None
        try:
            system_prompt, user_content = self._split_prompt(prompt)

            kwargs = {
                "modelId": self._model_id,
                "messages": [{"role": "user", "content": user_content}],
                "inferenceConfig": {"maxTokens": max_tokens, "temperature": temperature},
            }

            if system_prompt:
                kwargs["system"] = [
                    {"text": system_prompt},
                    {"cachePoint": {"type": "default"}},
                ]

            if json_schema:
                kwargs["outputConfig"] = {
                    "textFormat": {
                        "type": "json_schema",
                        "structure": {"jsonSchema": {
                            "schema": json_schema,
                            "name": "bot_response",
                        }},
                    }
                }

            if self._guardrail_id:
                kwargs["guardrailConfig"] = {
                    "guardrailIdentifier": self._guardrail_id,
                    "guardrailVersion": "1",
                    "trace": "enabled",
                }

            resp = self._client.converse(**kwargs)

            if resp.get("stopReason") == "guardrail_intervened":
                logger.warning("Guardrail intervened: %s", resp.get("trace", ""))
                return None

            output = resp.get("output", {}).get("message", {}).get("content", [])
            if not output:
                raise ValueError("Empty Bedrock response")

            self._failures = 0
            usage = resp.get("usage", {})
            logger.info("Bedrock: input=%s, output=%s, cacheRead=%s, cacheWrite=%s",
                        usage.get("inputTokens"), usage.get("outputTokens"),
                        usage.get("cacheReadInputTokens"), usage.get("cacheWriteInputTokens"))
            return output[0]["text"].strip()
        except Exception as e:
            self._failures += 1
            logger.error(f"Bedrock failed ({self._failures}/{_CIRCUIT_BREAKER_THRESHOLD}): {e}")
            if self._failures >= _CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open = True
                logger.error("Circuit breaker OPEN")
            return None

    def _split_prompt(self, prompt):
        """Split rendered prompt into trusted system prompt and untrusted user content.

        Everything before <knowledge_base> is trusted instructions (system prompt).
        Everything from <knowledge_base> onward contains user data (guarded).
        """
        marker = "<knowledge_base>"
        idx = prompt.find(marker)
        if idx == -1:
            # No marker — send everything as user content (unguarded)
            return None, [{"text": prompt}]

        system = prompt[:idx].strip()
        user_data = prompt[idx:].strip()

        if self._guardrail_id:
            user_content = [{"guardContent": {"text": {"text": user_data}}}]
        else:
            user_content = [{"text": user_data}]

        return system, user_content
