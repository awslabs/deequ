import logging

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, BotoCoreError

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
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
        )
        self._guardrail_id = cfg.guardrail_id
        self._guardrail_version = cfg.guardrail_version
        self._failures = 0
        self._circuit_open = False  # Resets per-process; GHA runs are ephemeral

    @property
    def available(self):
        return not self._circuit_open

    def invoke(self, system_prompt, user_prompt, max_tokens=4096,
               temperature=0.3, json_schema=None):
        """Invoke Bedrock Converse API with guardrail on user message only.

        Follows the GlueML pattern (BedrockModelHelper.java):
        - system_prompt: Instructions + trusted context (KB, diffs, codebase).
            Passed as plain text SystemContentBlock with cachePoint. The
            guardrail does NOT assess system prompts without guardContent.
        - user_prompt: Untrusted user input (issue title/body, PR title/body,
            comments). When guardrail is configured, wrapped in guardContent
            so the guardrail scans it for prompt injection.
        """
        if self._circuit_open:
            logger.warning("Circuit breaker open, skipping Bedrock call")
            return None
        try:
            if self._guardrail_id:
                user_content = [{"guardContent": {"text": {"text": user_prompt}}}]
            else:
                user_content = [{"text": user_prompt}]

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
                    "guardrailVersion": self._guardrail_version,
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
        except (ClientError, BotoCoreError, ValueError, ConnectionError) as e:
            self._failures += 1
            logger.error(f"Bedrock failed ({self._failures}/{_CIRCUIT_BREAKER_THRESHOLD}): {e}")
            if self._failures >= _CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open = True
                logger.error("Circuit breaker OPEN")
            return None
