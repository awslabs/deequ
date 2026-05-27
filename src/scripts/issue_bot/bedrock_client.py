import logging

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger("issue_bot")

_CIRCUIT_BREAKER_THRESHOLD = 3


class BedrockClient:
    def __init__(self, cfg):
        self._model_id = cfg.bedrock_model_id
        self._default_timeout = cfg.bedrock_timeout
        self._client = boto3.client(
            "bedrock-runtime",
            config=BotoConfig(
                read_timeout=self._default_timeout,
                connect_timeout=self._default_timeout,
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
        )
        self._guardrail_id = cfg.guardrail_id
        self._guardrail_version = cfg.guardrail_version
        self._failures = 0
        self._circuit_open = False  # Resets per-process; GHA runs are ephemeral

    def _client_for_timeout(self, timeout_seconds):
        """Return a client whose read_timeout is at most timeout_seconds.
        Single-attempt retry policy: retry storms aren't useful when the
        caller is already on a tight wall-clock budget."""
        if timeout_seconds is None or timeout_seconds >= self._default_timeout:
            return self._client
        if timeout_seconds <= 0:
            timeout_seconds = 1  # botocore rejects 0/negative
        return boto3.client(
            "bedrock-runtime",
            config=BotoConfig(
                read_timeout=timeout_seconds,
                connect_timeout=min(timeout_seconds, self._default_timeout),
                retries={"max_attempts": 1, "mode": "standard"},
            ),
        )

    @property
    def available(self):
        return not self._circuit_open

    @property
    def has_guardrail(self):
        """Whether a guardrail is configured on this client. Read by
        agent_loop to decide whether to compose guardContent blocks."""
        return bool(self._guardrail_id)

    def invoke(self, system_prompt, user_prompt, max_tokens=4096,
               temperature=0.3, json_schema=None):
        """Invoke Bedrock Converse API with guardrail on user message only.

        - system_prompt: Instructions + trusted context (KB, diffs, codebase).
            Passed as a plain SystemContentBlock. The guardrail does not assess
            system prompts without guardContent.
        - user_prompt: Untrusted user input (issue title/body, PR title/body,
            comments). When guardrail is configured, wrapped in guardContent
            so the guardrail scans it for prompt injection.

        No cachePoint is set: production logs (last 30 runs / 44 calls) show
        the cache read rate at 0% because the cached prefix includes per-PR
        diff bytes, so the cache key never repeats. A cachePoint here would
        only pay the 1.25x cache-write premium without ever yielding a read.
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
                kwargs["system"] = [{"text": system_prompt}]

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
            return (output[0].get("text") or "").strip()
        except (ClientError, BotoCoreError, ValueError, KeyError, ConnectionError) as e:
            self._failures += 1
            logger.error(f"Bedrock failed ({self._failures}/{_CIRCUIT_BREAKER_THRESHOLD}): {e}")
            if self._failures >= _CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open = True
                logger.error("Circuit breaker OPEN")
            return None

    def invoke_with_usage(self, system_prompt, user_prompt, max_tokens=4096,
                           temperature=0.3, json_schema=None,
                           timeout_seconds=None):
        """Like invoke() but also returns the response's token usage dict.
        Returns (None, {}) on failure.

        timeout_seconds (optional): per-call read/connect timeout, used by
        the Reporter to bound the call at the remaining wall-clock budget.

        No cachePoint: the Reporter (sole caller) embeds the per-PR diff
        in its system prompt, so the cache prefix never repeats.
        """
        if self._circuit_open:
            logger.warning("Circuit breaker open, skipping Bedrock call")
            return None, {}
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
                kwargs["system"] = [{"text": system_prompt}]
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
            client = self._client_for_timeout(timeout_seconds)
            resp = client.converse(**kwargs)
            if resp.get("stopReason") == "guardrail_intervened":
                logger.warning("Guardrail intervened: %s", resp.get("trace", ""))
                return None, resp.get("usage", {}) or {}
            output = resp.get("output", {}).get("message", {}).get("content", [])
            if not output:
                raise ValueError("Empty Bedrock response")
            self._failures = 0
            usage = resp.get("usage", {}) or {}
            logger.info(
                "Bedrock: input=%s, output=%s, cacheRead=%s, cacheWrite=%s",
                usage.get("inputTokens"), usage.get("outputTokens"),
                usage.get("cacheReadInputTokens"), usage.get("cacheWriteInputTokens"),
            )
            return (output[0].get("text") or "").strip(), usage
        except (ClientError, BotoCoreError, ValueError, KeyError, ConnectionError) as e:
            self._failures += 1
            logger.error(f"Bedrock failed ({self._failures}/{_CIRCUIT_BREAKER_THRESHOLD}): {e}")
            if self._failures >= _CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open = True
                logger.error("Circuit breaker OPEN")
            return None, {}

    def converse_with_tools(self, system_prompt, messages, tool_specs,
                             max_tokens=8000, temperature=0.3):
        """One turn of Converse with toolConfig. Returns the raw response
        dict (or None on failure / guardrail intervention / circuit open).

        cachePoint is set here (unlike invoke()/invoke_with_usage) because
        Investigator and Critic share the static prefix from
        _build_static_context — the Investigator's first turn warms a cache
        the Critic's first turn can read. Validate via cacheReadInputTokens
        on the Critic stage in the artifact metrics.
        """
        if self._circuit_open:
            logger.warning("Circuit breaker open, skipping Bedrock tool-use call")
            return None
        try:
            wrapped_messages = self._wrap_first_user_for_guardrail(messages)
            kwargs = {
                "modelId": self._model_id,
                "messages": wrapped_messages,
                "inferenceConfig": {"maxTokens": max_tokens, "temperature": temperature},
                "toolConfig": {"tools": tool_specs},
            }
            if system_prompt:
                kwargs["system"] = [
                    {"text": system_prompt},
                    {"cachePoint": {"type": "default"}},
                ]
            if self._guardrail_id:
                kwargs["guardrailConfig"] = {
                    "guardrailIdentifier": self._guardrail_id,
                    "guardrailVersion": self._guardrail_version,
                    "trace": "enabled",
                }
            resp = self._client.converse(**kwargs)
            if resp.get("stopReason") == "guardrail_intervened":
                logger.warning("Guardrail intervened on tool-use turn: %s", resp.get("trace", ""))
                return None
            self._failures = 0
            return resp
        except (ClientError, BotoCoreError, ValueError, ConnectionError) as e:
            self._failures += 1
            logger.error(f"Bedrock tool-use call failed ({self._failures}/{_CIRCUIT_BREAKER_THRESHOLD}): {e}")
            if self._failures >= _CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open = True
                logger.error("Circuit breaker OPEN")
            return None

    def _wrap_first_user_for_guardrail(self, messages):
        """Wrap the first user message's text in guardContent for guardrail
        scanning. Skipped when the caller has already set a guardContent
        block (signals an explicit trust boundary, don't re-wrap adjacent
        trusted text).

        Assumes callers using the [trusted, guardContent(untrusted)]
        composition pattern from agent_loop._build_user_content. Reusing
        this helper with arbitrary content layouts (e.g., multiple
        text blocks alongside a guardContent block) requires re-thinking
        the wrap rule — a text block in any position alongside a
        guardContent block currently passes through unwrapped.
        """
        if not self._guardrail_id or not messages:
            return messages
        out = []
        guarded = False
        for msg in messages:
            if not guarded and msg.get("role") == "user":
                content = msg.get("content", [])
                if any("guardContent" in block for block in content):
                    out.append(msg)
                    guarded = True
                    continue
                new_content = []
                for block in content:
                    if "text" in block:
                        new_content.append({"guardContent": {"text": {"text": block["text"]}}})
                    else:
                        new_content.append(block)
                out.append({**msg, "content": new_content})
                guarded = True
            else:
                out.append(msg)
        return out
