import json
import logging

import boto3
from botocore.config import Config as BotoConfig

logger = logging.getLogger("issue_bot")

_CIRCUIT_BREAKER_THRESHOLD = 3


class BedrockClient:
    def __init__(self, cfg):
        self._model_id = cfg.bedrock_model_id
        self._api_version = cfg.bedrock_api_version
        self._client = boto3.client(
            "bedrock-runtime",
            config=BotoConfig(
                read_timeout=cfg.bedrock_timeout,
                connect_timeout=cfg.bedrock_timeout,
                retries={"max_attempts": 1, "mode": "standard"},
            ),
        )
        self._failures = 0
        self._circuit_open = False

    @property
    def available(self):
        return not self._circuit_open

    def invoke(self, prompt, max_tokens=1500, temperature=0.3):
        if self._circuit_open:
            logger.warning("Circuit breaker open, skipping Bedrock call")
            return None
        try:
            resp = self._client.invoke_model(
                modelId=self._model_id,
                body=json.dumps({
                    "anthropic_version": self._api_version,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                }),
            )
            result = json.loads(resp["body"].read())
            if not result.get("content"):
                raise ValueError("Empty Bedrock response")
            self._failures = 0
            return result["content"][0]["text"].strip()
        except Exception as e:
            self._failures += 1
            logger.error(f"Bedrock failed ({self._failures}/{_CIRCUIT_BREAKER_THRESHOLD}): {e}")
            if self._failures >= _CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open = True
                logger.error("Circuit breaker OPEN")
            return None
