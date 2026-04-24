import logging
import boto3

logger = logging.getLogger("issue_bot")


class KnowledgeBase:
    def __init__(self, cfg):
        self._bucket = cfg.kb_s3_bucket
        self._key = cfg.kb_s3_key
        self._max_chars = cfg.max_context_chars
        self._content = ""

    def load(self):
        if self._bucket and self._key:
            try:
                resp = boto3.client("s3").get_object(Bucket=self._bucket, Key=self._key)
                self._content = resp["Body"].read().decode("utf-8")
                logger.info(f"KB loaded from S3: {len(self._content)} chars")
                return
            except Exception as e:
                logger.warning(f"S3 KB failed: {e}")
        logger.warning("No KB available")

    def build_context(self, issue_text, repo_snippets=""):
        parts = []
        if self._content:
            parts.append(self._content)
        if repo_snippets:
            parts.append(f"## Relevant Source Code\n{repo_snippets}")
        context = "\n\n".join(parts)
        if len(context) > self._max_chars:
            context = self._truncate_by_relevance(context, issue_text)
        return context

    def _truncate_by_relevance(self, content, issue_text):
        keywords = set(issue_text.lower().split())
        sections = content.split("\n## ")
        if len(sections) <= 1:
            return content[:self._max_chars]
        scored = []
        for i, s in enumerate(sections):
            score = sum(1 for w in keywords if w in s.lower()) + max(0, 10 - i)
            scored.append((score, s))
        scored.sort(key=lambda x: x[0], reverse=True)
        result = ""
        for _, s in scored:
            chunk = f"\n## {s}" if result else s
            if len(result) + len(chunk) > self._max_chars:
                break
            result += chunk
        return result
