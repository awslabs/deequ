import re
import logging

logger = logging.getLogger("issue_bot")

# Lightweight backup checks — primary protection is via Bedrock Guardrails.
_INJECTION_MARKERS = [
    "my system prompt is",
    "my instructions are",
    "here are my internal",
    "ignore previous instructions",
]


def sanitize(text):
    if not text:
        return text
    lower = text.lower()
    for m in _INJECTION_MARKERS:
        if m in lower:
            logger.error(f"BLOCKED: injection marker '{m}'")
            return None
    text = _fix_accidental_issue_refs(text)
    return text


def _fix_accidental_issue_refs(text):
    """Wrap #N references outside code blocks in backticks to prevent GitHub auto-linking."""
    lines = text.split("\n")
    in_code_block = False
    fixed = []
    for line in lines:
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
        if not in_code_block:
            line = re.sub(r'(?<!`)#(\d+)(?!\w)(?!`)', r'`#\1`', line)
        fixed.append(line)
    return "\n".join(fixed)
