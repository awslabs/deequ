import re
import logging

logger = logging.getLogger("issue_bot")

# Primary defense: Bedrock Guardrails. These are the required backup layer.
_SECRET_PATTERNS = [
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"ghp_[0-9a-zA-Z]{36}"),
    re.compile(r"gho_[0-9a-zA-Z]{36}"),
    re.compile(r"ghs_[0-9a-zA-Z]{36}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{22,}"),
    re.compile(r"xox[bpras]-[A-Za-z0-9\-]+"),
    re.compile(r"https?://[^\s]*\.corp\.amazon\.com[^\s]*"),
    re.compile(r"https?://[^\s]*\.a2z\.com[^\s]*"),
    re.compile(r"https?://[^\s]*\.amazon\.dev[^\s]*"),
    re.compile(r"hooks\.slack\.com/services/\S+"),
]

_INJECTION_MARKERS = [
    "my system prompt is",
    "my instructions are",
    "here are my internal",
    "ignore previous instructions",
]


def sanitize(text):
    if not text:
        return text
    for p in _SECRET_PATTERNS:
        if p.search(text):
            logger.error(f"BLOCKED: secret pattern {p.pattern}")
            return None
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
