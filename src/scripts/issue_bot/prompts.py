import hashlib
import os


def get_issue_prompt():
    return os.getenv("ISSUE_CLASSIFY_PROMPT", "")


def get_issue_respond_prompt():
    return os.getenv("ISSUE_RESPOND_PROMPT", "")


def get_pr_file_review_prompt():
    return os.getenv("PR_FILE_REVIEW_PROMPT", "")


def get_followup_prompt():
    return os.getenv("FOLLOWUP_PROMPT", "")


def prompt_version(template):
    return hashlib.sha256(template.encode()).hexdigest()[:8]
