import hashlib
import logging
import os

import boto3

logger = logging.getLogger("issue_bot")

_sm_client = None


def _get_sm_client():
    global _sm_client
    if _sm_client is None:
        _sm_client = boto3.client("secretsmanager")
    return _sm_client


def _read_from_sm(secret_id):
    if not secret_id:
        return ""
    try:
        resp = _get_sm_client().get_secret_value(SecretId=secret_id)
        return resp["SecretString"]
    except Exception as e:
        logger.error("Failed to read prompt from Secrets Manager: %s", type(e).__name__)
        return ""


def _get_prompt(env_var, sm_env_var):
    val = os.getenv(env_var, "")
    if val:
        return val
    return _read_from_sm(os.getenv(sm_env_var, ""))


def get_issue_prompt():
    return _get_prompt("ISSUE_CLASSIFY_PROMPT", "SM_ISSUE_CLASSIFY_PROMPT")


def get_issue_respond_prompt():
    return _get_prompt("ISSUE_RESPOND_PROMPT", "SM_ISSUE_RESPOND_PROMPT")


def get_pr_file_review_prompt():
    return _get_prompt("PR_FILE_REVIEW_PROMPT", "SM_PR_FILE_REVIEW_PROMPT")


def get_followup_prompt():
    return _get_prompt("FOLLOWUP_PROMPT", "SM_FOLLOWUP_PROMPT")


def prompt_version(template):
    return hashlib.sha256(template.encode()).hexdigest()[:8]
