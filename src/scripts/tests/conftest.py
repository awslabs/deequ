import pytest


@pytest.fixture(autouse=True)
def isolate_sm_env(monkeypatch):
    """Strip Secrets Manager env vars so dev shell envs don't leak into tests.

    Tests that need a specific prompt set it via PR_FILE_REVIEW_PROMPT etc.
    Without this, a developer with SM_* set in their shell would trigger
    real AWS calls during prompts._get_prompt fallback.
    """
    for var in (
        "SM_ISSUE_CLASSIFY_PROMPT",
        "SM_ISSUE_RESPOND_PROMPT",
        "SM_PR_FILE_REVIEW_PROMPT",
        "SM_PR_FILE_REVIEW_REPORT_PROMPT",
        "SM_PR_INVESTIGATOR_PROMPT",
        "SM_PR_CRITIC_PROMPT",
        "SM_PR_REPORTER_PROMPT",
        "SM_FOLLOWUP_PROMPT",
        "PR_FILE_REVIEW_REPORT_PROMPT",
        "PR_INVESTIGATOR_PROMPT",
        "PR_CRITIC_PROMPT",
        "PR_REPORTER_PROMPT",
        "BOT_AGENT_PIPELINE",
    ):
        monkeypatch.delenv(var, raising=False)
