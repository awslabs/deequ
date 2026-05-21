import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("issue_bot")


class Config:
    def __init__(self):
        self.github_token = _require("GITHUB_TOKEN")
        self.event_type = _require("EVENT_TYPE")
        self.event_action = os.getenv("EVENT_ACTION", "")
        self.issue_number = _require("ISSUE_NUMBER")
        if not self.issue_number.isdigit():
            logger.error(f"ISSUE_NUMBER must be numeric: {self.issue_number}")
            sys.exit(1)
        self.repo = _require("GITHUB_REPOSITORY")
        self.actor = os.getenv("GITHUB_ACTOR", "")
        self.event_before = os.getenv("EVENT_BEFORE", "")
        self.event_after = os.getenv("EVENT_AFTER", "")

        self.bedrock_model_id = os.getenv("BEDROCK_MODEL_ID", "us.anthropic.claude-opus-4-6-v1")

        self.kb_s3_bucket = os.getenv("KB_S3_BUCKET", "")
        self.kb_s3_key = os.getenv("KB_S3_KEY", "")

        self.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
        self.guardrail_id = os.getenv("GUARDRAIL_ID", "")
        self.guardrail_version = os.getenv("GUARDRAIL_VERSION") or "DRAFT"

        self.dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
        self.enable_slack = bool(self.slack_webhook_url)
        self.enable_repo_search = os.getenv("ENABLE_REPO_SEARCH", "true").lower() == "true"

        self.upstream_repo = os.getenv("UPSTREAM_REPO", "awslabs/deequ")
        self.codebase_src_dir = os.getenv("CODEBASE_SRC_DIR", "src/main/scala")
        self.codebase_file_ext = os.getenv("CODEBASE_FILE_EXT", ".scala")

        self.bedrock_timeout = 240
        self.max_context_chars = 800000
        self.max_github_search_results = 8
        self.github_api_timeout = 10
        self.allowed_labels = {
            "bug", "enhancement", "question", "documentation",
            "help-wanted", "dqdl", "analyzer", "spark-compatibility",
        }

        # Agentic pipeline (Investigator/Critic/Reporter). Enabled only when
        # BOT_AGENT_PIPELINE is set to one of: 1, true, yes, on (case-insensitive,
        # whitespace-stripped). Any other value (including no/off/empty) keeps
        # the legacy two-phase flow active. Conservative: default off.
        self.agent_pipeline = os.getenv("BOT_AGENT_PIPELINE", "").strip().lower() in ("1", "true", "yes", "on")

        # Structural caps for runaway protection. Token usage is reported in
        # artifacts but is not used to enforce a budget — operators monitor
        # cost via the artifact metrics, not via this code.
        self.investigator_max_turns = _int_env("BOT_INVESTIGATOR_MAX_TURNS", 15, minimum=1)
        self.investigator_max_tool_calls = _int_env("BOT_INVESTIGATOR_MAX_TOOL_CALLS", 50, minimum=1)
        self.investigator_max_tool_output_chars = _int_env(
            "BOT_INVESTIGATOR_MAX_TOOL_OUTPUT", 400_000, minimum=1024,
        )
        self.investigator_wall_clock_seconds = _int_env(
            "BOT_INVESTIGATOR_WALL_CLOCK_S", 300, minimum=10,
        )

        self.critic_max_turns = _int_env("BOT_CRITIC_MAX_TURNS", 10, minimum=1)
        self.critic_max_tool_calls = _int_env("BOT_CRITIC_MAX_TOOL_CALLS", 30, minimum=1)
        self.critic_max_tool_output_chars = _int_env(
            "BOT_CRITIC_MAX_TOOL_OUTPUT", 200_000, minimum=1024,
        )
        self.critic_max_diff_chars = _int_env("BOT_CRITIC_MAX_DIFF_CHARS", 200_000, minimum=1024)
        self.critic_wall_clock_seconds = _int_env("BOT_CRITIC_WALL_CLOCK_S", 240, minimum=10)


def _require(name):
    val = os.getenv(name)
    if not val:
        logger.error(f"Missing required env var: {name}")
        sys.exit(1)
    return val


def _int_env(name, default, minimum=None):
    """Parse an integer env var, falling back to `default` on garbage input.

    Logs a warning rather than crashing when the value is non-numeric so a
    typo in workflow vars (e.g., "15m") doesn't kill analyze() before any
    artifact can be written. `minimum` clamps absurdly low values to a safe
    floor.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw.strip())
        except (TypeError, ValueError):
            logger.warning(
                "Env var %s=%r is not an integer; using default %d",
                name, raw, default,
            )
            value = default
    if minimum is not None and value < minimum:
        logger.warning(
            "Env var %s=%d below minimum %d; clamping to minimum",
            name, value, minimum,
        )
        value = minimum
    return value
