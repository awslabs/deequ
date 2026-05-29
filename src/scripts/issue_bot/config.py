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

        self.bedrock_model_id = os.getenv("BEDROCK_MODEL_ID", "us.anthropic.claude-opus-4-7")
        # Reporter is a JSON-formatting + filter step; it doesn't reason. A
        # cheaper model (Haiku 4.5) handles structured output cleanly at ~5x
        # less cost. Defaults to bedrock_model_id so the swap is opt-in via
        # workflow env var. .strip() guards against stray whitespace from
        # the workflow vars context.
        self.reporter_model_id = (os.getenv("BEDROCK_REPORTER_MODEL_ID") or "").strip() or self.bedrock_model_id
        if self.reporter_model_id != self.bedrock_model_id:
            logger.info(
                "Reporter model override active: %s (default: %s)",
                self.reporter_model_id, self.bedrock_model_id,
            )
        # Critic uses converse_with_tools (multi-turn loop). Override goes
        # through agent_loop.run → converse_with_tools so the same model is
        # used for every turn within one Critic run.
        self.critic_model_id = (os.getenv("BEDROCK_CRITIC_MODEL_ID") or "").strip() or self.bedrock_model_id
        if self.critic_model_id != self.bedrock_model_id:
            logger.info(
                "Critic model override active: %s (default: %s)",
                self.critic_model_id, self.bedrock_model_id,
            )

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

        # Structural caps for runaway protection.
        self.investigator_max_turns = _int_env("BOT_INVESTIGATOR_MAX_TURNS", 15)
        self.investigator_max_tool_calls = _int_env("BOT_INVESTIGATOR_MAX_TOOL_CALLS", 50)
        self.investigator_max_tool_output_chars = _int_env(
            "BOT_INVESTIGATOR_MAX_TOOL_OUTPUT", 400_000,
        )

        self.critic_max_turns = _int_env("BOT_CRITIC_MAX_TURNS", 10)
        self.critic_max_tool_calls = _int_env("BOT_CRITIC_MAX_TOOL_CALLS", 30)
        self.critic_max_tool_output_chars = _int_env(
            "BOT_CRITIC_MAX_TOOL_OUTPUT", 200_000,
        )
        # Cap on diff bytes in either agent's user prompt. Non-positive
        # would silently drop the diff entirely → fail safe to default.
        agent_max_diff = _int_env("BOT_AGENT_MAX_DIFF_CHARS", 200_000)
        if agent_max_diff <= 0:
            logger.warning(
                "BOT_AGENT_MAX_DIFF_CHARS=%d is non-positive; using default 200_000",
                agent_max_diff,
            )
            agent_max_diff = 200_000
        self.agent_max_diff_chars = agent_max_diff
        self.pipeline_wall_clock_seconds = _int_env("BOT_PIPELINE_WALL_CLOCK_S", 480)
        # Reporter latency on slow Bedrock days runs 20-45s; below this
        # margin a Reporter started in budget could finish out of budget.
        self.reporter_min_remaining_seconds = _int_env("BOT_REPORTER_MIN_REMAINING_S", 60)


def _require(name):
    val = os.getenv(name)
    if not val:
        logger.error(f"Missing required env var: {name}")
        sys.exit(1)
    return val


def _int_env(name, default):
    """Parse an integer env var, falling back to `default` on garbage input.
    A typo in workflow vars (e.g., "15m") shouldn't kill analyze() before
    any artifact can be written.
    """
    try:
        return int(os.getenv(name) or default)
    except (TypeError, ValueError):
        logger.warning("Env var %s is not an integer; using default %d", name, default)
        return default
