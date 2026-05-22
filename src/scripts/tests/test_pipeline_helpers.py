"""Tests for helpers in main.py used by the agent pipeline.

These cover the regressions surfaced by adversarial review:
  - flag parser: BOT_AGENT_PIPELINE accepts only positive values
  - _has_confirmed_findings: anchored, strict
  - _critic_overturn_rate: anchored, ignores rationale substrings
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from issue_bot.main import _has_confirmed_findings, _critic_overturn_rate


def test_has_confirmed_findings_simple():
    notes = "ID: C1\nSTATUS: CONFIRMED\nCOMMENT: bug"
    assert _has_confirmed_findings(notes) is True


def test_has_confirmed_findings_disproved_only():
    notes = "ID: C1\nSTATUS: DISPROVED\nCOMMENT: not a bug"
    assert _has_confirmed_findings(notes) is False


def test_has_confirmed_findings_empty():
    assert _has_confirmed_findings("") is False
    assert _has_confirmed_findings(None) is False


def test_has_confirmed_findings_ignores_substring_in_comment():
    """A COMMENT field mentioning CONFIRMED on a DISPROVED finding must NOT match."""
    notes = (
        "ID: C1\n"
        "STATUS: DISPROVED\n"
        "COMMENT: I had initially CONFIRMED this but found counter-evidence\n"
    )
    assert _has_confirmed_findings(notes) is False


def test_has_confirmed_findings_case_insensitive():
    assert _has_confirmed_findings("status: confirmed") is True
    assert _has_confirmed_findings("Status: Confirmed") is True


def test_has_confirmed_findings_mixed():
    """Both CONFIRMED and DISPROVED present → still True."""
    notes = (
        "ID: C1\nSTATUS: DISPROVED\n"
        "ID: C2\nSTATUS: CONFIRMED\n"
        "ID: C3\nSTATUS: DISPROVED\n"
    )
    assert _has_confirmed_findings(notes) is True


def test_has_confirmed_findings_status_with_extra_text():
    """STATUS: CONFIRMED-DEFERRED should NOT match (anchored at end of CONFIRMED)."""
    notes = "STATUS: CONFIRMED-DEFERRED\n"
    assert _has_confirmed_findings(notes) is False


def test_critic_overturn_rate_basic():
    text = "VERDICT: C1 | UPHELD | ok\nVERDICT: C2 | OVERTURNED | not real\n"
    assert _critic_overturn_rate(text) == 0.5


def test_critic_overturn_rate_all_upheld():
    text = "VERDICT: C1 | UPHELD | ok\nVERDICT: C2 | UPHELD | ok\n"
    assert _critic_overturn_rate(text) == 0.0


def test_critic_overturn_rate_all_overturned():
    text = "VERDICT: C1 | OVERTURNED | no\nVERDICT: C2 | OVERTURNED | no\n"
    assert _critic_overturn_rate(text) == 1.0


def test_critic_overturn_rate_no_verdicts():
    assert _critic_overturn_rate("") is None
    assert _critic_overturn_rate("ALL_DISPROVED: nothing to verify") is None


def test_critic_overturn_rate_ignores_substring_in_rationale():
    """An UPHELD verdict whose rationale contains 'OVERTURNED' must count as UPHELD only."""
    text = "VERDICT: C1 | UPHELD | OVERTURNED reasoning was wrong, finding holds\n"
    # Anchored at line start: only UPHELD counts. Overturn rate = 0.
    assert _critic_overturn_rate(text) == 0.0


def test_critic_overturn_rate_ignores_substring_in_other_overturned_rationale():
    text = "VERDICT: C1 | OVERTURNED | the UPHELD-style claim is wrong\n"
    assert _critic_overturn_rate(text) == 1.0


def test_critic_overturn_rate_handles_mixed_case():
    text = "verdict: c1 | upheld | ok\nVERDICT: C2 | OVERTURNED | no\n"
    # Both should match (case-insensitive)
    assert _critic_overturn_rate(text) == 0.5


def test_critic_overturn_rate_skips_lines_without_verdict_prefix():
    text = (
        "Some preamble that mentions UPHELD findings\n"
        "VERDICT: C1 | UPHELD | ok\n"
        "Some explanation\n"
    )
    # Only the anchored line counts
    assert _critic_overturn_rate(text) == 0.0


def test_agent_pipeline_flag_off_when_unset(monkeypatch):
    """Conservative parser: pipeline OFF when flag unset."""
    monkeypatch.delenv("BOT_AGENT_PIPELINE", raising=False)
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.agent_pipeline is False


@pytest.mark.parametrize("value", ["", "false", "False", "FALSE", "0", "no", "NO", "off", "OFF", "  ", "disable"])
def test_agent_pipeline_flag_off_for_negative_values(monkeypatch, value):
    """Anything that isn't in the positive allowlist keeps pipeline OFF."""
    monkeypatch.setenv("BOT_AGENT_PIPELINE", value)
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.agent_pipeline is False, f"Expected OFF for value={value!r}"


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "True", "yes", "YES", "on", "ON", "  1  ", "  true  "])
def test_agent_pipeline_flag_on_for_positive_values(monkeypatch, value):
    monkeypatch.setenv("BOT_AGENT_PIPELINE", value)
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.agent_pipeline is True, f"Expected ON for value={value!r}"


def test_int_env_falls_back_on_garbage(monkeypatch):
    """Garbage env input must NOT crash module import; falls back to default."""
    monkeypatch.setenv("BOT_INVESTIGATOR_MAX_TURNS", "15m")
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.investigator_max_turns == 15  # default


from issue_bot.main import _canonicalize_path, _extract_diff_files


def test_canonicalize_path_strips_leading_dot_slash():
    assert _canonicalize_path("./src/foo.py") == "src/foo.py"


def test_canonicalize_path_collapses_double_slash():
    assert _canonicalize_path("src//foo.py") == "src/foo.py"


def test_canonicalize_path_handles_backslashes():
    assert _canonicalize_path("src\\foo.py") == "src/foo.py"


def test_canonicalize_path_idempotent():
    p = "src/main/scala/X.scala"
    assert _canonicalize_path(p) == p
    assert _canonicalize_path(_canonicalize_path(p)) == p


def test_canonicalize_path_handles_empty():
    assert _canonicalize_path("") == ""
    assert _canonicalize_path(None) == ""


def test_canonicalize_path_dot_paths_become_empty():
    # posixpath.normpath collapses these to "." which must NOT pass through
    # as a member of incremental_files or a c["file"] value.
    assert _canonicalize_path(".") == ""
    assert _canonicalize_path("./") == ""
    assert _canonicalize_path(".//") == ""


def test_extract_diff_files_returns_canonical_paths():
    diff = "diff --git a/./src/foo.py b/./src/foo.py\n@@ ...\n+x\n"
    files = _extract_diff_files(diff)
    # Stored canonically (no './' prefix) so Reporter c["file"] comparisons work
    assert "src/foo.py" in files
    assert "./src/foo.py" not in files


def _setup_minimal_env(monkeypatch):
    """Set env vars Config requires (avoids sys.exit during construction)."""
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")


from issue_bot.main import _truncate_diff_for_user_prompt, _format_pr_input


def test_truncate_diff_under_cap_passthrough():
    diff = "diff --git a/f.py b/f.py\n+x\n"
    assert _truncate_diff_for_user_prompt(diff, 1000) == diff


def test_truncate_diff_over_cap_truncates_with_marker():
    diff = "x" * 500
    out = _truncate_diff_for_user_prompt(diff, 100)
    assert len(out) < 500
    assert "diff truncated at 100 chars" in out


def test_truncate_diff_at_exact_boundary_passthrough():
    diff = "x" * 100
    assert _truncate_diff_for_user_prompt(diff, 100) == diff


def test_format_pr_input_shape_is_canonical():
    """Investigator and Critic must frame PR title/body identically so the
    guardrail wraps the same shape on both."""
    out = _format_pr_input("Add feature", "What this does")
    assert out == "<pr>\nTitle: Add feature\nBody: What this does\n</pr>"


def test_agent_max_diff_chars_default(monkeypatch):
    monkeypatch.delenv("BOT_AGENT_MAX_DIFF_CHARS", raising=False)
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.agent_max_diff_chars == 200_000


def test_agent_max_diff_chars_env_override(monkeypatch):
    monkeypatch.setenv("BOT_AGENT_MAX_DIFF_CHARS", "50000")
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.agent_max_diff_chars == 50_000


def test_agent_max_diff_chars_falls_back_on_garbage(monkeypatch):
    monkeypatch.setenv("BOT_AGENT_MAX_DIFF_CHARS", "not-a-number")
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.agent_max_diff_chars == 200_000


@pytest.mark.parametrize("bad_value", ["0", "-1", "-200000"])
def test_agent_max_diff_chars_non_positive_falls_back(monkeypatch, bad_value):
    """A non-positive value would silently make _truncate_diff_for_user_prompt
    drop the entire diff (diff[:0] == ''), leaving the model with no context
    and inviting hallucinated findings. Must fail safe to the default."""
    monkeypatch.setenv("BOT_AGENT_MAX_DIFF_CHARS", bad_value)
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.agent_max_diff_chars == 200_000


from issue_bot.main import (
    _CRITIC_STAGE_EVENTS, _ESCALATE_EVENTS, _KNOWN_PIPELINE_EVENTS,
)


def test_pipeline_event_classification_complete():
    """Every known event must be classifiable as either critic-stage or
    escalate (or both). An event in neither set would be silently ignored
    by _run_agent_pipeline and let the pipeline post a clean review while
    dropping confirmed findings."""
    classified = _CRITIC_STAGE_EVENTS | _ESCALATE_EVENTS
    unclassified = _KNOWN_PIPELINE_EVENTS - classified
    assert unclassified == frozenset(), (
        f"Events not classified into _CRITIC_STAGE_EVENTS or _ESCALATE_EVENTS: "
        f"{sorted(unclassified)}. Add them so the pipeline routes them correctly."
    )


def test_pipeline_event_known_set_includes_documented_events():
    """The known-event set must include every value _run_critic_and_reporter
    documents in its docstring as a possible return."""
    documented = {
        "no_confirmed_findings",
        "critic_failed",
        "reporter_deadline_exceeded",
        "reporter_failed",
        "unknown_pipeline_event",
    }
    assert documented <= _KNOWN_PIPELINE_EVENTS, (
        f"Documented events not in known set: {documented - _KNOWN_PIPELINE_EVENTS}"
    )
