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


def test_int_env_falls_back_on_garbage(monkeypatch, caplog):
    """Garbage env input must NOT crash module import; falls back to default
    with a warning."""
    monkeypatch.setenv("BOT_INVESTIGATOR_MAX_TURNS", "15m")
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.investigator_max_turns == 15  # default


def test_int_env_clamps_below_minimum(monkeypatch):
    """Negative or absurdly low values clamp to the configured minimum."""
    monkeypatch.setenv("BOT_INVESTIGATOR_MAX_TURNS", "-5")
    _setup_minimal_env(monkeypatch)
    from issue_bot.config import Config
    cfg = Config()
    assert cfg.investigator_max_turns >= 1


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
