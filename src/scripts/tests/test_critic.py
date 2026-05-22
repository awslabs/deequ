"""Tests for the Investigator + Critic + Reporter pipeline integration in main.py.

Mocks Bedrock and GitHub to drive the pipeline end-to-end with the
BOT_AGENT_PIPELINE flag enabled. Verifies:
  - clean PRs hit the fast path (skip Critic and Reporter)
  - PRs with confirmed findings invoke Critic and Reporter
  - Critic OVERTURNED findings are dropped
  - Critic UPHELD findings reach the artifact
  - Pipeline metrics and prompt_ids are recorded
  - Adaptive caps are computed from PR size
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _setup_pipeline_env(tmp_path, monkeypatch, event_action="opened"):
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    monkeypatch.setenv("ISSUE_NUMBER", "100")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("EVENT_ACTION", event_action)
    monkeypatch.setenv("EVENT_BEFORE", "")
    monkeypatch.setenv("EVENT_AFTER", "")
    monkeypatch.setenv("GITHUB_ACTOR", "contributor")
    monkeypatch.setenv("KB_S3_BUCKET", "")
    monkeypatch.setenv("KB_S3_KEY", "")
    monkeypatch.setenv("PR_INVESTIGATOR_PROMPT", "Investigate. Today: {current_date}")
    monkeypatch.setenv("PR_CRITIC_PROMPT", "Critique. Today: {current_date}")
    monkeypatch.setenv("PR_REPORTER_PROMPT", "Report. Today: {current_date}")
    monkeypatch.setenv("BOT_AGENT_PIPELINE", "1")
    monkeypatch.setenv("GITHUB_WORKSPACE", str(tmp_path))
    import issue_bot.main as bot_main
    monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))


def _run_pipeline(tmp_path, monkeypatch, mock,
                  investigator_text, critic_text, reporter_json,
                  event_action="opened", event_before="", event_after=""):
    """Invoke the pipeline with mocked bedrock; return artifact dict + mocks."""
    _setup_pipeline_env(tmp_path, monkeypatch, event_action=event_action)
    if event_before or event_after:
        monkeypatch.setenv("EVENT_BEFORE", event_before)
        monkeypatch.setenv("EVENT_AFTER", event_after)
    with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
         mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
         mock.patch("issue_bot.github_client.GitHubClient.get_compare_diff") as mock_compare, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
         mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
         mock.patch("issue_bot.github_client.GitHubClient.get_ci_status") as mock_ci, \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
         mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
         mock.patch("issue_bot.bedrock_client.BedrockClient.converse_with_tools") as mock_ctools, \
         mock.patch("issue_bot.bedrock_client.BedrockClient.invoke_with_usage") as mock_invoke:

        mock_pr.return_value = {
            "user": {"login": "contributor"}, "title": "Add feature",
            "body": "What this PR does", "state": "open",
            "html_url": "https://github.com/x", "head": {"sha": "abc123"},
        }
        mock_comments.return_value = (
            [{"user": {"login": "github-actions[bot]"}, "body": "prior"}]
            if event_action == "synchronize" else []
        )
        mock_diff.return_value = "diff --git a/f.py b/f.py\n+new line\n"
        mock_rc.return_value = []
        mock_compare.return_value = (
            "diff --git a/f.py b/f.py\n+new line\n"
            if event_action == "synchronize" else ""
        )
        mock_files.return_value = [{"filename": "f.py", "changes": 50}]
        mock_map.return_value = ""
        mock_kb.return_value = ""
        mock_ci.return_value = (True, "")

        # Investigator: one tool_use turn, then final text
        # Critic: one tool_use turn, then final text
        # The fast path skips critic entirely if no CONFIRMED findings.
        def converse_side_effect(system_prompt, messages, tool_specs,
                                 max_tokens=8000, temperature=0.3,
                                 timeout_seconds=None):
            # Identify which agent by scanning the system prompt
            if "Investigate" in system_prompt:
                return _bedrock_text_resp(investigator_text)
            if "Critique" in system_prompt:
                return _bedrock_text_resp(critic_text)
            return _bedrock_text_resp("???")

        mock_ctools.side_effect = converse_side_effect
        # Reporter uses invoke_with_usage which returns (text, usage_dict).
        mock_invoke.return_value = (
            reporter_json,
            {"inputTokens": 50, "outputTokens": 20, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
        )

        from issue_bot.main import analyze
        analyze()

        with open(str(tmp_path / "result.json")) as f:
            return json.load(f), mock_invoke, mock_ctools


def _bedrock_text_resp(text):
    """Construct a Bedrock Converse response with end_turn and a single text block."""
    return {
        "stopReason": "end_turn",
        "output": {"message": {"role": "assistant", "content": [{"text": text}]}},
        "usage": {"inputTokens": 100, "outputTokens": 50,
                  "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
    }


def test_clean_pr_fast_path_skips_critic_and_reporter(tmp_path, monkeypatch):
    import unittest.mock as mock
    investigator_notes = "No confirmed issues found."
    artifact, mock_invoke, mock_ctools = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text="should not be called",
        reporter_json='{"analysis": []}',
    )
    assert artifact["action"] == "RESPOND"
    assert "No issues found" in artifact["response"]
    assert artifact["inline_comments"] == []
    # Critic and Reporter both skipped
    assert mock_ctools.call_count == 1, "Only the investigator should have been called"
    assert mock_invoke.call_count == 0, "Reporter should not have been invoked"
    assert artifact["metrics"]["critic"]["skipped"] is True
    assert artifact["metrics"]["critic"]["skip_reason"] == "no_confirmed_findings"
    assert artifact["metrics"]["reporter"]["skipped"] is True
    assert artifact["pipeline"] == "agentic"


def test_confirmed_finding_triggers_critic_and_reporter(tmp_path, monkeypatch):
    import unittest.mock as mock
    investigator_notes = (
        "ID: C1\n"
        "FILE: f.py\n"
        "LINE: 1\n"
        "HYPOTHESIS: real bug\n"
        "FALSIFICATION_ATTEMPT: searched, none found\n"
        "STATUS: CONFIRMED\n"
        "SEVERITY: BUG\n"
        "COMMENT: real bug\n"
        "EVIDENCE: line 1\n"
        "TRIGGER: x\n"
    )
    critic_verdicts = "VERDICT: C1 | UPHELD | verified line 1"
    reporter_json = json.dumps({"analysis": [
        {"file": "f.py", "line": 1, "hypothesis": "real bug",
         "falsification_attempt": "searched", "disproved": False,
         "finding": {"severity": "BUG", "comment": "real bug",
                     "evidence": "line 1", "trigger": "x"}},
    ]})
    artifact, mock_invoke, mock_ctools = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text=critic_verdicts,
        reporter_json=reporter_json,
    )
    assert artifact["action"] == "RESPOND"
    assert len(artifact["inline_comments"]) == 1
    assert artifact["inline_comments"][0]["file"] == "f.py"
    # Investigator + Critic = 2 converse_with_tools calls
    assert mock_ctools.call_count == 2
    # Reporter ran via legacy invoke (single-shot, schema-enforced)
    assert mock_invoke.call_count == 1
    assert artifact["metrics"]["critic"]["skipped"] is False
    assert artifact["metrics"]["critic_overturn_rate"] == 0.0


def test_overturned_finding_dropped_by_reporter(tmp_path, monkeypatch):
    import unittest.mock as mock
    investigator_notes = (
        "ID: C1\nFILE: f.py\nLINE: 1\nHYPOTHESIS: maybe\n"
        "FALSIFICATION_ATTEMPT: weak\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\nTRIGGER: t\n"
    )
    critic_verdicts = "VERDICT: C1 | OVERTURNED | could not find the cited line"
    # Reporter respects the verdict and emits empty analysis
    reporter_json = json.dumps({"analysis": []})
    artifact, _, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text=critic_verdicts,
        reporter_json=reporter_json,
    )
    assert artifact["action"] == "RESPOND"
    assert artifact["inline_comments"] == []
    assert artifact["metrics"]["critic_overturn_rate"] == 1.0


def test_pipeline_records_prompt_ids_per_agent(tmp_path, monkeypatch):
    import unittest.mock as mock
    artifact, _, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text="No confirmed issues found.",
        critic_text="",
        reporter_json="",
    )
    assert "prompt_ids" in artifact
    assert "investigator" in artifact["prompt_ids"]
    assert "critic" in artifact["prompt_ids"]
    assert "reporter" in artifact["prompt_ids"]
    # All three should be 8-char SHA prefixes
    for k, v in artifact["prompt_ids"].items():
        assert isinstance(v, str)
        assert len(v) in (3, 8)  # "n/a" or hash


def test_pipeline_falls_back_to_legacy_when_flag_off(tmp_path, monkeypatch):
    """When BOT_AGENT_PIPELINE is unset, the legacy flow runs."""
    import unittest.mock as mock
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    monkeypatch.setenv("ISSUE_NUMBER", "100")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("EVENT_ACTION", "opened")
    monkeypatch.setenv("EVENT_BEFORE", "")
    monkeypatch.setenv("EVENT_AFTER", "")
    monkeypatch.setenv("GITHUB_ACTOR", "contributor")
    monkeypatch.setenv("KB_S3_BUCKET", "")
    monkeypatch.setenv("KB_S3_KEY", "")
    monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "Legacy review. Today: {current_date}")
    monkeypatch.setenv("PR_FILE_REVIEW_REPORT_PROMPT", "Legacy report. Today: {current_date}")
    # NO BOT_AGENT_PIPELINE set
    import issue_bot.main as bot_main
    monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

    with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
         mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
         mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
         mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
         mock.patch("issue_bot.github_client.GitHubClient.get_ci_status") as mock_ci, \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
         mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
         mock.patch("issue_bot.bedrock_client.BedrockClient.converse_with_tools") as mock_ctools, \
         mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_invoke:

        mock_pr.return_value = {
            "user": {"login": "contributor"}, "title": "T", "body": "B",
            "state": "open", "html_url": "https://github.com/x", "head": {"sha": "abc123"},
        }
        mock_comments.return_value = []
        mock_diff.return_value = "diff"
        mock_rc.return_value = []
        mock_files.return_value = [{"filename": "f.py", "changes": 1}]
        mock_content.return_value = "content"
        mock_map.return_value = ""
        mock_kb.return_value = ""
        mock_ci.return_value = (True, "")
        mock_invoke.side_effect = ["No confirmed issues found.", json.dumps({"analysis": []})]
        bot_main.analyze()

        with open(str(tmp_path / "result.json")) as f:
            artifact = json.load(f)

        # Legacy path uses invoke (twice), not converse_with_tools
        assert mock_ctools.call_count == 0
        assert mock_invoke.call_count == 2
        # Legacy artifact has no "pipeline" field
        assert "pipeline" not in artifact


def test_pipeline_metrics_record_token_usage(tmp_path, monkeypatch):
    import unittest.mock as mock
    artifact, _, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text="No confirmed issues found.",
        critic_text="",
        reporter_json="",
    )
    metrics = artifact["metrics"]
    assert "investigator" in metrics
    assert metrics["investigator"]["input_tokens"] == 100
    assert metrics["investigator"]["output_tokens"] == 50
    assert "totals" in metrics
    assert metrics["totals"]["input_tokens"] == 100


def test_pipeline_handles_missing_prompts(tmp_path, monkeypatch):
    """If any prompt fails to load, escalate."""
    import unittest.mock as mock
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    monkeypatch.setenv("ISSUE_NUMBER", "100")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("EVENT_ACTION", "opened")
    monkeypatch.setenv("EVENT_BEFORE", "")
    monkeypatch.setenv("EVENT_AFTER", "")
    monkeypatch.setenv("GITHUB_ACTOR", "contributor")
    monkeypatch.setenv("KB_S3_BUCKET", "")
    monkeypatch.setenv("KB_S3_KEY", "")
    monkeypatch.setenv("BOT_AGENT_PIPELINE", "1")
    # Only set the investigator prompt — critic and reporter are missing
    monkeypatch.setenv("PR_INVESTIGATOR_PROMPT", "Inv. Today: {current_date}")
    import issue_bot.main as bot_main
    monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

    with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
         mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
         mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map"), \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context", return_value=""), \
         mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None):

        mock_pr.return_value = {
            "user": {"login": "contributor"}, "title": "T", "body": "B",
            "state": "open", "html_url": "https://github.com/x", "head": {"sha": "abc"},
        }
        mock_comments.return_value = []
        bot_main.analyze()
        with open(str(tmp_path / "result.json")) as f:
            artifact = json.load(f)
        assert artifact["action"] == "ESCALATE"
        assert "prompt_load_failed" in artifact["reason"]


def test_pipeline_filters_invalid_line_numbers(tmp_path, monkeypatch):
    """Reporter output with line=0 should be filtered out."""
    import unittest.mock as mock
    investigator_notes = (
        "ID: C1\nFILE: f.py\nLINE: 1\nHYPOTHESIS: h\n"
        "FALSIFICATION_ATTEMPT: fa\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\nTRIGGER: t\n"
    )
    critic_text = "VERDICT: C1 | UPHELD | verified"
    reporter_json = json.dumps({"analysis": [
        {"file": "f.py", "line": 0, "hypothesis": "h",
         "falsification_attempt": "fa", "disproved": False,
         "finding": {"severity": "BUG", "comment": "c", "evidence": "e"}},
    ]})
    artifact, _, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text=critic_text,
        reporter_json=reporter_json,
    )
    assert artifact["inline_comments"] == []


def test_reporter_cost_in_totals(tmp_path, monkeypatch):
    """Reporter token usage must be added to metrics.totals."""
    import unittest.mock as mock
    investigator_notes = (
        "ID: C1\nFILE: f.py\nLINE: 1\nHYPOTHESIS: h\n"
        "FALSIFICATION_ATTEMPT: fa\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\nTRIGGER: t\n"
    )
    critic_text = "VERDICT: C1 | UPHELD | verified"
    reporter_json = json.dumps({"analysis": [
        {"file": "f.py", "line": 1, "hypothesis": "h",
         "falsification_attempt": "fa", "disproved": False,
         "finding": {"severity": "BUG", "comment": "c", "evidence": "e"}},
    ]})
    artifact, _, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text=critic_text,
        reporter_json=reporter_json,
    )
    # Investigator + Critic each contribute 100 inputTokens (mocked).
    # Reporter contributes 50 inputTokens, 20 outputTokens (mocked).
    metrics = artifact["metrics"]
    assert metrics["reporter"]["input_tokens"] == 50
    assert metrics["reporter"]["output_tokens"] == 20
    # totals = inv (100/50) + crit (100/50) + rep (50/20)
    assert metrics["totals"]["input_tokens"] == 250
    assert metrics["totals"]["output_tokens"] == 120


def test_critic_diff_truncation_on_large_diff(tmp_path, monkeypatch):
    """When the diff > 200K chars, Critic's user message must contain the
    truncation marker, not the full diff."""
    import unittest.mock as mock
    _setup_pipeline_env(tmp_path, monkeypatch, event_action="opened")
    huge_diff = "diff --git a/f.py b/f.py\n" + ("+ x\n" * 60000)  # ~240K chars
    investigator_notes = (
        "ID: C1\nFILE: f.py\nLINE: 1\nHYPOTHESIS: h\n"
        "FALSIFICATION_ATTEMPT: fa\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\nTRIGGER: t\n"
    )
    captured_messages = []

    with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
         mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
         mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
         mock.patch("issue_bot.github_client.GitHubClient.get_ci_status") as mock_ci, \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
         mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
         mock.patch("issue_bot.bedrock_client.BedrockClient.converse_with_tools") as mock_ctools, \
         mock.patch("issue_bot.bedrock_client.BedrockClient.invoke_with_usage") as mock_invoke:

        mock_pr.return_value = {
            "user": {"login": "contributor"}, "title": "T", "body": "B",
            "state": "open", "html_url": "https://github.com/x", "head": {"sha": "abc"},
        }
        mock_comments.return_value = []
        mock_diff.return_value = huge_diff
        mock_rc.return_value = []
        mock_files.return_value = [{"filename": "f.py", "changes": 10}]
        mock_map.return_value = ""
        mock_kb.return_value = ""
        mock_ci.return_value = (True, "")
        mock_invoke.return_value = (json.dumps({"analysis": []}), {"inputTokens": 1, "outputTokens": 1, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0})

        def converse(system_prompt, messages, tool_specs, max_tokens=8000,
                     temperature=0.3, timeout_seconds=None):
            captured_messages.append({"system": system_prompt[:200], "messages": messages})
            text = (
                "ID: C1\nSTATUS: CONFIRMED\nSEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\n"
                if "Investigate" in system_prompt
                else "VERDICT: C1 | UPHELD | ok"
            )
            return _bedrock_text_resp(text)

        mock_ctools.side_effect = converse
        from issue_bot.main import analyze
        analyze()

    # Find the Critic's call (second converse) and verify its first user message
    # contains the truncation marker, not the full huge diff
    critic_call = captured_messages[1]
    user_msg_text = critic_call["messages"][0]["content"][0]
    if isinstance(user_msg_text, dict):
        text = user_msg_text.get("text", "") or user_msg_text.get("guardContent", {}).get("text", {}).get("text", "")
    else:
        text = str(user_msg_text)
    assert "diff truncated at 200000 chars" in text
    assert len(text) < 220_000  # should be truncated, not 240K


def test_pipeline_fails_closed_on_missing_investigator_prompt(tmp_path, monkeypatch):
    """If PR_INVESTIGATOR_PROMPT and SM are unset, escalate (do not silently
    fall back to the legacy review prompt)."""
    import unittest.mock as mock
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    monkeypatch.setenv("ISSUE_NUMBER", "100")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("EVENT_ACTION", "opened")
    monkeypatch.setenv("EVENT_BEFORE", "")
    monkeypatch.setenv("EVENT_AFTER", "")
    monkeypatch.setenv("GITHUB_ACTOR", "contributor")
    monkeypatch.setenv("KB_S3_BUCKET", "")
    monkeypatch.setenv("KB_S3_KEY", "")
    monkeypatch.setenv("BOT_AGENT_PIPELINE", "1")
    monkeypatch.setenv("PR_CRITIC_PROMPT", "Critique. {current_date}")
    monkeypatch.setenv("PR_REPORTER_PROMPT", "Report. {current_date}")
    # Set the LEGACY prompt to ensure the silent-fallback path is NOT taken
    monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "Legacy review. {current_date}")
    # Investigator prompt deliberately NOT set
    import issue_bot.main as bot_main
    monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

    with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
         mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
         mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map"), \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context", return_value=""), \
         mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None):

        mock_pr.return_value = {
            "user": {"login": "contributor"}, "title": "T", "body": "B",
            "state": "open", "html_url": "https://github.com/x", "head": {"sha": "abc"},
        }
        mock_comments.return_value = []
        bot_main.analyze()
        with open(str(tmp_path / "result.json")) as f:
            artifact = json.load(f)
        assert artifact["action"] == "ESCALATE"
        assert "investigator" in artifact["reason"]


def test_critic_overturn_rate_calculated(tmp_path, monkeypatch):
    import unittest.mock as mock
    # Three findings, one upheld, two overturned → rate = 2/3
    investigator_notes = "\n".join([
        f"ID: C{i}\nFILE: f.py\nLINE: {i}\nHYPOTHESIS: h\n"
        "FALSIFICATION_ATTEMPT: fa\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\nTRIGGER: t\n"
        for i in (1, 2, 3)
    ])
    critic_text = "\n".join([
        "VERDICT: C1 | UPHELD | ok",
        "VERDICT: C2 | OVERTURNED | not real",
        "VERDICT: C3 | OVERTURNED | also not real",
    ])
    reporter_json = json.dumps({"analysis": [
        {"file": "f.py", "line": 1, "hypothesis": "h",
         "falsification_attempt": "fa", "disproved": False,
         "finding": {"severity": "BUG", "comment": "c", "evidence": "e"}},
    ]})
    artifact, _, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text=critic_text,
        reporter_json=reporter_json,
    )
    assert artifact["metrics"]["critic_overturn_rate"] == pytest.approx(2 / 3, abs=0.01)

def test_critic_failure_escalates_does_not_post_clean(tmp_path, monkeypatch):
    """If the Critic returns empty/error, the pipeline must ESCALATE rather
    than synthesize a fake 'all disproved' verdict and silently drop findings.
    Reporter must NOT be invoked. Investigator narrative is preserved.
    """
    import unittest.mock as mock
    investigator_notes = (
        "ID: C1\nFILE: f.py\nLINE: 1\nHYPOTHESIS: real bug\n"
        "FALSIFICATION_ATTEMPT: searched\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: real bug\nEVIDENCE: line 1\nTRIGGER: x\n"
    )
    artifact, mock_invoke, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text="",  # critic returned empty (e.g. crashed)
        reporter_json="",
    )
    assert artifact["action"] == "ESCALATE"
    assert artifact["reason"] == "critic_failed"
    assert artifact["inline_comments"] == []
    # Reporter must NOT have been invoked: posting empty reporter output
    # would have lost the investigator's findings silently.
    assert mock_invoke.call_count == 0
    # Investigator narrative is preserved so an operator can triage.
    assert artifact.get("investigator_summary", "").startswith("ID: C1")


def test_critic_with_structural_cap_text_reaches_reporter(tmp_path, monkeypatch):
    """A Critic that hits a structural cap (max_tool_calls etc.) produces an
    error string AND valid partial verdicts. The pipeline must NOT escalate
    on that — the Reporter should receive the partial text and apply its
    normal UPHELD filter. Regression guard for the bug-class this PR targets.
    """
    import unittest.mock as mock
    investigator_notes = (
        "ID: C1\nFILE: f.py\nLINE: 1\nHYPOTHESIS: real bug\n"
        "FALSIFICATION_ATTEMPT: searched\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: real bug\nEVIDENCE: line 1\nTRIGGER: x\n"
    )
    # Critic emits a valid VERDICT line before cap-terminating; the loop
    # would surface this as result.text + result.error="structural cap hit".
    critic_text = "VERDICT: C1 | UPHELD | verified line 1 (structural cap hit)"
    reporter_json = json.dumps({"analysis": [
        {"file": "f.py", "line": 1, "hypothesis": "real bug",
         "falsification_attempt": "searched", "disproved": False,
         "finding": {"severity": "BUG", "comment": "real bug",
                     "evidence": "line 1", "trigger": "x"}},
    ]})
    artifact, mock_invoke, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text=critic_text,
        reporter_json=reporter_json,
    )
    # Pipeline RESPONDS, not ESCALATES — partial verdict text is enough to
    # drive the Reporter and post valid findings.
    assert artifact["action"] == "RESPOND"
    assert mock_invoke.call_count == 1
    assert len(artifact["inline_comments"]) == 1
    assert artifact["inline_comments"][0]["file"] == "f.py"


def test_metrics_totals_equal_sum_of_stages(tmp_path, monkeypatch):
    """metrics.totals.input_tokens must equal the sum of per-stage tokens
    so dashboards reading totals don't drift from per-stage views.
    """
    import unittest.mock as mock
    investigator_notes = (
        "ID: C1\nFILE: f.py\nLINE: 1\nHYPOTHESIS: h\n"
        "FALSIFICATION_ATTEMPT: fa\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\nTRIGGER: t\n"
    )
    artifact, _, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text="VERDICT: C1 | UPHELD | ok",
        reporter_json=json.dumps({"analysis": [
            {"file": "f.py", "line": 1, "hypothesis": "h",
             "falsification_attempt": "fa", "disproved": False,
             "finding": {"severity": "BUG", "comment": "c", "evidence": "e"}},
        ]}),
    )
    m = artifact["metrics"]
    expected_in = m["investigator"]["input_tokens"] + m["critic"]["input_tokens"] + m["reporter"]["input_tokens"]
    expected_out = m["investigator"]["output_tokens"] + m["critic"]["output_tokens"] + m["reporter"]["output_tokens"]
    assert m["totals"]["input_tokens"] == expected_in
    assert m["totals"]["output_tokens"] == expected_out


def test_reporter_skipped_when_pipeline_deadline_exceeded(monkeypatch):
    """Direct unit test of _run_critic_and_reporter: with the deadline
    already in the past after the Critic finishes, the Reporter must be
    skipped and the helper must return skip_reason 'reporter_deadline_exceeded'.

    Direct call (not via analyze()) so we can drive the Critic happy-path
    independently of the Investigator's deadline check.
    """
    import time as _time
    import unittest.mock as mock
    from issue_bot.main import _run_critic_and_reporter
    from issue_bot import agent_loop
    from issue_bot.config import Config

    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    cfg = Config()

    investigator_notes = (
        "ID: C1\nFILE: f.py\nLINE: 1\nHYPOTHESIS: real bug\n"
        "FALSIFICATION_ATTEMPT: searched\nSTATUS: CONFIRMED\n"
        "SEVERITY: BUG\nCOMMENT: real bug\nEVIDENCE: line 1\nTRIGGER: x\n"
    )

    fake_critic = agent_loop.AgentResult(
        text="VERDICT: C1 | UPHELD | verified",
        turns=1, tool_calls=0, tool_output_chars=0,
        input_tokens=10, output_tokens=5,
    )

    bedrock = mock.MagicMock()
    # Reporter (invoke_with_usage) MUST NOT be called.
    bedrock.invoke_with_usage = mock.MagicMock(return_value=("never", {}))

    # Patch agent_loop.run so the "Critic" returns a real verdict without
    # actually doing a Bedrock call.
    with mock.patch("issue_bot.main.agent_loop.run", return_value=fake_critic):
        crit_result, skip_reason, comments, rep_metrics = _run_critic_and_reporter(
            cfg=cfg, bedrock=bedrock, tool_runner=None,
            critic_system="Critique.", critic_caps=agent_loop.AgentCaps(max_turns=1),
            reporter_system="Report.",
            diff="diff", title="T", body="B",
            investigator_text=investigator_notes,
            pipeline_deadline=_time.monotonic() - 1.0,  # already past
        )

    assert bedrock.invoke_with_usage.call_count == 0
    assert skip_reason == "reporter_deadline_exceeded"
    assert comments == []
    # Same string in both places: dashboards filtering on either field
    # see the same event without drift.
    assert rep_metrics["skip_reason"] == "reporter_deadline_exceeded"


def test_reporter_deadline_does_not_mislabel_critic_skip_reason(monkeypatch):
    """Regression: when the Reporter is skipped due to wall-clock deadline,
    the Critic actually ran successfully — its metrics row must NOT be
    tagged with a reporter-side skip_reason. The skip_reason belongs on the
    Reporter's metrics row (and the artifact's top-level reason field).
    """
    import time as _time
    import unittest.mock as mock
    from issue_bot.main import _run_agent_pipeline
    from issue_bot import agent_loop
    from issue_bot.config import Config

    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    monkeypatch.setenv("PR_INVESTIGATOR_PROMPT", "Investigate.")
    monkeypatch.setenv("PR_CRITIC_PROMPT", "Critique.")
    monkeypatch.setenv("PR_REPORTER_PROMPT", "Report.")
    cfg = Config()

    investigator_notes = (
        "ID: C1\nSTATUS: CONFIRMED\nSEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\n"
    )
    fake_inv = agent_loop.AgentResult(
        text=investigator_notes, turns=1, input_tokens=10, output_tokens=5,
    )
    fake_critic = agent_loop.AgentResult(
        text="VERDICT: C1 | UPHELD | verified", turns=1, input_tokens=10, output_tokens=5,
    )

    artifact_holder = {}

    def capture_artifact(payload):
        artifact_holder.update(payload)

    bedrock = mock.MagicMock()
    bedrock.invoke_with_usage = mock.MagicMock(return_value=("never", {}))

    gh = mock.MagicMock()
    gh.repo_root = "."
    gh.get_pr_diff.return_value = "diff"
    gh.get_pr_review_comments.return_value = []
    gh.get_pr_files.return_value = [{"filename": "f.py", "changes": 5}]
    gh.get_compare_diff.return_value = ""
    gh.get_ci_status.return_value = (True, "")

    item = {"head": {"sha": "abc"}}
    cfg.event_after = "abc"

    # Force deadline-exceeded path: monkeypatch pipeline_wall_clock_seconds to 0
    cfg.pipeline_wall_clock_seconds = 0

    with mock.patch("issue_bot.main.agent_loop.run", side_effect=[fake_inv, fake_critic]), \
         mock.patch("issue_bot.main._write_artifact", side_effect=capture_artifact):
        _run_agent_pipeline(
            cfg=cfg, gh=gh, bedrock=bedrock, number=1, title="T", body="B",
            html_url="https://github.com/x", item=item, context="",
            codebase_map="", comments_data=[], is_pr_update=False,
        )

    # Reporter NOT invoked due to expired deadline.
    assert bedrock.invoke_with_usage.call_count == 0
    # Guard against the patch silently no-op'ing if _write_artifact gets
    # renamed: an empty holder would otherwise pass the .get() asserts below.
    assert artifact_holder, "no artifact captured — check the patch target"
    # Pipeline ESCALATEd with the right top-level reason.
    assert artifact_holder["action"] == "ESCALATE"
    assert artifact_holder["reason"] == "reporter_deadline_exceeded"
    # Critic stage row carries its real metrics, not a reporter-side skip_reason.
    crit_metrics = artifact_holder["metrics"]["critic"]
    assert crit_metrics.get("skip_reason") != "reporter_deadline_exceeded", (
        "Critic mislabeled with a reporter-side skip_reason — pollutes per-stage analytics."
    )
    # Reporter row carries the deadline reason; same string as artifact reason.
    rep_metrics = artifact_holder["metrics"]["reporter"]
    assert rep_metrics["skip_reason"] == "reporter_deadline_exceeded"


def test_reporter_skipped_when_remaining_below_safety_threshold(monkeypatch):
    """If remaining wall-clock budget is below cfg.reporter_min_remaining_seconds,
    the Reporter is skipped pre-emptively. This bounds the worst case where
    the Reporter could otherwise run for its full bedrock_timeout (240s)
    starting just before the deadline and blow the workflow cap (600s).
    """
    import time as _time
    import unittest.mock as mock
    from issue_bot.main import _run_critic_and_reporter
    from issue_bot import agent_loop
    from issue_bot.config import Config

    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    monkeypatch.setenv("BOT_REPORTER_MIN_REMAINING_S", "60")
    cfg = Config()
    assert cfg.reporter_min_remaining_seconds == 60

    fake_critic = agent_loop.AgentResult(text="VERDICT: C1 | UPHELD | ok")
    bedrock = mock.MagicMock()
    bedrock.invoke_with_usage = mock.MagicMock(return_value=("never", {}))

    # 30s remaining < 60s threshold → skip pre-emptively
    with mock.patch("issue_bot.main.agent_loop.run", return_value=fake_critic):
        _, skip_reason, _, _ = _run_critic_and_reporter(
            cfg=cfg, bedrock=bedrock, tool_runner=None,
            critic_system="Critique.", critic_caps=agent_loop.AgentCaps(max_turns=1),
            reporter_system="Report.",
            diff="diff", title="T", body="B",
            investigator_text="ID: C1\nSTATUS: CONFIRMED\n",
            pipeline_deadline=_time.monotonic() + 30,
        )

    assert bedrock.invoke_with_usage.call_count == 0
    assert skip_reason == "reporter_deadline_exceeded"


def test_reporter_min_remaining_seconds_default(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    monkeypatch.delenv("BOT_REPORTER_MIN_REMAINING_S", raising=False)
    from issue_bot.config import Config
    cfg = Config()
    # Default tuned to >= realistic Reporter latency (20-45s on slow days)
    # so a Reporter that starts within budget can finish within budget.
    assert cfg.reporter_min_remaining_seconds == 60


def test_reporter_call_passes_remaining_budget_as_timeout(monkeypatch):
    """When the Reporter does run, its bedrock call gets a per-call timeout
    capped at the remaining wall-clock budget so a slow Bedrock day can't
    blow the GHA workflow cap (600s)."""
    import time as _time
    import unittest.mock as mock
    from issue_bot.main import _run_critic_and_reporter
    from issue_bot import agent_loop
    from issue_bot.config import Config

    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    cfg = Config()

    fake_critic = agent_loop.AgentResult(text="VERDICT: C1 | UPHELD | ok")
    bedrock = mock.MagicMock()
    bedrock.invoke_with_usage = mock.MagicMock(return_value=(
        json.dumps({"analysis": []}),
        {"inputTokens": 5, "outputTokens": 2,
         "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
    ))

    deadline = _time.monotonic() + 90  # 90s remaining
    with mock.patch("issue_bot.main.agent_loop.run", return_value=fake_critic):
        _run_critic_and_reporter(
            cfg=cfg, bedrock=bedrock, tool_runner=None,
            critic_system="Critique.", critic_caps=agent_loop.AgentCaps(max_turns=1),
            reporter_system="Report.",
            diff="diff", title="T", body="B",
            investigator_text="ID: C1\nSTATUS: CONFIRMED\n",
            pipeline_deadline=deadline,
        )

    assert bedrock.invoke_with_usage.call_count == 1
    call_kwargs = bedrock.invoke_with_usage.call_args[1]
    timeout = call_kwargs.get("timeout_seconds")
    # Deadline was set 90s out and only ms have elapsed inside the test.
    # Tight bounds catch off-by-one or wrong-units regressions in the
    # deadline math; loose bounds (e.g., 1 <= timeout <= 90) would let
    # a buggy value of 5s pass.
    assert timeout is not None
    assert 85 <= timeout <= 90


def test_reporter_failure_escalates_does_not_post_clean(monkeypatch):
    """If the Reporter call returns None (Bedrock unavailable, timeout,
    throttled), the pipeline must ESCALATE, not RESPOND with empty findings.
    Otherwise confirmed findings would be silently dropped — the same
    impact as a Critic failure, and treated the same way.
    """
    import time as _time
    import unittest.mock as mock
    from issue_bot.main import _run_critic_and_reporter
    from issue_bot import agent_loop
    from issue_bot.config import Config

    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    cfg = Config()

    fake_critic = agent_loop.AgentResult(text="VERDICT: C1 | UPHELD | ok")
    bedrock = mock.MagicMock()
    # Reporter call fails (Bedrock returned None for any reason).
    bedrock.invoke_with_usage = mock.MagicMock(return_value=(None, {}))

    with mock.patch("issue_bot.main.agent_loop.run", return_value=fake_critic):
        crit_result, event, comments, rep_metrics = _run_critic_and_reporter(
            cfg=cfg, bedrock=bedrock, tool_runner=None,
            critic_system="Critique.", critic_caps=agent_loop.AgentCaps(max_turns=1),
            reporter_system="Report.",
            diff="diff", title="T", body="B",
            investigator_text="ID: C1\nSTATUS: CONFIRMED\n",
            pipeline_deadline=_time.monotonic() + 600,  # ample budget
        )

    assert event == "reporter_failed"
    assert comments == []
    assert rep_metrics["skip_reason"] == "bedrock_unavailable"


def test_reporter_malformed_output_escalates(monkeypatch):
    """If the Reporter returns text that fails JSON parsing, we cannot trust
    the findings list. Escalate rather than risk posting nothing on real
    findings."""
    import time as _time
    import unittest.mock as mock
    from issue_bot.main import _run_critic_and_reporter
    from issue_bot import agent_loop
    from issue_bot.config import Config

    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    cfg = Config()

    fake_critic = agent_loop.AgentResult(text="VERDICT: C1 | UPHELD | ok")
    bedrock = mock.MagicMock()
    # Returns non-JSON text — parser will fail.
    bedrock.invoke_with_usage = mock.MagicMock(return_value=(
        "<<not json>>",
        {"inputTokens": 5, "outputTokens": 1,
         "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
    ))

    with mock.patch("issue_bot.main.agent_loop.run", return_value=fake_critic):
        _, event, comments, rep_metrics = _run_critic_and_reporter(
            cfg=cfg, bedrock=bedrock, tool_runner=None,
            critic_system="Critique.", critic_caps=agent_loop.AgentCaps(max_turns=1),
            reporter_system="Report.",
            diff="diff", title="T", body="B",
            investigator_text="ID: C1\nSTATUS: CONFIRMED\n",
            pipeline_deadline=_time.monotonic() + 600,
        )

    assert event == "reporter_failed"
    assert comments == []
    assert rep_metrics["parse_failed"] is True


def test_unknown_pipeline_event_escalates_does_not_mislabel_critic(monkeypatch):
    """Forward-compat: if _run_critic_and_reporter ever returns an unknown
    event name (typo, future event missing from both classification sets),
    the pipeline must ESCALATE with reason='unknown_pipeline_event' and NOT
    re-tag metrics["critic"] with that string."""
    import unittest.mock as mock
    from issue_bot.main import _run_agent_pipeline
    from issue_bot import agent_loop
    from issue_bot.config import Config

    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    monkeypatch.setenv("PR_INVESTIGATOR_PROMPT", "Investigate.")
    monkeypatch.setenv("PR_CRITIC_PROMPT", "Critique.")
    monkeypatch.setenv("PR_REPORTER_PROMPT", "Report.")
    cfg = Config()

    fake_inv = agent_loop.AgentResult(
        text="ID: C1\nSTATUS: CONFIRMED\nSEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\n",
        turns=1, input_tokens=10, output_tokens=5,
    )
    fake_critic = agent_loop.AgentResult(
        text="VERDICT: C1 | UPHELD | ok", turns=1, input_tokens=10, output_tokens=5,
    )

    artifact_holder = {}
    bedrock = mock.MagicMock()
    gh = mock.MagicMock()
    gh.repo_root = "."
    gh.get_pr_diff.return_value = "diff"
    gh.get_pr_review_comments.return_value = []
    gh.get_pr_files.return_value = [{"filename": "f.py", "changes": 5}]
    gh.get_compare_diff.return_value = ""
    gh.get_ci_status.return_value = (True, "")

    item = {"head": {"sha": "abc"}}
    cfg.event_after = "abc"

    # Patch _run_critic_and_reporter to return a typo'd event name.
    bad_metrics = {
        "skipped": False, "skip_reason": "garbage_value", "turns": 0, "tool_calls": 0,
        "tool_output_chars": 0, "input_tokens": 0, "output_tokens": 0,
        "cache_read_tokens": 0, "cache_write_tokens": 0,
        "max_turns_reached": False, "error": None, "parse_failed": False,
    }

    def capture_artifact(payload):
        artifact_holder.update(payload)

    with mock.patch("issue_bot.main.agent_loop.run", return_value=fake_inv), \
         mock.patch("issue_bot.main._run_critic_and_reporter",
                    return_value=(fake_critic, "garbage_event_typo", [], bad_metrics)), \
         mock.patch("issue_bot.main._write_artifact", side_effect=capture_artifact):
        _run_agent_pipeline(
            cfg=cfg, gh=gh, bedrock=bedrock, number=1, title="T", body="B",
            html_url="https://github.com/x", item=item, context="",
            codebase_map="", comments_data=[], is_pr_update=False,
        )

    assert artifact_holder, "no artifact captured — check the patch target"
    assert artifact_holder["action"] == "ESCALATE"
    assert artifact_holder["reason"] == "unknown_pipeline_event"
    # Critic ran — its skip_reason must NOT be "unknown_pipeline_event".
    assert artifact_holder["metrics"]["critic"].get("skip_reason") not in (
        "unknown_pipeline_event", "garbage_event_typo",
    )


def test_reporter_runs_when_deadline_in_future(monkeypatch):
    """Sanity check the inverse: a deadline comfortably in the future does
    NOT cause the Reporter to be skipped."""
    import time as _time
    import unittest.mock as mock
    from issue_bot.main import _run_critic_and_reporter
    from issue_bot import agent_loop
    from issue_bot.config import Config

    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
    monkeypatch.setenv("ISSUE_NUMBER", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
    cfg = Config()

    investigator_notes = (
        "ID: C1\nSTATUS: CONFIRMED\nSEVERITY: BUG\nCOMMENT: c\nEVIDENCE: e\n"
    )
    fake_critic = agent_loop.AgentResult(text="VERDICT: C1 | UPHELD | ok")

    bedrock = mock.MagicMock()
    bedrock.invoke_with_usage = mock.MagicMock(return_value=(
        json.dumps({"analysis": []}),
        {"inputTokens": 5, "outputTokens": 2, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
    ))

    with mock.patch("issue_bot.main.agent_loop.run", return_value=fake_critic):
        _, skip_reason, _, _ = _run_critic_and_reporter(
            cfg=cfg, bedrock=bedrock, tool_runner=None,
            critic_system="Critique.", critic_caps=agent_loop.AgentCaps(max_turns=1),
            reporter_system="Report.",
            diff="diff", title="T", body="B",
            investigator_text=investigator_notes,
            # Comfortably above reporter_min_remaining_seconds (default 60).
            pipeline_deadline=_time.monotonic() + 600,
        )
    assert bedrock.invoke_with_usage.call_count == 1
    assert skip_reason is None


def test_investigator_diff_truncation_on_large_diff(tmp_path, monkeypatch):
    """When the diff > BOT_AGENT_MAX_DIFF_CHARS, the Investigator's user
    message must contain the truncation marker, not the full diff. Same
    cap as the Critic to bound input tokens on giant PRs."""
    import unittest.mock as mock
    _setup_pipeline_env(tmp_path, monkeypatch, event_action="opened")
    # Lower the cap to make the test fast; default is 200K.
    monkeypatch.setenv("BOT_AGENT_MAX_DIFF_CHARS", "5000")
    huge_diff = "diff --git a/f.py b/f.py\n" + ("+ x\n" * 4000)  # ~16K chars
    captured = []

    with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
         mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
         mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
         mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
         mock.patch("issue_bot.github_client.GitHubClient.get_ci_status") as mock_ci, \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
         mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
         mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
         mock.patch("issue_bot.bedrock_client.BedrockClient.converse_with_tools") as mock_ctools, \
         mock.patch("issue_bot.bedrock_client.BedrockClient.invoke_with_usage") as mock_invoke:

        mock_pr.return_value = {
            "user": {"login": "contributor"}, "title": "T", "body": "B",
            "state": "open", "html_url": "https://github.com/x", "head": {"sha": "abc"},
        }
        mock_comments.return_value = []
        mock_diff.return_value = huge_diff
        mock_rc.return_value = []
        mock_files.return_value = [{"filename": "f.py", "changes": 5}]
        mock_map.return_value = ""
        mock_kb.return_value = ""
        mock_ci.return_value = (True, "")
        mock_invoke.return_value = (json.dumps({"analysis": []}),
                                     {"inputTokens": 1, "outputTokens": 1,
                                      "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0})

        def converse(system_prompt, messages, tool_specs, max_tokens=8000,
                     temperature=0.3, timeout_seconds=None):
            captured.append({"system": system_prompt[:80], "messages": messages})
            text = (
                "No confirmed issues found." if "Investigate" in system_prompt
                else "ALL_DISPROVED: nothing to verify"
            )
            return _bedrock_text_resp(text)

        mock_ctools.side_effect = converse
        from issue_bot.main import analyze
        analyze()

    # Investigator is the FIRST converse call.
    inv_call = captured[0]
    inv_user = inv_call["messages"][0]["content"][0]
    text = inv_user.get("text", "") if isinstance(inv_user, dict) else str(inv_user)
    assert "diff truncated at 5000 chars" in text
    # Must be < raw diff (16K) — truncated.
    assert len(text) < 16_000


def test_metrics_schema_uniform_across_stages(tmp_path, monkeypatch):
    """Every stage's metrics dict carries the same canonical key set so
    dashboards iterating stages don't KeyError."""
    import unittest.mock as mock
    investigator_notes = "No confirmed issues found."
    artifact, _, _ = _run_pipeline(
        tmp_path, monkeypatch, mock,
        investigator_text=investigator_notes,
        critic_text="",
        reporter_json="",
    )
    canonical_keys = {
        "skipped", "skip_reason", "turns", "tool_calls", "tool_output_chars",
        "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens",
        "max_turns_reached", "error", "parse_failed",
    }
    for stage in ("investigator", "critic", "reporter"):
        assert set(artifact["metrics"][stage].keys()) >= canonical_keys, (
            f"stage {stage} missing keys: {canonical_keys - set(artifact['metrics'][stage].keys())}"
        )

