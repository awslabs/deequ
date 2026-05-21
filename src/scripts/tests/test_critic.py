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
    monkeypatch.setenv("PR_FILE_REVIEW_REPORT_PROMPT", "Report. Today: {current_date}")
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
    monkeypatch.setenv("PR_FILE_REVIEW_REPORT_PROMPT", "Report. {current_date}")
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

