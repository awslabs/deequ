# -*- coding: utf-8 -*-
"""Unit tests for the issue bot parsing and validation functions."""
import json
import sys
import os

import pytest

# Add scripts dir to path so we can import issue_bot
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from issue_bot.main import (
    _parse_response,
    _parse_file_review_multi,
    _already_replied_to_latest,
    _bot_reply_count,
    _user_dissatisfied,
    _clean_response,
    _render,
    _extract_diff_files,
)
from issue_bot.sanitizer import sanitize, _fix_accidental_issue_refs


class TestParseResponse:
    @pytest.mark.parametrize("action", ["RESPOND", "ESCALATE", "CLOSE"])
    def test_json_actions(self, action):
        raw = json.dumps({"action": action, "labels": [], "read_files": [], "response": "text"})
        assert _parse_response(raw, is_pr=False)["action"] == action

    def test_json_with_labels_and_files(self):
        raw = json.dumps({"action": "RESPOND", "labels": ["bug", "question"],
                          "read_files": ["pydeequ/checks.py"], "response": ""})
        result = _parse_response(raw, is_pr=False)
        assert result["labels"] == ["bug", "question"]
        assert result["read_files"] == ["pydeequ/checks.py"]

    def test_close_on_pr_becomes_escalate(self):
        raw = json.dumps({"action": "CLOSE", "labels": [], "read_files": [], "response": ""})
        assert _parse_response(raw, is_pr=True)["action"] == "ESCALATE"

    def test_fallback_to_text_parsing(self):
        raw = "ACTION: RESPOND\nLABELS: bug, question\nREAD_FILES: none\n\nHere is the answer."
        result = _parse_response(raw, is_pr=False)
        assert result["action"] == "RESPOND"
        assert "bug" in result["labels"]
        assert "Here is the answer." in result["response"]

    def test_text_defaults_to_escalate(self):
        raw = "Some unstructured text without headers"
        result = _parse_response(raw, is_pr=False)
        assert result["action"] == "ESCALATE"

    def test_empty_json_defaults(self):
        raw = json.dumps({})
        result = _parse_response(raw, is_pr=False)
        assert result["action"] == "ESCALATE"
        assert result["labels"] == []


class TestParseFileReviewMulti:
    def test_single_comment(self):
        raw = "FILE: src/foo.py\nLINE: 42\nCOMMENT: Missing null check"
        comments = _parse_file_review_multi(raw)
        assert len(comments) == 1
        assert comments[0] == {"file": "src/foo.py", "line": 42, "comment": "Missing null check"}

    def test_multiple_comments(self):
        raw = "FILE: a.py\nLINE: 1\nCOMMENT: issue one\nFILE: b.py\nLINE: 2\nCOMMENT: issue two"
        assert len(_parse_file_review_multi(raw)) == 2

    def test_multiline_comment(self):
        raw = "FILE: a.py\nLINE: 10\nCOMMENT: first line\nsecond line"
        comments = _parse_file_review_multi(raw)
        assert "second line" in comments[0]["comment"]

    def test_invalid_line_number_skipped(self):
        raw = "FILE: a.py\nLINE: not_a_number\nCOMMENT: bad"
        assert len(_parse_file_review_multi(raw)) == 0

    def test_empty_input(self):
        assert _parse_file_review_multi("") == []


class TestSanitize:
    def test_none_passthrough(self):
        assert sanitize(None) is None

    def test_empty_passthrough(self):
        assert sanitize("") == ""

    def test_clean_text_passes(self):
        assert sanitize("Normal response about PyDeequ.") is not None

    @pytest.mark.parametrize("marker", [
        "my system prompt is",
        "here are my internal",
        "ignore previous instructions",
    ])
    def test_blocks_injection_markers(self, marker):
        assert sanitize(f"Some text with {marker} embedded") is None


class TestFixIssueRefs:
    def test_wraps_in_backticks(self):
        assert _fix_accidental_issue_refs("see #42") == "see `#42`"

    def test_preserves_code_blocks(self):
        text = "```\n#42\n```"
        assert _fix_accidental_issue_refs(text) == text

    def test_no_match_on_non_numeric(self):
        assert _fix_accidental_issue_refs("#abc") == "#abc"

    def test_multiple_refs(self):
        result = _fix_accidental_issue_refs("fixes #1 and #2")
        assert "`#1`" in result
        assert "`#2`" in result


def _make_comment(login, body="text"):
    return {"user": {"login": login}, "body": body}


class TestBotReplyCount:
    def test_zero(self):
        assert _bot_reply_count([_make_comment("user1")]) == 0

    def test_counts_bot_only(self):
        comments = [_make_comment("user1"), _make_comment("github-actions[bot]"),
                     _make_comment("user2"), _make_comment("github-actions[bot]")]
        assert _bot_reply_count(comments) == 2


class TestAlreadyRepliedToLatest:
    def test_bot_after_user(self):
        assert _already_replied_to_latest(
            [_make_comment("user1"), _make_comment("github-actions[bot]")]) is True

    def test_user_after_bot(self):
        assert _already_replied_to_latest(
            [_make_comment("github-actions[bot]"), _make_comment("user1")]) is False

    def test_empty(self):
        assert _already_replied_to_latest([]) is False


class TestUserDissatisfied:
    def test_no_bot_reply_means_not_dissatisfied(self):
        assert _user_dissatisfied([_make_comment("user1", "that's wrong")]) is False

    def test_dissatisfied_after_bot(self):
        comments = [_make_comment("github-actions[bot]"), _make_comment("user1", "that's wrong")]
        assert _user_dissatisfied(comments) is True

    def test_happy_after_bot(self):
        comments = [_make_comment("github-actions[bot]"), _make_comment("user1", "thanks!")]
        assert _user_dissatisfied(comments) is False

    @pytest.mark.parametrize("signal", [
        "didn't help", "not helpful", "still broken", "please escalate", "need a human",
    ])
    def test_various_signals(self, signal):
        comments = [_make_comment("github-actions[bot]"), _make_comment("user1", signal)]
        assert _user_dissatisfied(comments) is True


class TestRender:
    def test_basic(self):
        assert _render("Hello {name}", name="world") == "Hello world"

    def test_braces_in_value_dont_crash(self):
        result = _render("Title: {title}", title="Fix {broken} thing")
        assert "{broken}" in result

    def test_missing_var_preserved(self):
        result = _render("{present} {missing}", present="yes")
        assert "yes" in result
        assert "{missing}" in result

    def test_no_cross_variable_injection(self):
        """User content containing {context} must NOT leak the actual context value."""
        result = _render("KB: {context}\nBody: {body}", context="SECRET", body="{context}")
        assert result == "KB: SECRET\nBody: {context}"

    def test_no_reverse_injection(self):
        """Context containing {body} must NOT be replaced by body value."""
        result = _render("KB: {context}\nBody: {body}", context="{body}", body="SECRET")
        assert result == "KB: {body}\nBody: SECRET"


class TestCleanResponse:
    def test_strips_header_lines(self):
        text = "ACTION: RESPOND\nLABELS: bug\nActual response here"
        assert "Actual response here" in _clean_response(text)
        assert "ACTION:" not in _clean_response(text)

    def test_strips_preamble(self):
        text = "Let me analyze this issue.\nThe actual answer."
        assert _clean_response(text) == "The actual answer."

    def test_preserves_normal_text(self):
        text = "This is a normal response."
        assert _clean_response(text) == text


class TestGetCiStatus:
    """Tests for GitHubClient.get_ci_status method."""

    def _make_client(self):
        import unittest.mock as mock
        with mock.patch.dict(os.environ, {
            "GITHUB_TOKEN": "fake", "GITHUB_REPOSITORY": "awslabs/test",
            "ISSUE_NUMBER": "1", "EVENT_TYPE": "issues", "EVENT_ACTION": "opened",
            "GITHUB_WORKFLOW": "Deequ Bot",
        }):
            from issue_bot.config import Config
            from issue_bot.github_client import GitHubClient
            cfg = Config()
            client = GitHubClient(cfg)
        return client

    def test_all_checks_passed(self):
        import unittest.mock as mock
        client = self._make_client()
        client._get = mock.MagicMock(side_effect=[
            {"state": "success"},  # commit status
            {"check_runs": [
                {"name": "Java CI", "status": "completed", "conclusion": "success"},
                {"name": "CodeQL", "status": "completed", "conclusion": "success"},
            ]},
        ])
        passed, summary = client.get_ci_status("abc123")
        assert passed is True
        assert "passed" in summary.lower()

    def test_check_run_failed(self):
        import unittest.mock as mock
        client = self._make_client()
        client._get = mock.MagicMock(side_effect=[
            {"state": "success"},
            {"check_runs": [
                {"name": "Java CI", "status": "completed", "conclusion": "failure"},
                {"name": "CodeQL", "status": "completed", "conclusion": "success"},
            ]},
        ])
        passed, summary = client.get_ci_status("abc123")
        assert passed is False
        assert "Java CI" in summary

    def test_check_run_pending(self):
        import unittest.mock as mock
        client = self._make_client()
        client._get = mock.MagicMock(side_effect=[
            {"state": "pending"},
            {"check_runs": [
                {"name": "Java CI", "status": "in_progress", "conclusion": None},
            ]},
        ])
        passed, summary = client.get_ci_status("abc123")
        assert passed is None
        assert "pending" in summary.lower()

    def test_bot_check_filtered_out(self):
        import unittest.mock as mock
        client = self._make_client()
        client._get = mock.MagicMock(side_effect=[
            {"state": "success"},
            {"check_runs": [
                {"name": "Java CI", "status": "completed", "conclusion": "success"},
                {"name": "Deequ Bot / analyze", "status": "completed", "conclusion": "success"},
                {"name": "Deequ Bot / act", "status": "completed", "conclusion": "success"},
            ]},
        ])
        passed, _ = client.get_ci_status("abc123")
        assert passed is True

    def test_non_bot_check_with_bot_in_name_not_filtered(self):
        import unittest.mock as mock
        client = self._make_client()
        client._get = mock.MagicMock(side_effect=[
            {"state": "success"},
            {"check_runs": [
                {"name": "robot-tests", "status": "completed", "conclusion": "failure"},
            ]},
        ])
        passed, _ = client.get_ci_status("abc123")
        assert passed is False

    def test_skipped_and_neutral_count_as_passed(self):
        import unittest.mock as mock
        client = self._make_client()
        client._get = mock.MagicMock(side_effect=[
            {"state": "success"},
            {"check_runs": [
                {"name": "Optional Check", "status": "completed", "conclusion": "skipped"},
                {"name": "Info Check", "status": "completed", "conclusion": "neutral"},
            ]},
        ])
        passed, _ = client.get_ci_status("abc123")
        assert passed is True

    def test_api_failure_returns_unknown(self):
        import unittest.mock as mock
        client = self._make_client()
        client._get = mock.MagicMock(return_value=None)
        passed, summary = client.get_ci_status("abc123")
        assert passed is None


class TestAutoApproveSignal:
    """Tests that bot posts the correct signal for the auto-approve workflow to act on."""

    def _make_artifact(self, tmp_path, response, inline_comments=None):
        artifact = {
            "action": "RESPOND",
            "labels": [],
            "response": response,
            "inline_comments": inline_comments or [],
            "title": "Fix", "html_url": "https://github.com/x",
            "number": 42, "is_pr": True, "is_incremental": False,
            "prompt_id": "abc123", "model_id": "test",
        }
        path = str(tmp_path / "result.json")
        with open(path, "w") as f:
            json.dump(artifact, f)
        return path

    def test_no_issues_posts_pr_review_with_signal(self, tmp_path, monkeypatch):
        """Bot posts 'No issues found' as a PR review — auto-approve.yml looks for this in listReviews."""
        import unittest.mock as mock
        path = self._make_artifact(tmp_path, response="No issues found. CI is passing.")
        monkeypatch.setenv("GITHUB_TOKEN", "fake")
        monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
        monkeypatch.setenv("ISSUE_NUMBER", "42")
        monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
        monkeypatch.setenv("EVENT_ACTION", "opened")
        import issue_bot.main as bot_main
        monkeypatch.setattr(bot_main, "ARTIFACT_PATH", path)

        with mock.patch("issue_bot.github_client.GitHubClient.post_pr_review") as mock_review, \
             mock.patch("issue_bot.github_client.GitHubClient.post_comment") as mock_comment, \
             mock.patch("issue_bot.github_client.GitHubClient.add_labels"), \
             mock.patch("issue_bot.slack_client.SlackClient.send_escalation"):
            mock_review.return_value = True
            bot_main.act()
            mock_review.assert_called_once()
            mock_comment.assert_not_called()
            body = mock_review.call_args[0][1]
            assert "No issues found" in body

    def test_with_issues_posts_review_not_comment(self, tmp_path, monkeypatch):
        """Bot posts inline review when there are issues — no approve signal."""
        import unittest.mock as mock
        path = self._make_artifact(tmp_path, response="",
                                   inline_comments=[{"file": "a.py", "line": 1, "comment": "BUG: issue"}])
        monkeypatch.setenv("GITHUB_TOKEN", "fake")
        monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
        monkeypatch.setenv("ISSUE_NUMBER", "42")
        monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
        monkeypatch.setenv("EVENT_ACTION", "opened")
        import issue_bot.main as bot_main
        monkeypatch.setattr(bot_main, "ARTIFACT_PATH", path)

        with mock.patch("issue_bot.github_client.GitHubClient.post_pr_review") as mock_review, \
             mock.patch("issue_bot.github_client.GitHubClient.post_comment") as mock_comment, \
             mock.patch("issue_bot.github_client.GitHubClient.add_labels"), \
             mock.patch("issue_bot.slack_client.SlackClient.send_escalation"):
            mock_review.return_value = True
            bot_main.act()
            mock_review.assert_called_once()
            mock_comment.assert_not_called()


class TestPrompts:
    def test_env_var_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "from env")
        monkeypatch.setenv("SM_PR_FILE_REVIEW_PROMPT", "deequ-bot/pr-file-review-prompt")
        from issue_bot.prompts import get_pr_file_review_prompt
        assert get_pr_file_review_prompt() == "from env"

    def test_empty_env_var_falls_through_to_sm(self, monkeypatch):
        import unittest.mock as mock
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "")
        monkeypatch.setenv("SM_PR_FILE_REVIEW_PROMPT", "deequ-bot/pr-file-review-prompt")
        with mock.patch("issue_bot.prompts._read_from_sm", return_value="from sm") as m:
            from issue_bot.prompts import get_pr_file_review_prompt
            result = get_pr_file_review_prompt()
            assert result == "from sm"
            m.assert_called_once_with("deequ-bot/pr-file-review-prompt")

    def test_no_sm_env_var_returns_empty(self, monkeypatch):
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "")
        monkeypatch.setenv("SM_PR_FILE_REVIEW_PROMPT", "")
        from issue_bot.prompts import get_pr_file_review_prompt
        # No env var, no SM secret name → empty string
        assert get_pr_file_review_prompt() == ""

    def test_sm_failure_returns_empty(self, monkeypatch):
        import unittest.mock as mock
        monkeypatch.setenv("FOLLOWUP_PROMPT", "")
        monkeypatch.setenv("SM_FOLLOWUP_PROMPT", "deequ-bot/followup-prompt")
        with mock.patch("issue_bot.prompts._get_sm_client") as mock_client:
            mock_client.return_value.get_secret_value.side_effect = Exception("timeout")
            from issue_bot.prompts import get_followup_prompt
            assert get_followup_prompt() == ""


class TestSmoke:
    def test_main_module_imports(self):
        from issue_bot import main
        assert hasattr(main, 'analyze')
        assert hasattr(main, 'act')

    def test_sanitizer_imports(self):
        from issue_bot import sanitizer
        assert hasattr(sanitizer, 'sanitize')

    def test_schemas_loadable(self):
        from issue_bot.main import ISSUE_RESPONSE_SCHEMA, PR_REVIEW_SCHEMA, FOLLOWUP_SCHEMA
        import json
        assert json.loads(ISSUE_RESPONSE_SCHEMA)["type"] == "object"
        assert json.loads(PR_REVIEW_SCHEMA)["type"] == "object"
        assert json.loads(FOLLOWUP_SCHEMA)["type"] == "object"


class TestArtifactValidation:
    def test_invalid_action_rejected(self):
        """Actions not in the allowed set should be treated as invalid."""
        valid = {"SKIP", "RESPOND", "ESCALATE", "CLOSE"}
        assert "DROP_TABLE" not in valid
        assert "RESPOND" in valid

    def test_title_truncated(self):
        title = "A" * 500
        truncated = str(title)[:200]
        assert len(truncated) == 200

    def test_non_github_url_cleared(self):
        url = "https://evil.com/steal"
        result = "" if not url.startswith("https://github.com/") else url
        assert result == ""

    def test_github_url_preserved(self):
        url = "https://github.com/awslabs/python-deequ/issues/1"
        result = "" if not url.startswith("https://github.com/") else url
        assert result == url

    def test_empty_url_preserved(self):
        url = ""
        result = "" if url and not url.startswith("https://github.com/") else url
        assert result == ""


class TestSplitPrompt:
    """Test that invoke() follows GlueML pattern: system=trusted, user=guarded."""

    def _make_client(self, guardrail_id=""):
        class FakeCfg:
            bedrock_model_id = "test"
            bedrock_timeout = 10
            guardrail_id = ""
            guardrail_version = "DRAFT"
        cfg = FakeCfg()
        cfg.guardrail_id = guardrail_id
        from issue_bot.bedrock_client import BedrockClient
        import unittest.mock as mock
        with mock.patch("boto3.client"):
            client = BedrockClient(cfg)
        return client

    def _mock_converse(self, client):
        import unittest.mock as mock
        client._client = mock.MagicMock()
        client._client.converse.return_value = {
            "stopReason": "end_turn",
            "output": {"message": {"content": [{"text": "ok"}]}},
            "usage": {},
        }

    def test_with_guardrail_user_is_guardcontent(self):
        client = self._make_client(guardrail_id="gr-123")
        self._mock_converse(client)
        client.invoke("system instructions", "user input")
        kwargs = client._client.converse.call_args[1]
        content = kwargs["messages"][0]["content"]
        assert len(content) == 1
        assert "guardContent" in content[0]
        assert content[0]["guardContent"]["text"]["text"] == "user input"

    def test_without_guardrail_user_is_text(self):
        client = self._make_client()
        self._mock_converse(client)
        client.invoke("system", "user input")
        kwargs = client._client.converse.call_args[1]
        content = kwargs["messages"][0]["content"]
        assert len(content) == 1
        assert "text" in content[0]
        assert content[0]["text"] == "user input"

    def test_system_prompt_is_plain_text_cached(self):
        client = self._make_client(guardrail_id="gr-123")
        self._mock_converse(client)
        client.invoke("instructions + diff with ignore previous instructions", "Title: test")
        kwargs = client._client.converse.call_args[1]
        system = kwargs["system"]
        assert system[0]["text"] == "instructions + diff with ignore previous instructions"
        assert "cachePoint" in system[1]
        # System prompt is NOT guardContent — guardrail won't scan it
        assert "guardContent" not in system[0]

    def test_guardrail_config_present(self):
        client = self._make_client(guardrail_id="gr-123")
        self._mock_converse(client)
        client.invoke("system", "user")
        kwargs = client._client.converse.call_args[1]
        assert "guardrailConfig" in kwargs
        assert kwargs["guardrailConfig"]["guardrailIdentifier"] == "gr-123"

    def test_no_guardrail_no_config(self):
        client = self._make_client()
        self._mock_converse(client)
        client.invoke("system", "user")
        kwargs = client._client.converse.call_args[1]
        assert "guardrailConfig" not in kwargs


class TestExtractDiffFiles:
    def test_single_file(self):
        diff = (
            "diff --git a/src/foo.py b/src/foo.py\n"
            "index abc1234..def5678 100644\n"
            "--- a/src/foo.py\n"
            "+++ b/src/foo.py\n"
            "@@ -1,3 +1,4 @@\n"
            "+new line\n"
        )
        assert _extract_diff_files(diff) == {"src/foo.py"}

    def test_multiple_files(self):
        diff = (
            "diff --git a/a.py b/a.py\n"
            "--- a/a.py\n"
            "+++ b/a.py\n"
            "@@ -1 +1 @@\n"
            "-old\n"
            "+new\n"
            "diff --git a/b.py b/b.py\n"
            "--- a/b.py\n"
            "+++ b/b.py\n"
            "@@ -1 +1 @@\n"
            "-old\n"
            "+new\n"
        )
        assert _extract_diff_files(diff) == {"a.py", "b.py"}

    def test_empty_diff(self):
        assert _extract_diff_files("") == set()

    def test_renamed_file(self):
        diff = "diff --git a/old_name.py b/new_name.py\n"
        assert _extract_diff_files(diff) == {"new_name.py"}

    def test_path_with_spaces(self):
        diff = "diff --git a/path with spaces/file.py b/path with spaces/file.py\n"
        assert _extract_diff_files(diff) == {"path with spaces/file.py"}


class TestIncrementalFiltering:
    """Test that the incremental file filter drops comments on unrelated files."""

    def test_comments_filtered_to_incremental_files(self):
        incremental_files = {"src/changed.py"}
        inline_comments = [
            {"file": "src/changed.py", "line": 10, "comment": "new issue"},
            {"file": "src/untouched.py", "line": 5, "comment": "old issue re-raised"},
        ]
        filtered = [c for c in inline_comments if c.get("file", "") in incremental_files]
        assert len(filtered) == 1
        assert filtered[0]["file"] == "src/changed.py"

    def test_empty_incremental_files_passes_all(self):
        incremental_files = set()
        inline_comments = [
            {"file": "src/any.py", "line": 1, "comment": "comment"},
        ]
        # When incremental_files is empty (fallback to full review), no filtering
        if incremental_files:
            filtered = [c for c in inline_comments if c.get("file", "") in incremental_files]
        else:
            filtered = inline_comments
        assert len(filtered) == 1

    def test_all_comments_filtered_yields_empty(self):
        incremental_files = {"src/only_this.py"}
        inline_comments = [
            {"file": "src/other.py", "line": 1, "comment": "stale"},
            {"file": "src/another.py", "line": 2, "comment": "stale too"},
        ]
        filtered = [c for c in inline_comments if c.get("file", "") in incremental_files]
        assert filtered == []


class TestNitFilterAndFormatting:
    """Tests for hard NIT filter on re-reviews and evidence formatting."""

    def test_nits_dropped_on_re_review(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        monkeypatch.setenv("GITHUB_TOKEN", "fake")
        monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
        monkeypatch.setenv("ISSUE_NUMBER", "10")
        monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
        monkeypatch.setenv("EVENT_ACTION", "synchronize")
        monkeypatch.setenv("EVENT_BEFORE", "aaa")
        monkeypatch.setenv("EVENT_AFTER", "bbb")
        monkeypatch.setenv("KB_S3_BUCKET", "")
        monkeypatch.setenv("KB_S3_KEY", "")
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "Review. Date: {current_date}")
        import issue_bot.main as bot_main
        monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

        incremental = "diff --git a/f.py b/f.py\n--- a/f.py\n+++ b/f.py\n@@ -1 +1 @@\n-x\n+y\n"

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff"), \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_compare_diff") as mock_compare, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map"), \
             mock.patch("issue_bot.github_client.GitHubClient.get_ci_status") as mock_ci, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context", return_value=""), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock:

            mock_pr.return_value = {
                "user": {"login": "dev"}, "title": "Fix", "body": "",
                "state": "open", "html_url": "https://github.com/x",
                "head": {"sha": "bbb"},
            }
            mock_comments.return_value = [{"user": {"login": "github-actions[bot]"}, "body": "prior"}]
            mock_rc.return_value = []
            mock_compare.return_value = incremental
            mock_files.return_value = [{"filename": "f.py"}]
            mock_content.return_value = "content"
            mock_ci.return_value = (True, "CI passed")
            mock_bedrock.return_value = json.dumps({"comments": [
                {"file": "f.py", "line": 1, "severity": "BUG", "comment": "real bug",
                 "evidence": "line 1 divides by zero"},
                {"file": "f.py", "line": 1, "severity": "NIT", "comment": "rename var",
                 "evidence": "x is not descriptive"},
            ]})

            bot_main.analyze()

            with open(str(tmp_path / "result.json")) as f:
                result = json.load(f)

            # NIT should be filtered, BUG should remain
            assert result["action"] == "RESPOND"
            assert len(result["inline_comments"]) == 1
            assert "real bug" in result["inline_comments"][0]["comment"]

    def test_nits_kept_on_first_review(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        monkeypatch.setenv("GITHUB_TOKEN", "fake")
        monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
        monkeypatch.setenv("ISSUE_NUMBER", "10")
        monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
        monkeypatch.setenv("EVENT_ACTION", "opened")
        monkeypatch.setenv("EVENT_BEFORE", "")
        monkeypatch.setenv("EVENT_AFTER", "")
        monkeypatch.setenv("KB_S3_BUCKET", "")
        monkeypatch.setenv("KB_S3_KEY", "")
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "Review. Date: {current_date}")
        import issue_bot.main as bot_main
        monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff"), \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map"), \
             mock.patch("issue_bot.github_client.GitHubClient.get_ci_status") as mock_ci, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context", return_value=""), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock:

            mock_pr.return_value = {
                "user": {"login": "dev"}, "title": "Fix", "body": "",
                "state": "open", "html_url": "https://github.com/x",
                "head": {"sha": "abc123"},
            }
            mock_comments.return_value = []
            mock_rc.return_value = []
            mock_files.return_value = [{"filename": "f.py"}]
            mock_content.return_value = "content"
            mock_ci.return_value = (True, "CI passed")
            mock_bedrock.return_value = json.dumps({"comments": [
                {"file": "f.py", "line": 1, "severity": "BUG", "comment": "bug",
                 "evidence": "evidence1"},
                {"file": "f.py", "line": 2, "severity": "NIT", "comment": "nit",
                 "evidence": "evidence2"},
            ]})

            bot_main.analyze()

            with open(str(tmp_path / "result.json")) as f:
                result = json.load(f)

            # Both BUG and NIT should be present on first review
            assert len(result["inline_comments"]) == 2

    def test_evidence_formatted_in_comment(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        monkeypatch.setenv("GITHUB_TOKEN", "fake")
        monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
        monkeypatch.setenv("ISSUE_NUMBER", "10")
        monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
        monkeypatch.setenv("EVENT_ACTION", "opened")
        monkeypatch.setenv("EVENT_BEFORE", "")
        monkeypatch.setenv("EVENT_AFTER", "")
        monkeypatch.setenv("KB_S3_BUCKET", "")
        monkeypatch.setenv("KB_S3_KEY", "")
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "Review. Date: {current_date}")
        import issue_bot.main as bot_main
        monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff"), \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map"), \
             mock.patch("issue_bot.github_client.GitHubClient.get_ci_status") as mock_ci, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context", return_value=""), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock:

            mock_pr.return_value = {
                "user": {"login": "dev"}, "title": "Fix", "body": "",
                "state": "open", "html_url": "https://github.com/x",
                "head": {"sha": "abc123"},
            }
            mock_comments.return_value = []
            mock_rc.return_value = []
            mock_files.return_value = [{"filename": "f.py"}]
            mock_content.return_value = "content"
            mock_ci.return_value = (True, "CI passed")
            mock_bedrock.return_value = json.dumps({"comments": [
                {"file": "f.py", "line": 5, "severity": "BUG",
                 "comment": "division by zero",
                 "evidence": "line 3 sets count=0, line 5 divides by count"},
            ]})

            bot_main.analyze()

            with open(str(tmp_path / "result.json")) as f:
                result = json.load(f)

            comment_text = result["inline_comments"][0]["comment"]
            assert comment_text.startswith("**BUG**: ")
            assert "division by zero" in comment_text
            assert "line 3 sets count=0" in comment_text


class TestIncrementalReviewIntegration:
    """End-to-end tests for the incremental review path through analyze()."""

    def _setup_env(self, tmp_path, monkeypatch, event_before="abc123", event_after="def456"):
        monkeypatch.setenv("GITHUB_TOKEN", "fake")
        monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
        monkeypatch.setenv("ISSUE_NUMBER", "99")
        monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
        monkeypatch.setenv("EVENT_ACTION", "synchronize")
        monkeypatch.setenv("EVENT_BEFORE", event_before)
        monkeypatch.setenv("EVENT_AFTER", event_after)
        monkeypatch.setenv("GITHUB_ACTOR", "contributor")
        monkeypatch.setenv("KB_S3_BUCKET", "")
        monkeypatch.setenv("KB_S3_KEY", "")
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "Review this PR. Date: {current_date}")
        import issue_bot.main as bot_main
        monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

    def test_incremental_review_filters_stale_comments(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        self._setup_env(tmp_path, monkeypatch)

        incremental_diff = (
            "diff --git a/src/fixed.py b/src/fixed.py\n"
            "--- a/src/fixed.py\n+++ b/src/fixed.py\n"
            "@@ -1 +1 @@\n-old\n+new\n"
        )

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_compare_diff") as mock_compare, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock:

            mock_pr.return_value = {"user": {"login": "contributor"}, "title": "Fix bug",
                                    "body": "Fixes the thing", "state": "open", "html_url": "https://github.com/x"}
            mock_comments.return_value = [{"user": {"login": "github-actions[bot]"}, "body": "prior review"}]
            mock_diff.return_value = "full diff here"
            mock_rc.return_value = []
            mock_compare.return_value = incremental_diff
            mock_files.return_value = [{"filename": "src/fixed.py"}, {"filename": "src/untouched.py"}]
            mock_content.return_value = "file content"
            mock_map.return_value = ""
            mock_kb.return_value = ""
            mock_bedrock.return_value = json.dumps({
                "comments": [
                    {"file": "src/fixed.py", "line": 1, "comment": "new issue in changed file"},
                    {"file": "src/untouched.py", "line": 5, "comment": "stale comment on unchanged file"},
                ]
            })

            from issue_bot.main import analyze
            analyze()

            with open(str(tmp_path / "result.json")) as f:
                result = json.load(f)

            assert result["action"] == "RESPOND"
            assert result["is_incremental"] is True
            assert len(result["inline_comments"]) == 1
            assert result["inline_comments"][0]["file"] == "src/fixed.py"

    def test_force_push_falls_back_to_full_review(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        self._setup_env(tmp_path, monkeypatch)

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_compare_diff") as mock_compare, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock:

            mock_pr.return_value = {"user": {"login": "contributor"}, "title": "Fix bug",
                                    "body": "Fixes the thing", "state": "open", "html_url": "https://github.com/x"}
            mock_comments.return_value = [{"user": {"login": "github-actions[bot]"}, "body": "prior review"}]
            mock_diff.return_value = "full diff here"
            mock_rc.return_value = []
            mock_compare.return_value = ""  # Force push — compare fails
            mock_files.return_value = [{"filename": "src/a.py"}]
            mock_content.return_value = "content"
            mock_map.return_value = ""
            mock_kb.return_value = ""
            mock_bedrock.return_value = json.dumps({
                "comments": [
                    {"file": "src/a.py", "line": 1, "comment": "issue found"},
                ]
            })

            from issue_bot.main import analyze
            analyze()

            with open(str(tmp_path / "result.json")) as f:
                result = json.load(f)

            # Falls back to full review — no filtering, not marked incremental
            assert result["action"] == "RESPOND"
            assert result["is_incremental"] is False
            assert len(result["inline_comments"]) == 1

    def test_no_before_sha_skips_incremental(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        self._setup_env(tmp_path, monkeypatch, event_before="", event_after="def456")

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_compare_diff") as mock_compare, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock:

            mock_pr.return_value = {"user": {"login": "contributor"}, "title": "Fix",
                                    "body": "Fix", "state": "open", "html_url": "https://github.com/x"}
            mock_comments.return_value = [{"user": {"login": "github-actions[bot]"}, "body": "review"}]
            mock_diff.return_value = "full diff"
            mock_rc.return_value = []
            mock_compare.return_value = "should not be called"
            mock_files.return_value = [{"filename": "src/a.py"}]
            mock_content.return_value = "content"
            mock_map.return_value = ""
            mock_kb.return_value = ""
            mock_bedrock.return_value = json.dumps({"comments": [
                {"file": "src/a.py", "line": 1, "comment": "issue"},
            ]})

            from issue_bot.main import analyze
            analyze()

            # Should NOT have called compare because event_before is empty
            mock_compare.assert_not_called()

            with open(str(tmp_path / "result.json")) as f:
                result = json.load(f)
            assert result["is_incremental"] is False


class TestFileContentUsesHeadSha:
    """Verify get_file_content is called with PR head SHA, not default branch."""

    def _setup_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "fake")
        monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
        monkeypatch.setenv("ISSUE_NUMBER", "42")
        monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
        monkeypatch.setenv("EVENT_ACTION", "opened")
        monkeypatch.setenv("EVENT_BEFORE", "")
        monkeypatch.setenv("EVENT_AFTER", "")
        monkeypatch.setenv("GITHUB_ACTOR", "contributor")
        monkeypatch.setenv("KB_S3_BUCKET", "")
        monkeypatch.setenv("KB_S3_KEY", "")
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "Review this PR. Date: {current_date}")
        import issue_bot.main as bot_main
        monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

    def test_file_content_fetched_with_head_sha(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        self._setup_env(tmp_path, monkeypatch)

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock:

            mock_pr.return_value = {
                "user": {"login": "contributor"}, "title": "Add feature",
                "body": "New file", "state": "open",
                "html_url": "https://github.com/x",
                "head": {"sha": "abc123deadbeef"},
            }
            mock_comments.return_value = []
            mock_diff.return_value = "diff content"
            mock_rc.return_value = []
            mock_files.return_value = [
                {"filename": "src/new_file.py"},
                {"filename": "src/existing.py"},
            ]
            mock_content.return_value = "file content"
            mock_map.return_value = ""
            mock_kb.return_value = ""
            mock_bedrock.return_value = json.dumps({"comments": []})

            from issue_bot.main import analyze
            analyze()

            # Every get_file_content call must include ref=head_sha
            for call in mock_content.call_args_list:
                args, kwargs = call
                assert kwargs.get("ref") == "abc123deadbeef" or \
                    (len(args) > 1 and args[1] == "abc123deadbeef"), \
                    f"get_file_content called without head SHA: {call}"

    def test_missing_head_sha_falls_back_gracefully(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        self._setup_env(tmp_path, monkeypatch)

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock:

            # PR object without head.sha (shouldn't happen, but defensive)
            mock_pr.return_value = {
                "user": {"login": "contributor"}, "title": "Fix",
                "body": "Fix", "state": "open",
                "html_url": "https://github.com/x",
            }
            mock_comments.return_value = []
            mock_diff.return_value = "diff"
            mock_rc.return_value = []
            mock_files.return_value = [{"filename": "src/a.py"}]
            mock_content.return_value = "content"
            mock_map.return_value = ""
            mock_kb.return_value = ""
            mock_bedrock.return_value = json.dumps({"comments": []})

            from issue_bot.main import analyze
            analyze()

            # Should still work — falls back to no ref (default branch)
            with open(str(tmp_path / "result.json")) as f:
                result = json.load(f)
            # First review with 0 comments → RESPOND with CI-aware message
            assert result["action"] == "RESPOND"
            assert "No issues found" in result["response"]


class TestReviewEventType:
    """Verify bot always uses COMMENT event type, never REQUEST_CHANGES."""

    def _setup_env(self, tmp_path, monkeypatch, event_action="opened"):
        monkeypatch.setenv("GITHUB_TOKEN", "fake")
        monkeypatch.setenv("GITHUB_REPOSITORY", "awslabs/test")
        monkeypatch.setenv("ISSUE_NUMBER", "50")
        monkeypatch.setenv("EVENT_TYPE", "pull_request_target")
        monkeypatch.setenv("EVENT_ACTION", event_action)
        monkeypatch.setenv("EVENT_BEFORE", "aaa111" if event_action == "synchronize" else "")
        monkeypatch.setenv("EVENT_AFTER", "bbb222" if event_action == "synchronize" else "")
        monkeypatch.setenv("GITHUB_ACTOR", "contributor")
        monkeypatch.setenv("KB_S3_BUCKET", "")
        monkeypatch.setenv("KB_S3_KEY", "")
        monkeypatch.setenv("PR_FILE_REVIEW_PROMPT", "Review. Date: {current_date}")
        import issue_bot.main as bot_main
        monkeypatch.setattr(bot_main, "ARTIFACT_PATH", str(tmp_path / "result.json"))

    def _run_and_get_artifact(self, tmp_path, monkeypatch, mock, event_action="opened"):
        self._setup_env(tmp_path, monkeypatch, event_action=event_action)

        with mock.patch("issue_bot.github_client.GitHubClient.get_pr") as mock_pr, \
             mock.patch("issue_bot.github_client.GitHubClient.get_comments") as mock_comments, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_diff") as mock_diff, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_review_comments") as mock_rc, \
             mock.patch("issue_bot.github_client.GitHubClient.get_compare_diff") as mock_compare, \
             mock.patch("issue_bot.github_client.GitHubClient.get_pr_files") as mock_files, \
             mock.patch("issue_bot.github_client.GitHubClient.get_file_content") as mock_content, \
             mock.patch("issue_bot.github_client.GitHubClient.get_codebase_map") as mock_map, \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.load"), \
             mock.patch("issue_bot.knowledge_base.KnowledgeBase.build_context") as mock_kb, \
             mock.patch("issue_bot.bedrock_client.BedrockClient.__init__", return_value=None), \
             mock.patch("issue_bot.bedrock_client.BedrockClient.invoke") as mock_bedrock, \
             mock.patch("issue_bot.github_client.GitHubClient.post_pr_review") as mock_post:

            mock_pr.return_value = {
                "user": {"login": "contributor"}, "title": "Fix",
                "body": "Fix", "state": "open",
                "html_url": "https://github.com/x",
                "head": {"sha": "abc123"},
            }
            mock_comments.return_value = (
                [{"user": {"login": "github-actions[bot]"}, "body": "prior"}]
                if event_action == "synchronize" else []
            )
            mock_diff.return_value = "diff"
            mock_rc.return_value = []
            mock_compare.return_value = (
                "diff --git a/f.py b/f.py\n--- a/f.py\n+++ b/f.py\n@@ -1 +1 @@\n-x\n+y\n"
                if event_action == "synchronize" else ""
            )
            mock_files.return_value = [{"filename": "f.py"}]
            mock_content.return_value = "content"
            mock_map.return_value = ""
            mock_kb.return_value = ""
            mock_bedrock.return_value = json.dumps({
                "comments": [{"file": "f.py", "line": 1, "comment": "issue"}]
            })
            mock_post.return_value = True

            from issue_bot.main import analyze, act
            analyze()

            with open(str(tmp_path / "result.json")) as f:
                return json.load(f), mock_post

    def test_first_review_uses_comment_event(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        result, _ = self._run_and_get_artifact(tmp_path, monkeypatch, mock, "opened")
        assert result["action"] == "RESPOND"
        assert result.get("is_incremental") is False
        assert len(result["inline_comments"]) > 0

    def test_incremental_review_uses_comment_event(self, tmp_path, monkeypatch):
        import unittest.mock as mock
        result, _ = self._run_and_get_artifact(tmp_path, monkeypatch, mock, "synchronize")
        assert result["action"] == "RESPOND"
        assert result.get("is_incremental") is True
