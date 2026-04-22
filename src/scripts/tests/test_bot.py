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
)
from issue_bot.sanitizer import sanitize, _fix_accidental_issue_refs


# ---------------------------------------------------------------------------
# _parse_response
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# _parse_file_review_multi
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# sanitize
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Bot reply tracking
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# _render (safe template substitution)
# ---------------------------------------------------------------------------

class TestRender:
    def test_basic(self):
        assert _render("Hello {name}", name="world") == "Hello world"

    def test_braces_in_value_dont_crash(self):
        result = _render("Title: {title}", title="Fix {broken} thing")
        assert "{broken}" in result

    def test_missing_var_preserved(self):
        result = _render("{present} {missing}", present="yes")
        assert "yes" in result
        assert "missing" in result  # safe_substitute keeps unresolved vars


# ---------------------------------------------------------------------------
# _clean_response
# ---------------------------------------------------------------------------

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
