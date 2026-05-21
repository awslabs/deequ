"""
Deequ Bot — two-phase orchestration.

  analyze: read-only phase, produces JSON artifact
  act:     write-only phase, reads artifact and posts to GitHub/Slack
"""

import json
import re
import sys
import os
import datetime
import logging
import uuid

from .config import Config
from .bedrock_client import BedrockClient
from .github_client import GitHubClient
from .knowledge_base import KnowledgeBase
from .slack_client import SlackClient
from .sanitizer import sanitize
from . import prompts
from . import agent_loop
from .tools import TOOL_SPECS, ToolRunner

logger = logging.getLogger("issue_bot")

ARTIFACT_PATH = os.getenv("ARTIFACT_PATH", "/tmp/bot_result.json")
_MAX_BOT_REPLIES = 2


def _render(template_str, **kwargs):
    """Render a prompt template safely using unique tokens per invocation.
    Prevents cross-variable injection (user body containing {context} won't leak KB)."""
    token_id = uuid.uuid4().hex
    tokens = {}
    result = template_str
    for key, value in kwargs.items():
        token = f"__TMPL_{token_id}_{key}__"
        result = result.replace("{" + key + "}", token)
        tokens[token] = str(value)
    for token, value in tokens.items():
        result = result.replace(token, value)
    return result


def _load_schema(name):
    """Load a JSON schema file from the schemas directory."""
    path = os.path.join(os.path.dirname(__file__), "schemas", name)
    with open(path) as f:
        return f.read()


ISSUE_RESPONSE_SCHEMA = _load_schema("issue_response.json")
PR_REVIEW_SCHEMA = _load_schema("pr_review_response.json")
FOLLOWUP_SCHEMA = _load_schema("followup_response.json")


def analyze():
    cfg = Config()
    gh = GitHubClient(cfg)
    bedrock = BedrockClient(cfg)
    kb = KnowledgeBase(cfg)
    kb.load()

    number = cfg.issue_number
    is_followup = cfg.event_type == "issue_comment" and cfg.event_action == "created"

    item = None
    if cfg.event_type in ("pull_request", "pull_request_target"):
        is_pr = True
    elif cfg.event_type in ("issues", "issue_comment"):
        is_pr = False
    else:
        # workflow_dispatch or unknown — check via API
        item = gh.get_issue(number)
        is_pr = bool(item and item.get("pull_request"))

    if item is None:
        item = gh.get_pr(number) if is_pr else gh.get_issue(number)
    if not item:
        _write_artifact({"action": "SKIP", "reason": "fetch_failed"})
        return

    author = item.get("user", {}).get("login", "")
    if author.endswith("[bot]"):
        _write_artifact({"action": "SKIP", "reason": "author_is_bot"})
        return

    if item.get("state") == "closed" and not is_pr:
        _write_artifact({"action": "SKIP", "reason": "issue_closed"})
        return

    title = item.get("title", "") or ""
    body = item.get("body", "") or ""
    html_url = item.get("html_url", "")
    comments_data = gh.get_comments(number)
    comments_text = _format_comments(comments_data)

    is_pr_update = is_pr and cfg.event_action == "synchronize"
    is_reopened = not is_pr and cfg.event_action == "reopened"

    if is_reopened:
        _write_artifact({
            "action": "ESCALATE", "labels": [], "response": "",
            "reason": "issue_reopened", "title": title,
            "html_url": html_url, "number": number, "is_pr": is_pr,
            "prompt_id": "n/a", "model_id": cfg.bedrock_model_id,
        })
        return

    if not is_followup and not is_pr_update and any(
            c.get("user", {}).get("login") == "github-actions[bot]" for c in comments_data):
        _write_artifact({"action": "SKIP", "reason": "already_commented"})
        return

    if is_followup and comments_data:
        if comments_data[-1].get("user", {}).get("login") == "github-actions[bot]":
            _write_artifact({"action": "SKIP", "reason": "bot_last_comment"})
            return
        if _already_replied_to_latest(comments_data):
            _write_artifact({"action": "SKIP", "reason": "already_replied_to_comment"})
            return
        if _bot_reply_count(comments_data) >= _MAX_BOT_REPLIES:
            _write_artifact({
                "action": "ESCALATE", "labels": [], "response": "",
                "reason": "max_replies_reached", "title": title,
                "html_url": html_url, "number": number, "is_pr": is_pr,
                "prompt_id": "n/a", "model_id": cfg.bedrock_model_id,
            })
            return
        if _user_dissatisfied(comments_data):
            _write_artifact({
                "action": "ESCALATE", "labels": [], "response": "",
                "reason": "user_dissatisfied", "title": title,
                "html_url": html_url, "number": number, "is_pr": is_pr,
                "prompt_id": "n/a", "model_id": cfg.bedrock_model_id,
            })
            return

    issue_text = f"{title} {body}"
    context = kb.build_context(issue_text)
    codebase_map = gh.get_codebase_map() if not is_followup else ""

    if is_pr and cfg.agent_pipeline:
        _run_agent_pipeline(
            cfg=cfg, gh=gh, bedrock=bedrock,
            number=number, title=title, body=body, html_url=html_url, item=item,
            context=context, codebase_map=codebase_map,
            comments_data=comments_data, is_pr_update=is_pr_update,
        )
        return

    if is_pr:
        tmpl = prompts.get_pr_file_review_prompt()
        if not tmpl:
            _write_artifact({"action": "ESCALATE", "labels": [], "response": "",
                "reason": "prompt_load_failed", "title": title, "html_url": html_url,
                "number": number, "is_pr": True, "prompt_id": "n/a", "model_id": cfg.bedrock_model_id})
            return
        diff = gh.get_pr_diff(number)
        review_comments = gh.get_pr_review_comments(number)
        existing_feedback = _format_pr_feedback(comments_data, review_comments)

        # Incremental review: on synchronize, compute what changed since last push
        incremental_diff = ""
        incremental_files = set()
        if is_pr_update and cfg.event_before and cfg.event_after:
            incremental_diff = gh.get_compare_diff(cfg.event_before, cfg.event_after)
            if incremental_diff:
                incremental_files = _extract_diff_files(incremental_diff)

        # Fetch full source files at the SHA the diff is anchored to
        head_sha = cfg.event_after or item.get("head", {}).get("sha", "")
        pr_files = gh.get_pr_files(number)
        full_sources = ""
        for pf in pr_files:
            fname = pf.get("filename", "")
            content = gh.get_file_content(fname, ref=head_sha) if head_sha else gh.get_file_content(fname)
            if content:
                entry = f"\n### `{fname}`\n```\n{content}\n```\n"
                if len(full_sources) + len(entry) > 3_000_000:
                    full_sources += f"\n### `{fname}` — SKIPPED (context budget)\n"
                    break
                full_sources += entry

        # Build incremental review instructions
        incremental_section = ""
        if incremental_diff:
            incremental_section = (
                "\n<incremental_review_instructions>\n"
                "This is a RE-REVIEW after the author pushed new commits. "
                "The <incremental_diff> below shows ONLY what changed since the last push. "
                "You MUST limit your comments to lines/files in the incremental diff. "
                "Do NOT re-raise issues on unchanged code — the author already saw prior feedback. "
                "Do NOT comment on lines that are not part of the incremental diff. "
                "If the incremental diff only fixes issues from prior feedback, respond with zero comments."
                "\n</incremental_review_instructions>\n"
                f"<incremental_diff>\n{incremental_diff}\n</incremental_diff>\n"
            )

        # Build context for Phase 1 (full context including source files)
        phase1_context = (
            f"\n\n<knowledge_base>\n{context}\n</knowledge_base>\n"
            f"<codebase_map>\n{codebase_map}\n</codebase_map>\n"
            f"<full_source_files>\n{full_sources}\n</full_source_files>\n"
            f"<diff>\n{diff}\n</diff>\n"
            f"<existing_feedback>\n{existing_feedback}\n</existing_feedback>\n"
            f"{incremental_section}"
        )
        user_prompt = f"<pr>\nTitle: {title}\nBody: {body}\n</pr>"

        # PHASE 1: Investigation (free-form, no schema)
        phase1_prompt = _render(tmpl, current_date=datetime.date.today().isoformat()) + phase1_context
        investigation = bedrock.invoke(phase1_prompt, user_prompt, max_tokens=8000)
        if investigation is None:
            _write_artifact({
                "action": "ESCALATE", "reason": "bedrock_unavailable", "title": title,
                "html_url": html_url, "number": number, "is_pr": True,
                "prompt_id": prompts.prompt_version(tmpl), "model_id": cfg.bedrock_model_id,
            })
            return

        # PHASE 2: Always run — structured reporting (schema-enforced falsification)
        # Phase 2 gets diff + investigation notes but NOT full_source_files (already analyzed in Phase 1)
        report_tmpl = prompts.get_pr_file_review_report_prompt()
        if not report_tmpl:
            _write_artifact({
                "action": "ESCALATE", "reason": "prompt_load_failed", "title": title,
                "html_url": html_url, "number": number, "is_pr": True,
                "prompt_id": "n/a", "model_id": cfg.bedrock_model_id,
            })
            return
        report_system = (
            _render(report_tmpl, current_date=datetime.date.today().isoformat())
            + f"\n<diff>\n{diff}\n</diff>\n"
            + f"<existing_feedback>\n{existing_feedback}\n</existing_feedback>\n"
        )
        # Investigation notes go in user message (scanned by guardrail)
        report_user = (
            f"<investigation_notes>\n{investigation}\n</investigation_notes>\n\n"
            f"<pr>\nTitle: {title}\nBody: {body}\n</pr>"
        )

        raw = bedrock.invoke(report_system, report_user,
                             max_tokens=8000, json_schema=PR_REVIEW_SCHEMA)
        if raw is None:
            _write_artifact({
                "action": "ESCALATE", "reason": "bedrock_unavailable", "title": title,
                "html_url": html_url, "number": number, "is_pr": True,
                "prompt_id": prompts.prompt_version(tmpl), "model_id": cfg.bedrock_model_id,
            })
            return
        try:
            pr_result = json.loads(raw)
            if not isinstance(pr_result, dict):
                raise TypeError("Phase 2 root is not an object")
            analysis = pr_result.get("analysis", [])
            if not isinstance(analysis, list):
                raise TypeError("Phase 2 analysis is not a list")
            confirmed = []
            for a in analysis:
                if not isinstance(a, dict):
                    continue
                if a.get("disproved") is True:
                    continue
                finding = a.get("finding")
                if not isinstance(finding, dict):
                    continue
                confirmed.append(a)
            inline_comments = [
                {
                    "file": c.get("file") or "",
                    "line": c.get("line") or 0,
                    "severity": c["finding"].get("severity") or "",
                    "comment": c["finding"].get("comment") or "",
                    "evidence": c["finding"].get("evidence") or "",
                }
                for c in confirmed
            ]
        except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
            logger.error("Phase 2 returned unexpected format (%s): %s",
                         type(e).__name__, (raw or "")[:500])
            inline_comments = []

        # Hard filter: drop comments without a usable file path or positive line number
        inline_comments = [
            c for c in inline_comments
            if c.get("file") and isinstance(c.get("line"), int) and c["line"] > 0
        ]

        # Hard filter: on incremental review, drop comments on files not in the incremental diff
        if incremental_files and inline_comments:
            inline_comments = [
                c for c in inline_comments
                if c.get("file", "") in incremental_files
            ]

        # Hard filter: drop NITs on re-reviews (code-enforced, not prompt-dependent)
        if is_pr_update and inline_comments:
            inline_comments = [
                c for c in inline_comments
                if c.get("severity", "").upper() != "NIT"
            ]

        # Format comments: prepend severity, append evidence as context
        for c in inline_comments:
            severity = c.get("severity", "")
            evidence = c.get("evidence", "")
            prefix = f"**{severity}**: " if severity else ""
            suffix = "\n\n> " + evidence.replace("\n", "\n> ") if evidence else ""
            c["comment"] = prefix + c.get("comment", "") + suffix

        # Check CI status to give accurate signal to human reviewers
        ci_passed, ci_summary = gh.get_ci_status(head_sha) if head_sha else (None, "")

        if not inline_comments:
            if ci_passed is True:
                response = "No issues found. CI is passing.\n<!-- deequ-bot:clean -->"
            elif ci_passed is False:
                response = f"No code issues found, but {ci_summary}."
            else:
                response = "No issues found.\n<!-- deequ-bot:clean -->"
        else:
            response = ""

        _write_artifact({
            "action": "RESPOND",
            "labels": [], "response": response,
            "inline_comments": inline_comments,
            "title": title, "html_url": html_url, "number": number,
            "is_pr": True, "is_incremental": bool(incremental_diff),
            "prompt_id": prompts.prompt_version(tmpl),
            "model_id": cfg.bedrock_model_id,
        })
        return

    elif is_followup:
        tmpl = prompts.get_followup_prompt()
        if not tmpl:
            _write_artifact({"action": "ESCALATE", "labels": [], "response": "",
                "reason": "prompt_load_failed", "title": title, "html_url": html_url,
                "number": number, "is_pr": is_pr, "prompt_id": "n/a", "model_id": cfg.bedrock_model_id})
            return
        system_prompt = tmpl + f"\n\n<knowledge_base>\n{context}\n</knowledge_base>"
        user_prompt = f"<issue>\nTitle: {title}\nBody: {body}\n</issue>\n<conversation>\n{comments_text}\n</conversation>"
        prompt_id = prompts.prompt_version(tmpl)
    else:
        tmpl = prompts.get_issue_prompt()
        if not tmpl:
            _write_artifact({"action": "ESCALATE", "labels": [], "response": "",
                "reason": "prompt_load_failed", "title": title, "html_url": html_url,
                "number": number, "is_pr": is_pr, "prompt_id": "n/a", "model_id": cfg.bedrock_model_id})
            return
        system_prompt = tmpl + (
            f"\n\n<knowledge_base>\n{context}\n</knowledge_base>\n"
            f"<codebase_map>\n{codebase_map}\n</codebase_map>"
        )
        user_prompt = f"<issue>\nTitle: {title}\nBody: {body}\n</issue>\n<conversation>\n{comments_text}\n</conversation>"
        prompt_id = prompts.prompt_version(tmpl)

    schema = FOLLOWUP_SCHEMA if is_followup else ISSUE_RESPONSE_SCHEMA
    raw = bedrock.invoke(system_prompt, user_prompt, json_schema=schema)

    if raw is None:
        _write_artifact({
            "action": "ESCALATE", "labels": [], "response": "",
            "reason": "bedrock_unavailable", "title": title,
            "html_url": html_url, "number": number, "is_pr": is_pr,
            "prompt_id": prompt_id, "model_id": cfg.bedrock_model_id,
        })
        return

    parsed = _parse_response(raw, is_pr)

    if parsed.get("read_files") and cfg.enable_repo_search:
        snippets = _read_requested_files(gh, parsed["read_files"], cfg)
        if snippets:
            respond_tmpl = prompts.get_issue_respond_prompt()
            if respond_tmpl:
                respond_system = respond_tmpl + (
                    f"\n\n<knowledge_base>\n{context}\n</knowledge_base>\n"
                    f"<source_code>\n{snippets}\n</source_code>"
                )
                respond_user = f"<issue>\nTitle: {title}\nBody: {body}\n</issue>\n<conversation>\n{comments_text}\n</conversation>"
                raw2 = bedrock.invoke(respond_system, respond_user,
                                      json_schema=ISSUE_RESPONSE_SCHEMA)
                if raw2:
                    parsed2 = _parse_response(raw2, is_pr)
                    parsed2["labels"] = parsed2.get("labels") or parsed.get("labels", [])
                    parsed = parsed2

    _write_artifact({
        "action": parsed["action"], "labels": parsed.get("labels", []),
        "response": parsed.get("response", ""),
        "inline_comments": parsed.get("inline_comments", []),
        "title": title, "html_url": html_url, "number": number,
        "is_pr": is_pr, "prompt_id": prompt_id, "model_id": cfg.bedrock_model_id,
    })


def act():
    cfg = Config()
    gh = GitHubClient(cfg)
    slack = SlackClient(cfg)

    result = _read_artifact()
    if not result:
        logger.error("No artifact found")
        return

    # Validate artifact has required fields
    action = result.get("action", "SKIP")
    if action not in ("SKIP", "RESPOND", "ESCALATE", "CLOSE"):
        logger.error(f"Invalid action in artifact: {action}")
        return

    number = result.get("number", cfg.issue_number)
    is_pr = result.get("is_pr", False)
    title = str(result.get("title", ""))[:200]  # Truncate to prevent injection
    html_url = result.get("html_url", "")
    if html_url and not html_url.startswith("https://github.com/"):
        html_url = ""
    raw_labels = result.get("labels", [])
    if not isinstance(raw_labels, list):
        raw_labels = []
    labels = [l for l in raw_labels if isinstance(l, str) and l in cfg.allowed_labels]
    response = result.get("response", "")
    prompt_id = result.get("prompt_id", "unknown")
    model_id = result.get("model_id", "unknown")

    if action == "SKIP":
        logger.info(f"Skip #{number}: {result.get('reason')}")
        return

    footer = (
        f"\n\n---\n*Generated by AI (model: {model_id}, prompt: {prompt_id}) "
        f"— may not be fully accurate. Reply if this doesn't help.*"
    )

    # Pre-process: sanitize response before dispatch
    if action == "RESPOND":
        safe = sanitize(response)
        if safe is None:
            action = "ESCALATE"
            response = ""
        elif not safe and not result.get("inline_comments"):
            action = "ESCALATE"
            response = ""
        else:
            response = safe or ""

    if action == "RESPOND":
        inline_comments = result.get("inline_comments", [])
        if not isinstance(inline_comments, list):
            inline_comments = []
        # Sanitize inline comment text and keep the sanitized version
        sanitized_comments = []
        for ic in inline_comments:
            if not isinstance(ic, dict):
                continue
            safe_comment = sanitize(ic.get("comment", ""))
            if safe_comment is not None:
                sanitized_comments.append({**ic, "comment": safe_comment})
        inline_comments = sanitized_comments
        if is_pr and inline_comments:
            gh.post_pr_review(number, response + footer, inline_comments, event="COMMENT")
        elif is_pr and response and not inline_comments:
            gh.post_pr_review(number, response + footer, [], event="COMMENT")
        elif not response and not inline_comments:
            logger.info(f"Skip #{number}: nothing to post after sanitization")
        else:
            gh.post_comment(number, response + footer)
        gh.add_labels(number, labels)
        if "bug" in labels:
            slack.send_escalation(number, title, html_url, labels)
        elif "enhancement" in labels:
            slack.send_escalation(number, title, html_url, labels)
        logger.info(f"Responded to #{number}")

    elif action == "ESCALATE":
        reason = result.get("reason", "")
        if reason == "user_dissatisfied":
            ack = (
                "I understand my previous response wasn't helpful. "
                "I've notified the maintainer team and they will follow up directly." + footer
            )
        elif reason == "max_replies_reached":
            ack = (
                "I've reached the limit of what I can assist with on this issue. "
                "The maintainer team has been notified and will take over." + footer
            )
        elif reason == "issue_reopened":
            ack = (
                "This issue has been reopened. "
                "A maintainer has been notified and will follow up." + footer
            )
        else:
            if response:
                ack = (
                    response + "\n\n"
                    "This has also been flagged for our maintainer team to review." + footer
                )
            else:
                ack = (
                    "Thank you for reporting this.\n\n"
                    "This has been flagged for review by our maintainer team. "
                    "We'll get back to you as soon as possible." + footer
                )
        gh.post_comment(number, ack)
        gh.add_labels(number, labels)
        slack.send_escalation(number, title, html_url, labels)
        logger.info(f"Escalated #{number}")

    elif action == "CLOSE" and not is_pr:
        msg = (
            "This issue may not be related to the Deequ data quality library. "
            "The maintainer team has been notified and will review." + footer
        )
        gh.post_comment(number, msg)
        gh.add_labels(number, labels)
        slack.send_escalation(number, title, html_url, labels)
        logger.info(f"Flagged #{number} as potentially off-topic")

    else:
        logger.warning(f"Unhandled action '{action}' for #{number}, escalating")
        gh.post_comment(number, "This has been flagged for review by our maintainer team." + footer)
        slack.send_escalation(number, title, html_url, labels)


def _bot_reply_count(comments):
    return sum(1 for c in comments if c.get("user", {}).get("login") == "github-actions[bot]")


def _already_replied_to_latest(comments):
    """True if the bot already posted after the most recent non-bot comment."""
    last_user_idx = -1
    last_bot_idx = -1
    for i, c in enumerate(comments):
        if c.get("user", {}).get("login") == "github-actions[bot]":
            last_bot_idx = i
        else:
            last_user_idx = i
    return last_bot_idx > last_user_idx >= 0


_DISSATISFACTION_SIGNALS = [
    "that's wrong", "thats wrong", "that is wrong",
    "this is wrong", "this is incorrect", "incorrect answer",
    "didn't help", "doesn't help", "not helpful", "unhelpful",
    "wrong answer", "bad answer", "not correct", "that's not right",
    "still broken", "still not working", "doesn't work",
    "please escalate", "need a human", "talk to a human",
    "maintainer", "real person",
]


def _user_dissatisfied(comments):
    bot_has_replied = any(c.get("user", {}).get("login") == "github-actions[bot]" for c in comments)
    if not bot_has_replied:
        return False
    for c in reversed(comments):
        login = c.get("user", {}).get("login", "")
        if login == "github-actions[bot]":
            break
        if not login:
            continue
        body = (c.get("body") or "").lower()
        if any(s in body for s in _DISSATISFACTION_SIGNALS):
            return True
    return False


_HEADER_PREFIXES = ("ACTION:", "LABELS:", "READ_FILES:", "SEARCH:", "SEARCH_TERMS:")


def _parse_response(raw, is_pr):
    # Try structured JSON first (from Bedrock structured output)
    try:
        parsed = json.loads(raw)
        result = {
            "action": parsed.get("action", "ESCALATE"),
            "labels": parsed.get("labels", []),
            "read_files": parsed.get("read_files", []),
            "response": parsed.get("response", ""),
            "inline_comments": [],
        }
        if is_pr and result["action"] == "CLOSE":
            result["action"] = "ESCALATE"
        return result
    except (json.JSONDecodeError, TypeError):
        pass

    # Fallback: parse free-text format
    lines = raw.strip().split("\n")
    result = {"action": "ESCALATE", "labels": [], "response": "", "read_files": [], "inline_comments": []}
    response_lines = []

    for line in lines:
        upper = line.strip().upper()
        if upper.startswith("ACTION:"):
            val = line.split(":", 1)[1].strip().upper()
            if val in ("RESPOND", "ESCALATE", "CLOSE"):
                result["action"] = val
            continue
        elif upper.startswith("LABELS:"):
            raw_labels = line.split(":", 1)[1].strip()
            result["labels"] = [l.strip() for l in raw_labels.split(",") if l.strip().lower() not in ("none", "")]
            continue
        elif upper.startswith("READ_FILES:"):
            raw_files = line.split(":", 1)[1].strip()
            result["read_files"] = [f.strip() for f in raw_files.split(",") if f.strip().lower() not in ("none", "")]
            continue
        elif upper.startswith(("SEARCH:", "SEARCH_TERMS:")):
            continue
        response_lines.append(line)

    full_text = "\n".join(response_lines).strip()

    if is_pr and "INLINE:" in full_text and "FILE:" in full_text:
        result["response"], result["inline_comments"] = _parse_pr_review(full_text)
    else:
        result["response"] = _clean_response(full_text)

    if is_pr and result["action"] == "CLOSE":
        result["action"] = "ESCALATE"
    return result


def _parse_file_review_multi(raw):
    """Parse multi-file review output into inline comments."""
    comments = []
    current_file = None
    current_line = None
    current_comment = []

    for line in raw.strip().split("\n"):
        stripped = line.strip()
        upper = stripped.upper()
        if upper.startswith("FILE:"):
            if current_file and current_line and current_comment:
                comments.append({"file": current_file, "line": current_line, "comment": "\n".join(current_comment).strip()})
            current_file = stripped.split(":", 1)[1].strip()
            current_line = None
            current_comment = []
        elif upper.startswith("LINE:"):
            if current_file and current_line and current_comment:
                comments.append({"file": current_file, "line": current_line, "comment": "\n".join(current_comment).strip()})
            try:
                current_line = int(stripped.split(":", 1)[1].strip())
                current_comment = []
            except ValueError:
                current_line = None
        elif upper.startswith("COMMENT:"):
            current_comment = [stripped.split(":", 1)[1].strip()]
        elif current_comment is not None and current_file:
            current_comment.append(stripped)

    if current_file and current_line and current_comment:
        comments.append({"file": current_file, "line": current_line, "comment": "\n".join(current_comment).strip()})

    return comments




def _parse_pr_review(text):
    """Split PR review into summary and inline comments."""
    summary_part = ""
    inline_comments = []

    parts = text.split("INLINE:")
    summary_part = parts[0].replace("SUMMARY:", "").strip()

    if len(parts) > 1:
        inline_text = parts[1].strip()
        if inline_text.lower() == "none":
            return _clean_response(summary_part), []

        current = {}
        for line in inline_text.split("\n"):
            stripped = line.strip()
            upper = stripped.upper()
            if upper.startswith("FILE:"):
                if current.get("file") and current.get("comment"):
                    inline_comments.append(current)
                current = {"file": stripped.split(":", 1)[1].strip()}
            elif upper.startswith("LINE:"):
                try:
                    current["line"] = int(stripped.split(":", 1)[1].strip())
                except ValueError:
                    pass
            elif upper.startswith("COMMENT:"):
                current["comment"] = stripped.split(":", 1)[1].strip()
            elif current.get("comment"):
                current["comment"] += "\n" + stripped

        if current.get("file") and current.get("comment"):
            inline_comments.append(current)

    return _clean_response(summary_part), inline_comments


def _clean_response(text):
    """Remove any leaked headers or internal thinking from the response."""
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        upper = line.strip().upper()
        if upper.startswith(_HEADER_PREFIXES):
            continue
        cleaned.append(line)
    result = "\n".join(cleaned).strip()
    # Remove leading preamble like "Let me request..." or "I'll analyze..."
    while result and result.split("\n")[0].strip().lower().startswith((
        "let me ", "i'll ", "i will ", "i need to ", "first,", "sure,",
        "since i don't", "since i do not",
    )):
        result = "\n".join(result.split("\n")[1:]).strip()
    return result


def _format_comments(comments):
    if not comments:
        return "(none)"
    return "\n".join(
        f"{c.get('user', {}).get('login', '?')}: {c.get('body', '') or ''}"
        for c in comments
    )


def _format_pr_feedback(issue_comments, review_comments):
    parts = []
    for c in issue_comments:
        author = c.get("user", {}).get("login", "?")
        body = c.get("body", "") or ""
        parts.append(f"{author}: {body}")
    for c in review_comments:
        author = c.get("user", {}).get("login", "?")
        path = c.get("path", "")
        line = c.get("line") or c.get("original_line") or "?"
        body = c.get("body", "") or ""
        parts.append(f"{author} on {path}:{line}: {body}")
    return "\n".join(parts) if parts else "(no existing feedback)"


def _extract_diff_files(diff_text):
    """Extract the set of file paths touched in a unified diff."""
    files = set()
    for line in diff_text.split("\n"):
        m = re.match(r'^diff --git a/.+ b/(.+)$', line)
        if m:
            files.add(m.group(1))
    return files


def _read_requested_files(gh, file_paths, cfg):
    snippets = []
    for path in file_paths[:cfg.max_github_search_results]:
        if ".." in path or path.startswith("/"):
            continue
        content = gh.read_local_file(path)
        if not content:
            content = gh.get_file_content(path, repo=cfg.upstream_repo)
        if content:
            snippets.append(f"### {path}\n```scala\n{content}\n```")
    return "\n\n".join(snippets)


def _write_artifact(data):
    os.makedirs(os.path.dirname(ARTIFACT_PATH) or "/tmp", exist_ok=True)
    with open(ARTIFACT_PATH, "w") as f:
        json.dump(data, f)
    logger.info(f"Artifact: action={data.get('action')}")


def _read_artifact():
    try:
        with open(ARTIFACT_PATH) as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Artifact read failed: {e}")
        return None


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


_STATUS_CONFIRMED_RE = re.compile(
    r"^\s*STATUS\s*:\s*CONFIRMED\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _has_confirmed_findings(notes):
    """Check for any line matching exactly 'STATUS: CONFIRMED' in investigator notes.

    Anchored at line start/end with optional surrounding whitespace. Avoids
    false positives where 'CONFIRMED' appears in a COMMENT or rationale.
    """
    if not notes:
        return False
    return _STATUS_CONFIRMED_RE.search(notes) is not None


def _run_agent_pipeline(*, cfg, gh, bedrock, number, title, body, html_url, item,
                        context, codebase_map, comments_data, is_pr_update):
    """Investigator + Critic + Reporter pipeline. Replaces the legacy two-phase
    flow when cfg.agent_pipeline is True. Writes the analyze artifact and returns.
    """
    investigator_tmpl = prompts.get_pr_investigator_prompt()
    critic_tmpl = prompts.get_pr_critic_prompt()
    reporter_tmpl = prompts.get_pr_file_review_report_prompt()
    missing = []
    if not investigator_tmpl:
        missing.append("investigator")
    if not critic_tmpl:
        missing.append("critic")
    if not reporter_tmpl:
        missing.append("reporter")
    if missing:
        logger.error(
            "Pipeline enabled but required prompt(s) missing: %s. Failing closed "
            "(escalating). Set the corresponding SM_* env var or PR_*_PROMPT.",
            ", ".join(missing),
        )
        _write_artifact({
            "action": "ESCALATE", "labels": [], "response": "",
            "reason": f"prompt_load_failed: {','.join(missing)}",
            "title": title, "html_url": html_url, "number": number, "is_pr": True,
            "prompt_id": "n/a", "model_id": cfg.bedrock_model_id,
        })
        return

    diff = gh.get_pr_diff(number)
    review_comments = gh.get_pr_review_comments(number)
    existing_feedback = _format_pr_feedback(comments_data, review_comments)

    incremental_diff = ""
    incremental_files = set()
    if is_pr_update and cfg.event_before and cfg.event_after:
        incremental_diff = gh.get_compare_diff(cfg.event_before, cfg.event_after)
        if incremental_diff:
            incremental_files = _extract_diff_files(incremental_diff)

    head_sha = cfg.event_after or item.get("head", {}).get("sha", "")
    pr_files = gh.get_pr_files(number)
    diff_lines = sum(int(pf.get("changes", 0) or 0) for pf in pr_files)

    # Adaptive caps (PR-size proportional within configured upper bounds).
    # Floor is min(5, ceiling) so users can throttle below the default floor by
    # setting BOT_INVESTIGATOR_MAX_TURNS=3 etc.
    inv_ceiling = max(1, cfg.investigator_max_turns)
    inv_floor = min(5, inv_ceiling)
    investigator_caps = agent_loop.AgentCaps(
        max_turns=_clamp(diff_lines // 30, inv_floor, inv_ceiling),
        max_tool_calls=cfg.investigator_max_tool_calls,
        max_tool_output_chars=cfg.investigator_max_tool_output_chars,
    )
    crit_ceiling = max(1, cfg.critic_max_turns)
    crit_floor = min(3, crit_ceiling)
    critic_caps = agent_loop.AgentCaps(
        max_turns=_clamp(investigator_caps.max_turns // 2, crit_floor, crit_ceiling),
        max_tool_calls=cfg.critic_max_tool_calls,
        max_tool_output_chars=cfg.critic_max_tool_output_chars,
    )

    incremental_section = ""
    if incremental_diff:
        incremental_section = (
            "\n<incremental_review_instructions>\n"
            "This is a RE-REVIEW after the author pushed new commits. "
            "Limit findings to lines/files in the incremental diff below. "
            "Do not re-raise issues on unchanged code.\n"
            "</incremental_review_instructions>\n"
            f"<incremental_diff>\n{incremental_diff}\n</incremental_diff>\n"
        )

    # Static priming context (reused across Investigator and Critic for cache hit).
    static_context = (
        f"\n<knowledge_base>\n{context}\n</knowledge_base>\n"
        f"<codebase_map>\n{codebase_map}\n</codebase_map>\n"
        f"<existing_feedback>\n{existing_feedback}\n</existing_feedback>\n"
        f"{incremental_section}"
    )

    investigator_system = _render(
        investigator_tmpl, current_date=datetime.date.today().isoformat()
    ) + static_context
    critic_system = _render(
        critic_tmpl, current_date=datetime.date.today().isoformat()
    ) + static_context
    reporter_system = (
        _render(reporter_tmpl, current_date=datetime.date.today().isoformat())
        + f"\n<diff>\n{diff}\n</diff>\n"
        + f"<existing_feedback>\n{existing_feedback}\n</existing_feedback>\n"
    )

    investigator_user = (
        f"<diff>\n{diff}\n</diff>\n"
        f"<pr>\nTitle: {title}\nBody: {body}\n</pr>"
    )

    tool_runner = ToolRunner(cfg, gh.repo_root)
    cost_tracker = {
        "cost_usd": 0.0,
        "cap_usd": cfg.agent_max_review_cost_usd,
        "cap_hit": False,
    }
    pricing = cfg.bedrock_pricing

    inv_result = agent_loop.run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt=investigator_system, user_prompt=investigator_user,
        tool_specs=TOOL_SPECS, tool_runner=tool_runner,
        caps=investigator_caps, cost_tracker=cost_tracker, pricing=pricing,
    )

    metrics = {
        "investigator": _agent_metrics(inv_result),
        "critic": {"skipped": True, "skip_reason": None},
        "reporter": {"input_tokens": 0, "output_tokens": 0, "skipped": True},
        "totals": {},
        "cost_cap_hit": cost_tracker["cap_hit"],
        "max_turns_reached": inv_result.max_turns_reached,
        "critic_overturn_rate": None,
    }

    if not inv_result.text:
        _write_artifact_pipeline(
            cfg=cfg, action="ESCALATE", reason="investigator_empty",
            title=title, html_url=html_url, number=number,
            inv_tmpl=investigator_tmpl, critic_tmpl=critic_tmpl, reporter_tmpl=reporter_tmpl,
            inline_comments=[], response="", is_incremental=bool(incremental_diff),
            metrics=_finalize_metrics(metrics, inv_result, None, None, cost_tracker),
            tool_trace=inv_result.tool_trace,
        )
        return

    confirmed_present = _has_confirmed_findings(inv_result.text)

    crit_result = None
    rep_inline_comments = []
    rep_input_tokens = 0
    rep_output_tokens = 0

    if not confirmed_present:
        # FAST PATH: no confirmed findings → skip Critic and Reporter
        metrics["critic"]["skip_reason"] = "no_confirmed_findings"
        metrics["reporter"]["skipped"] = True
    elif cost_tracker["cap_hit"]:
        metrics["critic"]["skip_reason"] = "cost_cap_hit_after_investigator"
        metrics["reporter"]["skipped"] = True
    else:
        # Cap Critic's first-turn input. Investigator notes are bounded by
        # max_tokens_per_call; the diff is unbounded and dominates large PRs.
        # Truncate the diff so combined input stays well under Bedrock's 200K
        # token window. Critic can fetch full files via read_file as needed.
        max_critic_diff_chars = 200_000
        critic_diff = diff
        if len(critic_diff) > max_critic_diff_chars:
            critic_diff = (
                critic_diff[:max_critic_diff_chars]
                + f"\n... [diff truncated at {max_critic_diff_chars} chars; "
                "use read_file to fetch specific files referenced in investigator notes]"
            )
        critic_user = (
            f"<investigator_notes>\n{inv_result.text}\n</investigator_notes>\n"
            f"<diff>\n{critic_diff}\n</diff>\n"
            f"<pr>\nTitle: {title}\nBody: {body}\n</pr>"
        )
        crit_result = agent_loop.run(
            bedrock_client=bedrock, agent_name="critic",
            system_prompt=critic_system, user_prompt=critic_user,
            tool_specs=TOOL_SPECS, tool_runner=tool_runner,
            caps=critic_caps, cost_tracker=cost_tracker, pricing=pricing,
        )
        metrics["critic"] = _agent_metrics(crit_result)

        if cost_tracker["cap_hit"]:
            metrics["reporter"]["skipped"] = True
            metrics["reporter"]["skip_reason"] = "cost_cap_hit_after_critic"
        else:
            reporter_user = (
                f"<investigator_notes>\n{inv_result.text}\n</investigator_notes>\n\n"
                f"<critic_verdicts>\n{crit_result.text or 'ALL_DISPROVED: critic produced no verdicts'}\n</critic_verdicts>\n\n"
                f"<pr>\nTitle: {title}\nBody: {body}\n</pr>"
            )
            raw, rep_input_tokens, rep_output_tokens = _invoke_reporter_tracked(
                bedrock, reporter_system, reporter_user, cost_tracker, pricing,
            )
            metrics["reporter"]["skipped"] = False
            metrics["reporter"]["input_tokens"] = rep_input_tokens
            metrics["reporter"]["output_tokens"] = rep_output_tokens
            if raw is None:
                metrics["reporter"]["skip_reason"] = "bedrock_unavailable"
            else:
                rep_inline_comments = _parse_reporter_output(raw)

    # Apply post-processing filters (same as legacy path)
    rep_inline_comments = [
        c for c in rep_inline_comments
        if c.get("file") and isinstance(c.get("line"), int) and c["line"] > 0
    ]
    if incremental_files and rep_inline_comments:
        rep_inline_comments = [
            c for c in rep_inline_comments if c.get("file", "") in incremental_files
        ]
    if is_pr_update and rep_inline_comments:
        rep_inline_comments = [
            c for c in rep_inline_comments
            if c.get("severity", "").upper() != "NIT"
        ]
    for c in rep_inline_comments:
        severity = c.get("severity", "")
        evidence = c.get("evidence", "")
        prefix = f"**{severity}**: " if severity else ""
        suffix = "\n\n> " + evidence.replace("\n", "\n> ") if evidence else ""
        c["comment"] = prefix + c.get("comment", "") + suffix

    ci_passed, ci_summary = gh.get_ci_status(head_sha) if head_sha else (None, "")
    if not rep_inline_comments:
        if ci_passed is True:
            response = "No issues found. CI is passing.\n<!-- deequ-bot:clean -->"
        elif ci_passed is False:
            response = f"No code issues found, but {ci_summary}."
        else:
            response = "No issues found.\n<!-- deequ-bot:clean -->"
    else:
        response = ""

    _write_artifact_pipeline(
        cfg=cfg, action="RESPOND", reason=None,
        title=title, html_url=html_url, number=number,
        inv_tmpl=investigator_tmpl, critic_tmpl=critic_tmpl, reporter_tmpl=reporter_tmpl,
        inline_comments=rep_inline_comments, response=response,
        is_incremental=bool(incremental_diff),
        metrics=_finalize_metrics(metrics, inv_result, crit_result, None, cost_tracker),
        tool_trace=(inv_result.tool_trace + (crit_result.tool_trace if crit_result else [])),
    )


def _invoke_reporter_tracked(bedrock, system, user_msg, cost_tracker, pricing):
    """Invoke Reporter (single-shot, schema-enforced) while updating cost_tracker.
    Returns (raw_text, input_tokens, output_tokens). On failure: (None, 0, 0)."""
    raw, usage = bedrock.invoke_with_usage(
        system, user_msg, max_tokens=8000, json_schema=PR_REVIEW_SCHEMA,
    )
    in_t = (usage.get("inputTokens") or 0) if usage else 0
    out_t = (usage.get("outputTokens") or 0) if usage else 0
    cr_t = (usage.get("cacheReadInputTokens") or 0) if usage else 0
    cw_t = (usage.get("cacheWriteInputTokens") or 0) if usage else 0
    delta = agent_loop.estimate_cost_usd(in_t, out_t, cr_t, cw_t, pricing)
    cost_tracker["cost_usd"] = cost_tracker.get("cost_usd", 0.0) + delta
    if cost_tracker["cost_usd"] >= cost_tracker.get("cap_usd", float("inf")):
        cost_tracker["cap_hit"] = True
    return raw, in_t, out_t


def _agent_metrics(r):
    return {
        "skipped": False,
        "skip_reason": None,
        "turns": r.turns,
        "tool_calls": r.tool_calls,
        "tool_output_chars": r.tool_output_chars,
        "input_tokens": r.input_tokens,
        "output_tokens": r.output_tokens,
        "cache_read_tokens": r.cache_read_tokens,
        "cache_write_tokens": r.cache_write_tokens,
        "max_turns_reached": r.max_turns_reached,
        "error": r.error,
    }


def _finalize_metrics(metrics, inv, crit, _rep, cost_tracker):
    overturn = _critic_overturn_rate(crit.text if (crit and crit.text) else "")
    metrics["critic_overturn_rate"] = overturn
    rep_in = metrics.get("reporter", {}).get("input_tokens", 0) or 0
    rep_out = metrics.get("reporter", {}).get("output_tokens", 0) or 0
    metrics["totals"] = {
        "input_tokens": (inv.input_tokens if inv else 0) + (crit.input_tokens if crit else 0) + rep_in,
        "output_tokens": (inv.output_tokens if inv else 0) + (crit.output_tokens if crit else 0) + rep_out,
        "cost_usd": round(cost_tracker.get("cost_usd", 0.0), 4),
    }
    metrics["cost_cap_hit"] = cost_tracker.get("cap_hit", False)
    return metrics


_VERDICT_RE = re.compile(
    r"^\s*VERDICT\s*:\s*\S+\s*\|\s*(UPHELD|OVERTURNED)\b",
    re.IGNORECASE | re.MULTILINE,
)


def _critic_overturn_rate(critic_text):
    """Compute overturn rate by parsing VERDICT lines anchored at line start.

    Avoids substring false-positives where 'UPHELD' or 'OVERTURNED' appears
    in rationale text. Returns None if no verdicts parsed.
    """
    if not critic_text:
        return None
    upheld = 0
    overturned = 0
    for m in _VERDICT_RE.finditer(critic_text):
        verdict = m.group(1).upper()
        if verdict == "UPHELD":
            upheld += 1
        elif verdict == "OVERTURNED":
            overturned += 1
    total = upheld + overturned
    if total == 0:
        return None
    return round(overturned / total, 3)


def _parse_reporter_output(raw):
    """Parse the Reporter's JSON output and return inline_comments list.
    Same defensive parsing as the legacy path; never raises."""
    try:
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise TypeError("reporter root is not an object")
        analysis = result.get("analysis", [])
        if not isinstance(analysis, list):
            raise TypeError("reporter analysis is not a list")
        out = []
        for a in analysis:
            if not isinstance(a, dict):
                continue
            if a.get("disproved") is True:
                continue
            finding = a.get("finding")
            if not isinstance(finding, dict):
                continue
            out.append({
                "file": a.get("file") or "",
                "line": a.get("line") or 0,
                "severity": finding.get("severity") or "",
                "comment": finding.get("comment") or "",
                "evidence": finding.get("evidence") or "",
            })
        return out
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
        logger.error("Reporter output parse failed (%s): %s",
                     type(e).__name__, (raw or "")[:500])
        return []


def _write_artifact_pipeline(*, cfg, action, reason, title, html_url, number,
                              inv_tmpl, critic_tmpl, reporter_tmpl,
                              inline_comments, response, is_incremental,
                              metrics, tool_trace):
    """Artifact writer for pipeline runs. Includes metrics and trace."""
    artifact = {
        "action": action,
        "labels": [],
        "response": response,
        "inline_comments": inline_comments,
        "title": title, "html_url": html_url, "number": number,
        "is_pr": True, "is_incremental": is_incremental,
        "prompt_id": prompts.prompt_version(inv_tmpl) if inv_tmpl else "n/a",
        "prompt_ids": {
            "investigator": prompts.prompt_version(inv_tmpl) if inv_tmpl else "n/a",
            "critic": prompts.prompt_version(critic_tmpl) if critic_tmpl else "n/a",
            "reporter": prompts.prompt_version(reporter_tmpl) if reporter_tmpl else "n/a",
        },
        "model_id": cfg.bedrock_model_id,
        "metrics": metrics,
        "tool_trace": _truncate_trace(tool_trace),
        "pipeline": "agentic",
    }
    if reason:
        artifact["reason"] = reason
    _write_artifact(artifact)


def _truncate_trace(trace, max_chars=100_000):
    """Cap trace size in the artifact to avoid huge JSON blobs.

    On truncation, appends a sentinel entry recording how many entries were
    dropped. The arithmetic `len(trace) - len(out)` is computed BEFORE
    `out.append(marker)` (Python evaluates the dict literal first), so it
    correctly reports entries-not-kept (excluding the sentinel itself).
    """
    if not trace:
        return []
    serialized = json.dumps(trace)
    if len(serialized) <= max_chars:
        return trace
    out = []
    running = 2  # for the brackets
    for entry in trace:
        s = json.dumps(entry)
        if running + len(s) + 2 > max_chars:
            out.append({"truncated": True, "remaining_entries": len(trace) - len(out)})
            break
        out.append(entry)
        running += len(s) + 1
    return out


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("analyze", "act"):
        print("Usage: python -m issue_bot.main <analyze|act>")
        sys.exit(1)
    {"analyze": analyze, "act": act}[sys.argv[1]]()


if __name__ == "__main__":
    main()
