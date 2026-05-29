"""Multi-turn tool-use loop for Bedrock Converse.

Each turn calls Bedrock with toolConfig; if stopReason is "tool_use" the
loop executes the requested tools and continues, otherwise it returns the
final text. Owns budget enforcement (turns, tool calls, tool output chars),
records each tool call in the result's tool_trace, and runs a forced
no-tool commit phase when the tool budget is exhausted so the model is
physically constrained to emit text instead of more tool calls.
"""
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from .tools import DEFAULT_PER_TOOL_CALL_CAPS

logger = logging.getLogger("issue_bot")


@dataclass
class AgentCaps:
    """Per-agent budgets enforced by the loop. Defaults match the registered
    tool set in tools.py; pass per_tool_max_calls explicitly to override."""
    max_turns: int = 15
    max_tool_calls: int = 50
    max_tool_output_chars: int = 400_000
    per_tool_max_calls: Dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_PER_TOOL_CALL_CAPS)
    )
    max_tokens_per_call: int = 8000


@dataclass
class AgentResult:
    """Outcome of an agent loop run. `text` is the full aggregated assistant
    text across every turn so partial commits are preserved when a later
    turn ends in tool calls. `error` is a one-line label for logs."""
    text: str = ""
    turns: int = 0
    tool_calls: int = 0
    tool_output_chars: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    max_turns_reached: bool = False
    commit_phase_ran: bool = False
    error: Optional[str] = None
    tool_trace: List[Dict[str, Any]] = field(default_factory=list)


def run(
    bedrock_client,
    agent_name: str,
    system_prompt: str,
    user_prompt: str,
    tool_specs: List[Dict[str, Any]],
    tool_runner,
    caps: AgentCaps,
    pipeline_deadline: Optional[float] = None,
    untrusted_user_prompt: Optional[str] = None,
    commit_phase_user_prompt: Optional[str] = None,
    model_id: Optional[str] = None,
):
    """Execute an agent's tool-use loop. Returns an AgentResult.

    user_prompt is trusted; untrusted_user_prompt (PR/issue user input)
    goes in a guardContent block when the client has a guardrail so model
    paraphrases of repo content don't false-trip the scanner.

    commit_phase_user_prompt (optional) drives the forced no-tool turn
    that runs when the tool budget is exhausted. The Bedrock call on that
    turn omits toolConfig so the model is physically unable to call tools
    and must emit text. If unset, the loop terminates without a commit
    phase (legacy behavior).

    Termination: caps.max_turns / max_tool_calls / max_tool_output_chars /
    per_tool_max_calls, or pipeline_deadline. Result.text is the aggregated
    assistant text across every turn so partial commitments are preserved.
    """
    result = AgentResult()
    messages = [{
        "role": "user",
        "content": _build_user_content(
            user_prompt, untrusted_user_prompt,
            has_guardrail=getattr(bedrock_client, "has_guardrail", False),
        ),
    }]
    per_tool_calls = defaultdict(int)

    def _finalize(termination_reason: Optional[str] = None):
        if termination_reason and not result.error:
            result.error = termination_reason
        if commit_phase_user_prompt and _is_budget_exhaustion(termination_reason):
            _run_commit_phase(
                bedrock_client, agent_name, system_prompt, messages,
                commit_phase_user_prompt, tool_specs, caps, result, pipeline_deadline,
                model_id=model_id,
            )
        result.text = _aggregate_all_assistant_text(messages)
        return result

    for turn in range(max(0, caps.max_turns)):
        result.turns = turn + 1

        if pipeline_deadline is not None and time.monotonic() >= pipeline_deadline:
            logger.warning(
                "[%s turn=%d] pipeline wall-clock reached, terminating",
                agent_name, turn + 1,
            )
            return _finalize("pipeline wall-clock deadline reached")

        try:
            resp = bedrock_client.converse_with_tools(
                system_prompt=system_prompt,
                messages=messages,
                tool_specs=tool_specs,
                max_tokens=caps.max_tokens_per_call,
                model_id=model_id,
            )
        except Exception as e:
            logger.error("[%s turn=%d] bedrock error: %s", agent_name, turn + 1, type(e).__name__)
            return _finalize(f"bedrock error: {type(e).__name__}")

        if resp is None:
            return _finalize("bedrock returned None (circuit-breaker or transient failure)")

        usage = resp.get("usage") or {}
        result.input_tokens += usage.get("inputTokens") or 0
        result.output_tokens += usage.get("outputTokens") or 0
        result.cache_read_tokens += usage.get("cacheReadInputTokens") or 0
        result.cache_write_tokens += usage.get("cacheWriteInputTokens") or 0
        logger.debug(
            "[%s turn=%d] in=%d out=%d cacheR=%d cacheW=%d",
            agent_name, turn + 1, result.input_tokens, result.output_tokens,
            result.cache_read_tokens, result.cache_write_tokens,
        )

        msg = resp.get("output", {}).get("message", {})
        if not msg:
            return _finalize("empty bedrock message")
        messages.append(msg)

        if resp.get("stopReason") != "tool_use":
            return _finalize(None)

        tool_results = _run_tool_calls(
            msg, agent_name, turn + 1, tool_runner, caps, result, per_tool_calls,
        )
        if not tool_results:
            return _finalize("stopReason was tool_use but no toolUse blocks emitted")

        # Bedrock requires tool_use to be followed by toolResult, so we
        # append before the cap check — otherwise commit phase inherits an
        # unfollowed tool_use turn and the request is rejected.
        messages.append({"role": "user", "content": tool_results})

        if (result.tool_calls >= caps.max_tool_calls
                or result.tool_output_chars >= caps.max_tool_output_chars):
            logger.info("[%s turn=%d] structural cap hit", agent_name, turn + 1)
            return _finalize("structural cap hit; terminating before next turn")

    result.max_turns_reached = True
    logger.warning("[%s] max_turns (%d) reached", agent_name, caps.max_turns)
    return _finalize(f"max_turns ({caps.max_turns}) reached")


_BUDGET_EXHAUSTION_REASONS = frozenset({
    "pipeline wall-clock deadline reached",
    "structural cap hit; terminating before next turn",
})
# max_turns reached is also a budget exhaustion case; we match it by
# substring rather than exact text since the message includes the cap value.
_BUDGET_EXHAUSTION_PREFIXES = ("max_turns",)


def _is_budget_exhaustion(reason: Optional[str]) -> bool:
    if not reason:
        return False
    if reason in _BUDGET_EXHAUSTION_REASONS:
        return True
    return any(reason.startswith(p) for p in _BUDGET_EXHAUSTION_PREFIXES)


def _run_commit_phase(bedrock_client, agent_name, system_prompt, messages,
                      commit_phase_user_prompt, tool_specs, caps, result,
                      pipeline_deadline, model_id: Optional[str] = None):
    """One Bedrock turn after the loop's tool budget is exhausted, prompting
    the model to emit text only. tool_specs is kept on the request because
    Bedrock validates toolUse/toolResult content blocks in the conversation
    history against toolConfig — omitting it would trip ValidationException
    on any history that includes a prior tool call. Suppression of new tool
    calls is enforced by the commit-phase user prompt, not by removing
    toolConfig.
    """
    if pipeline_deadline is not None and time.monotonic() >= pipeline_deadline:
        logger.warning(
            "[%s] commit phase skipped: pipeline wall-clock already past",
            agent_name,
        )
        return
    # Bedrock rejects two consecutive same-role messages. Merge the commit
    # prompt into the trailing user turn (which usually carries toolResults)
    # instead of appending a second user message.
    if messages and messages[-1].get("role") == "user":
        last_content = list(messages[-1].get("content", []))
        last_content.append({"text": commit_phase_user_prompt})
        messages[-1] = {"role": "user", "content": last_content}
    else:
        messages.append({"role": "user", "content": [{"text": commit_phase_user_prompt}]})

    result.commit_phase_ran = True

    try:
        resp = bedrock_client.converse_with_tools(
            system_prompt=system_prompt,
            messages=messages,
            tool_specs=tool_specs,
            max_tokens=caps.max_tokens_per_call,
            model_id=model_id,
        )
    except Exception as e:
        logger.error("[%s] commit phase bedrock error: %s", agent_name, type(e).__name__)
        return
    if resp is None:
        logger.warning("[%s] commit phase returned None", agent_name)
        return

    usage = resp.get("usage") or {}
    result.input_tokens += usage.get("inputTokens") or 0
    result.output_tokens += usage.get("outputTokens") or 0
    result.cache_read_tokens += usage.get("cacheReadInputTokens") or 0
    result.cache_write_tokens += usage.get("cacheWriteInputTokens") or 0

    msg = resp.get("output", {}).get("message", {})
    if msg:
        messages.append(msg)
    logger.info("[%s] commit phase emitted %d output tokens", agent_name, usage.get("outputTokens") or 0)


def _build_user_content(trusted, untrusted, *, has_guardrail):
    """Two-block form only when an untrusted segment AND a guardrail are
    both present; otherwise concatenate into one text block."""
    if untrusted and has_guardrail:
        return [
            {"text": trusted},
            {"guardContent": {"text": {"text": untrusted}}},
        ]
    if untrusted:
        return [{"text": f"{trusted}\n{untrusted}"}]
    return [{"text": trusted}]


def _run_tool_calls(msg, agent_name, turn, tool_runner, caps, result, per_tool_calls):
    """Execute every toolUse block in `msg` and return the toolResult blocks
    to append to the conversation. Mutates `result` and `per_tool_calls`."""
    tool_results = []
    for block in msg.get("content", []):
        if "toolUse" not in block:
            continue
        tu = block["toolUse"]
        tool_name = tu.get("name", "")
        tool_args = tu.get("input") or {}
        tool_use_id = tu.get("toolUseId", "")
        tool_text = _execute_or_budget(
            tool_name, tool_args, agent_name, turn, tool_runner,
            caps, result, per_tool_calls,
        )
        tool_results.append({
            "toolResult": {
                "toolUseId": tool_use_id,
                "content": [{"text": tool_text}],
            },
        })
    return tool_results


def _execute_or_budget(tool_name, tool_args, agent_name, turn, tool_runner,
                       caps, result, per_tool_calls):
    """Run the tool, or return a budget-exhausted text the model will read."""
    per_tool_cap = caps.per_tool_max_calls.get(tool_name)
    if per_tool_cap is not None and per_tool_calls[tool_name] >= per_tool_cap:
        logger.info("[%s turn=%d] %s budget exhausted", agent_name, turn, tool_name)
        return (
            f"BUDGET EXHAUSTED: tool '{tool_name}' has been called {per_tool_cap} "
            "times. Investigate with information already gathered, or use a "
            "different tool."
        )
    if result.tool_calls >= caps.max_tool_calls:
        logger.info("[%s turn=%d] total tool budget exhausted", agent_name, turn)
        return (
            f"BUDGET EXHAUSTED: total tool calls reached {caps.max_tool_calls}. "
            "Conclude investigation with information already gathered."
        )
    if result.tool_output_chars >= caps.max_tool_output_chars:
        logger.info("[%s turn=%d] tool output budget exhausted", agent_name, turn)
        return (
            f"BUDGET EXHAUSTED: total tool output {result.tool_output_chars} chars "
            f"reached cap {caps.max_tool_output_chars}. Conclude investigation."
        )

    t0 = time.monotonic()
    text = tool_runner.run(tool_name, tool_args)
    if not isinstance(text, str):
        text = str(text)
    latency_ms = int((time.monotonic() - t0) * 1000)
    per_tool_calls[tool_name] += 1
    result.tool_calls += 1
    result.tool_output_chars += len(text)
    logger.info(
        "[%s turn=%d] %s(%s) → %d chars in %dms",
        agent_name, turn, tool_name, _summarize_args(tool_args), len(text), latency_ms,
    )
    result.tool_trace.append({
        "agent": agent_name,
        "turn": turn,
        "tool": tool_name,
        "args": _capped_args(tool_args),
        "result_summary": text[:200] + ("..." if len(text) > 200 else ""),
        "result_chars": len(text),
        "latency_ms": latency_ms,
    })
    return text


_MAX_TRACE_ARG_CHARS = 500


def _capped_args(args):
    """Cap each arg value so a single huge model-supplied string can't
    swamp the artifact's tool trace."""
    if not isinstance(args, dict):
        return repr(args)[:_MAX_TRACE_ARG_CHARS]
    out = {}
    for k, v in args.items():
        if isinstance(v, str) and len(v) > _MAX_TRACE_ARG_CHARS:
            out[k] = v[:_MAX_TRACE_ARG_CHARS] + "...[truncated]"
        else:
            out[k] = v
    return out


def _extract_text(msg):
    """Join text blocks from an assistant message; "" if none."""
    return "\n".join(
        b.get("text", "") for b in msg.get("content", []) if "text" in b
    ).strip()


def _aggregate_all_assistant_text(messages):
    """Concatenate text blocks from every assistant message in order.
    Preserves partial commitments emitted in mid-loop turns even when later
    turns end in tool calls or the loop terminates abnormally."""
    parts = []
    for msg in messages:
        if msg.get("role") != "assistant":
            continue
        text = _extract_text(msg)
        if text:
            parts.append(text)
    return "\n\n".join(parts)


def _summarize_args(args):
    """One-line repr of tool args, truncated for log output."""
    if not isinstance(args, dict):
        return repr(args)[:80]
    parts = []
    for k, v in args.items():
        if isinstance(v, str):
            parts.append(f"{k}={v[:60]!r}")
        else:
            parts.append(f"{k}={v!r}")
    return ", ".join(parts)
