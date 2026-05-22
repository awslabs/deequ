"""Multi-turn tool-use loop for Bedrock Converse.

Each turn calls Bedrock with toolConfig; if stopReason is "tool_use" the
loop executes the requested tools and continues, otherwise it returns the
final text. Owns budget enforcement (turns, tool calls, tool output chars)
and records each tool call in the result's tool_trace.
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
    """Outcome of an agent loop run. Empty `text` means the agent had no
    usable output and downstream stages should skip; `error` is a one-line
    label for logs."""
    text: str = ""
    turns: int = 0
    tool_calls: int = 0
    tool_output_chars: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    max_turns_reached: bool = False
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
):
    """Execute an agent's tool-use loop. Returns an AgentResult.

    user_prompt is trusted; untrusted_user_prompt (PR/issue user input)
    goes in a guardContent block when the client has a guardrail so model
    paraphrases of repo content don't false-trip the scanner.

    Termination: caps.max_turns / max_tool_calls / max_tool_output_chars /
    per_tool_max_calls, or pipeline_deadline.
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

    for turn in range(max(0, caps.max_turns)):
        result.turns = turn + 1

        if pipeline_deadline is not None and time.monotonic() >= pipeline_deadline:
            result.error = "pipeline wall-clock deadline reached"
            result.text = result.text or _recover_last_assistant_text(messages)
            logger.warning(
                "[%s turn=%d] pipeline wall-clock reached, terminating",
                agent_name, turn + 1,
            )
            return result

        try:
            resp = bedrock_client.converse_with_tools(
                system_prompt=system_prompt,
                messages=messages,
                tool_specs=tool_specs,
                max_tokens=caps.max_tokens_per_call,
            )
        except Exception as e:
            logger.error("[%s turn=%d] bedrock error: %s", agent_name, turn + 1, type(e).__name__)
            result.error = f"bedrock error: {type(e).__name__}"
            return result

        if resp is None:
            result.error = "bedrock returned None (circuit-breaker or transient failure)"
            return result

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
            result.error = "empty bedrock message"
            return result
        messages.append(msg)

        if resp.get("stopReason") != "tool_use":
            result.text = _extract_text(msg)
            return result

        tool_results = _run_tool_calls(
            msg, agent_name, turn + 1, tool_runner, caps, result, per_tool_calls,
        )
        if not tool_results:
            # Bedrock protocol violation: stopReason=tool_use with no toolUse
            # blocks. We capture any accompanying text and exit; if the text
            # happens to be parseable downstream the caller can use it.
            result.text = _extract_text(msg)
            result.error = "stopReason was tool_use but no toolUse blocks emitted"
            return result

        # Stop before paying another Bedrock turn against exhausted budgets.
        if (result.tool_calls >= caps.max_tool_calls
                or result.tool_output_chars >= caps.max_tool_output_chars):
            result.error = "structural cap hit; terminating before next turn"
            result.text = _extract_text(msg) or (
                f"Investigation terminated on turn {turn + 1}: structural cap reached."
            )
            return result

        messages.append({"role": "user", "content": tool_results})

    result.max_turns_reached = True
    result.text = _recover_last_assistant_text(messages) or (
        f"Investigation hit max_turns ({caps.max_turns}) without conclusion. "
        f"Made {result.tool_calls} tool call(s)."
    )
    logger.warning("[%s] max_turns (%d) reached", agent_name, caps.max_turns)
    return result


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


def _recover_last_assistant_text(messages):
    """Walk back to the most recent assistant message and extract its text.
    Used on max_turns / wall-clock exits so partial findings aren't dropped
    when the trailing message is the user-side toolResult."""
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            return _extract_text(msg)
    return ""


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
