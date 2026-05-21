"""Multi-turn tool-use loop for Bedrock Converse.

Used by both the Investigator and the Critic. Each turn:
  1. Call Bedrock with toolConfig.
  2. If stopReason is "tool_use", execute the requested tools and append
     the toolResults; loop.
  3. Otherwise, return the final text.

Owns budget enforcement (turns, tool calls, tool output chars) and
tool-call provenance recording. Token usage is reported in the result
for downstream observability; this module does NOT enforce cost limits.
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
    tool set in tools.py; pass per_tool_max_calls explicitly to override.

    The wall-clock cap is the ultimate runaway protection: structural budgets
    bound *steady-state* cost but a stuck loop can still pay the per-turn
    Bedrock cost up to max_turns. wall_clock_seconds bounds total elapsed
    time end-to-end.
    """
    max_turns: int = 15
    max_tool_calls: int = 50
    max_tool_output_chars: int = 400_000
    per_tool_max_calls: Dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_PER_TOOL_CALL_CAPS)
    )
    max_tokens_per_call: int = 8000
    wall_clock_seconds: int = 300


@dataclass
class AgentResult:
    """Outcome of an agent loop run.

    `error` is set on any non-clean exit (Bedrock failure, structural cap,
    wall-clock cap, max_turns, protocol violation). Callers must check
    `fatal` to decide whether `text` is usable: structural exits often
    produce partial-but-valid output, fatal exits do not.
    """
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
    fatal: bool = False
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
):
    """Execute an agent's tool-use loop. Returns an AgentResult.

    Termination is bounded structurally:
      - caps.max_turns total Bedrock turns
      - caps.max_tool_calls total tool executions
      - caps.max_tool_output_chars cumulative tool output size
      - caps.per_tool_max_calls per-tool budget
      - caps.wall_clock_seconds elapsed time for THIS agent
      - pipeline_deadline (optional) absolute monotonic deadline shared
        across all stages of the pipeline; whichever is sooner wins
    """
    result = AgentResult()
    messages = [{"role": "user", "content": [{"text": user_prompt}]}]
    per_tool_calls = defaultdict(int)
    own_deadline = time.monotonic() + max(1, caps.wall_clock_seconds)
    deadline = own_deadline if pipeline_deadline is None else min(own_deadline, pipeline_deadline)

    for turn in range(max(0, caps.max_turns)):
        result.turns = turn + 1

        if time.monotonic() >= deadline:
            result.error = f"wall-clock cap ({caps.wall_clock_seconds}s) reached"
            # Bounded exit; any text recovered from prior turns is usable.
            result.text = result.text or _recover_last_assistant_text(messages)
            logger.warning(
                "[%s turn=%d] wall-clock cap reached, terminating",
                agent_name, turn + 1,
            )
            return result

        try:
            resp = bedrock_client.converse_with_tools(
                system_prompt=system_prompt,
                messages=messages,
                tool_specs=tool_specs,
                max_tokens=caps.max_tokens_per_call,
                timeout_seconds=max(1, deadline - time.monotonic()),
            )
        except Exception as e:
            logger.error("[%s turn=%d] bedrock error: %s", agent_name, turn + 1, type(e).__name__)
            # Truncate exception message: Bedrock validation errors can echo
            # prompt fragments which may include PR-content. Strip control
            # chars that could mangle JSON-serialized artifacts.
            msg_str = "".join(c for c in str(e)[:200] if c.isprintable() or c in " \t")
            result.error = f"bedrock error: {type(e).__name__}: {msg_str}"
            result.fatal = True
            return result

        if resp is None:
            result.error = "bedrock returned None (circuit-breaker or transient failure)"
            result.fatal = True
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
            result.fatal = True
            return result
        messages.append(msg)

        if resp.get("stopReason") != "tool_use":
            result.text = _extract_text(msg)
            return result

        tool_results = _run_tool_calls(
            msg, agent_name, turn + 1, tool_runner, caps, result, per_tool_calls,
        )
        if not tool_results:
            result.text = _extract_text(msg)
            result.error = "stopReason was tool_use but no toolUse blocks emitted"
            result.fatal = True
            return result

        # If structural caps were hit during tool execution, terminate now
        # rather than paying another Bedrock turn that will only emit more
        # tool_use blocks against exhausted budgets.
        if (result.tool_calls >= caps.max_tool_calls
                or result.tool_output_chars >= caps.max_tool_output_chars):
            result.error = "structural cap hit; terminating before next turn"
            result.text = _extract_text(msg) or (
                f"Investigation terminated on turn {turn + 1}: structural cap "
                "reached (max_tool_calls or max_tool_output_chars)."
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


def _run_tool_calls(msg, agent_name, turn, tool_runner, caps, result, per_tool_calls):
    """Execute every toolUse block in `msg`. Returns the list of toolResult blocks
    to append to the conversation. Updates `result` in place (tool_calls,
    tool_output_chars, tool_trace) and `per_tool_calls`."""
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
    """Execute a tool call or return a budget-exhausted message. Records
    successful runs in result.tool_trace and increments counters."""
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
    """Cap each arg value to avoid a single huge model-supplied value
    swamping the artifact's tool trace."""
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
    """Concatenate text blocks from an assistant message. Returns "" if none."""
    return "\n".join(
        b.get("text", "") for b in msg.get("content", []) if "text" in b
    ).strip()


def _recover_last_assistant_text(messages):
    """Walk back through the conversation to the most recent assistant message
    and extract any text it carried (possibly alongside tool_use blocks).
    Used on max_turns exit so partial findings aren't dropped."""
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            return _extract_text(msg)
    return ""


def _summarize_args(args):
    """One-line representation of tool args for log output."""
    if not isinstance(args, dict):
        return repr(args)[:80]
    parts = []
    for k, v in args.items():
        if isinstance(v, str):
            parts.append(f"{k}={v[:60]!r}")
        else:
            parts.append(f"{k}={v!r}")
    return ", ".join(parts)
