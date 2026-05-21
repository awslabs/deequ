"""
Multi-turn tool-use loop for Bedrock Converse.

Runs an agent (Investigator or Critic) through repeated rounds of:
  1. Bedrock Converse call with toolConfig
  2. If stopReason == "tool_use", execute the requested tools
  3. Append tool results, continue
  4. Otherwise, terminate with the final text

Owns budget enforcement (turns, tool calls, tool output chars, cost),
provenance tracking (tool trace), and graceful termination on caps.
Both Investigator and Critic use the same loop with different caps and
prompts.
"""
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

logger = logging.getLogger("issue_bot")


@dataclass
class AgentCaps:
    """Per-agent budgets enforced by the loop."""
    max_turns: int = 15
    max_tool_calls: int = 50
    max_tool_output_chars: int = 400_000
    per_tool_max_calls: Dict[str, int] = field(default_factory=lambda: {
        "grep_codebase": 50,
        "read_file": 30,
        "list_dir": 20,
        "find_callers": 20,
        "find_tests_for": 10,
    })
    max_tokens_per_call: int = 8000


@dataclass
class AgentResult:
    """Outcome of an agent loop run."""
    text: str = ""
    turns: int = 0
    tool_calls: int = 0
    tool_output_chars: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    max_turns_reached: bool = False
    cost_cap_hit: bool = False
    error: Optional[str] = None
    tool_trace: List[Dict[str, Any]] = field(default_factory=list)


def _estimate_cost_usd(input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, pricing):
    """Convert token counts to USD using the provided per-million rates.

    pricing is a dict with keys: input_per_million, output_per_million,
    cache_read_per_million, cache_write_per_million.
    """
    return (
        (input_tokens / 1_000_000) * pricing["input_per_million"]
        + (output_tokens / 1_000_000) * pricing["output_per_million"]
        + (cache_read_tokens / 1_000_000) * pricing["cache_read_per_million"]
        + (cache_write_tokens / 1_000_000) * pricing["cache_write_per_million"]
    )


def run(
    bedrock_client,
    agent_name: str,
    system_prompt: str,
    user_prompt: str,
    tool_specs: List[Dict[str, Any]],
    tool_runner,
    caps: AgentCaps,
    cost_tracker,
    pricing: Dict[str, float],
):
    """Execute an agent's tool-use loop.

    bedrock_client: instance of BedrockClient (must support converse_with_tools).
    tool_runner: callable (name, args_dict) -> str. Implements actual tool dispatch.
    cost_tracker: shared mutable dict {"cost_usd": float, "cap_hit": bool}.
        The loop enforces the cap by reading/writing cost_tracker before each turn.
    pricing: per-million-token rates for the model in use.

    Returns an AgentResult.
    """
    result = AgentResult()
    messages = [{"role": "user", "content": [{"text": user_prompt}]}]
    per_tool_calls = defaultdict(int)

    for turn in range(caps.max_turns):
        result.turns = turn + 1

        # Pre-turn cost cap check
        if cost_tracker.get("cap_hit"):
            result.cost_cap_hit = True
            result.error = "cost cap hit before turn"
            logger.warning("[%s turn=%d] cost cap hit, terminating", agent_name, turn + 1)
            break

        try:
            resp = bedrock_client.converse_with_tools(
                system_prompt=system_prompt,
                messages=messages,
                tool_specs=tool_specs,
                max_tokens=caps.max_tokens_per_call,
            )
        except Exception as e:
            logger.error("[%s turn=%d] bedrock error: %s", agent_name, turn + 1, type(e).__name__)
            result.error = f"bedrock error: {type(e).__name__}: {e}"
            break

        if resp is None:
            result.error = "bedrock returned None (circuit-breaker or transient failure)"
            break

        usage = resp.get("usage", {}) or {}
        in_t = usage.get("inputTokens", 0) or 0
        out_t = usage.get("outputTokens", 0) or 0
        cr_t = usage.get("cacheReadInputTokens", 0) or 0
        cw_t = usage.get("cacheWriteInputTokens", 0) or 0
        result.input_tokens += in_t
        result.output_tokens += out_t
        result.cache_read_tokens += cr_t
        result.cache_write_tokens += cw_t
        delta_cost = _estimate_cost_usd(in_t, out_t, cr_t, cw_t, pricing)
        cost_tracker["cost_usd"] = cost_tracker.get("cost_usd", 0.0) + delta_cost
        if cost_tracker["cost_usd"] >= cost_tracker.get("cap_usd", float("inf")):
            cost_tracker["cap_hit"] = True
            result.cost_cap_hit = True
        logger.info(
            "[%s turn=%d] in=%d out=%d cacheR=%d cacheW=%d cost_so_far=$%.4f",
            agent_name, turn + 1, in_t, out_t, cr_t, cw_t,
            cost_tracker.get("cost_usd", 0.0),
        )

        msg = resp.get("output", {}).get("message", {})
        if not msg:
            result.error = "empty bedrock message"
            break
        messages.append(msg)

        stop = resp.get("stopReason")
        if stop != "tool_use":
            # Final text emitted; collect and exit
            result.text = "\n".join(
                b.get("text", "") for b in msg.get("content", []) if "text" in b
            ).strip()
            return result

        # Execute tool calls in this turn
        tool_results_content = []
        for block in msg.get("content", []):
            if "toolUse" not in block:
                continue
            tu = block["toolUse"]
            tool_name = tu.get("name", "")
            tool_args = tu.get("input", {}) or {}
            tool_use_id = tu.get("toolUseId", "")

            # Enforce per-tool budget
            per_tool_cap = caps.per_tool_max_calls.get(tool_name, 9999)
            if per_tool_calls[tool_name] >= per_tool_cap:
                tool_text = (
                    f"BUDGET EXHAUSTED: tool '{tool_name}' has been called "
                    f"{per_tool_cap} times. Investigate with information already "
                    "gathered, or use a different tool."
                )
                logger.info("[%s turn=%d] %s budget exhausted", agent_name, turn + 1, tool_name)
            elif result.tool_calls >= caps.max_tool_calls:
                tool_text = (
                    f"BUDGET EXHAUSTED: total tool calls reached {caps.max_tool_calls}. "
                    "Conclude investigation with information already gathered."
                )
                logger.info("[%s turn=%d] total tool budget exhausted", agent_name, turn + 1)
            elif result.tool_output_chars >= caps.max_tool_output_chars:
                tool_text = (
                    f"BUDGET EXHAUSTED: total tool output {result.tool_output_chars} chars "
                    f"reached cap {caps.max_tool_output_chars}. Conclude investigation."
                )
                logger.info("[%s turn=%d] tool output budget exhausted", agent_name, turn + 1)
            else:
                t0 = time.monotonic()
                tool_text = tool_runner.run(tool_name, tool_args)
                if not isinstance(tool_text, str):
                    tool_text = str(tool_text)
                latency_ms = int((time.monotonic() - t0) * 1000)
                per_tool_calls[tool_name] += 1
                result.tool_calls += 1
                result.tool_output_chars += len(tool_text)
                logger.info(
                    "[%s turn=%d] %s(%s) → %d chars in %dms",
                    agent_name, turn + 1, tool_name,
                    _summarize_args(tool_args), len(tool_text), latency_ms,
                )
                result.tool_trace.append({
                    "agent": agent_name,
                    "turn": turn + 1,
                    "tool": tool_name,
                    "args": tool_args,
                    "result_summary": tool_text[:200] + ("..." if len(tool_text) > 200 else ""),
                    "result_chars": len(tool_text),
                    "latency_ms": latency_ms,
                })

            tool_results_content.append({
                "toolResult": {
                    "toolUseId": tool_use_id,
                    "content": [{"text": tool_text}],
                },
            })

        if not tool_results_content:
            # stopReason said tool_use but no toolUse blocks present → bail
            result.text = "\n".join(
                b.get("text", "") for b in msg.get("content", []) if "text" in b
            ).strip()
            result.error = "stopReason was tool_use but no toolUse blocks emitted"
            return result

        messages.append({"role": "user", "content": tool_results_content})

    # Loop exited via max_turns. The last appended message is typically a user
    # toolResult (we appended it after the last bedrock call), so walk backward
    # to find the most recent assistant message and extract any text it carried
    # alongside its tool_use blocks.
    result.max_turns_reached = True
    for msg in reversed(messages):
        if msg.get("role") != "assistant":
            continue
        text_blocks = [
            b.get("text", "")
            for b in msg.get("content", [])
            if "text" in b and isinstance(b.get("text"), str)
        ]
        recovered = "\n".join(text_blocks).strip()
        if recovered:
            result.text = recovered
        break
    if not result.text:
        result.text = (
            f"Investigation hit max_turns ({caps.max_turns}) without conclusion. "
            f"Made {result.tool_calls} tool call(s)."
        )
    logger.warning("[%s] max_turns (%d) reached", agent_name, caps.max_turns)
    return result


def _summarize_args(args):
    """One-line summary of tool args for logging."""
    if not isinstance(args, dict):
        return repr(args)[:80]
    parts = []
    for k, v in args.items():
        if isinstance(v, str):
            parts.append(f"{k}={v[:60]!r}")
        else:
            parts.append(f"{k}={v!r}")
    return ", ".join(parts)
