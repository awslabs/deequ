"""Tests for issue_bot.agent_loop — the multi-turn tool-use loop."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from issue_bot.agent_loop import run, AgentCaps


PRICING = {
    "input_per_million": 15.0,
    "output_per_million": 75.0,
    "cache_read_per_million": 1.5,
    "cache_write_per_million": 18.75,
}


class FakeBedrockClient:
    """Records calls; returns canned responses in sequence."""
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def converse_with_tools(self, system_prompt, messages, tool_specs, max_tokens=8000, temperature=0.3):
        self.calls.append({
            "system_prompt": system_prompt,
            "messages": [m for m in messages],
            "tool_specs": tool_specs,
            "max_tokens": max_tokens,
        })
        if not self._responses:
            return None
        return self._responses.pop(0)


class FakeToolRunner:
    """Returns the same canned text for every tool call."""
    def __init__(self, response="tool result"):
        self.response = response
        self.calls = []

    def run(self, name, args):
        self.calls.append({"name": name, "args": args})
        return self.response


def _resp(content_blocks, stop_reason="end_turn", usage=None):
    return {
        "stopReason": stop_reason,
        "output": {"message": {"role": "assistant", "content": content_blocks}},
        "usage": usage or {"inputTokens": 100, "outputTokens": 50, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
    }


def test_terminates_when_model_emits_text():
    bedrock = FakeBedrockClient([_resp([{"text": "investigation done"}])])
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 1.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=5), cost_tracker=cost, pricing=PRICING,
    )
    assert result.text == "investigation done"
    assert result.turns == 1
    assert result.tool_calls == 0
    assert not result.max_turns_reached
    assert tr.calls == []


def test_executes_tool_call_then_terminates():
    bedrock = FakeBedrockClient([
        _resp(
            [{"toolUse": {"toolUseId": "t1", "name": "grep_codebase", "input": {"pattern": "Foo"}}}],
            stop_reason="tool_use",
        ),
        _resp([{"text": "found Foo, done"}]),
    ])
    tr = FakeToolRunner("Foo.scala:1:case class Foo")
    cost = {"cost_usd": 0.0, "cap_usd": 1.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=5), cost_tracker=cost, pricing=PRICING,
    )
    assert result.text == "found Foo, done"
    assert result.turns == 2
    assert result.tool_calls == 1
    assert tr.calls[0]["name"] == "grep_codebase"


def test_hits_max_turns():
    # Always returns tool_use; never terminates
    tu_resp = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_resp] * 10)
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=3), cost_tracker=cost, pricing=PRICING,
    )
    assert result.max_turns_reached is True
    assert result.turns == 3


def test_per_tool_budget_returns_error_then_continues():
    bedrock = FakeBedrockClient([
        _resp(
            [{"toolUse": {"toolUseId": "t1", "name": "grep_codebase", "input": {"pattern": "a"}}}],
            stop_reason="tool_use",
        ),
        _resp(
            [{"toolUse": {"toolUseId": "t2", "name": "grep_codebase", "input": {"pattern": "b"}}}],
            stop_reason="tool_use",
        ),
        _resp([{"text": "done"}]),
    ])
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    caps = AgentCaps(max_turns=5, per_tool_max_calls={"grep_codebase": 1})
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=caps, cost_tracker=cost, pricing=PRICING,
    )
    # Second grep should have hit the per-tool budget; the runner should have
    # been called once (real call) — the second is short-circuited to a budget
    # message that the model reads as toolResult.
    assert len(tr.calls) == 1
    assert result.text == "done"


def test_total_tool_call_budget():
    """max_tool_calls cap stops further tool executions."""
    tu_resp = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_resp, tu_resp, tu_resp, _resp([{"text": "done"}])])
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    caps = AgentCaps(max_turns=10, max_tool_calls=2)
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=caps, cost_tracker=cost, pricing=PRICING,
    )
    # First two calls run; third is short-circuited
    assert len(tr.calls) == 2


def test_cost_cap_triggers_termination():
    bedrock = FakeBedrockClient([
        _resp(
            [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
            stop_reason="tool_use",
            usage={"inputTokens": 1_000_000, "outputTokens": 0, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
        ),
        _resp([{"text": "no"}]),
    ])
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 1.0, "cap_hit": False}  # cap = $1
    caps = AgentCaps(max_turns=10)
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=caps, cost_tracker=cost, pricing=PRICING,
    )
    # First turn cost = 1M * $15/M = $15 → exceeds $1 cap
    assert cost["cap_hit"] is True
    # Loop should have terminated before a second bedrock call (after the tools execute)
    assert result.cost_cap_hit is True


def test_bedrock_returns_none_terminates():
    bedrock = FakeBedrockClient([None])
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=5), cost_tracker=cost, pricing=PRICING,
    )
    assert result.error is not None
    assert "None" in result.error or "circuit" in result.error.lower() or "transient" in result.error.lower()


def test_multiple_tool_calls_in_one_turn():
    bedrock = FakeBedrockClient([
        _resp(
            [
                {"toolUse": {"toolUseId": "t1", "name": "grep_codebase", "input": {"pattern": "A"}}},
                {"toolUse": {"toolUseId": "t2", "name": "read_file", "input": {"path": "x.scala"}}},
            ],
            stop_reason="tool_use",
        ),
        _resp([{"text": "done"}]),
    ])
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=5), cost_tracker=cost, pricing=PRICING,
    )
    assert len(tr.calls) == 2
    assert result.tool_calls == 2


def test_tool_trace_records_each_call():
    bedrock = FakeBedrockClient([
        _resp(
            [{"toolUse": {"toolUseId": "t1", "name": "grep_codebase", "input": {"pattern": "Foo"}}}],
            stop_reason="tool_use",
        ),
        _resp([{"text": "done"}]),
    ])
    tr = FakeToolRunner("result text")
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="my-agent",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=5), cost_tracker=cost, pricing=PRICING,
    )
    assert len(result.tool_trace) == 1
    entry = result.tool_trace[0]
    assert entry["agent"] == "my-agent"
    assert entry["tool"] == "grep_codebase"
    assert entry["args"] == {"pattern": "Foo"}


def test_token_accounting():
    bedrock = FakeBedrockClient([
        _resp(
            [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
            stop_reason="tool_use",
            usage={"inputTokens": 100, "outputTokens": 50, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
        ),
        _resp(
            [{"text": "done"}],
            usage={"inputTokens": 200, "outputTokens": 30, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
        ),
    ])
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=5), cost_tracker=cost, pricing=PRICING,
    )
    assert result.input_tokens == 300
    assert result.output_tokens == 80


def test_max_turns_recovers_text_from_assistant_message():
    """When max_turns hits and the last assistant turn carried text alongside
    tool_use blocks, that text must be recovered (not replaced by boilerplate).
    """
    # Each turn: assistant emits text + tool_use; user appends toolResult.
    # After max_turns, the last assistant message has both text and tool_use blocks.
    tu_with_text = _resp(
        [
            {"text": "I am still investigating; partial finding C1 is..."},
            {"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}},
        ],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_with_text] * 5)
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=2), cost_tracker=cost, pricing=PRICING,
    )
    assert result.max_turns_reached is True
    # Text must have been recovered, not replaced with boilerplate
    assert "partial finding C1" in result.text
    assert "max_turns" not in result.text


def test_max_turns_falls_back_to_boilerplate_if_no_text():
    """If no assistant message ever carried text, the boilerplate fires."""
    tu_only = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_only] * 5)
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tr,
        caps=AgentCaps(max_turns=2), cost_tracker=cost, pricing=PRICING,
    )
    assert result.max_turns_reached is True
    assert "max_turns" in result.text


def test_shared_cost_tracker_across_agents():
    """Two agent loops share a cost_tracker to enforce a global budget."""
    bedrock = FakeBedrockClient([
        _resp(
            [{"text": "agent 1 done"}],
            usage={"inputTokens": 30_000, "outputTokens": 0, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
        ),
        _resp(
            [{"text": "agent 2 done"}],
            usage={"inputTokens": 30_000, "outputTokens": 0, "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
        ),
    ])
    tr = FakeToolRunner()
    cost = {"cost_usd": 0.0, "cap_usd": 100.0, "cap_hit": False}
    r1 = run(bedrock_client=bedrock, agent_name="a", system_prompt="s", user_prompt="u",
             tool_specs=[], tool_runner=tr, caps=AgentCaps(max_turns=2),
             cost_tracker=cost, pricing=PRICING)
    r2 = run(bedrock_client=bedrock, agent_name="b", system_prompt="s", user_prompt="u",
             tool_specs=[], tool_runner=tr, caps=AgentCaps(max_turns=2),
             cost_tracker=cost, pricing=PRICING)
    assert r1.text == "agent 1 done"
    assert r2.text == "agent 2 done"
    # Cumulative cost should equal sum of both (60K input tokens × $15/M = $0.90)
    assert cost["cost_usd"] == pytest.approx(0.90, abs=0.001)
