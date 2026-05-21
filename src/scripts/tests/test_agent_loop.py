"""Tests for issue_bot.agent_loop — the multi-turn tool-use loop."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from issue_bot.agent_loop import run, AgentCaps


class FakeBedrockClient:
    """Records calls; returns canned responses in sequence."""
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def converse_with_tools(self, system_prompt, messages, tool_specs,
                             max_tokens=8000, temperature=0.3,
                             timeout_seconds=None):
        self.calls.append({
            "system_prompt": system_prompt,
            "messages": list(messages),
            "tool_specs": tool_specs,
            "max_tokens": max_tokens,
            "timeout_seconds": timeout_seconds,
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
        "usage": usage or {
            "inputTokens": 100, "outputTokens": 50,
            "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0,
        },
    }


def _run(bedrock, tool_runner, caps=None, agent_name="investigator"):
    """Helper to invoke the loop with default params for tests."""
    return run(
        bedrock_client=bedrock, agent_name=agent_name,
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=tool_runner,
        caps=caps or AgentCaps(max_turns=5),
    )


def test_terminates_when_model_emits_text():
    bedrock = FakeBedrockClient([_resp([{"text": "investigation done"}])])
    tr = FakeToolRunner()
    result = _run(bedrock, tr)
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
    result = _run(bedrock, tr)
    assert result.text == "found Foo, done"
    assert result.turns == 2
    assert result.tool_calls == 1
    assert tr.calls[0]["name"] == "grep_codebase"


def test_hits_max_turns():
    tu_resp = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_resp] * 10)
    tr = FakeToolRunner()
    result = _run(bedrock, tr, caps=AgentCaps(max_turns=3))
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
    caps = AgentCaps(max_turns=5, per_tool_max_calls={"grep_codebase": 1})
    result = _run(bedrock, tr, caps=caps)
    # Second grep hits the per-tool budget; the runner is only called once
    # because the second is short-circuited to a budget message.
    assert len(tr.calls) == 1
    assert result.text == "done"


def test_total_tool_call_budget():
    tu_resp = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_resp, tu_resp, tu_resp, _resp([{"text": "done"}])])
    tr = FakeToolRunner()
    caps = AgentCaps(max_turns=10, max_tool_calls=2)
    _run(bedrock, tr, caps=caps)
    assert len(tr.calls) == 2


def test_bedrock_returns_none_terminates():
    bedrock = FakeBedrockClient([None])
    tr = FakeToolRunner()
    result = _run(bedrock, tr)
    assert result.error is not None
    assert "None" in result.error or "transient" in result.error.lower()
    assert result.max_turns_reached is False


def test_bedrock_exception_terminates():
    class ExplodingClient:
        def converse_with_tools(self, **_):
            raise RuntimeError("boom")
    tr = FakeToolRunner()
    result = _run(ExplodingClient(), tr)
    assert result.error is not None
    assert "RuntimeError" in result.error
    assert result.max_turns_reached is False


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
    result = _run(bedrock, tr)
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
    result = _run(bedrock, tr, agent_name="my-agent")
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
            usage={"inputTokens": 100, "outputTokens": 50,
                   "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
        ),
        _resp(
            [{"text": "done"}],
            usage={"inputTokens": 200, "outputTokens": 30,
                   "cacheReadInputTokens": 0, "cacheWriteInputTokens": 0},
        ),
    ])
    tr = FakeToolRunner()
    result = _run(bedrock, tr)
    assert result.input_tokens == 300
    assert result.output_tokens == 80


def test_max_turns_recovers_text_from_assistant_message():
    """When max_turns hits and the last assistant turn carried text alongside
    tool_use blocks, that text must be recovered (not replaced by boilerplate).
    """
    tu_with_text = _resp(
        [
            {"text": "I am still investigating; partial finding C1 is..."},
            {"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}},
        ],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_with_text] * 5)
    tr = FakeToolRunner()
    result = _run(bedrock, tr, caps=AgentCaps(max_turns=2))
    assert result.max_turns_reached is True
    assert "partial finding C1" in result.text
    assert "max_turns" not in result.text


def test_max_turns_falls_back_to_boilerplate_if_no_text():
    tu_only = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_only] * 5)
    tr = FakeToolRunner()
    result = _run(bedrock, tr, caps=AgentCaps(max_turns=2))
    assert result.max_turns_reached is True
    assert "max_turns" in result.text


def test_fatal_flag_only_set_on_fatal_errors():
    """Bedrock errors / empty responses are fatal; structural caps are not.
    The pipeline relies on this distinction to decide between escalating
    and accepting partial output.
    """
    # Fatal: bedrock returns None
    bedrock_none = FakeBedrockClient([None])
    r1 = _run(bedrock_none, FakeToolRunner())
    assert r1.error is not None
    assert r1.fatal is True

    # Non-fatal: hits max_turns with valid text
    tu_with_text = _resp(
        [
            {"text": "VERDICT: C1 | UPHELD | ok"},
            {"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}},
        ],
        stop_reason="tool_use",
    )
    bedrock_loop = FakeBedrockClient([tu_with_text] * 5)
    r2 = _run(bedrock_loop, FakeToolRunner(), caps=AgentCaps(max_turns=2))
    assert r2.max_turns_reached is True
    assert r2.fatal is False
    assert "VERDICT" in r2.text


def test_wall_clock_cap_terminates_loop():
    """A pipeline_deadline that has already passed terminates immediately
    and is NOT marked fatal — partial findings should still flow downstream.
    """
    import time as _time
    tu = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu] * 5)
    result = run(
        bedrock_client=bedrock, agent_name="critic",
        system_prompt="sys", user_prompt="user",
        tool_specs=[], tool_runner=FakeToolRunner(),
        caps=AgentCaps(max_turns=10),
        pipeline_deadline=_time.monotonic() - 1,  # already in the past
    )
    assert result.error and "wall-clock" in result.error
    assert result.fatal is False
