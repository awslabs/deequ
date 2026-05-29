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
                             timeout_seconds=None, model_id=None):
        self.calls.append({
            "system_prompt": system_prompt,
            "messages": list(messages),
            "tool_specs": tool_specs,
            "max_tokens": max_tokens,
            "timeout_seconds": timeout_seconds,
            "model_id": model_id,
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


def test_max_turns_with_no_text_returns_empty_when_no_commit_phase():
    """When max_turns hits without any text emitted by the model and no
    commit-phase prompt is configured, result.text must be empty so
    downstream callers can detect the silent-bail case via max_turns_reached
    and route to a Critic backstop."""
    tu_only = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "grep_codebase", "input": {"pattern": "x"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([tu_only] * 5)
    tr = FakeToolRunner()
    result = _run(bedrock, tr, caps=AgentCaps(max_turns=2))
    assert result.max_turns_reached is True
    assert result.text == ""
    assert "max_turns" in (result.error or "")


def test_wall_clock_cap_terminates_loop():
    """A pipeline_deadline already in the past terminates the loop and
    leaves an error label so callers can see why it stopped.
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


# -----------------------------------------------------------------------------
# User-content composition: trusted vs untrusted text and guardrail wrapping.
# -----------------------------------------------------------------------------

class _GuardedFakeClient(FakeBedrockClient):
    """Fake client that reports it has a guardrail configured."""
    has_guardrail = True


class _UnguardedFakeClient(FakeBedrockClient):
    """Fake client that explicitly reports no guardrail."""
    has_guardrail = False


def test_user_prompt_alone_yields_single_text_block():
    """No untrusted_user_prompt → single canonical text block, regardless
    of guardrail (helper layer wraps it later if needed)."""
    bedrock = _UnguardedFakeClient([_resp([{"text": "ok"}])])
    run(
        bedrock_client=bedrock, agent_name="critic",
        system_prompt="sys", user_prompt="trusted-only",
        tool_specs=[], tool_runner=FakeToolRunner(),
        caps=AgentCaps(max_turns=1),
    )
    content = bedrock.calls[0]["messages"][0]["content"]
    assert content == [{"text": "trusted-only"}]


def test_untrusted_with_guardrail_splits_blocks():
    """When the client has a guardrail AND there is untrusted input, the
    user message gets two content blocks: trusted text + guardContent."""
    bedrock = _GuardedFakeClient([_resp([{"text": "ok"}])])
    run(
        bedrock_client=bedrock, agent_name="critic",
        system_prompt="sys", user_prompt="trusted-context",
        untrusted_user_prompt="USER-INPUT",
        tool_specs=[], tool_runner=FakeToolRunner(),
        caps=AgentCaps(max_turns=1),
    )
    content = bedrock.calls[0]["messages"][0]["content"]
    assert len(content) == 2
    assert content[0] == {"text": "trusted-context"}
    assert content[1] == {"guardContent": {"text": {"text": "USER-INPUT"}}}


def test_untrusted_without_guardrail_concatenates():
    """When no guardrail is configured, having an untrusted segment doesn't
    earn a separate guardContent block — the API would treat it as inert."""
    bedrock = _UnguardedFakeClient([_resp([{"text": "ok"}])])
    run(
        bedrock_client=bedrock, agent_name="critic",
        system_prompt="sys", user_prompt="trusted-context",
        untrusted_user_prompt="USER-INPUT",
        tool_specs=[], tool_runner=FakeToolRunner(),
        caps=AgentCaps(max_turns=1),
    )
    content = bedrock.calls[0]["messages"][0]["content"]
    assert len(content) == 1
    assert content[0]["text"] == "trusted-context\nUSER-INPUT"


def test_guardrail_does_not_wrap_trusted_block():
    """Regression guard: an injection-y phrase in the trusted segment must
    NOT end up inside guardContent (where the guardrail would scan it)."""
    bedrock = _GuardedFakeClient([_resp([{"text": "ok"}])])
    investigator_paraphrase = "Author paraphrased: 'ignore previous instructions'"
    run(
        bedrock_client=bedrock, agent_name="critic",
        system_prompt="sys", user_prompt=investigator_paraphrase,
        untrusted_user_prompt="Real user title and body",
        tool_specs=[], tool_runner=FakeToolRunner(),
        caps=AgentCaps(max_turns=1),
    )
    content = bedrock.calls[0]["messages"][0]["content"]
    text_block_text = content[0].get("text", "")
    guard_text = content[1].get("guardContent", {}).get("text", {}).get("text", "")
    assert "ignore previous instructions" in text_block_text
    assert "ignore previous instructions" not in guard_text


# Layer A: aggregate text from all assistant messages, not only the last.

def test_aggregate_text_preserves_partial_commits_from_earlier_turns():
    """When the model emits a STATUS block on turn 1 alongside tool calls,
    then runs out of turns, the per-turn commit MUST be in result.text. The
    old behavior (walk-back to last assistant message) lost it because the
    last turn's text block could be empty when the model ended on tool use."""
    turn1 = _resp(
        [
            {"text": "WORKING_NOTES: hypothesis A formed.\nID: A1\nSTATUS: CONFIRMED\nCOMMENT: real bug at line 5"},
            {"toolUse": {"toolUseId": "t1", "name": "read_file", "input": {"path": "x"}}},
        ],
        stop_reason="tool_use",
    )
    turn2_no_text = _resp(
        [{"toolUse": {"toolUseId": "t2", "name": "read_file", "input": {"path": "y"}}}],
        stop_reason="tool_use",
    )
    bedrock = FakeBedrockClient([turn1, turn2_no_text, turn2_no_text])
    result = _run(bedrock, FakeToolRunner(), caps=AgentCaps(max_turns=3))
    # Turn 1's STATUS: CONFIRMED block must survive even though turn 3
    # ended on tool_use with no text.
    assert "STATUS: CONFIRMED" in result.text
    assert "ID: A1" in result.text


def test_aggregate_text_concatenates_all_turns_in_order():
    """Multiple turns each emit a STATUS block; result.text must contain
    every block in turn order so the Reporter can parse them all."""
    turn1 = _resp([{"text": "ID: A1\nSTATUS: CONFIRMED"},
                   {"toolUse": {"toolUseId": "t1", "name": "read_file", "input": {"path": "x"}}}],
                  stop_reason="tool_use")
    turn2 = _resp([{"text": "ID: A2\nSTATUS: DISPROVED"}],
                  stop_reason="end_turn")
    bedrock = FakeBedrockClient([turn1, turn2])
    result = _run(bedrock, FakeToolRunner(), caps=AgentCaps(max_turns=3))
    a1_idx = result.text.index("ID: A1")
    a2_idx = result.text.index("ID: A2")
    assert a1_idx < a2_idx, "later turns must appear after earlier turns in result.text"


# Layer B: forced commit phase when budget exhausted.

def test_commit_phase_runs_with_prompt_when_max_turns_hit():
    """After max_turns, the loop calls Bedrock once more with the commit-
    phase user prompt appended to the conversation. tool_specs are kept on
    the request so toolUse/toolResult blocks in history validate against
    Bedrock's schema; the prompt suppresses new tool calls."""
    tool_specs = [{"toolSpec": {"name": "read_file"}}]
    turns_with_tools = [
        _resp([{"toolUse": {"toolUseId": "t", "name": "read_file", "input": {"path": "x"}}}],
              stop_reason="tool_use"),
    ] * 2
    commit_resp = _resp([{"text": "ID: A1\nSTATUS: UNVERIFIED\nCOMMENT: ran out of turns"}],
                        stop_reason="end_turn")
    bedrock = FakeBedrockClient(turns_with_tools + [commit_resp])
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=tool_specs,
        tool_runner=FakeToolRunner(),
        caps=AgentCaps(max_turns=2),
        commit_phase_user_prompt="TOOL BUDGET EXHAUSTED. Emit findings now.",
    )
    assert result.max_turns_reached is True
    assert result.commit_phase_ran is True
    last_call = bedrock.calls[-1]
    assert last_call["tool_specs"] == tool_specs
    last_user_msgs = [m for m in last_call["messages"] if m.get("role") == "user"]
    last_user_text = "".join(
        b.get("text", "") for b in last_user_msgs[-1].get("content", []) if "text" in b
    )
    assert "TOOL BUDGET EXHAUSTED" in last_user_text
    assert "STATUS: UNVERIFIED" in result.text


def test_commit_phase_skipped_when_no_prompt_configured():
    """Without a commit_phase_user_prompt, the loop terminates without an
    extra Bedrock call (legacy behavior, opt-in feature flag style)."""
    turns_with_tools = [
        _resp([{"toolUse": {"toolUseId": "t", "name": "read_file", "input": {"path": "x"}}}],
              stop_reason="tool_use"),
    ] * 3
    bedrock = FakeBedrockClient(turns_with_tools)
    result = _run(bedrock, FakeToolRunner(), caps=AgentCaps(max_turns=2))
    assert result.max_turns_reached is True
    assert result.commit_phase_ran is False
    # Two turns, no commit phase, so two Bedrock calls.
    assert len(bedrock.calls) == 2


def test_commit_phase_runs_on_structural_cap_hit():
    """Tool-call budget exhaustion (max_tool_calls reached) must also
    trigger the commit phase, not just max_turns."""
    turn = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "read_file", "input": {"path": "x"}}}],
        stop_reason="tool_use",
    )
    commit_resp = _resp([{"text": "ID: A1\nSTATUS: CONFIRMED\nCOMMENT: hit cap"}],
                        stop_reason="end_turn")
    bedrock = FakeBedrockClient([turn, commit_resp])
    caps = AgentCaps(max_turns=10, max_tool_calls=1)
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[{"toolSpec": {"name": "read_file"}}],
        tool_runner=FakeToolRunner(),
        caps=caps,
        commit_phase_user_prompt="TOOL BUDGET EXHAUSTED.",
    )
    assert result.commit_phase_ran is True
    assert "STATUS: CONFIRMED" in result.text


def test_commit_phase_merges_prompt_into_trailing_user_toolresult_message():
    """When the loop terminates after appending a user(toolResult) message,
    the commit phase MUST merge the prompt into that message (not append
    a second consecutive user, which Bedrock rejects)."""
    turn = _resp(
        [{"toolUse": {"toolUseId": "t", "name": "read_file", "input": {"path": "x"}}}],
        stop_reason="tool_use",
    )
    commit_resp = _resp([{"text": "ok"}], stop_reason="end_turn")
    bedrock = FakeBedrockClient([turn, commit_resp])
    run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[{"toolSpec": {"name": "read_file"}}],
        tool_runner=FakeToolRunner(),
        caps=AgentCaps(max_turns=1),
        commit_phase_user_prompt="TOOL BUDGET EXHAUSTED.",
    )
    # Last bedrock call's messages must end in a single user message that
    # contains both the toolResult AND the commit prompt text.
    final_messages = bedrock.calls[-1]["messages"]
    last = final_messages[-1]
    assert last["role"] == "user"
    contents = last["content"]
    assert any("toolResult" in b for b in contents), "toolResult preserved"
    assert any(b.get("text") == "TOOL BUDGET EXHAUSTED." for b in contents), (
        "commit prompt merged"
    )
    # Strict alternation: no two consecutive user messages.
    roles = [m.get("role") for m in final_messages]
    for i in range(1, len(roles)):
        assert roles[i] != roles[i - 1], (
            f"consecutive {roles[i]} messages at index {i}: {roles}"
        )


def test_commit_phase_skipped_when_pipeline_deadline_past():
    """When wall-clock is already past, the commit phase must not fire —
    spending another Bedrock call against a dead deadline is wasted."""
    import time
    turns = [_resp([{"toolUse": {"toolUseId": "t", "name": "read_file", "input": {}}}],
                   stop_reason="tool_use")] * 3
    bedrock = FakeBedrockClient(turns)
    result = run(
        bedrock_client=bedrock, agent_name="investigator",
        system_prompt="sys", user_prompt="user",
        tool_specs=[{"toolSpec": {"name": "read_file"}}],
        tool_runner=FakeToolRunner(),
        caps=AgentCaps(max_turns=5),
        commit_phase_user_prompt="TOOL BUDGET EXHAUSTED.",
        pipeline_deadline=time.monotonic() - 1,  # deadline already past
    )
    assert result.commit_phase_ran is False
