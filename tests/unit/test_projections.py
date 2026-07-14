"""Projection tests: message threading, tool-call reconstruction, partial skipping."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from keel_core.agents import AgentSpec, Scope
from keel_core.events import Event, EventType
from keel_core.loop import ToolRegistry, _build_request
from keel_core.projections import project_messages
from keel_core.state import InMemoryEventStore
from keel_core.types import ScopeKind


def _event(seq: int, etype: EventType, payload: dict[str, object]) -> Event:
    return Event(
        type=etype,
        seq=seq,
        session_id="s1",
        scope_id="u:1",
        run_id="r1",
        ts=datetime.now(UTC),
        payload=payload,
    )


def test_plain_user_assistant_turns() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "user", "text": "hi"}),
        _event(2, EventType.message_token, {"role": "assistant", "text": "hello"}),
    ]
    assert project_messages(events) == [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello"},
    ]


def test_partial_deltas_are_skipped() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "assistant", "text": "he", "partial": True}),
        _event(2, EventType.message_token, {"role": "assistant", "text": "hello"}),
    ]
    assert project_messages(events) == [{"role": "assistant", "content": "hello"}]


def test_tool_call_thread_is_reconstructed() -> None:
    """A tool-use turn -> assistant message w/ tool_calls, then a linked tool message."""
    events = [
        _event(1, EventType.message_token, {"role": "user", "text": "list files"}),
        _event(
            2,
            EventType.tool_call,
            {"tool": "ls", "call_id": "c1", "args": {"path": "."}},
        ),
        _event(
            3,
            EventType.tool_result,
            {"call_id": "c1", "ok": True, "output": "a.txt\nb.txt"},
        ),
        _event(4, EventType.message_token, {"role": "assistant", "text": "There are 2 files."}),
    ]
    messages = project_messages(events)

    assert messages[0] == {"role": "user", "content": "list files"}
    # Assistant message carrying the tool call (content may be empty).
    assistant = messages[1]
    assert assistant["role"] == "assistant"
    assert assistant["tool_calls"][0]["id"] == "c1"
    assert assistant["tool_calls"][0]["function"]["name"] == "ls"
    assert assistant["tool_calls"][0]["function"]["arguments"] == '{"path": "."}'  # JSON string
    # The tool result is linked back by tool_call_id and follows the assistant msg.
    assert messages[2] == {"role": "tool", "tool_call_id": "c1", "content": "a.txt\nb.txt"}
    # The final assistant summary.
    assert messages[3] == {"role": "assistant", "content": "There are 2 files."}


def test_assistant_text_and_tool_call_merge_in_one_turn() -> None:
    """Assistant text emitted before its tool call belongs to the same message."""
    events = [
        _event(1, EventType.message_token, {"role": "assistant", "text": "Let me check."}),
        _event(2, EventType.tool_call, {"tool": "read", "call_id": "c9", "args": {}}),
        _event(3, EventType.tool_result, {"call_id": "c9", "ok": True, "output": "body"}),
    ]
    messages = project_messages(events)
    assert messages[0]["role"] == "assistant"
    assert messages[0]["content"] == "Let me check."
    assert messages[0]["tool_calls"][0]["id"] == "c9"  # merged, not a separate message
    assert messages[1]["role"] == "tool"


def test_adjacent_plain_assistant_events_coalesce_only_in_projection() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "user", "text": "start"}),
        _event(2, EventType.message_token, {"role": "assistant", "text": "answer"}),
        _event(
            3,
            EventType.message_token,
            {
                "role": "assistant",
                "text": "background result",
                "job_id": "job_1",
                "partial": False,
            },
        ),
        _event(4, EventType.message_token, {"role": "user", "text": "continue"}),
    ]
    assert len(events) == 4
    assert project_messages(events) == [
        {"role": "user", "content": "start"},
        {
            "role": "assistant",
            "content": "answer\n\nbackground result",
        },
        {"role": "user", "content": "continue"},
    ]


def test_plain_assistant_does_not_merge_across_tool_call_or_tool_result() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "assistant", "text": "checking"}),
        _event(2, EventType.tool_call, {"tool": "read", "call_id": "c1", "args": {}}),
        _event(3, EventType.tool_result, {"call_id": "c1", "ok": True, "output": "body"}),
        _event(
            4,
            EventType.message_token,
            {"role": "assistant", "text": "background result"},
        ),
    ]
    messages = project_messages(events)
    assert [message["role"] for message in messages] == ["assistant", "tool", "assistant"]
    assert messages[0]["tool_calls"][0]["id"] == "c1"
    assert messages[2]["content"] == "background result"


def test_job_injection_between_assistant_text_and_tool_call_is_deferred() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "assistant", "text": "checking"}),
        _event(
            2,
            EventType.message_token,
            {"role": "assistant", "text": "background result", "job_id": "job_1"},
        ),
        _event(3, EventType.tool_call, {"tool": "read", "call_id": "c1", "args": {}}),
        _event(4, EventType.tool_result, {"call_id": "c1", "ok": True, "output": "body"}),
    ]

    messages = project_messages(events)
    assert [message["role"] for message in messages] == ["assistant", "tool", "assistant"]
    assert messages[0]["content"] == "checking"
    assert messages[0]["tool_calls"][0]["id"] == "c1"
    assert messages[2]["content"] == "background result"


def test_job_injection_between_tool_call_and_result_is_deferred() -> None:
    events = [
        _event(1, EventType.tool_call, {"tool": "read", "call_id": "c1", "args": {}}),
        _event(
            2,
            EventType.message_token,
            {"role": "assistant", "text": "background result", "job_id": "job_1"},
        ),
        _event(3, EventType.tool_result, {"call_id": "c1", "ok": True, "output": "body"}),
    ]

    messages = project_messages(events)
    assert [message["role"] for message in messages] == ["assistant", "tool", "assistant"]
    assert messages[0]["tool_calls"][0]["id"] == "c1"
    assert messages[1]["tool_call_id"] == "c1"
    assert messages[2]["content"] == "background result"


def test_job_injection_during_run_is_deferred_until_run_thread_completes() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "user", "text": "read"}),
        _event(2, EventType.run_started, {}),
        _event(
            3,
            EventType.message_token,
            {"role": "assistant", "text": "background result", "job_id": "job_1"},
        ),
        _event(4, EventType.tool_call, {"tool": "read", "call_id": "c1", "args": {}}),
        _event(5, EventType.tool_result, {"call_id": "c1", "ok": True, "output": "body"}),
        _event(6, EventType.message_token, {"role": "assistant", "text": "read complete"}),
        _event(7, EventType.run_ended, {"reason": "completed"}),
    ]

    messages = project_messages(events)
    assert [message["role"] for message in messages] == [
        "user",
        "assistant",
        "tool",
        "assistant",
    ]
    assert messages[1]["tool_calls"][0]["id"] == "c1"
    assert messages[3]["content"] == "read complete\n\nbackground result"


def test_job_injection_immediately_before_run_start_cannot_absorb_tool_calls() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "user", "text": "read"}),
        _event(
            2,
            EventType.message_token,
            {"role": "assistant", "text": "background result", "job_id": "job_1"},
        ),
        _event(3, EventType.run_started, {}),
        _event(4, EventType.tool_call, {"tool": "read", "call_id": "c1", "args": {}}),
        _event(5, EventType.tool_result, {"call_id": "c1", "ok": True, "output": "body"}),
        _event(6, EventType.run_ended, {"reason": "completed"}),
    ]

    messages = project_messages(events)
    assert [message["role"] for message in messages] == [
        "user",
        "assistant",
        "tool",
        "assistant",
    ]
    assert messages[1]["tool_calls"][0]["id"] == "c1"
    assert messages[3]["content"] == "background result"


def test_new_run_reconciles_orphaned_active_run_without_losing_job_result() -> None:
    events = [
        _event(1, EventType.run_started, {}),
        _event(
            2,
            EventType.message_token,
            {"role": "assistant", "text": "background result", "job_id": "job_1"},
        ),
        Event(
            type=EventType.run_started,
            seq=3,
            session_id="s1",
            scope_id="u:1",
            run_id="r2",
            ts=datetime.now(UTC),
            payload={},
        ),
        Event(
            type=EventType.message_token,
            seq=4,
            session_id="s1",
            scope_id="u:1",
            run_id="r2",
            ts=datetime.now(UTC),
            payload={"role": "assistant", "text": "second run complete"},
        ),
        Event(
            type=EventType.run_ended,
            seq=5,
            session_id="s1",
            scope_id="u:1",
            run_id="r2",
            ts=datetime.now(UTC),
            payload={"reason": "completed"},
        ),
        _event(6, EventType.message_token, {"role": "user", "text": "continue"}),
    ]

    messages = project_messages(events)
    assert messages == [
        {
            "role": "assistant",
            "content": "second run complete\n\nbackground result",
        },
        {"role": "user", "content": "continue"},
    ]


@pytest.mark.parametrize(
    "model",
    ["openai/gpt-4o-mini", "anthropic/claude-3-5-sonnet-20241022"],
)
async def test_next_provider_request_has_no_adjacent_assistant_roles(model: str) -> None:
    store = InMemoryEventStore()
    for event in [
        _event(1, EventType.message_token, {"role": "user", "text": "start"}),
        _event(2, EventType.message_token, {"role": "assistant", "text": "answer"}),
        _event(
            3,
            EventType.message_token,
            {"role": "assistant", "text": "background result", "job_id": "job_1"},
        ),
        _event(4, EventType.message_token, {"role": "user", "text": "continue"}),
    ]:
        await store.append(event)
    agent = AgentSpec(
        id="projection-test",
        name="Projection Test",
        model=model,
        scope=Scope(id="u:1", kind=ScopeKind.personal),
    )

    request = await _build_request(agent, store, "s1", ToolRegistry())
    assert [message["role"] for message in request.messages] == [
        "user",
        "assistant",
        "user",
    ]
    assert request.messages[1]["content"] == "answer\n\nbackground result"
