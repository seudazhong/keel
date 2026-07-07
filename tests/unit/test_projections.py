"""Projection tests: message threading, tool-call reconstruction, partial skipping."""

from __future__ import annotations

from datetime import UTC, datetime

from keel_core.events import Event, EventType
from keel_core.projections import project_messages


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
