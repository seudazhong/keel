"""Unit tests for in-memory event-store session existence."""

from datetime import UTC, datetime

from keel_core.events import Event, EventType
from keel_core.state import InMemoryEventStore


async def test_in_memory_has_session_requires_matching_scope() -> None:
    store = InMemoryEventStore()
    session_id = "session-1"

    assert store.has_session(session_id, "A") is False
    await store.append(
        Event(
            type=EventType.message_token,
            seq=0,
            session_id=session_id,
            scope_id="A",
            ts=datetime.now(UTC),
            payload={"role": "user", "text": "seed"},
        )
    )

    assert store.has_session(session_id, "A") is True
    assert store.has_session(session_id, "B") is False
    assert store.has_session("missing", "A") is False
