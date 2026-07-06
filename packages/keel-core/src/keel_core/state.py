"""State — in-memory event store (WS-D, α).

An append-only :class:`EventStore` backed by a dict, assigning a monotonic
``seq`` per session on append. Used by the loop, unit tests, and the ``lite``
profile; the durable Postgres-backed store + projections land later in M1.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from keel_core.events import Event
from keel_core.types import SessionId


class InMemoryEventStore:
    """Non-durable :class:`~keel_core.protocols.EventStore` implementation."""

    def __init__(self) -> None:
        self._events: dict[str, list[Event]] = {}

    async def append(self, event: Event) -> None:
        bucket = self._events.setdefault(event.session_id, [])
        event.seq = len(bucket) + 1  # monotonic per session (replay cursor)
        bucket.append(event)

    def read(self, session_id: SessionId, after: int | None = None) -> AsyncIterator[Event]:
        return self._read(session_id, after)

    async def _read(self, session_id: SessionId, after: int | None) -> AsyncIterator[Event]:
        for event in self._events.get(session_id, []):
            if after is None or event.seq > after:
                yield event

    def snapshot(self, session_id: SessionId) -> list[Event]:
        """Return a copy of a session's events (test/debug helper)."""
        return list(self._events.get(session_id, []))
