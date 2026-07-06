"""Event fan-out over Redis Streams with replayable reads (spike S4).

Proves replayable fan-out: events are appended to a per-session stream and can be
re-read from any ``after=`` cursor (the SSE/WS replay cursor). Implements the
``EventStore`` seam; :meth:`RedisEventStore.tail` adds live tailing (``XREAD
BLOCK``) so a surface can stream a run to a client (SSE relay, WS).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as redis

from keel_core.events import Event
from keel_core.types import SessionId


class RedisEventStore:
    """Append-only event fan-out backed by one Redis Stream per session."""

    def __init__(self, client: redis.Redis, namespace: str = "events") -> None:
        self._redis = client
        self._namespace = namespace

    def _key(self, session_id: SessionId) -> str:
        return f"{self._namespace}:{session_id}"

    async def append(self, event: Event) -> None:
        """Durably append one event to its session stream."""
        await self._redis.xadd(self._key(event.session_id), {"data": event.model_dump_json()})

    def read(self, session_id: SessionId, after: int | None = None) -> AsyncIterator[Event]:
        """Stream events for a session with ``seq > after`` (replay cursor)."""
        return self._read(session_id, after)

    async def _read(self, session_id: SessionId, after: int | None) -> AsyncIterator[Event]:
        entries: list[tuple[Any, dict[Any, Any]]] = await self._redis.xrange(self._key(session_id))
        for _entry_id, fields in entries:
            event = Event.model_validate_json(fields["data"])
            if after is None or event.seq > after:
                yield event

    async def tail(
        self,
        session_id: SessionId,
        after: int | None = None,
        *,
        block_ms: int = 15000,
    ) -> AsyncIterator[Event]:
        """Yield events with ``seq > after``, then **block** for new ones (live tail).

        Replays the backlog from the ``after`` cursor, then follows the stream with
        ``XREAD BLOCK`` so a surface streams a run in real time. The generator runs
        until the caller stops iterating (client disconnect / a terminal event).
        """
        key = self._key(session_id)
        last_id = "0-0"
        backlog: list[tuple[Any, dict[Any, Any]]] = await self._redis.xrange(key)
        for entry_id, fields in backlog:
            last_id = entry_id
            event = Event.model_validate_json(fields["data"])
            if after is None or event.seq > after:
                yield event

        while True:
            response = await self._redis.xread({key: last_id}, block=block_ms, count=100)
            if not response:
                continue  # block window elapsed with no new events; keep waiting
            for _stream_key, entries in response:
                for entry_id, fields in entries:
                    last_id = entry_id
                    yield Event.model_validate_json(fields["data"])
