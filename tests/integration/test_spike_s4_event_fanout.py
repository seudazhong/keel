"""Spike S4 acceptance: replayable event fan-out over Redis Streams."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
import redis.asyncio as aioredis

from keel_core.eventbus import RedisEventStore
from keel_core.events import Event, EventType

pytestmark = pytest.mark.integration


def _event(seq: int, session_id: str) -> Event:
    return Event(
        type=EventType.message_token,
        seq=seq,
        session_id=session_id,
        scope_id="sc",
        ts=datetime.now(UTC),
    )


async def test_event_fanout_is_replayable(redis_client: aioredis.Redis) -> None:
    session_id = f"s-{uuid.uuid4().hex}"
    store = RedisEventStore(redis_client, namespace="test-events")
    try:
        for seq in (1, 2, 3):
            await store.append(_event(seq, session_id))

        all_events = [event.seq async for event in store.read(session_id)]
        replayed = [event.seq async for event in store.read(session_id, after=1)]

        assert all_events == [1, 2, 3]
        assert replayed == [2, 3]  # `after=` cursor replays only newer events
    finally:
        await redis_client.delete(f"test-events:{session_id}")
