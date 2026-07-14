"""Merged runtime event tail: durable replay/poll plus Redis live partials."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import UTC, datetime

from keel_core.events import Event, EventType
from keel_core.state import InMemoryEventStore
from keel_server.runtime import CompositeEventStore


class _QueueFanout:
    def __init__(self) -> None:
        self._events: asyncio.Queue[Event] = asyncio.Queue()
        self.started = asyncio.Event()
        self.closed = asyncio.Event()

    async def append(self, event: Event) -> None:
        await self._events.put(event.model_copy(deep=True))

    def tail(
        self,
        session_id: str,
        after: int | None = None,
        *,
        block_ms: int = 15000,
    ) -> AsyncIterator[Event]:
        del session_id, after, block_ms
        return self._tail()

    async def _tail(self) -> AsyncIterator[Event]:
        self.started.set()
        try:
            while True:
                yield await self._events.get()
        finally:
            self.closed.set()


class _CountingDurableStore(InMemoryEventStore):
    def __init__(self) -> None:
        super().__init__()
        self.read_count = 0

    def read(self, session_id: str, after: int | None = None) -> AsyncIterator[Event]:
        self.read_count += 1
        return super().read(session_id, after)


def _event(text: str, *, partial: bool = False) -> Event:
    return Event(
        type=EventType.message_token,
        seq=0,
        session_id="s1",
        scope_id="web:local",
        ts=datetime.now(UTC),
        payload={"role": "assistant", "text": text, "partial": partial},
    )


async def _close(stream: AsyncIterator[Event], fanout: _QueueFanout) -> None:
    await stream.aclose()  # type: ignore[attr-defined]
    await asyncio.wait_for(fanout.closed.wait(), timeout=0.5)


async def test_tail_polls_durable_events_appended_after_follow_starts() -> None:
    durable = InMemoryEventStore()
    fanout = _QueueFanout()
    store = CompositeEventStore(durable, fanout, poll_interval=0.01)  # type: ignore[arg-type]
    stream = store.tail("s1", 0)
    pending = asyncio.create_task(anext(stream))
    await fanout.started.wait()

    await durable.append(_event("durable only"))

    event = await asyncio.wait_for(pending, timeout=0.5)
    assert (event.seq, event.payload["text"]) == (1, "durable only")
    await _close(stream, fanout)


async def test_tail_suppresses_redis_duplicate_of_durable_event() -> None:
    durable = InMemoryEventStore()
    fanout = _QueueFanout()
    store = CompositeEventStore(durable, fanout, poll_interval=0.01)  # type: ignore[arg-type]
    stream = store.tail("s1", 0)
    first = asyncio.create_task(anext(stream))
    await fanout.started.wait()

    await store.append(_event("complete"))
    assert (await asyncio.wait_for(first, timeout=0.5)).payload["text"] == "complete"

    await fanout.append(_event("live", partial=True))
    second = await asyncio.wait_for(anext(stream), timeout=0.5)
    assert (second.seq, second.payload["text"]) == (0, "live")
    await _close(stream, fanout)


async def test_tail_emits_live_partial_with_zero_sequence() -> None:
    durable = InMemoryEventStore()
    fanout = _QueueFanout()
    store = CompositeEventStore(durable, fanout, poll_interval=0.01)  # type: ignore[arg-type]
    stream = store.tail("s1", 0)
    pending = asyncio.create_task(anext(stream))
    await fanout.started.wait()

    await fanout.append(_event("Hel", partial=True))

    event = await asyncio.wait_for(pending, timeout=0.5)
    assert event.seq == 0
    assert event.payload == {"role": "assistant", "text": "Hel", "partial": True}
    await _close(stream, fanout)


async def test_tail_polls_durable_before_completed_redis_event() -> None:
    durable = InMemoryEventStore()
    fanout = _QueueFanout()
    store = CompositeEventStore(durable, fanout, poll_interval=1.0)  # type: ignore[arg-type]
    stream = store.tail("s1", 0)
    first = asyncio.create_task(anext(stream))
    await fanout.started.wait()

    await durable.append(_event("durable first"))
    await store.append(_event("redis wakeup"))

    events = [
        await asyncio.wait_for(first, timeout=0.5),
        await asyncio.wait_for(anext(stream), timeout=0.5),
    ]
    assert [(event.seq, event.payload["text"]) for event in events] == [
        (1, "durable first"),
        (2, "redis wakeup"),
    ]
    await _close(stream, fanout)


async def test_tail_waits_between_polls_and_cleans_up_cancelled_follow() -> None:
    durable = _CountingDurableStore()
    fanout = _QueueFanout()
    store = CompositeEventStore(durable, fanout, poll_interval=60.0)  # type: ignore[arg-type]
    stream = store.tail("s1", 0)
    pending = asyncio.create_task(anext(stream))
    await fanout.started.wait()

    await asyncio.sleep(0)
    assert durable.read_count == 1

    pending.cancel()
    with suppress(asyncio.CancelledError):
        await pending
    await asyncio.wait_for(fanout.closed.wait(), timeout=0.5)
