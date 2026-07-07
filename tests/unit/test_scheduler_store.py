"""ScheduleStore + due_tick tests: at-most-once enqueue over an in-memory cursor."""

from __future__ import annotations

from datetime import datetime, timedelta

from keel_scheduler.atmostonce import InMemoryClaimStore
from keel_scheduler.store import InMemoryScheduleStore, ScheduleRow, due_tick

_NOW = datetime(2026, 7, 7, 9, 0)


def _row(next_at: datetime) -> ScheduleRow:
    return ScheduleRow(
        id="daily",
        scope_id="u:1",
        agent_id="digest",
        session_id="digest:u:1",
        trigger_kind="interval",
        spec="86400",
        next_run_at=next_at,
        interval_s=86400,
        enabled=True,
    )


async def test_due_tick_enqueues_once_and_advances() -> None:
    store = InMemoryScheduleStore([_row(_NOW)])
    claim = InMemoryClaimStore({"daily": _NOW})
    got: list[str] = []
    ids = await due_tick(schedules=store, claim=claim, now=_NOW, enqueue=got.append)
    assert ids == ["daily"] and got == ["daily"]
    assert claim.snapshot()["daily"] == _NOW + timedelta(seconds=86400)
    # A second tick at the same instant: the cursor already moved -> CAS fails -> no enqueue.
    got.clear()
    assert await due_tick(schedules=store, claim=claim, now=_NOW, enqueue=got.append) == []
    assert got == []


async def test_not_due_does_not_enqueue() -> None:
    later = _NOW + timedelta(hours=1)
    store = InMemoryScheduleStore([_row(later)])
    claim = InMemoryClaimStore({"daily": later})
    got: list[str] = []
    assert await due_tick(schedules=store, claim=claim, now=_NOW, enqueue=got.append) == []
    assert got == []


async def test_disabled_schedule_is_skipped() -> None:
    row = _row(_NOW)
    row.enabled = False
    store = InMemoryScheduleStore([row])
    claim = InMemoryClaimStore({"daily": _NOW})
    got: list[str] = []
    assert await due_tick(schedules=store, claim=claim, now=_NOW, enqueue=got.append) == []
