"""Persistent schedules + the due-tick glue over the proven at-most-once cursor.

``due_tick`` advances a schedule's cursor with an atomic compare-and-set **before**
enqueuing its run — a crash between advance and enqueue yields 0 or 1 runs, never two
(invariant I9). The claim and enqueue callables may be sync (in-memory tests) or async
(Postgres CAS + arq enqueue); ``due_tick`` awaits either."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


@dataclass
class ScheduleRow:
    id: str
    scope_id: str
    agent_id: str
    session_id: str
    trigger_kind: str  # 'cron' | 'interval' | 'once'
    spec: str
    next_run_at: datetime
    interval_s: int
    enabled: bool = True
    last_run_at: datetime | None = None
    last_status: str | None = None


class ScheduleStore(Protocol):
    async def due(self, now: datetime) -> list[ScheduleRow]: ...

    async def mark_run(self, schedule_id: str, when: datetime, status: str) -> None: ...


class Claimer(Protocol):
    """Atomic cursor advance: move ``next_run_at`` from ``expected`` to ``new``.

    Sync (in-memory) or async (Postgres CAS) — ``due_tick`` awaits either result."""

    def claim(
        self, schedule_id: str, expected: datetime, new: datetime
    ) -> bool | Awaitable[bool]: ...


@dataclass
class InMemoryScheduleStore:
    rows: list[ScheduleRow]

    async def due(self, now: datetime) -> list[ScheduleRow]:
        return [r for r in self.rows if r.enabled and r.next_run_at <= now]

    async def mark_run(self, schedule_id: str, when: datetime, status: str) -> None:
        for r in self.rows:
            if r.id == schedule_id:
                r.next_run_at = when
                r.last_status = status


async def due_tick(
    *,
    schedules: ScheduleStore,
    claim: Claimer,
    now: datetime,
    enqueue: Callable[[str], object],
) -> list[str]:
    """Enqueue every due schedule at most once (advance cursor before enqueue)."""
    enqueued: list[str] = []
    for row in await schedules.due(now):
        new_cursor = row.next_run_at + timedelta(seconds=row.interval_s)
        won = claim.claim(row.id, row.next_run_at, new_cursor)
        if inspect.isawaitable(won):
            won = await won
        if not won:
            continue  # another leader / a prior crashed tick already advanced it
        # Cursor advanced durably; a crash in enqueue leaves it advanced (at most once).
        result = enqueue(row.id)
        if inspect.isawaitable(result):
            await result
        enqueued.append(row.id)
    return enqueued


def _to_schedule_row(row: Any) -> ScheduleRow:
    return ScheduleRow(
        id=row["id"],
        scope_id=row["scope_id"],
        agent_id=row["agent_id"],
        session_id=row["session_id"],
        trigger_kind=row["trigger_kind"],
        spec=row["spec"],
        next_run_at=row["next_run_at"],
        interval_s=row["interval_s"],
        enabled=row["enabled"],
        last_run_at=row["last_run_at"],
        last_status=row["last_status"],
    )


class PostgresScheduleStore:
    """Durable, scope-bound ScheduleStore over Postgres."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def due(self, now: datetime) -> list[ScheduleRow]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM schedules WHERE scope_id = :scope AND enabled = true "
                            "AND next_run_at <= :now ORDER BY next_run_at"
                        ),
                        {"scope": self._scope_id, "now": now},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_schedule_row(r) for r in rows]

    async def mark_run(self, schedule_id: str, when: datetime, status: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "UPDATE schedules SET last_run_at = now(), last_status = :status "
                    "WHERE scope_id = :scope AND id = :id"
                ),
                {"status": status, "scope": self._scope_id, "id": schedule_id},
            )


class PostgresClaimStore:
    """Atomic compare-and-set on ``schedules.next_run_at`` (the at-most-once cursor)."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def claim(self, schedule_id: str, expected: datetime, new: datetime) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE schedules SET next_run_at = :new "
                    "WHERE scope_id = :scope AND id = :id AND next_run_at = :expected"
                ),
                {"new": new, "scope": self._scope_id, "id": schedule_id, "expected": expected},
            )
        return result.rowcount == 1
