"""Global run-dispatch outbox for cross-scope reconciliation (M3.6, review finding 4).

The scope-partitioned ``runs`` table is under ``FORCE ROW LEVEL SECURITY``, so a worker that
binds one ``app.scope_id`` can only *see* that scope's runs. To reconcile durable runs across
**every** per-Agent scope from a single worker process — without a per-scope cron and without a
``web:local``-only reconciler — admission records a minimal, non-sensitive dispatch intent in a
**global** index: ``(run_id, scope_id, state, next_attempt_at)`` plus a fenced reconciler lease.

The outbox carries **no** prompt / content / tool / argument payload — only the routing key a
worker needs to discover which scopes currently have open work. The scope-bound authority (the
run row, events, approvals) stays behind RLS; the outbox is only a dispatch pointer, so it is
safe for the non-owner runtime role to read/lease/delete (see migration
``0016_web_routing_isolation``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.types import RunId, ScopeId


def _now() -> datetime:
    return datetime.now(UTC)


# The single INSERT that records (idempotently, on ``run_id``) an open dispatch intent. Shared
# between the standalone outbox (:meth:`PostgresRunDispatchOutbox.record`) and the atomic
# queued-transition path (:meth:`keel_core.runs.PostgresRunStore.mark_queued`), so the intent
# can be written **in the same transaction** as the run's ``admitted -> queued`` transition —
# the run never reaches ``queued`` in the durable store without a discoverable dispatch intent
# (M3.6 finding 4). ON CONFLICT keeps a repair/retry idempotent.
_INSERT_INTENT = text(
    "INSERT INTO run_dispatch_outbox "
    "(run_id, scope_id, state, attempts, next_attempt_at, created_at, updated_at) "
    "VALUES (:run_id, :scope_id, 'pending', 0, :now, :now, :now) "
    "ON CONFLICT (run_id) DO NOTHING"
)


async def record_intent_in_connection(
    conn: AsyncConnection, run_id: RunId, scope_id: ScopeId, *, now: datetime | None = None
) -> None:
    """Record a dispatch intent inside the caller's transaction (no commit here).

    Lets the run store persist the intent atomically with the ``queued`` transition so a
    committed ``queued`` run is always accompanied by its cross-scope dispatch pointer: if the
    surrounding transaction rolls back, neither the transition nor the intent survives, so there
    is never a queued-but-undiscoverable run.
    """
    now = now or _now()
    await conn.execute(_INSERT_INTENT, {"run_id": run_id, "scope_id": scope_id, "now": now})


@dataclass(frozen=True)
class DispatchIntent:
    """A single open dispatch intent: which run, in which scope, needs a worker's attention."""

    run_id: RunId
    scope_id: ScopeId
    attempts: int = 0


class RunDispatchOutbox(Protocol):
    """The global dispatch index a worker scans to reconcile runs across all scopes."""

    async def record(
        self, run_id: RunId, scope_id: ScopeId, *, now: datetime | None = None
    ) -> None:
        """Record (idempotently) that ``run_id`` in ``scope_id`` has an open dispatch intent."""

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[DispatchIntent]:
        """Lease a batch of due intents (pending or lease-expired) for this worker."""

    async def reschedule(
        self, run_id: RunId, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None:
        """Release the lease and defer the next attempt (a still-active run to re-check)."""

    async def remove(self, run_id: RunId) -> None:
        """Delete a terminal run's intent (nothing left to dispatch)."""

    async def active_scopes(self) -> set[ScopeId]:
        """All scopes with at least one open intent (diagnostics/tests)."""


class InMemoryRunDispatchOutbox:
    """Process-local dispatch outbox double for unit tests (mirrors the Postgres semantics)."""

    @dataclass
    class _Row:
        scope_id: ScopeId
        attempts: int
        next_attempt_at: datetime
        lease_owner: str | None
        lease_expires_at: datetime | None

    def __init__(self) -> None:
        self._intents: dict[RunId, InMemoryRunDispatchOutbox._Row] = {}

    async def record(
        self, run_id: RunId, scope_id: ScopeId, *, now: datetime | None = None
    ) -> None:
        now = now or _now()
        if run_id not in self._intents:
            self._intents[run_id] = self._Row(
                scope_id=scope_id,
                attempts=0,
                next_attempt_at=now,
                lease_owner=None,
                lease_expires_at=None,
            )

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[DispatchIntent]:
        now = now or _now()
        claimed: list[DispatchIntent] = []
        for run_id, row in sorted(self._intents.items(), key=lambda kv: kv[1].next_attempt_at):
            if len(claimed) >= limit:
                break
            due = row.next_attempt_at <= now
            lease_free = row.lease_expires_at is None or row.lease_expires_at <= now
            if not (due and lease_free):
                continue
            row.lease_owner = worker_id
            row.lease_expires_at = now + timedelta(seconds=lease_seconds)
            row.attempts += 1
            claimed.append(
                DispatchIntent(run_id=run_id, scope_id=row.scope_id, attempts=row.attempts)
            )
        return claimed

    async def reschedule(
        self, run_id: RunId, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None:
        now = now or _now()
        row = self._intents.get(run_id)
        if row is not None:
            row.lease_owner = None
            row.lease_expires_at = None
            row.next_attempt_at = now + timedelta(seconds=delay_seconds)

    async def remove(self, run_id: RunId) -> None:
        self._intents.pop(run_id, None)

    async def active_scopes(self) -> set[ScopeId]:
        return {row.scope_id for row in self._intents.values()}


class PostgresRunDispatchOutbox:
    """Durable global dispatch outbox over Postgres (no RLS — the cross-scope dispatch index)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(
        self, run_id: RunId, scope_id: ScopeId, *, now: datetime | None = None
    ) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            await record_intent_in_connection(conn, run_id, scope_id, now=now)

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[DispatchIntent]:
        now = now or _now()
        lease_until = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(
                    text(
                        "UPDATE run_dispatch_outbox SET "
                        "  state = 'leased', lease_owner = :worker, lease_expires_at = :lease, "
                        "  attempts = attempts + 1, updated_at = :now "
                        "WHERE run_id IN ("
                        "  SELECT run_id FROM run_dispatch_outbox "
                        "  WHERE next_attempt_at <= :now "
                        "    AND (lease_expires_at IS NULL OR lease_expires_at <= :now) "
                        "  ORDER BY next_attempt_at "
                        "  FOR UPDATE SKIP LOCKED "
                        "  LIMIT :limit"
                        ") "
                        "RETURNING run_id, scope_id, attempts"
                    ),
                    {
                        "worker": worker_id,
                        "lease": lease_until,
                        "now": now,
                        "limit": limit,
                    },
                )
            ).all()
        return [
            DispatchIntent(run_id=row.run_id, scope_id=row.scope_id, attempts=row.attempts)
            for row in rows
        ]

    async def reschedule(
        self, run_id: RunId, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE run_dispatch_outbox SET "
                    "  state = 'pending', lease_owner = NULL, lease_expires_at = NULL, "
                    "  next_attempt_at = :next_at, updated_at = :now "
                    "WHERE run_id = :run_id"
                ),
                {
                    "run_id": run_id,
                    "next_at": now + timedelta(seconds=delay_seconds),
                    "now": now,
                },
            )

    async def remove(self, run_id: RunId) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM run_dispatch_outbox WHERE run_id = :run_id"),
                {"run_id": run_id},
            )

    async def active_scopes(self) -> set[ScopeId]:
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(text("SELECT DISTINCT scope_id FROM run_dispatch_outbox"))
            ).all()
        return {row.scope_id for row in rows}


__all__ = [
    "DispatchIntent",
    "InMemoryRunDispatchOutbox",
    "PostgresRunDispatchOutbox",
    "RunDispatchOutbox",
    "record_intent_in_connection",
]
