"""Global durable-job dispatch outbox for cross-scope reconciliation (M3.6, review finding 3).

The scope-partitioned ``jobs`` table is under row-level security keyed by ``app.scope_id``, so a
worker that binds one scope can only *see* that scope's jobs. To dispatch durable jobs (Knowledge
ingest/delete) across **every** per-Agent scope from a single worker process — without a per-scope
cron and without a ``web:local``-pinned dispatcher — Knowledge job admission records a minimal,
non-sensitive dispatch intent in a **global** index: ``(job_id, scope_id, kind, state,
next_attempt_at)`` plus a fenced reconciler lease.

The outbox carries **no** document content / payload / prompt / embedding — only the routing key a
worker needs to discover which scopes currently have dispatchable jobs. The scope-bound authority
(the job row + its payload) stays behind RLS; the outbox is only a dispatch pointer, so it is safe
for the non-owner runtime role to read/lease/delete (see migration ``0017_web_routing_isolation``).

This mirrors :mod:`keel_core.run_dispatch` for durable jobs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.types import ScopeId


def _now() -> datetime:
    return datetime.now(UTC)


# The single INSERT that records (idempotently, on ``job_id``) an open dispatch intent. Shared
# between the standalone outbox (:meth:`PostgresJobDispatchOutbox.record`) and the atomic
# enqueue path (:meth:`keel_core.jobs.PostgresJobStore.enqueue_once_with_dispatch_intent`), so the
# intent can be written **in the same transaction** as the job insert — a committed durable job
# always has a discoverable dispatch intent (finding 3). ON CONFLICT keeps a repair/retry
# idempotent (and refreshes the routing ``kind`` in case a prior partial write differed).
_INSERT_INTENT = text(
    "INSERT INTO job_dispatch_outbox "
    "(job_id, scope_id, kind, state, attempts, next_attempt_at, created_at, updated_at) "
    "VALUES (:job_id, :scope_id, :kind, 'pending', 0, :now, :now, :now) "
    "ON CONFLICT (job_id) DO NOTHING"
)


async def record_job_intent_in_connection(
    conn: AsyncConnection,
    job_id: str,
    scope_id: ScopeId,
    kind: str,
    *,
    now: datetime | None = None,
) -> None:
    """Record a dispatch intent inside the caller's transaction (no commit here).

    Lets the job store persist the intent atomically with the job insert so a committed durable
    job is always accompanied by its cross-scope dispatch pointer: if the surrounding transaction
    rolls back, neither the job nor the intent survives, so there is never a queued-but-
    undiscoverable job.
    """
    now = now or _now()
    await conn.execute(
        _INSERT_INTENT,
        {"job_id": job_id, "scope_id": scope_id, "kind": kind, "now": now},
    )


@dataclass(frozen=True)
class JobDispatchIntent:
    """A single open dispatch intent: which job, in which scope, of which kind, needs dispatch."""

    job_id: str
    scope_id: ScopeId
    kind: str
    attempts: int = 0


class JobDispatchOutbox(Protocol):
    """The global dispatch index a worker scans to dispatch durable jobs across all scopes."""

    async def record(
        self, job_id: str, scope_id: ScopeId, kind: str, *, now: datetime | None = None
    ) -> None:
        """Record (idempotently) that ``job_id`` in ``scope_id`` has an open dispatch intent."""

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[JobDispatchIntent]:
        """Lease a batch of due intents (pending or lease-expired) for this worker."""

    async def reschedule(
        self, job_id: str, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None:
        """Release the lease and defer the next attempt (a still-active job to re-dispatch)."""

    async def remove(self, job_id: str) -> None:
        """Delete a terminal job's intent (nothing left to dispatch)."""

    async def active_scopes(self) -> set[ScopeId]:
        """All scopes with at least one open intent (diagnostics/tests)."""


class InMemoryJobDispatchOutbox:
    """Process-local dispatch outbox double for unit tests (mirrors the Postgres semantics)."""

    @dataclass
    class _Row:
        scope_id: ScopeId
        kind: str
        attempts: int
        next_attempt_at: datetime
        lease_owner: str | None
        lease_expires_at: datetime | None

    def __init__(self) -> None:
        self._intents: dict[str, InMemoryJobDispatchOutbox._Row] = {}

    async def record(
        self, job_id: str, scope_id: ScopeId, kind: str, *, now: datetime | None = None
    ) -> None:
        now = now or _now()
        if job_id not in self._intents:
            self._intents[job_id] = self._Row(
                scope_id=scope_id,
                kind=kind,
                attempts=0,
                next_attempt_at=now,
                lease_owner=None,
                lease_expires_at=None,
            )

    async def record_in_connection(
        self,
        _conn: object,
        job_id: str,
        scope_id: ScopeId,
        kind: str,
        *,
        now: datetime | None = None,
    ) -> None:
        """In-memory analogue of the atomic intent write (no real transaction to join)."""
        await self.record(job_id, scope_id, kind, now=now)

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[JobDispatchIntent]:
        now = now or _now()
        claimed: list[JobDispatchIntent] = []
        for job_id, row in sorted(self._intents.items(), key=lambda kv: kv[1].next_attempt_at):
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
                JobDispatchIntent(
                    job_id=job_id,
                    scope_id=row.scope_id,
                    kind=row.kind,
                    attempts=row.attempts,
                )
            )
        return claimed

    async def reschedule(
        self, job_id: str, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None:
        now = now or _now()
        row = self._intents.get(job_id)
        if row is not None:
            row.lease_owner = None
            row.lease_expires_at = None
            row.next_attempt_at = now + timedelta(seconds=delay_seconds)

    async def remove(self, job_id: str) -> None:
        self._intents.pop(job_id, None)

    async def active_scopes(self) -> set[ScopeId]:
        return {row.scope_id for row in self._intents.values()}


class PostgresJobDispatchOutbox:
    """Durable global dispatch outbox over Postgres (no RLS — the cross-scope dispatch index)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(
        self, job_id: str, scope_id: ScopeId, kind: str, *, now: datetime | None = None
    ) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            await record_job_intent_in_connection(conn, job_id, scope_id, kind, now=now)

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[JobDispatchIntent]:
        now = now or _now()
        lease_until = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(
                    text(
                        "UPDATE job_dispatch_outbox SET "
                        "  state = 'leased', lease_owner = :worker, lease_expires_at = :lease, "
                        "  attempts = attempts + 1, updated_at = :now "
                        "WHERE job_id IN ("
                        "  SELECT job_id FROM job_dispatch_outbox "
                        "  WHERE next_attempt_at <= :now "
                        "    AND (lease_expires_at IS NULL OR lease_expires_at <= :now) "
                        "  ORDER BY next_attempt_at "
                        "  FOR UPDATE SKIP LOCKED "
                        "  LIMIT :limit"
                        ") "
                        "RETURNING job_id, scope_id, kind, attempts"
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
            JobDispatchIntent(
                job_id=row.job_id,
                scope_id=row.scope_id,
                kind=row.kind,
                attempts=row.attempts,
            )
            for row in rows
        ]

    async def reschedule(
        self, job_id: str, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE job_dispatch_outbox SET "
                    "  state = 'pending', lease_owner = NULL, lease_expires_at = NULL, "
                    "  next_attempt_at = :next_at, updated_at = :now "
                    "WHERE job_id = :job_id"
                ),
                {
                    "job_id": job_id,
                    "next_at": now + timedelta(seconds=delay_seconds),
                    "now": now,
                },
            )

    async def remove(self, job_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM job_dispatch_outbox WHERE job_id = :job_id"),
                {"job_id": job_id},
            )

    async def active_scopes(self) -> set[ScopeId]:
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(text("SELECT DISTINCT scope_id FROM job_dispatch_outbox"))
            ).all()
        return {row.scope_id for row in rows}


__all__ = [
    "InMemoryJobDispatchOutbox",
    "JobDispatchIntent",
    "JobDispatchOutbox",
    "PostgresJobDispatchOutbox",
    "record_job_intent_in_connection",
]
