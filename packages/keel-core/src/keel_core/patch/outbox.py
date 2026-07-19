"""Global patch-proposal dispatch outbox (non-RLS pointer/index) — M4 (WS-PP, P1).

The org-partitioned ``patch_proposals`` table is under ``FORCE ROW LEVEL SECURITY`` keyed by
``app.org_id``, so a worker bound to one org can only *see* that org's proposals. To reconcile
controlled-patch work (drive generation, then trusted writeback after a human approval) across
**every** org from a single worker process — without a per-org cron and without a privileged
``BYPASSRLS`` scan — proposal admission records a minimal, non-sensitive dispatch intent in a
**global** index: :class:`PatchProposalOutbox` (table ``patch_proposal_outbox``, migration
``0021_patch_proposal_outbox``).

The pointer carries **only** the routing keys a worker needs to discover which proposals have open
background work — ``proposal_id``, the owning ``org_id`` + validated per-Agent ``scope_id`` (so the
worker can re-enter that scope's RLS context), a coarse ``status_hint`` (``generating`` -> ``ready``
-> ``approved``, *not* the authoritative proposal status), an optional ``job_id`` back-reference and
a fenced reconciler lease. It carries **no** diff, task text, bundle bytes, token, or any other
sensitive payload; the content-addressed bundle and the tainted task digest stay behind RLS on
``patch_proposals``. So, exactly like the M3.6 dispatch outboxes, it is safe for the non-owner
runtime role to read/lease/update/delete even though it is deliberately not under RLS.

Fencing: unlike the M3.6 run/job dispatch outboxes (owner-only ``lease_expires_at``), every claimed
pointer is stamped with a **random** ``lease_token``; :meth:`~PatchProposalOutbox.reschedule`,
:meth:`~PatchProposalOutbox.complete`, :meth:`~PatchProposalOutbox.remove` and
:meth:`~PatchProposalOutbox.set_job_id` all require that exact token, so a worker holding a stale
lease can never ack, defer, retire, or annotate a pointer a newer worker has since re-leased. An
expired lease (``lease_expires_at <= now``) is reclaimable by ``claim_due``.

The ``*_in_connection`` methods let the authoritative proposal store write/update/delete a pointer
**in the same transaction** as the ``patch_proposals`` row it mirrors (so the durable status and its
dispatch pointer never diverge). Those store-owned mutations are deliberately *unfenced*: the store
holds the proposal's row lock and is the source of truth, not a worker racing on a lease.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.scoping import validate_scope_id

from .errors import PatchValidationError

# Bounds on caller-supplied claim/backoff inputs (fail closed on anything out of range so a
# worker cannot request an unbounded batch or a negative/absurd lease/backoff window).
_MAX_CLAIM_LIMIT = 500
_MIN_LEASE_SECONDS = 1
_MAX_LEASE_SECONDS = 3600
_MIN_DELAY_SECONDS = 0
_MAX_DELAY_SECONDS = 86_400


class PatchOutboxStatus(StrEnum):
    """Coarse dispatch hint for a proposal pointer (never its authoritative status).

    ``generating`` -> the worker drives generation; ``ready`` -> generation done, awaiting a human
    approval request; ``approved`` -> a human approved, the worker drives trusted writeback. The
    ``approval_pending`` and every terminal state carry **no** pointer (the store deletes it), so a
    pointer exists only while there is background work (or a TTL to reconcile).
    """

    generating = "generating"
    ready = "ready"
    approved = "approved"


def _now() -> datetime:
    return datetime.now(UTC)


def _coerce_status(value: PatchOutboxStatus | str) -> PatchOutboxStatus:
    try:
        return PatchOutboxStatus(value)
    except ValueError as exc:
        raise PatchValidationError(f"invalid patch outbox status hint: {value!r}") from exc


def _validate_write(proposal_id: str, org_id: str, scope_id: str) -> None:
    """Validate the immutable routing identity of a pointer before it is written (fail closed)."""
    if not proposal_id:
        raise PatchValidationError("patch outbox pointer requires a proposal_id")
    if not org_id:
        raise PatchValidationError("patch outbox pointer requires an org_id")
    # The scope is re-derived/validated centrally so a corrupted or foreign scope can never be
    # smuggled into the global index (and later adopted verbatim by a worker's RLS GUC).
    validate_scope_id(scope_id)


def _bounded_limit(limit: int) -> int:
    if limit < 1 or limit > _MAX_CLAIM_LIMIT:
        raise PatchValidationError(f"claim limit out of bounds (1..{_MAX_CLAIM_LIMIT}): {limit}")
    return limit


def _bounded_lease_seconds(lease_seconds: int) -> int:
    if lease_seconds < _MIN_LEASE_SECONDS or lease_seconds > _MAX_LEASE_SECONDS:
        raise PatchValidationError(
            f"lease seconds out of bounds "
            f"({_MIN_LEASE_SECONDS}..{_MAX_LEASE_SECONDS}): {lease_seconds}"
        )
    return lease_seconds


def _bounded_delay_seconds(delay_seconds: int) -> int:
    if delay_seconds < _MIN_DELAY_SECONDS or delay_seconds > _MAX_DELAY_SECONDS:
        raise PatchValidationError(
            f"delay seconds out of bounds "
            f"({_MIN_DELAY_SECONDS}..{_MAX_DELAY_SECONDS}): {delay_seconds}"
        )
    return delay_seconds


@dataclass(frozen=True, slots=True)
class PatchOutboxEntry:
    """An immutable snapshot of one dispatch pointer (a claim result or a diagnostic read).

    ``lease_token`` is populated on a :meth:`~PatchProposalOutbox.claim_due` result — it is the
    fencing token the worker must present to reschedule/complete/remove/annotate the pointer.
    """

    proposal_id: str
    org_id: str
    scope_id: str
    status_hint: PatchOutboxStatus
    job_id: str
    attempts: int
    lease_token: str | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    next_attempt_at: datetime | None = None
    expires_at: datetime | None = None


class PatchProposalOutbox(Protocol):
    """The global patch-dispatch index a worker scans to reconcile proposals across all orgs."""

    async def record(
        self,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        expires_at: datetime,
        status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
        job_id: str = "",
        now: datetime | None = None,
    ) -> None:
        """Record (idempotently, on ``proposal_id``) an open dispatch pointer for a proposal."""

    async def record_in_connection(
        self,
        conn: AsyncConnection | None,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        expires_at: datetime,
        status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
        job_id: str = "",
        now: datetime | None = None,
    ) -> None:
        """Record a pointer inside the caller's transaction (atomic with the proposal write)."""

    async def upsert_status_hint_in_connection(
        self,
        conn: AsyncConnection | None,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        status_hint: PatchOutboxStatus,
        expires_at: datetime,
        now: datetime | None = None,
    ) -> None:
        """Re-create-or-update a pointer's hint in the caller's transaction (e.g. -> approved)."""

    async def set_status_hint_in_connection(
        self,
        conn: AsyncConnection | None,
        proposal_id: str,
        status_hint: PatchOutboxStatus,
        *,
        now: datetime | None = None,
    ) -> None:
        """Update an existing pointer's coarse hint in the caller's transaction (unfenced)."""

    async def delete_in_connection(self, conn: AsyncConnection | None, proposal_id: str) -> None:
        """Delete a pointer in the caller's transaction (unfenced store-owned retire)."""

    async def get(self, proposal_id: str) -> PatchOutboxEntry | None:
        """Read one pointer (diagnostics / tests)."""

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 100,
        lease_seconds: int = 300,
    ) -> list[PatchOutboxEntry]:
        """Lease a bounded batch of due pointers, stamping each with a fresh random lease token."""

    async def set_job_id(
        self, proposal_id: str, job_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        """Back-reference the servicing job on a leased pointer (fenced by ``lease_token``)."""

    async def reschedule(
        self,
        proposal_id: str,
        *,
        lease_token: str,
        delay_seconds: int = 60,
        now: datetime | None = None,
    ) -> bool:
        """Release the lease and defer the next attempt (fenced; attempts kept for backoff)."""

    async def complete(
        self, proposal_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        """Release the lease, reset the retry budget, mark due now (fenced) — this phase is done."""

    async def remove(
        self, proposal_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        """Delete a pointer whose proposal is terminal (fenced by ``lease_token``)."""

    async def active_scopes(self) -> set[str]:
        """Every scope with at least one open pointer (diagnostics / tests)."""


# --- In-memory -----------------------------------------------------------------------


class InMemoryPatchProposalOutbox:
    """Process-local patch-dispatch outbox double for unit tests (mirrors Postgres semantics)."""

    @dataclass
    class _Row:
        org_id: str
        scope_id: str
        status_hint: PatchOutboxStatus
        job_id: str
        attempts: int
        next_attempt_at: datetime
        expires_at: datetime
        lease_owner: str | None = None
        lease_token: str | None = None
        lease_expires_at: datetime | None = None

    def __init__(self) -> None:
        self._rows: dict[str, InMemoryPatchProposalOutbox._Row] = {}

    def _txn_snapshot(self) -> dict[str, InMemoryPatchProposalOutbox._Row]:
        """Deep-copy the pointer map so an atomic store operation can roll back on failure."""
        return {pid: replace_row(row) for pid, row in self._rows.items()}

    def _txn_restore(self, snapshot: dict[str, InMemoryPatchProposalOutbox._Row]) -> None:
        self._rows = {pid: replace_row(row) for pid, row in snapshot.items()}

    def _entry(self, proposal_id: str, row: InMemoryPatchProposalOutbox._Row) -> PatchOutboxEntry:
        return PatchOutboxEntry(
            proposal_id=proposal_id,
            org_id=row.org_id,
            scope_id=row.scope_id,
            status_hint=row.status_hint,
            job_id=row.job_id,
            attempts=row.attempts,
            lease_token=row.lease_token,
            lease_owner=row.lease_owner,
            lease_expires_at=row.lease_expires_at,
            next_attempt_at=row.next_attempt_at,
            expires_at=row.expires_at,
        )

    async def record(
        self,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        expires_at: datetime,
        status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
        job_id: str = "",
        now: datetime | None = None,
    ) -> None:
        await self.record_in_connection(
            None,
            proposal_id=proposal_id,
            org_id=org_id,
            scope_id=scope_id,
            expires_at=expires_at,
            status_hint=status_hint,
            job_id=job_id,
            now=now,
        )

    async def record_in_connection(
        self,
        conn: AsyncConnection | None,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        expires_at: datetime,
        status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
        job_id: str = "",
        now: datetime | None = None,
    ) -> None:
        _validate_write(proposal_id, org_id, scope_id)
        hint = _coerce_status(status_hint)
        moment = now or _now()
        if proposal_id in self._rows:  # idempotent (ON CONFLICT DO NOTHING)
            return
        self._rows[proposal_id] = self._Row(
            org_id=org_id,
            scope_id=scope_id,
            status_hint=hint,
            job_id=job_id,
            attempts=0,
            next_attempt_at=moment,
            expires_at=expires_at,
        )

    async def upsert_status_hint_in_connection(
        self,
        conn: AsyncConnection | None,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        status_hint: PatchOutboxStatus,
        expires_at: datetime,
        now: datetime | None = None,
    ) -> None:
        _validate_write(proposal_id, org_id, scope_id)
        hint = _coerce_status(status_hint)
        moment = now or _now()
        row = self._rows.get(proposal_id)
        if row is None:
            self._rows[proposal_id] = self._Row(
                org_id=org_id,
                scope_id=scope_id,
                status_hint=hint,
                job_id="",
                attempts=0,
                next_attempt_at=moment,
                expires_at=expires_at,
            )
            return
        row.status_hint = hint
        row.expires_at = expires_at

    async def set_status_hint_in_connection(
        self,
        conn: AsyncConnection | None,
        proposal_id: str,
        status_hint: PatchOutboxStatus,
        *,
        now: datetime | None = None,
    ) -> None:
        hint = _coerce_status(status_hint)
        row = self._rows.get(proposal_id)
        if row is not None:
            row.status_hint = hint

    async def delete_in_connection(self, conn: AsyncConnection | None, proposal_id: str) -> None:
        self._rows.pop(proposal_id, None)

    async def get(self, proposal_id: str) -> PatchOutboxEntry | None:
        row = self._rows.get(proposal_id)
        return None if row is None else self._entry(proposal_id, row)

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 100,
        lease_seconds: int = 300,
    ) -> list[PatchOutboxEntry]:
        bounded = _bounded_limit(limit)
        lease = _bounded_lease_seconds(lease_seconds)
        moment = now or _now()
        lease_until = moment + timedelta(seconds=lease)
        claimed: list[PatchOutboxEntry] = []
        for proposal_id, row in sorted(self._rows.items(), key=lambda kv: kv[1].next_attempt_at):
            if len(claimed) >= bounded:
                break
            due = row.next_attempt_at <= moment
            lease_free = row.lease_expires_at is None or row.lease_expires_at <= moment
            if not (due and lease_free):
                continue
            row.lease_owner = worker_id
            row.lease_token = uuid.uuid4().hex
            row.lease_expires_at = lease_until
            row.attempts += 1
            claimed.append(self._entry(proposal_id, row))
        return claimed

    def _fenced(
        self, proposal_id: str, lease_token: str
    ) -> InMemoryPatchProposalOutbox._Row | None:
        row = self._rows.get(proposal_id)
        if row is None or row.lease_token is None or row.lease_token != lease_token:
            return None
        return row

    async def set_job_id(
        self, proposal_id: str, job_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        row = self._fenced(proposal_id, lease_token)
        if row is None:
            return False
        row.job_id = job_id
        return True

    async def reschedule(
        self,
        proposal_id: str,
        *,
        lease_token: str,
        delay_seconds: int = 60,
        now: datetime | None = None,
    ) -> bool:
        delay = _bounded_delay_seconds(delay_seconds)
        row = self._fenced(proposal_id, lease_token)
        if row is None:
            return False
        moment = now or _now()
        row.lease_owner = None
        row.lease_token = None
        row.lease_expires_at = None
        row.next_attempt_at = moment + timedelta(seconds=delay)
        return True

    async def complete(
        self, proposal_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        row = self._fenced(proposal_id, lease_token)
        if row is None:
            return False
        moment = now or _now()
        row.lease_owner = None
        row.lease_token = None
        row.lease_expires_at = None
        row.attempts = 0
        row.next_attempt_at = moment
        return True

    async def remove(
        self, proposal_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        if self._fenced(proposal_id, lease_token) is None:
            return False
        del self._rows[proposal_id]
        return True

    async def active_scopes(self) -> set[str]:
        return {row.scope_id for row in self._rows.values()}


def replace_row(
    row: InMemoryPatchProposalOutbox._Row,
) -> InMemoryPatchProposalOutbox._Row:
    """Shallow value-copy of an in-memory pointer row (for txn snapshot/restore)."""
    return InMemoryPatchProposalOutbox._Row(
        org_id=row.org_id,
        scope_id=row.scope_id,
        status_hint=row.status_hint,
        job_id=row.job_id,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        expires_at=row.expires_at,
        lease_owner=row.lease_owner,
        lease_token=row.lease_token,
        lease_expires_at=row.lease_expires_at,
    )


# --- Postgres ------------------------------------------------------------------------

_INSERT_POINTER = text(
    "INSERT INTO patch_proposal_outbox "
    "(proposal_id, org_id, scope_id, status_hint, job_id, attempts, "
    " next_attempt_at, expires_at, created_at, updated_at) "
    "VALUES (:pid, :org, :scope, :hint, :job, 0, :now, :expires, :now, :now) "
    "ON CONFLICT (proposal_id) DO NOTHING"
)

_UPSERT_POINTER = text(
    "INSERT INTO patch_proposal_outbox "
    "(proposal_id, org_id, scope_id, status_hint, job_id, attempts, "
    " next_attempt_at, expires_at, created_at, updated_at) "
    "VALUES (:pid, :org, :scope, :hint, '', 0, :now, :expires, :now, :now) "
    "ON CONFLICT (proposal_id) DO UPDATE SET "
    "  status_hint = EXCLUDED.status_hint, expires_at = EXCLUDED.expires_at, "
    "  updated_at = EXCLUDED.updated_at"
)

_SET_HINT = text(
    "UPDATE patch_proposal_outbox SET status_hint = :hint, updated_at = :now "
    "WHERE proposal_id = :pid"
)

_DELETE_POINTER = text("DELETE FROM patch_proposal_outbox WHERE proposal_id = :pid")


async def record_pointer_in_connection(
    conn: AsyncConnection,
    *,
    proposal_id: str,
    org_id: str,
    scope_id: str,
    expires_at: datetime,
    status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
    job_id: str = "",
    now: datetime | None = None,
) -> None:
    """Idempotently record a pointer inside the caller's transaction (module-level helper).

    Shared by :meth:`PostgresPatchProposalOutbox.record` and the atomic proposal-store create path
    so a proposal never reaches its durable ``generating`` state without a discoverable pointer; a
    rollback of the surrounding transaction drops both together.
    """
    _validate_write(proposal_id, org_id, scope_id)
    hint = _coerce_status(status_hint)
    moment = now or _now()
    await conn.execute(
        _INSERT_POINTER,
        {
            "pid": proposal_id,
            "org": org_id,
            "scope": scope_id,
            "hint": hint.value,
            "job": job_id,
            "now": moment,
            "expires": expires_at,
        },
    )


async def upsert_pointer_in_connection(
    conn: AsyncConnection,
    *,
    proposal_id: str,
    org_id: str,
    scope_id: str,
    status_hint: PatchOutboxStatus,
    expires_at: datetime,
    now: datetime | None = None,
) -> None:
    """Re-create-or-update a pointer's hint in the caller's transaction (e.g. -> ``approved``)."""
    _validate_write(proposal_id, org_id, scope_id)
    hint = _coerce_status(status_hint)
    moment = now or _now()
    await conn.execute(
        _UPSERT_POINTER,
        {
            "pid": proposal_id,
            "org": org_id,
            "scope": scope_id,
            "hint": hint.value,
            "now": moment,
            "expires": expires_at,
        },
    )


async def set_pointer_hint_in_connection(
    conn: AsyncConnection,
    proposal_id: str,
    status_hint: PatchOutboxStatus,
    *,
    now: datetime | None = None,
) -> None:
    """Update an existing pointer's coarse hint in the caller's transaction (unfenced)."""
    hint = _coerce_status(status_hint)
    await conn.execute(_SET_HINT, {"pid": proposal_id, "hint": hint.value, "now": now or _now()})


async def delete_pointer_in_connection(conn: AsyncConnection, proposal_id: str) -> None:
    """Delete a pointer in the caller's transaction (unfenced store-owned retire)."""
    await conn.execute(_DELETE_POINTER, {"pid": proposal_id})


class PostgresPatchProposalOutbox:
    """Durable global patch-dispatch outbox over Postgres (no RLS — the cross-org index)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(
        self,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        expires_at: datetime,
        status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
        job_id: str = "",
        now: datetime | None = None,
    ) -> None:
        async with self._engine.begin() as conn:
            await record_pointer_in_connection(
                conn,
                proposal_id=proposal_id,
                org_id=org_id,
                scope_id=scope_id,
                expires_at=expires_at,
                status_hint=status_hint,
                job_id=job_id,
                now=now,
            )

    async def record_in_connection(
        self,
        conn: AsyncConnection | None,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        expires_at: datetime,
        status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
        job_id: str = "",
        now: datetime | None = None,
    ) -> None:
        if conn is None:
            raise PatchValidationError("PostgresPatchProposalOutbox requires a live connection")
        await record_pointer_in_connection(
            conn,
            proposal_id=proposal_id,
            org_id=org_id,
            scope_id=scope_id,
            expires_at=expires_at,
            status_hint=status_hint,
            job_id=job_id,
            now=now,
        )

    async def upsert_status_hint_in_connection(
        self,
        conn: AsyncConnection | None,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        status_hint: PatchOutboxStatus,
        expires_at: datetime,
        now: datetime | None = None,
    ) -> None:
        if conn is None:
            raise PatchValidationError("PostgresPatchProposalOutbox requires a live connection")
        await upsert_pointer_in_connection(
            conn,
            proposal_id=proposal_id,
            org_id=org_id,
            scope_id=scope_id,
            status_hint=status_hint,
            expires_at=expires_at,
            now=now,
        )

    async def set_status_hint_in_connection(
        self,
        conn: AsyncConnection | None,
        proposal_id: str,
        status_hint: PatchOutboxStatus,
        *,
        now: datetime | None = None,
    ) -> None:
        if conn is None:
            raise PatchValidationError("PostgresPatchProposalOutbox requires a live connection")
        await set_pointer_hint_in_connection(conn, proposal_id, status_hint, now=now)

    async def delete_in_connection(self, conn: AsyncConnection | None, proposal_id: str) -> None:
        if conn is None:
            raise PatchValidationError("PostgresPatchProposalOutbox requires a live connection")
        await delete_pointer_in_connection(conn, proposal_id)

    async def get(self, proposal_id: str) -> PatchOutboxEntry | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT proposal_id, org_id, scope_id, status_hint, job_id, attempts, "
                            "lease_token, lease_owner, lease_expires_at, next_attempt_at, "
                            "expires_at FROM patch_proposal_outbox WHERE proposal_id = :pid"
                        ),
                        {"pid": proposal_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_entry(row)

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 100,
        lease_seconds: int = 300,
    ) -> list[PatchOutboxEntry]:
        bounded = _bounded_limit(limit)
        lease = _bounded_lease_seconds(lease_seconds)
        moment = now or _now()
        lease_until = moment + timedelta(seconds=lease)
        async with self._engine.begin() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "UPDATE patch_proposal_outbox SET "
                            "  lease_owner = :worker, lease_token = gen_random_uuid()::text, "
                            "  lease_expires_at = :lease, attempts = attempts + 1, "
                            "  updated_at = :now "
                            "WHERE proposal_id IN ("
                            "  SELECT proposal_id FROM patch_proposal_outbox "
                            "  WHERE next_attempt_at <= :now "
                            "    AND (lease_expires_at IS NULL OR lease_expires_at <= :now) "
                            "  ORDER BY next_attempt_at "
                            "  FOR UPDATE SKIP LOCKED "
                            "  LIMIT :limit"
                            ") "
                            "RETURNING proposal_id, org_id, scope_id, status_hint, job_id, "
                            "attempts, lease_token, lease_owner, lease_expires_at, "
                            "next_attempt_at, expires_at"
                        ),
                        {
                            "worker": worker_id,
                            "lease": lease_until,
                            "now": moment,
                            "limit": bounded,
                        },
                    )
                )
                .mappings()
                .all()
            )
        return [_to_entry(row) for row in rows]

    async def set_job_id(
        self, proposal_id: str, job_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text(
                    "UPDATE patch_proposal_outbox SET job_id = :job, updated_at = :now "
                    "WHERE proposal_id = :pid AND lease_token = :token"
                ),
                {"pid": proposal_id, "job": job_id, "token": lease_token, "now": now or _now()},
            )
        return result.rowcount == 1

    async def reschedule(
        self,
        proposal_id: str,
        *,
        lease_token: str,
        delay_seconds: int = 60,
        now: datetime | None = None,
    ) -> bool:
        delay = _bounded_delay_seconds(delay_seconds)
        moment = now or _now()
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text(
                    "UPDATE patch_proposal_outbox SET "
                    "  lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                    "  next_attempt_at = :next_at, updated_at = :now "
                    "WHERE proposal_id = :pid AND lease_token = :token"
                ),
                {
                    "pid": proposal_id,
                    "token": lease_token,
                    "next_at": moment + timedelta(seconds=delay),
                    "now": moment,
                },
            )
        return result.rowcount == 1

    async def complete(
        self, proposal_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        moment = now or _now()
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text(
                    "UPDATE patch_proposal_outbox SET "
                    "  lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                    "  attempts = 0, next_attempt_at = :now, updated_at = :now "
                    "WHERE proposal_id = :pid AND lease_token = :token"
                ),
                {"pid": proposal_id, "token": lease_token, "now": moment},
            )
        return result.rowcount == 1

    async def remove(
        self, proposal_id: str, *, lease_token: str, now: datetime | None = None
    ) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text(
                    "DELETE FROM patch_proposal_outbox "
                    "WHERE proposal_id = :pid AND lease_token = :token"
                ),
                {"pid": proposal_id, "token": lease_token},
            )
        return result.rowcount == 1

    async def active_scopes(self) -> set[str]:
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(text("SELECT DISTINCT scope_id FROM patch_proposal_outbox"))
            ).all()
        return {row.scope_id for row in rows}


def _to_entry(row: Mapping[Any, Any]) -> PatchOutboxEntry:
    return PatchOutboxEntry(
        proposal_id=row["proposal_id"],
        org_id=row["org_id"],
        scope_id=row["scope_id"],
        status_hint=PatchOutboxStatus(row["status_hint"]),
        job_id=row["job_id"],
        attempts=row["attempts"],
        lease_token=row["lease_token"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        next_attempt_at=row["next_attempt_at"],
        expires_at=row["expires_at"],
    )


__all__ = [
    "InMemoryPatchProposalOutbox",
    "PatchOutboxEntry",
    "PatchOutboxStatus",
    "PatchProposalOutbox",
    "PostgresPatchProposalOutbox",
    "delete_pointer_in_connection",
    "record_pointer_in_connection",
    "set_pointer_hint_in_connection",
    "upsert_pointer_in_connection",
]
