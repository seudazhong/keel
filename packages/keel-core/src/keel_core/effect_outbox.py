"""Global cross-scope reconciliation pointer (R1B) — the Effect analogue of
``patch_proposal_outbox`` (:mod:`keel_core.patch.outbox`).

``effects`` (migration ``0026_effect_ledger``) is RLS-scoped by ``scope_id`` exactly like
every other per-Agent data-plane table, so a single worker process bound to one scope at a
time cannot discover *which* Effects across every scope currently need attention — an
expired execution lease to reap, or an ``unknown`` Effect awaiting provider reconciliation
— without either a per-scope cron (does not scale) or a privileged ``BYPASSRLS`` scan
(defeats the isolation guarantee for the table's whole lifetime). Exactly like the patch
proposal outbox, admission records a minimal, non-sensitive dispatch pointer in a
**global**, non-RLS index: :class:`EffectReconciliationOutbox` (table
``effect_reconciliation_outbox``).

The pointer carries only routing keys (``effect_id``, ``scope_id``, ``org_id``,
``provider``, a coarse ``kind`` — ``lease_watch`` while an execution lease is outstanding,
``reconcile`` once the Effect is ``unknown``) plus a fenced reconciler lease. It carries
**no** action args, provider ref, or result — those stay behind RLS on ``effects``.

Two distinct write paths, exactly like the patch outbox:

* ``*_in_connection`` — unfenced, called by :class:`~keel_core.effect_store.EffectStore`
  in the *same* transaction as the authoritative ``effects`` row write, so the pointer can
  never diverge from the Effect it mirrors.
* ``claim_due`` / ``reschedule`` / ``complete`` — fenced by a random ``lease_token``,
  called by the reconciliation worker (a different process/owner) so a worker holding a
  stale lease can never reschedule or retire a pointer a newer worker has since re-leased.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

_MAX_CLAIM_LIMIT = 500
_MIN_LEASE_SECONDS = 1
_MAX_LEASE_SECONDS = 3600


class EffectOutboxKind(StrEnum):
    """Coarse routing hint — never the Effect's own authoritative status."""

    lease_watch = "lease_watch"  # an execution lease is outstanding; watch it for expiry
    reconcile = "reconcile"  # the Effect is unknown; awaiting provider reconciliation


def _now() -> datetime:
    return datetime.now(UTC)


def _bounded_limit(limit: int) -> int:
    if limit < 1 or limit > _MAX_CLAIM_LIMIT:
        raise ValueError(f"claim limit out of bounds (1..{_MAX_CLAIM_LIMIT}): {limit}")
    return limit


def _bounded_lease_seconds(lease_seconds: int) -> int:
    if lease_seconds < _MIN_LEASE_SECONDS or lease_seconds > _MAX_LEASE_SECONDS:
        raise ValueError(
            f"lease seconds out of bounds "
            f"({_MIN_LEASE_SECONDS}..{_MAX_LEASE_SECONDS}): {lease_seconds}"
        )
    return lease_seconds


@dataclass(frozen=True, slots=True)
class EffectOutboxEntry:
    effect_id: str
    scope_id: str
    org_id: str
    provider: str
    kind: EffectOutboxKind
    due_at: datetime
    attempts: int
    lease_token: str | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None


class EffectReconciliationOutbox(Protocol):
    async def upsert_in_connection(
        self,
        conn: AsyncConnection | None,
        *,
        effect_id: str,
        scope_id: str,
        org_id: str,
        provider: str,
        kind: EffectOutboxKind,
        due_at: datetime,
    ) -> None:
        """Idempotently (re)point at ``effect_id`` in the caller's own transaction."""
        ...

    async def remove_in_connection(self, conn: AsyncConnection | None, effect_id: str) -> None:
        """Retire a pointer in the caller's own transaction (the Effect is resolved)."""
        ...

    async def get(self, effect_id: str) -> EffectOutboxEntry | None: ...

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 100,
        lease_seconds: int = 120,
    ) -> list[EffectOutboxEntry]:
        """Lease a bounded batch of due pointers under a fresh random lease token."""
        ...

    async def reschedule(
        self,
        effect_id: str,
        *,
        lease_token: str,
        kind: EffectOutboxKind | None = None,
        delay_seconds: int = 60,
        now: datetime | None = None,
    ) -> bool:
        """Release the lease; optionally re-kind the pointer and defer its next attempt
        (fenced by ``lease_token``; attempts is incremented for backoff)."""
        ...

    async def complete(self, effect_id: str, *, lease_token: str) -> bool:
        """Retire a pointer whose Effect reached a resolved terminal outcome (fenced)."""
        ...


class InMemoryEffectReconciliationOutbox:
    """In-memory pointer index (tests / in-memory preview)."""

    def __init__(self) -> None:
        self._rows: dict[str, EffectOutboxEntry] = {}

    async def upsert_in_connection(
        self,
        conn: Any,
        *,
        effect_id: str,
        scope_id: str,
        org_id: str,
        provider: str,
        kind: EffectOutboxKind,
        due_at: datetime,
    ) -> None:
        previous = self._rows.get(effect_id)
        self._rows[effect_id] = EffectOutboxEntry(
            effect_id=effect_id,
            scope_id=scope_id,
            org_id=org_id,
            provider=provider,
            kind=kind,
            due_at=due_at,
            attempts=previous.attempts if previous is not None else 0,
        )

    async def remove_in_connection(self, conn: Any, effect_id: str) -> None:
        self._rows.pop(effect_id, None)

    async def get(self, effect_id: str) -> EffectOutboxEntry | None:
        return self._rows.get(effect_id)

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 100,
        lease_seconds: int = 120,
    ) -> list[EffectOutboxEntry]:
        limit = _bounded_limit(limit)
        lease_seconds = _bounded_lease_seconds(lease_seconds)
        moment = now or _now()
        due = [
            row
            for row in self._rows.values()
            if row.due_at <= moment
            and (row.lease_expires_at is None or row.lease_expires_at <= moment)
        ]
        due.sort(key=lambda row: row.due_at)
        claimed: list[EffectOutboxEntry] = []
        for row in due[:limit]:
            token = uuid.uuid4().hex
            updated = EffectOutboxEntry(
                effect_id=row.effect_id,
                scope_id=row.scope_id,
                org_id=row.org_id,
                provider=row.provider,
                kind=row.kind,
                due_at=row.due_at,
                attempts=row.attempts,
                lease_token=token,
                lease_owner=worker_id,
                lease_expires_at=moment + timedelta(seconds=lease_seconds),
            )
            self._rows[row.effect_id] = updated
            claimed.append(updated)
        return claimed

    async def reschedule(
        self,
        effect_id: str,
        *,
        lease_token: str,
        kind: EffectOutboxKind | None = None,
        delay_seconds: int = 60,
        now: datetime | None = None,
    ) -> bool:
        row = self._rows.get(effect_id)
        if row is None or row.lease_token != lease_token:
            return False
        moment = now or _now()
        self._rows[effect_id] = EffectOutboxEntry(
            effect_id=row.effect_id,
            scope_id=row.scope_id,
            org_id=row.org_id,
            provider=row.provider,
            kind=kind or row.kind,
            due_at=moment + timedelta(seconds=delay_seconds),
            attempts=row.attempts + 1,
        )
        return True

    async def complete(self, effect_id: str, *, lease_token: str) -> bool:
        row = self._rows.get(effect_id)
        if row is None or row.lease_token != lease_token:
            return False
        del self._rows[effect_id]
        return True


_COLUMNS = (
    "effect_id, scope_id, org_id, provider, kind, due_at, attempts, "
    "lease_owner, lease_token, lease_expires_at"
)


def _to_entry(row: Any) -> EffectOutboxEntry:
    return EffectOutboxEntry(
        effect_id=row.effect_id,
        scope_id=row.scope_id,
        org_id=row.org_id,
        provider=row.provider,
        kind=EffectOutboxKind(row.kind),
        due_at=row.due_at,
        attempts=row.attempts,
        lease_token=row.lease_token,
        lease_owner=row.lease_owner,
        lease_expires_at=row.lease_expires_at,
    )


class PostgresEffectReconciliationOutbox:
    """Durable, global (non-RLS) pointer over Postgres (``effect_reconciliation_outbox``)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def upsert_in_connection(
        self,
        conn: AsyncConnection | None,
        *,
        effect_id: str,
        scope_id: str,
        org_id: str,
        provider: str,
        kind: EffectOutboxKind,
        due_at: datetime,
    ) -> None:
        if conn is None:
            raise ValueError("upsert_in_connection requires an open connection")
        await conn.execute(
            text(
                "INSERT INTO effect_reconciliation_outbox "
                "(effect_id, scope_id, org_id, provider, kind, due_at) "
                "VALUES (:effect_id, :scope_id, :org_id, :provider, :kind, :due_at) "
                "ON CONFLICT (effect_id) DO UPDATE SET "
                "kind = EXCLUDED.kind, due_at = EXCLUDED.due_at, "
                "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                "updated_at = now()"
            ),
            {
                "effect_id": effect_id,
                "scope_id": scope_id,
                "org_id": org_id,
                "provider": provider,
                "kind": kind.value,
                "due_at": due_at,
            },
        )

    async def remove_in_connection(self, conn: AsyncConnection | None, effect_id: str) -> None:
        if conn is None:
            raise ValueError("remove_in_connection requires an open connection")
        await conn.execute(
            text("DELETE FROM effect_reconciliation_outbox WHERE effect_id = :effect_id"),
            {"effect_id": effect_id},
        )

    async def get(self, effect_id: str) -> EffectOutboxEntry | None:
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        f"SELECT {_COLUMNS} FROM effect_reconciliation_outbox "
                        "WHERE effect_id = :effect_id"
                    ),
                    {"effect_id": effect_id},
                )
            ).one_or_none()
        return None if row is None else _to_entry(row)

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 100,
        lease_seconds: int = 120,
    ) -> list[EffectOutboxEntry]:
        limit = _bounded_limit(limit)
        lease_seconds = _bounded_lease_seconds(lease_seconds)
        moment = now or _now()
        async with self._engine.begin() as conn:
            ids = (
                await conn.execute(
                    text(
                        "SELECT effect_id FROM effect_reconciliation_outbox "
                        "WHERE due_at <= :now "
                        "AND (lease_expires_at IS NULL OR lease_expires_at <= :now) "
                        "ORDER BY due_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED"
                    ),
                    {"now": moment, "limit": limit},
                )
            ).all()
            claimed: list[EffectOutboxEntry] = []
            for row in ids:
                token = uuid.uuid4().hex
                updated = (
                    await conn.execute(
                        text(
                            "UPDATE effect_reconciliation_outbox SET "
                            "lease_owner = :owner, lease_token = :token, "
                            "lease_expires_at = :expires, updated_at = now() "
                            "WHERE effect_id = :effect_id "
                            f"RETURNING {_COLUMNS}"
                        ),
                        {
                            "owner": worker_id,
                            "token": token,
                            "expires": moment + timedelta(seconds=lease_seconds),
                            "effect_id": row.effect_id,
                        },
                    )
                ).one()
                claimed.append(_to_entry(updated))
        return claimed

    async def reschedule(
        self,
        effect_id: str,
        *,
        lease_token: str,
        kind: EffectOutboxKind | None = None,
        delay_seconds: int = 60,
        now: datetime | None = None,
    ) -> bool:
        moment = now or _now()
        async with self._engine.begin() as conn:
            sql = (
                "UPDATE effect_reconciliation_outbox SET "
                "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                "attempts = attempts + 1, "
                "due_at = :due_at, updated_at = now()"
            )
            params: dict[str, Any] = {
                "due_at": moment + timedelta(seconds=delay_seconds),
                "effect_id": effect_id,
                "token": lease_token,
            }
            if kind is not None:
                sql += ", kind = :kind"
                params["kind"] = kind.value
            sql += " WHERE effect_id = :effect_id AND lease_token = :token"
            result = await conn.execute(text(sql), params)
        return bool(result.rowcount)

    async def complete(self, effect_id: str, *, lease_token: str) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text(
                    "DELETE FROM effect_reconciliation_outbox "
                    "WHERE effect_id = :effect_id AND lease_token = :token"
                ),
                {"effect_id": effect_id, "token": lease_token},
            )
        return bool(result.rowcount)


__all__ = [
    "EffectOutboxEntry",
    "EffectOutboxKind",
    "EffectReconciliationOutbox",
    "InMemoryEffectReconciliationOutbox",
    "PostgresEffectReconciliationOutbox",
]
