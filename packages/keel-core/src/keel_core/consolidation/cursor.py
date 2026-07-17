"""At-most-one consolidation lease + durable cursor (spec §5).

One row per scope in ``consolidation_cursors`` tracks the last consolidated event id
and a time-boxed lease. ``claim`` atomically takes the lease (or steals an expired one)
via a scoped CAS ``UPDATE ... RETURNING``; a concurrent claim gets ``None`` (busy).
``complete`` advances the cursor and releases the lease in one statement; ``fail``
releases without advancing so the same batch is retried. Scope-bound + RLS (ADR-0009).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


async def purge_scope(engine: AsyncEngine, scope_id: str) -> int:
    """Erase the consolidation cursor + lease for a scope (idempotent)."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        result = await conn.execute(
            text("DELETE FROM consolidation_cursors WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return int(result.rowcount or 0)


@dataclass(frozen=True)
class ConsolidationLease:
    """A held consolidation lease: the CAS token + the cursor at claim time."""

    scope_id: str
    token: str
    last_event_id: int


@dataclass(frozen=True)
class ConsolidationCursorState:
    """A read-only snapshot of a scope's cursor row (tests / observability)."""

    last_event_id: int
    last_status: str | None
    lease_token: str | None


class ConsolidationCursorStore:
    """Scope-bound durable cursor + lease over ``consolidation_cursors``."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def claim(self, now: datetime, *, lease_seconds: int = 600) -> ConsolidationLease | None:
        token = uuid.uuid4().hex
        expires_at = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "INSERT INTO consolidation_cursors (scope_id) VALUES (:scope) "
                    "ON CONFLICT (scope_id) DO NOTHING"
                ),
                {"scope": self._scope_id},
            )
            row = (
                await conn.execute(
                    text(
                        "UPDATE consolidation_cursors "
                        "SET lease_token = :token, lease_expires_at = :expires, "
                        "updated_at = now() "
                        "WHERE scope_id = :scope "
                        "AND (lease_token IS NULL OR lease_expires_at < :now) "
                        "RETURNING last_event_id"
                    ),
                    {
                        "token": token,
                        "expires": expires_at,
                        "scope": self._scope_id,
                        "now": now,
                    },
                )
            ).one_or_none()
        if row is None:
            return None
        return ConsolidationLease(self._scope_id, token, int(row.last_event_id))

    async def complete(self, lease: ConsolidationLease, last_event_id: int, status: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "UPDATE consolidation_cursors "
                    "SET last_event_id = :leid, last_status = :status, last_run_at = now(), "
                    "lease_token = NULL, lease_expires_at = NULL, updated_at = now() "
                    "WHERE scope_id = :scope AND lease_token = :token"
                ),
                {
                    "leid": last_event_id,
                    "status": status,
                    "scope": self._scope_id,
                    "token": lease.token,
                },
            )

    async def fail(self, lease: ConsolidationLease, status: str = "error") -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "UPDATE consolidation_cursors "
                    "SET last_status = :status, last_run_at = now(), "
                    "lease_token = NULL, lease_expires_at = NULL, updated_at = now() "
                    "WHERE scope_id = :scope AND lease_token = :token"
                ),
                {"status": status, "scope": self._scope_id, "token": lease.token},
            )

    async def get(self) -> ConsolidationCursorState | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "SELECT last_event_id, last_status, lease_token "
                        "FROM consolidation_cursors WHERE scope_id = :scope"
                    ),
                    {"scope": self._scope_id},
                )
            ).one_or_none()
        if row is None:
            return None
        return ConsolidationCursorState(
            int(row.last_event_id),
            None if row.last_status is None else str(row.last_status),
            None if row.lease_token is None else str(row.lease_token),
        )
