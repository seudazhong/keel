"""Global index of scopes with recurring connector schedules (M3.6 review finding 1).

Connector recurring schedules (sync/renewal cadence) live on the scope-partitioned
``connector_bindings`` table, which is under ``FORCE ROW LEVEL SECURITY`` keyed by
``app.scope_id`` — so a worker bound to one scope cannot *see* another scope's due schedules.
Before this index the recurring reconciler (``keel_worker.connectors.reconcile_connectors_tick``)
was pinned to the single process ``durable_scope``, so a connector connected under any per-Agent
scope (``agent:<org>/<agent>``) never had its sync/renewal fired — its schedule was orphaned.

``connector_active_scopes`` is the minimal, non-sensitive, **global** analogue of the run/job
dispatch outboxes: it records only the *set of scopes* that currently have at least one connected
binding with a recurring schedule. It carries no binding id, credential, cadence, or payload — only
the scope routing key a reconciler needs to discover *which* scopes to bind and reconcile. A single
worker can then enumerate every active scope, bind each in turn, and run the RLS-scoped
``reconcile_recurring`` there (which claims that scope's actually-due schedules and enqueues the
sync/renew jobs). Recording is idempotent; the scope is discarded once it has no scheduled bindings.

Like the dispatch outboxes this index is intentionally **not** under row-level security: it is the
one table a worker reads *across* scopes, and it exposes nothing beyond scope routing keys.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.types import ScopeId


def _now() -> datetime:
    return datetime.now(UTC)


class ConnectorScheduleIndex(Protocol):
    """The global set of scopes with recurring connector schedules to reconcile."""

    async def record(self, scope_id: ScopeId, *, now: datetime | None = None) -> None:
        """Record (idempotently) that ``scope_id`` has recurring connector schedule work."""

    async def discard(self, scope_id: ScopeId) -> None:
        """Drop ``scope_id`` once it has no scheduled bindings left (nothing to reconcile)."""

    async def active_scopes(self) -> set[ScopeId]:
        """Every scope with at least one recurring connector schedule."""


class InMemoryConnectorScheduleIndex:
    """Process-local schedule-scope index double for unit tests."""

    def __init__(self) -> None:
        self._scopes: set[ScopeId] = set()

    async def record(self, scope_id: ScopeId, *, now: datetime | None = None) -> None:
        self._scopes.add(scope_id)

    async def discard(self, scope_id: ScopeId) -> None:
        self._scopes.discard(scope_id)

    async def active_scopes(self) -> set[ScopeId]:
        return set(self._scopes)


class PostgresConnectorScheduleIndex:
    """Durable global schedule-scope index over Postgres (no RLS — the cross-scope index)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(self, scope_id: ScopeId, *, now: datetime | None = None) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO connector_active_scopes (scope_id, updated_at) "
                    "VALUES (:scope, :now) "
                    "ON CONFLICT (scope_id) DO UPDATE SET updated_at = EXCLUDED.updated_at"
                ),
                {"scope": scope_id, "now": now},
            )

    async def discard(self, scope_id: ScopeId) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM connector_active_scopes WHERE scope_id = :scope"),
                {"scope": scope_id},
            )

    async def active_scopes(self) -> set[ScopeId]:
        async with self._engine.begin() as conn:
            rows = (await conn.execute(text("SELECT scope_id FROM connector_active_scopes"))).all()
        return {row.scope_id for row in rows}


__all__ = [
    "ConnectorScheduleIndex",
    "InMemoryConnectorScheduleIndex",
    "PostgresConnectorScheduleIndex",
]
