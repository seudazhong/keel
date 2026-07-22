"""EffectStore — the durable Effect ledger (R1B, C4/C5).

:class:`EffectStore` is the single seam an outbound connector action (via
:class:`~keel_core.connectors.ConnectorTool`) reserves, executes, and confirms one
external mutation through. Two implementations mirror the codebase's established
in-memory/Postgres pattern (see :mod:`keel_core.outbox`, :mod:`keel_core.patch.store`):

* :class:`InMemoryEffectStore` — single-process, for tests and the in-memory preview.
* :class:`PostgresEffectStore` — durable, RLS-scoped (``effects``, migration
  ``0026_effect_ledger``), safe across a worker restart or a second replica.

Concurrency: :meth:`EffectStore.begin_execution` is an atomic compare-and-set (the
Postgres implementation is a single ``UPDATE ... WHERE status IN (...) RETURNING``) so
two concurrent callers racing the same Effect never both execute the provider mutation —
exactly one wins the lease and the other observes the loser's outcome instead of firing a
second request. Every mutation after ``begin_execution`` is fenced by the lease token
returned from it, so a caller holding a stale/expired lease can never confirm, fail, or
mark unknown an Effect a newer owner has since re-leased.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.effect_outbox import EffectOutboxKind, EffectReconciliationOutbox
from keel_core.effects import (
    EffectConflictError,
    EffectRecord,
    EffectStatus,
    canonical_args_or_digest,
)

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")

_MIN_LEASE_SECONDS = 1
_MAX_LEASE_SECONDS = 3600


def _now() -> datetime:
    return datetime.now(UTC)


def _bounded_lease_seconds(lease_seconds: int) -> int:
    if lease_seconds < _MIN_LEASE_SECONDS or lease_seconds > _MAX_LEASE_SECONDS:
        raise ValueError(
            f"lease seconds out of bounds ({_MIN_LEASE_SECONDS}..{_MAX_LEASE_SECONDS}): "
            f"{lease_seconds}"
        )
    return lease_seconds


@runtime_checkable
class EffectStore(Protocol):
    """Durable Effect ledger: reserve/execute/confirm one external mutation at-most-once."""

    async def create_or_get(
        self,
        *,
        scope_id: str,
        org_id: str,
        agent_id: str,
        actor_id: str,
        run_id: str,
        tool_name: str,
        provider: str,
        resource_id: str,
        action_name: str,
        action_hash: str,
        idempotency_key: str,
        args: Mapping[str, Any],
    ) -> EffectRecord:
        """Idempotently reserve (or return the existing) Effect for this exact identity.

        Identity is ``(scope_id, provider, action_name, idempotency_key)``. A second call
        with the same identity never inserts a second row; if its ``action_hash`` differs
        from the stored one, raises :class:`~keel_core.effects.EffectConflictError`
        (C5 — an idempotency key may never be silently reused for a different action)."""
        ...

    async def get(self, scope_id: str, effect_id: str) -> EffectRecord | None: ...

    async def begin_execution(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 120,
    ) -> EffectRecord | None:
        """Atomically claim the execution lease (fenced). ``None`` if not eligible now
        (already executing/confirmed/unknown/reconciled_confirmed, or lost the race)."""
        ...

    async def renew_execution_lease(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        lease_seconds: int = 120,
    ) -> bool:
        """Extend a still-owned executing lease; ``False`` means ownership was lost."""
        ...

    async def confirm(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        """``executing -> confirmed`` (fenced by ``lease_token``): the provider mutation
        is durably known to have succeeded exactly once."""
        ...

    async def mark_unknown(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        error: str,
    ) -> EffectRecord:
        """``executing -> unknown`` (fenced): the provider may have accepted the mutation
        before the response was lost. Retry is blocked until reconciliation (C4)."""
        ...

    async def mark_failed(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        error: str,
    ) -> EffectRecord:
        """``executing -> failed`` (fenced): an ordinary, provably-not-mutating failure
        (validation/auth/pre-send). Retryable."""
        ...

    async def reap_expired_lease(
        self, scope_id: str, effect_id: str, *, now: datetime | None = None
    ) -> EffectRecord | None:
        """``executing -> unknown`` when the execution lease has expired (crash recovery).

        Never resurrects an expired lease back to ``reserved``/pending-retry (a crash after
        a possible provider success must never look like it never happened, C4)."""
        ...

    async def reconcile_confirmed(
        self,
        scope_id: str,
        effect_id: str,
        *,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        """``unknown -> reconciled_confirmed``: the provider proved the mutation exists."""
        ...

    async def record_late_confirmation(
        self,
        scope_id: str,
        effect_id: str,
        *,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        """Persist direct success evidence after lease loss, before a retry has begun."""
        ...

    async def reconcile_absent(self, scope_id: str, effect_id: str) -> EffectRecord:
        """``unknown -> reconciled_absent``: the provider proved the mutation never
        landed. Exactly one controlled retry is now permitted (:data:`RETRYABLE_STATUSES`)."""
        ...

    async def list_for_scope(
        self,
        scope_id: str,
        *,
        status: EffectStatus | None = None,
        limit: int = 100,
    ) -> list[EffectRecord]: ...


def _identity_key(scope_id: str, provider: str, action_name: str, idempotency_key: str) -> str:
    return f"{scope_id}\x1f{provider}\x1f{action_name}\x1f{idempotency_key}"


@dataclass
class _Row:
    record: EffectRecord


class InMemoryEffectStore:
    """Single-process Effect ledger (tests / in-memory preview)."""

    def __init__(self, outbox: EffectReconciliationOutbox | None = None) -> None:
        self._by_id: dict[str, _Row] = {}
        self._by_identity: dict[str, str] = {}
        self._outbox = outbox

    def _put(self, record: EffectRecord) -> None:
        self._by_id[record.id] = _Row(record)
        key = _identity_key(
            record.scope_id, record.provider, record.action_name, record.idempotency_key
        )
        self._by_identity[key] = record.id

    async def create_or_get(
        self,
        *,
        scope_id: str,
        org_id: str,
        agent_id: str,
        actor_id: str,
        run_id: str,
        tool_name: str,
        provider: str,
        resource_id: str,
        action_name: str,
        action_hash: str,
        idempotency_key: str,
        args: Mapping[str, Any],
    ) -> EffectRecord:
        key = _identity_key(scope_id, provider, action_name, idempotency_key)
        existing_id = self._by_identity.get(key)
        if existing_id is not None:
            existing = self._by_id[existing_id].record
            if existing.action_hash != action_hash:
                raise EffectConflictError(
                    f"idempotency key {idempotency_key!r} is bound to a different action"
                )
            return existing
        now = _now()
        record = EffectRecord(
            id=uuid.uuid4().hex,
            scope_id=scope_id,
            org_id=org_id,
            agent_id=agent_id,
            actor_id=actor_id,
            run_id=run_id,
            tool_name=tool_name,
            provider=provider,
            resource_id=resource_id,
            action_name=action_name,
            action_hash=action_hash,
            idempotency_key=idempotency_key,
            canonical_args=canonical_args_or_digest(args),
            status=EffectStatus.reserved,
            attempt=0,
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
            provider_ref="",
            result="",
            error="",
            reconciliation_attempts=0,
            next_reconciliation_at=None,
            reconciled_at=None,
            created_at=now,
            updated_at=now,
        )
        self._put(record)
        return record

    async def get(self, scope_id: str, effect_id: str) -> EffectRecord | None:
        row = self._by_id.get(effect_id)
        if row is None or row.record.scope_id != scope_id:
            return None
        return row.record

    async def _outbox_upsert(
        self, record: EffectRecord, kind: EffectOutboxKind, due_at: datetime
    ) -> None:
        if self._outbox is None:
            return
        await self._outbox.upsert_in_connection(
            None,
            effect_id=record.id,
            scope_id=record.scope_id,
            org_id=record.org_id,
            provider=record.provider,
            kind=kind,
            due_at=due_at,
        )

    async def _outbox_remove(self, effect_id: str) -> None:
        if self._outbox is None:
            return
        await self._outbox.remove_in_connection(None, effect_id)

    async def begin_execution(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 120,
    ) -> EffectRecord | None:
        lease_seconds = _bounded_lease_seconds(lease_seconds)
        row = self._by_id.get(effect_id)
        if row is None or row.record.scope_id != scope_id:
            return None
        current = row.record
        if current.status not in (
            EffectStatus.reserved,
            EffectStatus.failed,
            EffectStatus.reconciled_absent,
        ):
            return None
        now = _now()
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        updated = replace(
            current,
            status=EffectStatus.executing,
            attempt=current.attempt + 1,
            lease_owner=lease_owner,
            lease_token=uuid.uuid4().hex,
            lease_expires_at=lease_expires_at,
            error="",
            updated_at=now,
        )
        self._put(updated)
        await self._outbox_upsert(updated, EffectOutboxKind.lease_watch, lease_expires_at)
        return updated

    async def renew_execution_lease(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        lease_seconds: int = 120,
    ) -> bool:
        lease_seconds = _bounded_lease_seconds(lease_seconds)
        row = self._by_id.get(effect_id)
        if row is None or row.record.scope_id != scope_id:
            return False
        current = row.record
        if current.status is not EffectStatus.executing or current.lease_token != lease_token:
            return False
        now = _now()
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        updated = replace(current, lease_expires_at=lease_expires_at, updated_at=now)
        self._put(updated)
        await self._outbox_upsert(updated, EffectOutboxKind.lease_watch, lease_expires_at)
        return True

    async def _fenced(
        self,
        scope_id: str,
        effect_id: str,
        lease_token: str,
        target: EffectStatus,
        **fields: Any,
    ) -> EffectRecord | None:
        row = self._by_id.get(effect_id)
        if row is None or row.record.scope_id != scope_id:
            return None
        current = row.record
        if current.status is not EffectStatus.executing or current.lease_token != lease_token:
            return None
        now = _now()
        updated = replace(
            current,
            status=target,
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
            updated_at=now,
            **fields,
        )
        self._put(updated)
        if target is EffectStatus.unknown:
            await self._outbox_upsert(updated, EffectOutboxKind.reconcile, now)
        else:
            await self._outbox_remove(effect_id)
        return updated

    async def confirm(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        updated = await self._fenced(
            scope_id,
            effect_id,
            lease_token,
            EffectStatus.confirmed,
            provider_ref=provider_ref,
            result=result,
            error="",
        )
        if updated is None:
            raise LookupError(f"effect {effect_id!r} is not an owned in-flight execution")
        return updated

    async def mark_unknown(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        error: str,
    ) -> EffectRecord:
        updated = await self._fenced(
            scope_id,
            effect_id,
            lease_token,
            EffectStatus.unknown,
            error=error,
            next_reconciliation_at=_now(),
        )
        if updated is None:
            raise LookupError(f"effect {effect_id!r} is not an owned in-flight execution")
        return updated

    async def mark_failed(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        error: str,
    ) -> EffectRecord:
        updated = await self._fenced(
            scope_id, effect_id, lease_token, EffectStatus.failed, error=error
        )
        if updated is None:
            raise LookupError(f"effect {effect_id!r} is not an owned in-flight execution")
        return updated

    async def reap_expired_lease(
        self, scope_id: str, effect_id: str, *, now: datetime | None = None
    ) -> EffectRecord | None:
        moment = now or _now()
        row = self._by_id.get(effect_id)
        if row is None or row.record.scope_id != scope_id:
            return None
        current = row.record
        if current.status is not EffectStatus.executing:
            return None
        if current.lease_expires_at is None or current.lease_expires_at > moment:
            return None
        updated = replace(
            current,
            status=EffectStatus.unknown,
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
            error="execution lease expired before confirmation",
            next_reconciliation_at=moment,
            updated_at=moment,
        )
        self._put(updated)
        await self._outbox_upsert(updated, EffectOutboxKind.reconcile, moment)
        return updated

    async def reconcile_confirmed(
        self,
        scope_id: str,
        effect_id: str,
        *,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        row = self._by_id.get(effect_id)
        if (
            row is None
            or row.record.scope_id != scope_id
            or row.record.status is not EffectStatus.unknown
        ):
            raise LookupError(f"effect {effect_id!r} is not unknown")
        current = row.record
        now = _now()
        updated = replace(
            current,
            status=EffectStatus.reconciled_confirmed,
            provider_ref=provider_ref or current.provider_ref,
            result=result or current.result,
            reconciled_at=now,
            updated_at=now,
        )
        self._put(updated)
        await self._outbox_remove(effect_id)
        return updated

    async def record_late_confirmation(
        self,
        scope_id: str,
        effect_id: str,
        *,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        row = self._by_id.get(effect_id)
        if row is None or row.record.scope_id != scope_id:
            raise LookupError(effect_id)
        current = row.record
        if current.status not in (
            EffectStatus.unknown,
            EffectStatus.reconciled_absent,
            EffectStatus.failed,
        ):
            raise LookupError(effect_id)
        now = _now()
        updated = replace(
            current,
            status=EffectStatus.reconciled_confirmed,
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
            provider_ref=provider_ref,
            result=result,
            error="",
            reconciled_at=now,
            updated_at=now,
        )
        self._put(updated)
        await self._outbox_remove(effect_id)
        return updated

    async def reconcile_absent(self, scope_id: str, effect_id: str) -> EffectRecord:
        row = self._by_id.get(effect_id)
        if (
            row is None
            or row.record.scope_id != scope_id
            or row.record.status is not EffectStatus.unknown
        ):
            raise LookupError(f"effect {effect_id!r} is not unknown")
        current = row.record
        now = _now()
        updated = replace(
            current,
            status=EffectStatus.reconciled_absent,
            reconciled_at=now,
            updated_at=now,
        )
        self._put(updated)
        await self._outbox_remove(effect_id)
        return updated

    async def list_for_scope(
        self,
        scope_id: str,
        *,
        status: EffectStatus | None = None,
        limit: int = 100,
    ) -> list[EffectRecord]:
        rows = [
            row.record
            for row in self._by_id.values()
            if row.record.scope_id == scope_id and (status is None or row.record.status is status)
        ]
        rows.sort(key=lambda r: r.created_at, reverse=True)
        return rows[:limit]

    def snapshot(self) -> list[EffectRecord]:
        """Every Effect across every scope (diagnostics/tests only)."""
        return [row.record for row in self._by_id.values()]


def _to_record(row: Mapping[Any, Any]) -> EffectRecord:
    return EffectRecord(
        id=row["id"],
        scope_id=row["scope_id"],
        org_id=row["org_id"],
        agent_id=row["agent_id"],
        actor_id=row["actor_id"],
        run_id=row["run_id"],
        tool_name=row["tool_name"],
        provider=row["provider"],
        resource_id=row["resource_id"],
        action_name=row["action_name"],
        action_hash=row["action_hash"],
        idempotency_key=row["idempotency_key"],
        canonical_args=row["canonical_args"],
        status=EffectStatus(row["status"]),
        attempt=row["attempt"],
        lease_owner=row["lease_owner"],
        lease_token=row["lease_token"],
        lease_expires_at=row["lease_expires_at"],
        provider_ref=row["provider_ref"],
        result=row["result"],
        error=row["error"],
        reconciliation_attempts=row["reconciliation_attempts"],
        next_reconciliation_at=row["next_reconciliation_at"],
        reconciled_at=row["reconciled_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


_COLUMNS = (
    "id, scope_id, org_id, agent_id, actor_id, run_id, tool_name, provider, resource_id, "
    "action_name, action_hash, idempotency_key, canonical_args, status, attempt, "
    "lease_owner, lease_token, lease_expires_at, provider_ref, result, error, "
    "reconciliation_attempts, next_reconciliation_at, reconciled_at, created_at, updated_at"
)


class PostgresEffectStore:
    """Durable, RLS-scoped Effect ledger over Postgres (``effects``, migration 0026)."""

    def __init__(
        self, engine: AsyncEngine, outbox: EffectReconciliationOutbox | None = None
    ) -> None:
        self._engine = engine
        self._outbox = outbox

    async def create_or_get(
        self,
        *,
        scope_id: str,
        org_id: str,
        agent_id: str,
        actor_id: str,
        run_id: str,
        tool_name: str,
        provider: str,
        resource_id: str,
        action_name: str,
        action_hash: str,
        idempotency_key: str,
        args: Mapping[str, Any],
    ) -> EffectRecord:
        canonical_args = canonical_args_or_digest(args)
        new_id = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            inserted = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO effects (id, scope_id, org_id, agent_id, actor_id, "
                            "run_id, tool_name, provider, resource_id, action_name, "
                            "action_hash, idempotency_key, canonical_args, status, attempt) "
                            "VALUES (:id, :scope, :org, :agent, :actor, :run, :tool, :provider, "
                            ":resource, :action, :hash, :key, :args, 'reserved', 0) "
                            "ON CONFLICT (scope_id, provider, action_name, idempotency_key) "
                            f"DO NOTHING RETURNING {_COLUMNS}"
                        ),
                        {
                            "id": new_id,
                            "scope": scope_id,
                            "org": org_id,
                            "agent": agent_id,
                            "actor": actor_id,
                            "run": run_id,
                            "tool": tool_name,
                            "provider": provider,
                            "resource": resource_id,
                            "action": action_name,
                            "hash": action_hash,
                            "key": idempotency_key,
                            "args": canonical_args,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if inserted is not None:
                return _to_record(inserted)
            existing = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLUMNS} FROM effects WHERE scope_id = :scope "
                            "AND provider = :provider AND action_name = :action "
                            "AND idempotency_key = :key"
                        ),
                        {
                            "scope": scope_id,
                            "provider": provider,
                            "action": action_name,
                            "key": idempotency_key,
                        },
                    )
                )
                .mappings()
                .one()
            )
        record = _to_record(existing)
        if record.action_hash != action_hash:
            raise EffectConflictError(
                f"idempotency key {idempotency_key!r} is bound to a different action"
            )
        return record

    async def get(self, scope_id: str, effect_id: str) -> EffectRecord | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLUMNS} FROM effects WHERE scope_id = :scope AND id = :id"
                        ),
                        {"scope": scope_id, "id": effect_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_record(row)

    async def begin_execution(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 120,
    ) -> EffectRecord | None:
        lease_seconds = _bounded_lease_seconds(lease_seconds)
        lease_token = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE effects SET status = 'executing', attempt = attempt + 1, "
                            "lease_owner = :owner, lease_token = :token, "
                            "lease_expires_at = now() + make_interval(secs => :seconds), "
                            "error = '', updated_at = now() "
                            "WHERE scope_id = :scope AND id = :id "
                            "AND status IN ('reserved', 'failed', 'reconciled_absent') "
                            f"RETURNING {_COLUMNS}"
                        ),
                        {
                            "owner": lease_owner,
                            "token": lease_token,
                            "seconds": lease_seconds,
                            "scope": scope_id,
                            "id": effect_id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            record = _to_record(row)
            if self._outbox is not None:
                await self._outbox.upsert_in_connection(
                    conn,
                    effect_id=record.id,
                    scope_id=record.scope_id,
                    org_id=record.org_id,
                    provider=record.provider,
                    kind=EffectOutboxKind.lease_watch,
                    due_at=record.lease_expires_at or _now(),
                )
        return record

    async def renew_execution_lease(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        lease_seconds: int = 120,
    ) -> bool:
        lease_seconds = _bounded_lease_seconds(lease_seconds)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE effects SET "
                            "lease_expires_at = now() + make_interval(secs => :seconds), "
                            "updated_at = now() "
                            "WHERE scope_id = :scope AND id = :id AND status = 'executing' "
                            "AND lease_token = :token "
                            f"RETURNING {_COLUMNS}"
                        ),
                        {
                            "seconds": lease_seconds,
                            "scope": scope_id,
                            "id": effect_id,
                            "token": lease_token,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return False
            record = _to_record(row)
            if self._outbox is not None:
                await self._outbox.upsert_in_connection(
                    conn,
                    effect_id=record.id,
                    scope_id=record.scope_id,
                    org_id=record.org_id,
                    provider=record.provider,
                    kind=EffectOutboxKind.lease_watch,
                    due_at=record.lease_expires_at or _now(),
                )
        return True

    async def _fenced_update(
        self,
        scope_id: str,
        effect_id: str,
        lease_token: str,
        target: EffectStatus,
        extra_sql: str,
        extra_params: dict[str, Any],
    ) -> EffectRecord:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"UPDATE effects SET status = :target, lease_owner = NULL, "
                            f"lease_token = NULL, lease_expires_at = NULL, updated_at = now() "
                            f"{extra_sql} "
                            "WHERE scope_id = :scope AND id = :id AND status = 'executing' "
                            "AND lease_token = :token "
                            f"RETURNING {_COLUMNS}"
                        ),
                        {
                            "target": target.value,
                            "scope": scope_id,
                            "id": effect_id,
                            "token": lease_token,
                            **extra_params,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise LookupError(f"effect {effect_id!r} is not an owned in-flight execution")
            record = _to_record(row)
            if self._outbox is not None:
                if target is EffectStatus.unknown:
                    await self._outbox.upsert_in_connection(
                        conn,
                        effect_id=record.id,
                        scope_id=record.scope_id,
                        org_id=record.org_id,
                        provider=record.provider,
                        kind=EffectOutboxKind.reconcile,
                        due_at=_now(),
                    )
                else:
                    await self._outbox.remove_in_connection(conn, effect_id)
        return record

    async def confirm(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        return await self._fenced_update(
            scope_id,
            effect_id,
            lease_token,
            EffectStatus.confirmed,
            ", provider_ref = :ref, result = :result, error = ''",
            {"ref": provider_ref, "result": result},
        )

    async def mark_unknown(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        error: str,
    ) -> EffectRecord:
        return await self._fenced_update(
            scope_id,
            effect_id,
            lease_token,
            EffectStatus.unknown,
            ", error = :error, next_reconciliation_at = now()",
            {"error": error},
        )

    async def mark_failed(
        self,
        scope_id: str,
        effect_id: str,
        *,
        lease_token: str,
        error: str,
    ) -> EffectRecord:
        return await self._fenced_update(
            scope_id,
            effect_id,
            lease_token,
            EffectStatus.failed,
            ", error = :error",
            {"error": error},
        )

    async def reap_expired_lease(
        self, scope_id: str, effect_id: str, *, now: datetime | None = None
    ) -> EffectRecord | None:
        moment = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE effects SET status = 'unknown', lease_owner = NULL, "
                            "lease_token = NULL, lease_expires_at = NULL, "
                            "error = 'execution lease expired before confirmation', "
                            "next_reconciliation_at = :now, updated_at = :now "
                            "WHERE scope_id = :scope AND id = :id AND status = 'executing' "
                            f"AND lease_expires_at <= :now RETURNING {_COLUMNS}"
                        ),
                        {"scope": scope_id, "id": effect_id, "now": moment},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            record = _to_record(row)
            if self._outbox is not None:
                await self._outbox.upsert_in_connection(
                    conn,
                    effect_id=record.id,
                    scope_id=record.scope_id,
                    org_id=record.org_id,
                    provider=record.provider,
                    kind=EffectOutboxKind.reconcile,
                    due_at=moment,
                )
        return record

    async def reconcile_confirmed(
        self,
        scope_id: str,
        effect_id: str,
        *,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE effects SET status = 'reconciled_confirmed', "
                            "provider_ref = COALESCE(NULLIF(:ref, ''), provider_ref), "
                            "result = COALESCE(NULLIF(:result, ''), result), "
                            "reconciled_at = now(), updated_at = now() "
                            "WHERE scope_id = :scope AND id = :id AND status = 'unknown' "
                            f"RETURNING {_COLUMNS}"
                        ),
                        {"ref": provider_ref, "result": result, "scope": scope_id, "id": effect_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise LookupError(f"effect {effect_id!r} is not unknown")
            record = _to_record(row)
            if self._outbox is not None:
                await self._outbox.remove_in_connection(conn, effect_id)
        return record

    async def record_late_confirmation(
        self,
        scope_id: str,
        effect_id: str,
        *,
        provider_ref: str,
        result: str,
    ) -> EffectRecord:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE effects SET status = 'reconciled_confirmed', "
                            "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                            "provider_ref = :ref, result = :result, error = '', "
                            "reconciled_at = now(), updated_at = now() "
                            "WHERE scope_id = :scope AND id = :id "
                            "AND status IN ('unknown', 'reconciled_absent', 'failed') "
                            f"RETURNING {_COLUMNS}"
                        ),
                        {
                            "scope": scope_id,
                            "id": effect_id,
                            "ref": provider_ref,
                            "result": result,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise LookupError(effect_id)
            record = _to_record(row)
            if self._outbox is not None:
                await self._outbox.remove_in_connection(conn, effect_id)
        return record

    async def reconcile_absent(self, scope_id: str, effect_id: str) -> EffectRecord:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE effects SET status = 'reconciled_absent', "
                            "reconciled_at = now(), updated_at = now() "
                            "WHERE scope_id = :scope AND id = :id AND status = 'unknown' "
                            f"RETURNING {_COLUMNS}"
                        ),
                        {"scope": scope_id, "id": effect_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise LookupError(f"effect {effect_id!r} is not unknown")
            record = _to_record(row)
            if self._outbox is not None:
                await self._outbox.remove_in_connection(conn, effect_id)
        return record

    async def list_for_scope(
        self,
        scope_id: str,
        *,
        status: EffectStatus | None = None,
        limit: int = 100,
    ) -> list[EffectRecord]:
        limit = max(1, min(limit, 500))
        sql = f"SELECT {_COLUMNS} FROM effects WHERE scope_id = :scope "
        params: dict[str, Any] = {"scope": scope_id}
        if status is not None:
            sql += "AND status = :status "
            params["status"] = status.value
        sql += "ORDER BY created_at DESC LIMIT :limit"
        params["limit"] = limit
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            rows = (await conn.execute(text(sql), params)).mappings().all()
        return [_to_record(row) for row in rows]


async def purge_scope(engine: AsyncEngine, scope_id: str) -> int:
    """Erase every Effect for a scope (idempotent). The reconciliation outbox pointer
    cascades away with its Effect (composite FK, ``ON DELETE CASCADE``)."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        result = await conn.execute(
            text("DELETE FROM effects WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return int(result.rowcount or 0)


__all__ = [
    "EffectStore",
    "InMemoryEffectStore",
    "PostgresEffectStore",
    "purge_scope",
]
