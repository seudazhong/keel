"""Durable tool approvals — the persistent sibling of runtime.ApprovalRegistry.

An approval raised by an *unattended* run cannot block on an in-memory future; it
is a durable row that survives process death and is resolved out-of-band. The run
suspends (loop.py) and resumes when the row is granted/denied/expired (G5)."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


async def purge_scope(engine: AsyncEngine, scope_id: str) -> int:
    """Erase every durable approval for a scope (idempotent). Returns rows removed."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        result = await conn.execute(
            text("DELETE FROM approvals WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return int(result.rowcount or 0)


@dataclass
class ApprovalRecord:
    id: str
    scope_id: str
    run_id: str
    session_id: str
    tool: str
    args: dict[str, Any]
    call_id: str
    idempotency_key: str
    reason: str
    status: str
    created_at: datetime
    expires_at: datetime
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    # M3.6 cross-surface binding: a decision is bound to the exact org/actor/run attempt
    # and a stable hash of (tool, args) so a stale/replayed approval cannot be reused for a
    # different action or an earlier attempt.
    org_id: str = ""
    actor: str = ""
    action_hash: str = ""
    run_attempt: int = 0


@runtime_checkable
class ApprovalStore(Protocol):
    """Durable store of pending/resolved tool approvals, scope-bound."""

    async def create_pending(
        self,
        *,
        scope_id: str,
        run_id: str,
        session_id: str,
        tool: str,
        args: dict[str, Any],
        call_id: str,
        idempotency_key: str,
        reason: str,
        expires_at: datetime,
        org_id: str = "",
        actor: str = "",
        action_hash: str = "",
        run_attempt: int = 0,
    ) -> str: ...

    async def get(self, approval_id: str) -> ApprovalRecord | None: ...

    async def list_pending(self, scope_id: str) -> list[ApprovalRecord]: ...

    async def pending_for_run(self, run_id: str) -> list[ApprovalRecord]: ...

    async def resolve(
        self,
        approval_id: str,
        status: str,
        resolved_by: str,
        *,
        expected_action_hash: str | None = None,
        expected_run_attempt: int | None = None,
    ) -> bool: ...

    async def expire_due(self, now: datetime) -> list[str]: ...


@dataclass
class InMemoryApprovalStore:
    """Deterministic in-memory ApprovalStore for tests and single-process dev."""

    _rows: dict[str, ApprovalRecord] = field(default_factory=dict)

    async def create_pending(
        self,
        *,
        scope_id: str,
        run_id: str,
        session_id: str,
        tool: str,
        args: dict[str, Any],
        call_id: str,
        idempotency_key: str,
        reason: str,
        expires_at: datetime,
        org_id: str = "",
        actor: str = "",
        action_hash: str = "",
        run_attempt: int = 0,
    ) -> str:
        approval_id = uuid.uuid4().hex
        self._rows[approval_id] = ApprovalRecord(
            id=approval_id,
            scope_id=scope_id,
            run_id=run_id,
            session_id=session_id,
            tool=tool,
            args=args,
            call_id=call_id,
            idempotency_key=idempotency_key,
            reason=reason,
            status="pending",
            created_at=datetime.now(UTC),
            expires_at=expires_at,
            org_id=org_id,
            actor=actor,
            action_hash=action_hash,
            run_attempt=run_attempt,
        )
        return approval_id

    async def get(self, approval_id: str) -> ApprovalRecord | None:
        return self._rows.get(approval_id)

    async def list_pending(self, scope_id: str) -> list[ApprovalRecord]:
        return [r for r in self._rows.values() if r.scope_id == scope_id and r.status == "pending"]

    async def pending_for_run(self, run_id: str) -> list[ApprovalRecord]:
        return [r for r in self._rows.values() if r.run_id == run_id and r.status == "pending"]

    async def resolve(
        self,
        approval_id: str,
        status: str,
        resolved_by: str,
        *,
        expected_action_hash: str | None = None,
        expected_run_attempt: int | None = None,
    ) -> bool:
        row = self._rows.get(approval_id)
        if row is None or row.status != "pending":
            return False
        # Fail closed on a stale/replayed decision bound to a different action or attempt.
        if expected_action_hash is not None and row.action_hash != expected_action_hash:
            return False
        if expected_run_attempt is not None and row.run_attempt != expected_run_attempt:
            return False
        row.status = status
        row.resolved_at = datetime.now(UTC)
        row.resolved_by = resolved_by
        return True

    async def expire_due(self, now: datetime) -> list[str]:
        expired: list[str] = []
        for row in self._rows.values():
            if row.status == "pending" and row.expires_at <= now:
                row.status = "expired"
                row.resolved_at = now
                expired.append(row.id)
        return expired


def _to_record(row: Any) -> ApprovalRecord:
    return ApprovalRecord(
        id=row["id"],
        scope_id=row["scope_id"],
        run_id=row["run_id"],
        session_id=row["session_id"],
        tool=row["tool"],
        args=row["args"],
        call_id=row["call_id"],
        idempotency_key=row["idempotency_key"],
        reason=row["reason"],
        status=row["status"],
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        resolved_at=row["resolved_at"],
        resolved_by=row["resolved_by"],
        org_id=row["org_id"],
        actor=row["actor"],
        action_hash=row["action_hash"],
        run_attempt=row["run_attempt"],
    )


class PostgresApprovalStore:
    """Durable, scope-bound ApprovalStore over Postgres (RLS as defense-in-depth)."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def create_pending(
        self,
        *,
        scope_id: str,
        run_id: str,
        session_id: str,
        tool: str,
        args: dict[str, Any],
        call_id: str,
        idempotency_key: str,
        reason: str,
        expires_at: datetime,
        org_id: str = "",
        actor: str = "",
        action_hash: str = "",
        run_attempt: int = 0,
    ) -> str:
        approval_id = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            await conn.execute(
                text(
                    "INSERT INTO approvals (id, scope_id, run_id, session_id, tool, args, "
                    "call_id, idempotency_key, reason, status, created_at, expires_at, "
                    "org_id, actor, action_hash, run_attempt) VALUES "
                    "(:id, :scope, :run_id, :session_id, :tool, CAST(:args AS jsonb), :call_id, "
                    ":key, :reason, 'pending', now(), :expires_at, :org_id, :actor, "
                    ":action_hash, :run_attempt)"
                ),
                {
                    "id": approval_id,
                    "scope": scope_id,
                    "run_id": run_id,
                    "session_id": session_id,
                    "tool": tool,
                    "args": json.dumps(args),
                    "call_id": call_id,
                    "key": idempotency_key,
                    "reason": reason,
                    "expires_at": expires_at,
                    "org_id": org_id,
                    "actor": actor,
                    "action_hash": action_hash,
                    "run_attempt": run_attempt,
                },
            )
        return approval_id

    async def get(self, approval_id: str) -> ApprovalRecord | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM approvals WHERE scope_id = :scope AND id = :id"),
                        {"scope": self._scope_id, "id": approval_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return _to_record(row) if row is not None else None

    async def list_pending(self, scope_id: str) -> list[ApprovalRecord]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM approvals WHERE scope_id = :scope "
                            "AND status = 'pending' ORDER BY created_at"
                        ),
                        {"scope": scope_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_record(r) for r in rows]

    async def pending_for_run(self, run_id: str) -> list[ApprovalRecord]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM approvals WHERE scope_id = :scope AND run_id = :run_id "
                            "AND status = 'pending'"
                        ),
                        {"scope": self._scope_id, "run_id": run_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_record(r) for r in rows]

    async def resolve(
        self,
        approval_id: str,
        status: str,
        resolved_by: str,
        *,
        expected_action_hash: str | None = None,
        expected_run_attempt: int | None = None,
    ) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE approvals SET status = :status, resolved_at = now(), "
                    "resolved_by = :by WHERE scope_id = :scope AND id = :id AND status = 'pending' "
                    "AND (CAST(:hash AS text) IS NULL OR action_hash = :hash) "
                    "AND (CAST(:attempt AS integer) IS NULL OR run_attempt = :attempt)"
                ),
                {
                    "status": status,
                    "by": resolved_by,
                    "scope": self._scope_id,
                    "id": approval_id,
                    "hash": expected_action_hash,
                    "attempt": expected_run_attempt,
                },
            )
        return result.rowcount == 1

    async def expire_due(self, now: datetime) -> list[str]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "UPDATE approvals SET status = 'expired', resolved_at = :now "
                            "WHERE scope_id = :scope AND status = 'pending' AND expires_at <= :now "
                            "RETURNING id"
                        ),
                        {"scope": self._scope_id, "now": now},
                    )
                )
                .scalars()
                .all()
            )
        return [str(r) for r in rows]
