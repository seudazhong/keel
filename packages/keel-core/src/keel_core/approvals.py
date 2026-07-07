"""Durable tool approvals — the persistent sibling of runtime.ApprovalRegistry.

An approval raised by an *unattended* run cannot block on an in-memory future; it
is a durable row that survives process death and is resolved out-of-band. The run
suspends (loop.py) and resumes when the row is granted/denied/expired (G5)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable


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
    ) -> str: ...

    async def get(self, approval_id: str) -> ApprovalRecord | None: ...

    async def list_pending(self, scope_id: str) -> list[ApprovalRecord]: ...

    async def pending_for_run(self, run_id: str) -> list[ApprovalRecord]: ...

    async def resolve(self, approval_id: str, status: str, resolved_by: str) -> bool: ...

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
        )
        return approval_id

    async def get(self, approval_id: str) -> ApprovalRecord | None:
        return self._rows.get(approval_id)

    async def list_pending(self, scope_id: str) -> list[ApprovalRecord]:
        return [
            r for r in self._rows.values() if r.scope_id == scope_id and r.status == "pending"
        ]

    async def pending_for_run(self, run_id: str) -> list[ApprovalRecord]:
        return [r for r in self._rows.values() if r.run_id == run_id and r.status == "pending"]

    async def resolve(self, approval_id: str, status: str, resolved_by: str) -> bool:
        row = self._rows.get(approval_id)
        if row is None or row.status != "pending":
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
