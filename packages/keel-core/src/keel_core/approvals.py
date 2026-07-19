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
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")

_INSERT_APPROVAL_SQL = (
    "INSERT INTO approvals (id, scope_id, run_id, session_id, tool, args, "
    "call_id, idempotency_key, reason, status, created_at, expires_at, "
    "org_id, actor, action_hash, run_attempt, batch_id) VALUES "
    "(:id, :scope, :run_id, :session_id, :tool, CAST(:args AS jsonb), :call_id, "
    ":key, :reason, 'pending', now(), :expires_at, :org_id, :actor, "
    ":action_hash, :run_attempt, :batch_id)"
)

_INSERT_APPROVAL = text(_INSERT_APPROVAL_SQL)

# Insert-or-get keyed by the interactive partial-unique index ``ux_approvals_run_call`` (migration
# 0014): ``(scope_id, run_id, call_id, run_attempt) WHERE batch_id <> ''``. A concurrent duplicate
# does nothing and returns no row; the caller then re-reads and verifies the full immutable binding.
_INSERT_APPROVAL_OR_GET = text(
    _INSERT_APPROVAL_SQL
    + " ON CONFLICT (scope_id, run_id, call_id, run_attempt) WHERE batch_id <> '' "
    "DO NOTHING RETURNING id"
)

_SELECT_APPROVAL_BY_KEY = text(
    "SELECT * FROM approvals WHERE scope_id = :scope AND run_id = :run_id "
    "AND call_id = :call_id AND run_attempt = :run_attempt AND batch_id <> ''"
)


def _approval_params(
    *,
    id: str,
    scope_id: str,
    run_id: str,
    session_id: str,
    tool: str,
    args: dict[str, Any],
    call_id: str,
    idempotency_key: str,
    reason: str,
    expires_at: datetime,
    org_id: str,
    actor: str,
    action_hash: str,
    run_attempt: int,
    batch_id: str,
) -> dict[str, Any]:
    """The single bind-parameter builder shared by every pending-approval INSERT."""
    return {
        "id": id,
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
        "batch_id": batch_id,
    }


async def insert_pending_in_transaction(
    conn: AsyncConnection,
    *,
    id: str,
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
    batch_id: str = "",
) -> None:
    """Insert one pending approval row inside the caller's transaction (no commit here).

    Lets a suspended tool batch persist its approval rows in the *same* transaction as the
    ``tool.call`` / ``approval.requested`` events (:func:`keel_core.loop._persist_suspension_batch`)
    so a crash can never leave an approval row without its events (or vice versa). The caller
    owns the transaction + ``app.scope_id`` GUC."""
    await conn.execute(
        _INSERT_APPROVAL,
        _approval_params(
            id=id,
            scope_id=scope_id,
            run_id=run_id,
            session_id=session_id,
            tool=tool,
            args=args,
            call_id=call_id,
            idempotency_key=idempotency_key,
            reason=reason,
            expires_at=expires_at,
            org_id=org_id,
            actor=actor,
            action_hash=action_hash,
            run_attempt=run_attempt,
            batch_id=batch_id,
        ),
    )


def _verify_binding(
    existing: ApprovalRecord,
    *,
    scope_id: str,
    run_id: str,
    session_id: str,
    tool: str,
    args: dict[str, Any],
    call_id: str,
    idempotency_key: str,
    org_id: str,
    actor: str,
    action_hash: str,
    run_attempt: int,
    batch_id: str,
) -> None:
    """Fail closed unless every immutable binding field of an existing approval matches the request.

    A ``create_pending_or_get`` that finds a row for the same interactive key must be an exact
    replay of the *same* action; any divergence (a different tool/args/call/actor/org/attempt/batch/
    idempotency key/session) means the caller is trying to reuse another action's approval and is
    rejected with :class:`~keel_core.patch.errors.PatchApprovalError` (fail closed, P5). The message
    names only the mismatched field(s) — never a secret, argument value, or payload."""
    from keel_core.patch.errors import PatchApprovalError

    mismatches = {
        "scope_id": existing.scope_id != scope_id,
        "run_id": existing.run_id != run_id,
        "session_id": existing.session_id != session_id,
        "tool": existing.tool != tool,
        "call_id": existing.call_id != call_id,
        "idempotency_key": existing.idempotency_key != idempotency_key,
        "org_id": existing.org_id != org_id,
        "actor": existing.actor != actor,
        "action_hash": existing.action_hash != action_hash,
        "run_attempt": existing.run_attempt != run_attempt,
        "batch_id": existing.batch_id != batch_id,
        "args": json.dumps(existing.args, sort_keys=True) != json.dumps(args, sort_keys=True),
    }
    bad = sorted(name for name, differs in mismatches.items() if differs)
    if bad:
        raise PatchApprovalError(f"approval binding mismatch: {', '.join(bad)}")


async def insert_pending_or_get_in_transaction(
    conn: AsyncConnection,
    *,
    id: str,
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
    batch_id: str,
) -> tuple[str, bool]:
    """Insert one pending approval, or return the existing one for the same interactive key.

    A truly idempotent create-or-get over the ``ux_approvals_run_call`` partial-unique index,
    entirely within the caller's transaction (**no** nested ``engine.begin`` — a proposal store can
    call this inside its single ``ready -> approval_pending`` transaction). Returns
    ``(approval_id, created)``: ``created`` is ``True`` for a fresh row, ``False`` when an
    equivalent row already existed. On a conflict the existing row's full immutable binding is
    re-verified (:func:`_verify_binding`) so a replay for a *different* action fails closed rather
    than silently adopting a foreign approval. The caller owns the transaction + ``app.scope_id``
    GUC.

    Requires a non-empty ``batch_id``: the unique index is partial on ``batch_id <> ''``, so an
    empty batch id would never deduplicate and the get half could not fire."""
    from keel_core.patch.errors import PatchApprovalError

    if not batch_id:
        raise PatchApprovalError("insert_pending_or_get requires a non-empty batch_id")
    inserted = (
        await conn.execute(
            _INSERT_APPROVAL_OR_GET,
            _approval_params(
                id=id,
                scope_id=scope_id,
                run_id=run_id,
                session_id=session_id,
                tool=tool,
                args=args,
                call_id=call_id,
                idempotency_key=idempotency_key,
                reason=reason,
                expires_at=expires_at,
                org_id=org_id,
                actor=actor,
                action_hash=action_hash,
                run_attempt=run_attempt,
                batch_id=batch_id,
            ),
        )
    ).first()
    if inserted is not None:
        return id, True
    existing_row = (
        (
            await conn.execute(
                _SELECT_APPROVAL_BY_KEY,
                {
                    "scope": scope_id,
                    "run_id": run_id,
                    "call_id": call_id,
                    "run_attempt": run_attempt,
                },
            )
        )
        .mappings()
        .one()
    )
    existing = _to_record(existing_row)
    _verify_binding(
        existing,
        scope_id=scope_id,
        run_id=run_id,
        session_id=session_id,
        tool=tool,
        args=args,
        call_id=call_id,
        idempotency_key=idempotency_key,
        org_id=org_id,
        actor=actor,
        action_hash=action_hash,
        run_attempt=run_attempt,
        batch_id=batch_id,
    )
    return existing.id, False


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
    # Every approval raised by one suspended tool batch shares this id, so a run resumes only
    # once *all* decisions in the batch are terminal (M3.6 blocker 5). Empty for legacy rows.
    batch_id: str = ""

    TERMINAL_STATUSES = ("granted", "denied", "expired")

    @property
    def is_terminal(self) -> bool:
        return self.status in ApprovalRecord.TERMINAL_STATUSES


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
        batch_id: str = "",
    ) -> str: ...

    async def create_pending_or_get(
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
        batch_id: str,
    ) -> tuple[str, bool]:
        """Create a pending approval, or idempotently return the existing one (create-or-get).

        The atomic primitive behind ``ready -> approval_pending``: keyed by the interactive
        partial-unique binding ``(scope_id, run_id, call_id, run_attempt)`` (``batch_id`` required,
        non-empty), it returns ``(approval_id, created)``. A concurrent second caller for the *same*
        action gets the *same* ``approval_id`` with ``created=False``; a caller whose immutable
        binding diverges fails closed with
        :class:`~keel_core.patch.errors.PatchApprovalError`. There is no non-idempotent fallback."""
        ...

    async def get(self, approval_id: str) -> ApprovalRecord | None: ...

    async def list_pending(self, scope_id: str) -> list[ApprovalRecord]: ...

    async def pending_for_run(self, run_id: str) -> list[ApprovalRecord]: ...

    async def list_for_run(self, run_id: str) -> list[ApprovalRecord]: ...

    async def batch_pending_count(self, run_id: str, batch_id: str) -> int: ...

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
        batch_id: str = "",
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
            batch_id=batch_id,
        )
        return approval_id

    async def create_pending_or_get(
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
        batch_id: str,
    ) -> tuple[str, bool]:
        """Create-or-get mirroring the Postgres ``ux_approvals_run_call`` semantics exactly."""
        from keel_core.patch.errors import PatchApprovalError

        if not batch_id:
            raise PatchApprovalError("create_pending_or_get requires a non-empty batch_id")
        for row in self._rows.values():
            if (
                row.scope_id == scope_id
                and row.run_id == run_id
                and row.call_id == call_id
                and row.run_attempt == run_attempt
                and row.batch_id != ""
            ):
                _verify_binding(
                    row,
                    scope_id=scope_id,
                    run_id=run_id,
                    session_id=session_id,
                    tool=tool,
                    args=args,
                    call_id=call_id,
                    idempotency_key=idempotency_key,
                    org_id=org_id,
                    actor=actor,
                    action_hash=action_hash,
                    run_attempt=run_attempt,
                    batch_id=batch_id,
                )
                return row.id, False
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
            batch_id=batch_id,
        )
        return approval_id, True

    async def get(self, approval_id: str) -> ApprovalRecord | None:
        return self._rows.get(approval_id)

    def _txn_snapshot(self) -> dict[str, ApprovalRecord]:
        """Capture a rollback snapshot so an atomic multi-store suspension can undo a partial
        batch on an injected failure. A suspended batch only ever *adds* rows, so a shallow
        copy of the id -> row map is enough: restoring drops exactly the rows it inserted."""
        return dict(self._rows)

    def _txn_restore(self, snapshot: dict[str, ApprovalRecord]) -> None:
        self._rows = dict(snapshot)

    async def list_pending(self, scope_id: str) -> list[ApprovalRecord]:
        return [r for r in self._rows.values() if r.scope_id == scope_id and r.status == "pending"]

    async def pending_for_run(self, run_id: str) -> list[ApprovalRecord]:
        return [r for r in self._rows.values() if r.run_id == run_id and r.status == "pending"]

    async def list_for_run(self, run_id: str) -> list[ApprovalRecord]:
        """Every approval (any status) raised for ``run_id``, oldest first.

        The durable source of truth for resume's call -> approval association: each row
        carries the complete immutable action payload (tool/args/call_id/action_hash/batch),
        so resume can reconstruct a suspended batch even if an ``approval.requested`` event was
        lost to a crash before it committed (M3.6 approval-event atomicity repair)."""
        return sorted(
            (r for r in self._rows.values() if r.run_id == run_id),
            key=lambda r: r.created_at,
        )

    async def batch_pending_count(self, run_id: str, batch_id: str) -> int:
        """How many approvals in this run's batch are still pending (0 == batch terminal)."""
        return sum(
            1
            for r in self._rows.values()
            if r.run_id == run_id and r.batch_id == batch_id and r.status == "pending"
        )

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
        batch_id=row["batch_id"],
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
        batch_id: str = "",
    ) -> str:
        approval_id = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            await insert_pending_in_transaction(
                conn,
                id=approval_id,
                scope_id=scope_id,
                run_id=run_id,
                session_id=session_id,
                tool=tool,
                args=args,
                call_id=call_id,
                idempotency_key=idempotency_key,
                reason=reason,
                expires_at=expires_at,
                org_id=org_id,
                actor=actor,
                action_hash=action_hash,
                run_attempt=run_attempt,
                batch_id=batch_id,
            )
        return approval_id

    async def create_pending_or_get(
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
        batch_id: str,
    ) -> tuple[str, bool]:
        approval_id = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            return await insert_pending_or_get_in_transaction(
                conn,
                id=approval_id,
                scope_id=scope_id,
                run_id=run_id,
                session_id=session_id,
                tool=tool,
                args=args,
                call_id=call_id,
                idempotency_key=idempotency_key,
                reason=reason,
                expires_at=expires_at,
                org_id=org_id,
                actor=actor,
                action_hash=action_hash,
                run_attempt=run_attempt,
                batch_id=batch_id,
            )

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

    async def list_for_run(self, run_id: str) -> list[ApprovalRecord]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM approvals WHERE scope_id = :scope AND run_id = :run_id "
                            "ORDER BY created_at"
                        ),
                        {"scope": self._scope_id, "run_id": run_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_record(r) for r in rows]

    async def batch_pending_count(self, run_id: str, batch_id: str) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            count = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM approvals WHERE scope_id = :scope "
                        "AND run_id = :run_id AND batch_id = :batch AND status = 'pending'"
                    ),
                    {"scope": self._scope_id, "run_id": run_id, "batch": batch_id},
                )
            ).scalar_one()
        return int(count)

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
