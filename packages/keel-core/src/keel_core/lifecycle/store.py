"""Durable erasure ledger, session tombstones, and retention overrides (M3.5, WS-K).

An :class:`ErasureStore` is bound to one scope. It persists:

* the erasure *request* (deduped by ``(scope_id, idempotency_key)``),
* a per-request *step ledger* so a crash/resume skips already-``done`` work and every
  store cleanup is idempotent, and
* *session tombstones* — the anti-resurrection record consulted by projection rebuilds.

The ``retention_policies`` overrides live here too so operators can adjust a scope's
retention without a code change.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.lifecycle.models import (
    ErasureRequest,
    ErasureStatus,
    ErasureStep,
    ErasureTarget,
    ErasureTargetKind,
    StepStatus,
)
from keel_core.lifecycle.policies import RetentionClass, RetentionPolicy

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


def _now() -> datetime:
    return datetime.now(UTC)


@runtime_checkable
class ErasureStore(Protocol):
    """Durable seam for the erasure coordinator (scope-bound)."""

    @property
    def scope_id(self) -> str: ...

    async def create(
        self,
        target: ErasureTarget,
        idempotency_key: str,
        *,
        request_id: str,
        requested_by: str | None = None,
        reason: str | None = None,
    ) -> ErasureRequest: ...

    async def get(self, request_id: str) -> ErasureRequest | None: ...

    async def list_requests(
        self, *, status: ErasureStatus | None = None, limit: int = 50
    ) -> list[ErasureRequest]: ...

    async def mark_running(self, request_id: str) -> ErasureRequest | None: ...

    async def bump_attempt(self, request_id: str, *, error: str | None = None) -> None: ...

    async def record_step(
        self,
        request_id: str,
        step: str,
        status: StepStatus,
        *,
        rows_affected: int = 0,
        detail: str | None = None,
    ) -> None: ...

    async def steps(self, request_id: str) -> list[ErasureStep]: ...

    async def finalize(
        self, request_id: str, status: ErasureStatus, *, external_incomplete: bool
    ) -> ErasureRequest | None: ...

    async def add_tombstone(
        self, session_id: str, *, request_id: str | None = None, reason: str | None = None
    ) -> None: ...

    async def tombstoned_sessions(self) -> set[str]: ...

    async def is_tombstoned(self, session_id: str) -> bool: ...

    async def set_retention(
        self, resource_class: str, retention_class: RetentionClass, ttl_seconds: int | None
    ) -> None: ...

    async def retention_overrides(self) -> dict[str, RetentionPolicy]: ...


class InMemoryErasureStore:
    """Non-durable erasure ledger (unit tests / lite profile)."""

    def __init__(self, scope_id: str) -> None:
        self._scope_id = scope_id
        self._requests: dict[str, ErasureRequest] = {}
        self._by_key: dict[str, str] = {}
        self._steps: dict[str, dict[str, ErasureStep]] = {}
        self._tombstones: set[str] = set()
        self._retention: dict[str, RetentionPolicy] = {}

    @property
    def scope_id(self) -> str:
        return self._scope_id

    async def create(
        self,
        target: ErasureTarget,
        idempotency_key: str,
        *,
        request_id: str,
        requested_by: str | None = None,
        reason: str | None = None,
    ) -> ErasureRequest:
        if target.scope_id != self._scope_id:
            raise ValueError("target scope does not match store scope")
        existing_id = self._by_key.get(idempotency_key)
        if existing_id is not None:
            return self._requests[existing_id]
        now = _now()
        request = ErasureRequest(
            id=request_id,
            scope_id=self._scope_id,
            target_kind=target.kind,
            target_id=target.resource_id,
            idempotency_key=idempotency_key,
            status=ErasureStatus.pending,
            requested_by=requested_by,
            reason=reason,
            created_at=now,
            updated_at=now,
        )
        self._requests[request_id] = request
        self._by_key[idempotency_key] = request_id
        self._steps.setdefault(request_id, {})
        return request

    async def get(self, request_id: str) -> ErasureRequest | None:
        return self._requests.get(request_id)

    async def list_requests(
        self, *, status: ErasureStatus | None = None, limit: int = 50
    ) -> list[ErasureRequest]:
        rows = sorted(
            self._requests.values(),
            key=lambda r: r.created_at or _now(),
            reverse=True,
        )
        if status is not None:
            rows = [r for r in rows if r.status is status]
        return rows[:limit]

    async def mark_running(self, request_id: str) -> ErasureRequest | None:
        return self._update(request_id, status=ErasureStatus.running)

    async def bump_attempt(self, request_id: str, *, error: str | None = None) -> None:
        current = self._requests.get(request_id)
        if current is None:
            return
        self._requests[request_id] = replace(
            current, attempts=current.attempts + 1, last_error=error, updated_at=_now()
        )

    async def record_step(
        self,
        request_id: str,
        step: str,
        status: StepStatus,
        *,
        rows_affected: int = 0,
        detail: str | None = None,
    ) -> None:
        ledger = self._steps.setdefault(request_id, {})
        ledger[step] = ErasureStep(
            request_id=request_id,
            scope_id=self._scope_id,
            step=step,
            status=status,
            rows_affected=rows_affected,
            detail=detail,
            updated_at=_now(),
        )

    async def steps(self, request_id: str) -> list[ErasureStep]:
        return list(self._steps.get(request_id, {}).values())

    async def finalize(
        self, request_id: str, status: ErasureStatus, *, external_incomplete: bool
    ) -> ErasureRequest | None:
        now = _now()
        return self._update(
            request_id,
            status=status,
            external_incomplete=external_incomplete,
            completed_at=now,
        )

    async def add_tombstone(
        self, session_id: str, *, request_id: str | None = None, reason: str | None = None
    ) -> None:
        self._tombstones.add(session_id)

    async def tombstoned_sessions(self) -> set[str]:
        return set(self._tombstones)

    async def is_tombstoned(self, session_id: str) -> bool:
        return session_id in self._tombstones

    async def set_retention(
        self, resource_class: str, retention_class: RetentionClass, ttl_seconds: int | None
    ) -> None:
        self._retention[resource_class] = RetentionPolicy(
            resource_class, retention_class, ttl_seconds
        )

    async def retention_overrides(self) -> dict[str, RetentionPolicy]:
        return dict(self._retention)

    def _update(self, request_id: str, **changes: Any) -> ErasureRequest | None:
        current = self._requests.get(request_id)
        if current is None:
            return None
        changes.setdefault("updated_at", _now())
        updated = replace(current, **changes)
        self._requests[request_id] = updated
        return updated


def _to_request(row: Any) -> ErasureRequest:
    return ErasureRequest(
        id=row["id"],
        scope_id=row["scope_id"],
        target_kind=ErasureTargetKind(row["target_kind"]),
        target_id=row["target_id"],
        idempotency_key=row["idempotency_key"],
        status=ErasureStatus(row["status"]),
        requested_by=row["requested_by"],
        reason=row["reason"],
        external_incomplete=bool(row["external_incomplete"]),
        attempts=int(row["attempts"]),
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
    )


_REQUEST_COLUMNS = (
    "id, scope_id, target_kind, target_id, idempotency_key, status, requested_by, reason, "
    "external_incomplete, attempts, last_error, created_at, updated_at, completed_at"
)


class PostgresErasureStore:
    """Durable, scope-bound erasure ledger over Postgres (M3.5 lifecycle tables)."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    @property
    def scope_id(self) -> str:
        return self._scope_id

    async def create(
        self,
        target: ErasureTarget,
        idempotency_key: str,
        *,
        request_id: str,
        requested_by: str | None = None,
        reason: str | None = None,
    ) -> ErasureRequest:
        if target.scope_id != self._scope_id:
            raise ValueError("target scope does not match store scope")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "INSERT INTO erasure_requests "
                    "(id, scope_id, target_kind, target_id, idempotency_key, requested_by, reason) "
                    "VALUES (:id, :scope, :kind, :target, :key, :by, :reason) "
                    "ON CONFLICT (scope_id, idempotency_key) DO NOTHING"
                ),
                {
                    "id": request_id,
                    "scope": self._scope_id,
                    "kind": target.kind.value,
                    "target": target.resource_id,
                    "key": idempotency_key,
                    "by": requested_by,
                    "reason": reason,
                },
            )
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_REQUEST_COLUMNS} FROM erasure_requests "
                            "WHERE scope_id = :scope AND idempotency_key = :key"
                        ),
                        {"scope": self._scope_id, "key": idempotency_key},
                    )
                )
                .mappings()
                .one()
            )
        return _to_request(row)

    async def get(self, request_id: str) -> ErasureRequest | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_REQUEST_COLUMNS} FROM erasure_requests "
                            "WHERE scope_id = :scope AND id = :id"
                        ),
                        {"scope": self._scope_id, "id": request_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_request(row)

    async def list_requests(
        self, *, status: ErasureStatus | None = None, limit: int = 50
    ) -> list[ErasureRequest]:
        sql = f"SELECT {_REQUEST_COLUMNS} FROM erasure_requests WHERE scope_id = :scope"
        params: dict[str, Any] = {"scope": self._scope_id, "limit": limit}
        if status is not None:
            sql += " AND status = :status"
            params["status"] = status.value
        sql += " ORDER BY created_at DESC LIMIT :limit"
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (await conn.execute(text(sql), params)).mappings().all()
        return [_to_request(row) for row in rows]

    async def mark_running(self, request_id: str) -> ErasureRequest | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE erasure_requests SET status = 'running', updated_at = now() "
                            "WHERE scope_id = :scope AND id = :id "
                            "AND status IN ('pending', 'running', 'failed', 'partial') "
                            f"RETURNING {_REQUEST_COLUMNS}"
                        ),
                        {"scope": self._scope_id, "id": request_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_request(row)

    async def bump_attempt(self, request_id: str, *, error: str | None = None) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "UPDATE erasure_requests SET attempts = attempts + 1, last_error = :error, "
                    "updated_at = now() WHERE scope_id = :scope AND id = :id"
                ),
                {"scope": self._scope_id, "id": request_id, "error": error},
            )

    async def record_step(
        self,
        request_id: str,
        step: str,
        status: StepStatus,
        *,
        rows_affected: int = 0,
        detail: str | None = None,
    ) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "INSERT INTO erasure_steps "
                    "(request_id, scope_id, step, status, rows_affected, detail) "
                    "VALUES (:req, :scope, :step, :status, :rows, :detail) "
                    "ON CONFLICT (request_id, step) DO UPDATE "
                    "SET status = EXCLUDED.status, rows_affected = EXCLUDED.rows_affected, "
                    "detail = EXCLUDED.detail, updated_at = now()"
                ),
                {
                    "req": request_id,
                    "scope": self._scope_id,
                    "step": step,
                    "status": status.value,
                    "rows": rows_affected,
                    "detail": detail,
                },
            )

    async def steps(self, request_id: str) -> list[ErasureStep]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT request_id, scope_id, step, status, rows_affected, detail, "
                            "updated_at FROM erasure_steps "
                            "WHERE scope_id = :scope AND request_id = :req ORDER BY step"
                        ),
                        {"scope": self._scope_id, "req": request_id},
                    )
                )
                .mappings()
                .all()
            )
        return [
            ErasureStep(
                request_id=row["request_id"],
                scope_id=row["scope_id"],
                step=row["step"],
                status=StepStatus(row["status"]),
                rows_affected=int(row["rows_affected"]),
                detail=row["detail"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def finalize(
        self, request_id: str, status: ErasureStatus, *, external_incomplete: bool
    ) -> ErasureRequest | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE erasure_requests SET status = :status, "
                            "external_incomplete = :ext, completed_at = now(), updated_at = now() "
                            "WHERE scope_id = :scope AND id = :id "
                            f"RETURNING {_REQUEST_COLUMNS}"
                        ),
                        {
                            "scope": self._scope_id,
                            "id": request_id,
                            "status": status.value,
                            "ext": external_incomplete,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_request(row)

    async def add_tombstone(
        self, session_id: str, *, request_id: str | None = None, reason: str | None = None
    ) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "INSERT INTO event_tombstones (scope_id, session_id, request_id, reason) "
                    "VALUES (:scope, :session, :req, :reason) "
                    "ON CONFLICT (scope_id, session_id) DO NOTHING"
                ),
                {
                    "scope": self._scope_id,
                    "session": session_id,
                    "req": request_id,
                    "reason": reason,
                },
            )

    async def tombstoned_sessions(self) -> set[str]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text("SELECT session_id FROM event_tombstones WHERE scope_id = :scope"),
                    {"scope": self._scope_id},
                )
            ).all()
        return {row.session_id for row in rows}

    async def is_tombstoned(self, session_id: str) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "SELECT 1 FROM event_tombstones "
                        "WHERE scope_id = :scope AND session_id = :session"
                    ),
                    {"scope": self._scope_id, "session": session_id},
                )
            ).one_or_none()
        return row is not None

    async def set_retention(
        self, resource_class: str, retention_class: RetentionClass, ttl_seconds: int | None
    ) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "INSERT INTO retention_policies "
                    "(scope_id, resource_class, retention_class, ttl_seconds) "
                    "VALUES (:scope, :rc, :cls, :ttl) "
                    "ON CONFLICT (scope_id, resource_class) DO UPDATE "
                    "SET retention_class = EXCLUDED.retention_class, "
                    "ttl_seconds = EXCLUDED.ttl_seconds, updated_at = now()"
                ),
                {
                    "scope": self._scope_id,
                    "rc": resource_class,
                    "cls": retention_class.value,
                    "ttl": ttl_seconds,
                },
            )

    async def retention_overrides(self) -> dict[str, RetentionPolicy]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT resource_class, retention_class, ttl_seconds "
                            "FROM retention_policies WHERE scope_id = :scope"
                        ),
                        {"scope": self._scope_id},
                    )
                )
                .mappings()
                .all()
            )
        return {
            row["resource_class"]: RetentionPolicy(
                row["resource_class"],
                RetentionClass(row["retention_class"]),
                None if row["ttl_seconds"] is None else int(row["ttl_seconds"]),
            )
            for row in rows
        }


__all__ = ["ErasureStore", "InMemoryErasureStore", "PostgresErasureStore"]
