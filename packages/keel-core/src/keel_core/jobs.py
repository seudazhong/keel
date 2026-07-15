"""Durable background-job contracts and stores (ADR-0010)."""

from __future__ import annotations

import asyncio
import builtins
import copy
import hashlib
import json
import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.config import Settings
from keel_core.events import Event, EventType
from keel_core.state import InMemoryEventStore, append_event_in_transaction

_MAX_JSON_DEPTH = 100
_MAX_ERROR_CODE_CHARS = 128
_MAX_IDENTITY_BYTES = 512
_PG_INTEGER_MAX = 2**31 - 1
_PG_BIGINT_MAX = 2**63 - 1


def _ensure_storage_safe_text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError(f"{field} must be storage-safe UTF-8 text without NUL characters")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} must be storage-safe UTF-8 text") from exc
    return value


class JobStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class JobTerminalIntent(StrEnum):
    failed = "failed"
    cancelled = "cancelled"


_TERMINAL_STATUSES = frozenset({JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled})


class CancelMode(StrEnum):
    immediate = "immediate"
    cooperative = "cooperative"
    disabled = "disabled"


class _PublicJobException(Exception):
    def __init__(self, code: str, public_message: str) -> None:
        code = code.strip()
        public_message = public_message.strip()
        if not code:
            raise ValueError("code must not be empty")
        if not public_message:
            raise ValueError("public_message must not be empty")
        code = _ensure_storage_safe_text(code, field="code")
        public_message = _ensure_storage_safe_text(public_message, field="public_message")
        if len(code) > _MAX_ERROR_CODE_CHARS:
            raise ValueError(f"code must not exceed {_MAX_ERROR_CODE_CHARS} characters")
        self.code = code
        self.public_message = public_message
        super().__init__(public_message)


class RetryableJobError(_PublicJobException):
    """A handler failure that may be retried while attempts remain."""


class PermanentJobError(_PublicJobException):
    """A handler failure that must become terminal immediately."""


class JobValidationError(_PublicJobException):
    """A bounded public validation failure raised by the framework/store."""

    def __str__(self) -> str:
        return f"{self.code}: {self.public_message}"


class JobLeaseLostError(Exception):
    """The operation did not hold the current running lease token."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"job lease lost: {job_id}")


class JobCancellationRequested(Exception):
    """Internal cooperative-cancellation signal raised at a checkpoint."""


def _validated_storage_text(value: str, *, field: str) -> str:
    try:
        return _ensure_storage_safe_text(value, field=field)
    except ValueError as exc:
        raise JobValidationError(
            "storage_text_invalid", f"{field} must be storage-safe UTF-8 text"
        ) from exc


def _normalize_json_value(
    value: Any,
    *,
    field: str,
    seen_containers: set[int] | None = None,
    depth: int = 0,
) -> Any:
    if depth > _MAX_JSON_DEPTH:
        raise JobValidationError(
            "json_too_deep",
            f"{field} exceeds the maximum JSON nesting depth of {_MAX_JSON_DEPTH}",
        )
    seen = seen_containers if seen_containers is not None else set()
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise JobValidationError(
                "json_serializable", f"{field} must contain only finite JSON numbers"
            )
        return value
    if isinstance(value, str):
        return _validated_storage_text(value, field=field)
    if isinstance(value, list):
        identity = id(value)
        if identity in seen:
            raise JobValidationError("json_cycle", f"{field} must not contain cyclic references")
        seen.add(identity)
        try:
            return [
                _normalize_json_value(
                    item,
                    field=field,
                    seen_containers=seen,
                    depth=depth + 1,
                )
                for item in value
            ]
        finally:
            seen.remove(identity)
    if isinstance(value, dict):
        identity = id(value)
        if identity in seen:
            raise JobValidationError("json_cycle", f"{field} must not contain cyclic references")
        seen.add(identity)
        normalized: dict[str, Any] = {}
        try:
            for key, item in value.items():
                if not isinstance(key, str):
                    raise JobValidationError(
                        "json_native_required", f"{field} object keys must be strings"
                    )
                safe_key = _validated_storage_text(key, field=f"{field} object key")
                normalized[safe_key] = _normalize_json_value(
                    item,
                    field=field,
                    seen_containers=seen,
                    depth=depth + 1,
                )
            return normalized
        finally:
            seen.remove(identity)
    raise JobValidationError(
        "json_native_required",
        f"{field} must contain only JSON-native objects, arrays, and scalar values",
    )


def _validated_json_object(value: dict[str, Any], *, field: str, max_bytes: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise JobValidationError("json_object_required", f"{field} must be a JSON object")
    try:
        normalized = cast(dict[str, Any], _normalize_json_value(value, field=field))
    except RecursionError as exc:  # defensive if the runtime recursion limit is unusually low
        raise JobValidationError(
            "json_too_deep", f"{field} exceeds the maximum JSON nesting depth"
        ) from exc
    try:
        encoded = json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise JobValidationError(
            "json_serializable", f"{field} must contain only JSON-serializable values"
        ) from exc
    if len(encoded) > max_bytes:
        raise JobValidationError(
            f"{field}_too_large", f"{field} exceeds {max_bytes} UTF-8 JSON bytes"
        )
    return normalized


@dataclass(frozen=True)
class JobLimits:
    payload_max_bytes: int = 65_536
    result_max_bytes: int = 65_536
    progress_message_max_chars: int = 1_000
    result_message_max_chars: int = 8_000
    error_message_max_chars: int = 2_000

    def __post_init__(self) -> None:
        if (
            min(
                self.payload_max_bytes,
                self.result_max_bytes,
                self.progress_message_max_chars,
                self.result_message_max_chars,
                self.error_message_max_chars,
            )
            <= 0
        ):
            raise ValueError("job limits must be positive")

    @classmethod
    def from_settings(cls, settings: Settings) -> JobLimits:
        return cls(
            payload_max_bytes=settings.job_payload_max_bytes,
            result_max_bytes=settings.job_result_max_bytes,
            progress_message_max_chars=settings.job_progress_message_max_chars,
            result_message_max_chars=settings.job_result_message_max_chars,
            error_message_max_chars=settings.job_error_message_max_chars,
        )

    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _validated_json_object(payload, field="payload", max_bytes=self.payload_max_bytes)

    def validate_result(self, result: dict[str, Any]) -> dict[str, Any]:
        return _validated_json_object(result, field="result", max_bytes=self.result_max_bytes)

    def result_message(self, value: str) -> str:
        normalized = _validated_storage_text(value, field="result_message").strip()
        if not normalized:
            raise JobValidationError("storage_text_invalid", "result_message must not be blank")
        return normalized[: self.result_message_max_chars]

    def error_message(self, value: str) -> str:
        normalized = _validated_storage_text(value, field="error_message").strip()
        if not normalized:
            raise JobValidationError("storage_text_invalid", "error_message must not be blank")
        return normalized[: self.error_message_max_chars]

    def progress_message(self, value: str) -> str:
        return _validated_storage_text(value, field="progress_message")[
            : self.progress_message_max_chars
        ]


@dataclass(frozen=True)
class JobRecord:
    id: str
    scope_id: str
    kind: str
    status: JobStatus
    cancel_mode: CancelMode
    payload: dict[str, Any]
    target_session_id: str | None
    idempotency_key: str
    attempt: int
    max_attempts: int
    next_attempt_at: datetime
    lease_token: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    cancel_requested_at: datetime | None
    terminal_intent: JobTerminalIntent | None
    terminal_intent_at: datetime | None
    progress_current: int
    progress_total: int | None
    progress_message: str | None
    progress_updated_at: datetime | None
    result: dict[str, Any] | None
    result_message: str | None
    error_kind: str | None
    error_message: str | None
    injected_event_seq: int | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class JobLease:
    job_id: str
    scope_id: str
    token: str
    kind: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    lease_seconds: int


@dataclass(frozen=True)
class JobResult:
    data: dict[str, Any]
    message: str

    def __post_init__(self) -> None:
        if not self.message.strip():
            raise ValueError("JobResult.message must not be empty")
        _ensure_storage_safe_text(self.message, field="JobResult.message")


@dataclass(frozen=True)
class JobError:
    kind: str
    message: str

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("JobError.kind must not be empty")
        if not self.message.strip():
            raise ValueError("JobError.message must not be empty")
        _ensure_storage_safe_text(self.kind, field="JobError.kind")
        _ensure_storage_safe_text(self.message, field="JobError.message")
        if len(self.kind) > _MAX_ERROR_CODE_CHARS:
            raise ValueError(f"JobError.kind must not exceed {_MAX_ERROR_CODE_CHARS} characters")


@dataclass(frozen=True)
class JobProgressResult:
    record: JobRecord
    cancel_requested: bool


class JobStore(Protocol):
    @property
    def scope_id(self) -> str: ...

    async def enqueue_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        cancel_mode: CancelMode = CancelMode.immediate,
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]: ...

    async def get(self, job_id: str) -> JobRecord | None: ...

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> builtins.list[JobRecord]: ...

    async def dispatchable(self, now: datetime, limit: int) -> builtins.list[str]: ...
    async def exhausted(self, now: datetime, limit: int) -> builtins.list[str]: ...

    async def claim(self, job_id: str, now: datetime, lease_seconds: int) -> JobLease | None: ...

    async def heartbeat(self, lease: JobLease, now: datetime) -> bool: ...

    async def progress(
        self,
        lease: JobLease,
        *,
        current: int,
        total: int | None,
        message: str | None,
        now: datetime,
    ) -> JobProgressResult: ...

    async def request_cancel(self, job_id: str, now: datetime) -> JobRecord | None: ...

    async def reserve_terminal(
        self,
        lease: JobLease,
        intent: JobTerminalIntent,
        *,
        now: datetime,
        error: JobError | None = None,
    ) -> JobRecord: ...

    async def finalize_terminal(self, lease: JobLease, now: datetime) -> JobRecord: ...
    async def reserve_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...
    async def finalize_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...

    async def requeue(
        self, lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord: ...

    async def succeed(self, lease: JobLease, result: JobResult, now: datetime) -> JobRecord: ...

    async def fail_terminal(self, lease: JobLease, error: JobError, now: datetime) -> JobRecord: ...

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord: ...
    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...
    async def finish_cancelled_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _final_attempt_lease_expired(row: JobRecord, now: datetime) -> bool:
    return (
        row.status is JobStatus.running
        and row.attempt >= row.max_attempts
        and row.lease_expires_at is not None
        and row.lease_expires_at <= now
    )


def _cancel_precedes_lease_expiry(row: JobRecord) -> bool:
    return (
        row.cancel_requested_at is not None
        and row.lease_expires_at is not None
        and row.cancel_requested_at < row.lease_expires_at
    )


def _attempts_exhausted_error() -> JobError:
    return JobError(
        "attempts_exhausted",
        "job attempts were exhausted after worker lease expiry",
    )


def _reserved_failure_error(row: JobRecord) -> JobError:
    if (
        row.terminal_intent is not JobTerminalIntent.failed
        or row.error_kind is None
        or row.error_message is None
    ):
        raise JobValidationError(
            "terminal_intent_invalid",
            "failed terminal intent requires a persisted job error",
        )
    return JobError(row.error_kind, row.error_message)


def _validated_identity(value: str, *, field: str, code: str) -> str:
    if not isinstance(value, str):
        raise JobValidationError(code, f"{field} must be storage-safe text")
    normalized = value.strip()
    if not normalized:
        raise JobValidationError(code, f"{field} must not be empty")
    try:
        safe = _ensure_storage_safe_text(normalized, field=field)
    except ValueError as exc:
        raise JobValidationError(code, f"{field} must be storage-safe UTF-8 text") from exc
    if len(safe.encode()) > _MAX_IDENTITY_BYTES:
        raise JobValidationError(code, f"{field} must not exceed {_MAX_IDENTITY_BYTES} UTF-8 bytes")
    return safe


def _normalized_utc_timestamp(value: datetime, *, field: str = "timestamp") -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise JobValidationError("timezone_required", f"{field} must include a timezone offset")
    return value.astimezone(UTC)


def _copy_record(record: JobRecord) -> JobRecord:
    return replace(
        record,
        payload=copy.deepcopy(record.payload),
        result=copy.deepcopy(record.result),
    )


def _terminal_message(
    record: JobRecord,
    status: JobStatus,
    *,
    result: JobResult | None,
    error: JobError | None,
) -> str:
    if status is JobStatus.succeeded:
        assert result is not None
        return result.message
    if status is JobStatus.failed:
        assert error is not None
        return f"后台任务 {record.kind} 失败：{error.kind}"
    return f"后台任务 {record.kind} 已取消。"


def _injection_event(record: JobRecord, status: JobStatus, text_value: str, now: datetime) -> Event:
    assert record.target_session_id is not None
    return Event(
        type=EventType.message_token,
        seq=0,
        session_id=record.target_session_id,
        scope_id=record.scope_id,
        ts=now,
        payload={
            "role": "assistant",
            "text": text_value,
            "partial": False,
            "job_id": record.id,
            "job_kind": record.kind,
            "job_status": status.value,
        },
    )


def _validate_limit(limit: int) -> None:
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")


_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


def _validate_enqueue_fields(kind: str, idempotency_key: str, max_attempts: int) -> tuple[str, str]:
    kind = _validated_identity(kind, field="kind", code="invalid_kind")
    idempotency_key = _validated_identity(
        idempotency_key,
        field="idempotency_key",
        code="invalid_idempotency_key",
    )
    if (
        isinstance(max_attempts, bool)
        or not isinstance(max_attempts, int)
        or not 1 <= max_attempts <= _PG_INTEGER_MAX
    ):
        raise JobValidationError(
            "invalid_max_attempts",
            f"max_attempts must be between 1 and {_PG_INTEGER_MAX}",
        )
    return kind, idempotency_key


def _validated_cancel_mode(value: object) -> CancelMode:
    if not isinstance(value, str):
        raise JobValidationError(
            "invalid_cancel_mode",
            "cancel_mode must be immediate, cooperative, or disabled",
        )
    try:
        return CancelMode(value)
    except (TypeError, ValueError) as exc:
        raise JobValidationError(
            "invalid_cancel_mode",
            "cancel_mode must be immediate, cooperative, or disabled",
        ) from exc


def _validated_terminal_intent(value: object) -> JobTerminalIntent:
    if not isinstance(value, str):
        raise JobValidationError(
            "invalid_terminal_intent",
            "terminal intent must be failed or cancelled",
        )
    try:
        return JobTerminalIntent(value)
    except (TypeError, ValueError) as exc:
        raise JobValidationError(
            "invalid_terminal_intent",
            "terminal intent must be failed or cancelled",
        ) from exc


def _optional_read_identity(value: str, *, field: str, code: str) -> str | None:
    try:
        return _validated_identity(value, field=field, code=code)
    except JobValidationError:
        return None


def _job_dedupe_lock_id(scope_id: str, kind: str, idempotency_key: str) -> int:
    encoded = json.dumps(
        [scope_id, kind, idempotency_key],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    digest = hashlib.sha256(encoded).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _to_job_record(row: Mapping[Any, Any]) -> JobRecord:
    payload = row["payload"] if isinstance(row["payload"], dict) else {}
    result = row["result"] if isinstance(row["result"], dict) else None
    return JobRecord(
        id=str(row["id"]),
        scope_id=str(row["scope_id"]),
        kind=str(row["kind"]),
        status=JobStatus(str(row["status"])),
        cancel_mode=CancelMode(str(row["cancel_mode"])),
        payload=copy.deepcopy(payload),
        target_session_id=row["target_session_id"],
        idempotency_key=str(row["idempotency_key"]),
        attempt=int(row["attempt"]),
        max_attempts=int(row["max_attempts"]),
        next_attempt_at=row["next_attempt_at"],
        lease_token=row["lease_token"],
        lease_expires_at=row["lease_expires_at"],
        heartbeat_at=row["heartbeat_at"],
        cancel_requested_at=row["cancel_requested_at"],
        terminal_intent=(
            None
            if row["terminal_intent"] is None
            else JobTerminalIntent(str(row["terminal_intent"]))
        ),
        terminal_intent_at=row["terminal_intent_at"],
        progress_current=int(row["progress_current"]),
        progress_total=None if row["progress_total"] is None else int(row["progress_total"]),
        progress_message=row["progress_message"],
        progress_updated_at=row["progress_updated_at"],
        result=copy.deepcopy(result),
        result_message=row["result_message"],
        error_kind=row["error_kind"],
        error_message=row["error_message"],
        injected_event_seq=(
            None if row["injected_event_seq"] is None else int(row["injected_event_seq"])
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def _validate_progress(current: int, total: int | None) -> None:
    invalid_current = (
        isinstance(current, bool)
        or not isinstance(current, int)
        or not 0 <= current <= _PG_BIGINT_MAX
    )
    invalid_total = total is not None and (
        isinstance(total, bool) or not isinstance(total, int) or not 0 <= total <= _PG_BIGINT_MAX
    )
    if invalid_current or invalid_total or (total is not None and current > total):
        raise JobValidationError(
            "invalid_progress",
            "progress requires PostgreSQL bigint integers with current <= total",
        )


class InMemoryJobStore:
    """Deterministic scope-bound JobStore for unit tests and the lite profile."""

    def __init__(
        self,
        scope_id: str,
        *,
        events: InMemoryEventStore | None = None,
        limits: JobLimits | None = None,
    ) -> None:
        try:
            self._scope_id = _validated_identity(
                scope_id, field="scope_id", code="invalid_scope_id"
            )
        except JobValidationError as exc:
            raise ValueError(exc.public_message) from exc
        self._events = events
        self._limits = limits or JobLimits()
        self._rows: dict[str, JobRecord] = {}
        self._dedupe: dict[tuple[str, str, str], str] = {}
        self._lock = asyncio.Lock()

    @property
    def scope_id(self) -> str:
        return self._scope_id

    def _owned(
        self,
        lease: JobLease,
        now: datetime,
        *,
        allow_terminal_intent: bool = False,
    ) -> JobRecord:
        row = self._rows.get(lease.job_id)
        if (
            row is None
            or lease.scope_id != self._scope_id
            or row.status is not JobStatus.running
            or row.lease_token != lease.token
            or row.lease_expires_at is None
            or row.lease_expires_at <= now
        ):
            raise JobLeaseLostError(lease.job_id)
        if row.terminal_intent is not None and not allow_terminal_intent:
            raise JobLeaseLostError(lease.job_id)
        return row

    def _reserve_terminal_locked(
        self,
        row: JobRecord,
        intent: JobTerminalIntent,
        *,
        now: datetime,
        error: JobError | None = None,
        require_cancel_request: bool = True,
    ) -> JobRecord:
        if row.terminal_intent is not None:
            return row
        if intent is JobTerminalIntent.cancelled:
            if require_cancel_request and row.cancel_requested_at is None:
                raise JobValidationError(
                    "cancellation_not_requested",
                    "job cancellation was not requested",
                )
            updated = replace(
                row,
                terminal_intent=intent,
                terminal_intent_at=now,
                updated_at=now,
            )
        else:
            if error is None:
                raise JobValidationError(
                    "terminal_error_required",
                    "failed terminal intent requires a job error",
                )
            safe_error = JobError(error.kind, self._limits.error_message(error.message))
            updated = replace(
                row,
                terminal_intent=intent,
                terminal_intent_at=now,
                error_kind=safe_error.kind,
                error_message=safe_error.message,
                updated_at=now,
            )
        self._rows[row.id] = updated
        return updated

    def _reserve_exhausted_locked(self, row: JobRecord, now: datetime) -> JobRecord | None:
        if (
            row.status is not JobStatus.running
            or row.lease_expires_at is None
            or row.lease_expires_at > now
        ):
            return None
        if row.terminal_intent is not None:
            return row
        if row.attempt < row.max_attempts:
            return None
        if _cancel_precedes_lease_expiry(row):
            return self._reserve_terminal_locked(
                row,
                JobTerminalIntent.cancelled,
                now=now,
            )
        return self._reserve_terminal_locked(
            row,
            JobTerminalIntent.failed,
            now=now,
            error=_attempts_exhausted_error(),
        )

    async def _finalize_reserved_locked(self, row: JobRecord, now: datetime) -> JobRecord:
        if row.terminal_intent is None:
            raise JobValidationError(
                "terminal_intent_missing",
                "job terminal intent has not been reserved",
            )
        error = (
            _reserved_failure_error(row)
            if row.terminal_intent is JobTerminalIntent.failed
            else None
        )
        return await self._finalize_locked(
            row,
            status=JobStatus(row.terminal_intent.value),
            now=now,
            error=error,
        )

    async def _finalize_locked(
        self,
        row: JobRecord,
        *,
        status: JobStatus,
        now: datetime,
        result: JobResult | None = None,
        error: JobError | None = None,
    ) -> JobRecord:
        safe_result = (
            None if result is None else copy.deepcopy(self._limits.validate_result(result.data))
        )
        safe_error = (
            None
            if error is None
            else JobError(error.kind, self._limits.error_message(error.message))
        )
        text_value = self._limits.result_message(
            _terminal_message(row, status, result=result, error=safe_error)
        )
        injected_seq = row.injected_event_seq
        if row.target_session_id is not None and injected_seq is None:
            if self._events is None or not self._events.has_session(
                row.target_session_id, self._scope_id
            ):
                raise JobValidationError(
                    "target_session_not_found",
                    "target session does not exist in the current scope",
                )
            event = _injection_event(row, status, text_value, now)
            await self._events.append(event)
            injected_seq = event.seq
        updated = replace(
            row,
            status=status,
            lease_token=None,
            lease_expires_at=None,
            heartbeat_at=None,
            result=safe_result if status is JobStatus.succeeded else None,
            result_message=text_value,
            error_kind=safe_error.kind if safe_error is not None else None,
            error_message=safe_error.message if safe_error is not None else None,
            injected_event_seq=injected_seq,
            updated_at=now,
            finished_at=now,
        )
        self._rows[row.id] = updated
        return _copy_record(updated)

    async def enqueue_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        cancel_mode: CancelMode = CancelMode.immediate,
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]:
        kind, idempotency_key = _validate_enqueue_fields(kind, idempotency_key, max_attempts)
        cancel_mode = _validated_cancel_mode(cancel_mode)
        timestamp = _normalized_utc_timestamp(now, field="now") if now is not None else _utcnow()
        key = (self._scope_id, kind, idempotency_key)
        async with self._lock:
            existing_id = self._dedupe.get(key)
            if existing_id is not None:
                return _copy_record(self._rows[existing_id]), False
            safe_payload = copy.deepcopy(self._limits.validate_payload(payload))
            safe_target_session_id = None
            if target_session_id is not None:
                safe_target_session_id = _validated_identity(
                    target_session_id,
                    field="target_session_id",
                    code="invalid_target_session_id",
                )
                if self._events is None or not self._events.has_session(
                    safe_target_session_id, self._scope_id
                ):
                    raise JobValidationError(
                        "target_session_not_found",
                        "target session does not exist in the current scope",
                    )
            job_id = f"job_{uuid.uuid4().hex}"
            record = JobRecord(
                id=job_id,
                scope_id=self._scope_id,
                kind=kind,
                status=JobStatus.queued,
                cancel_mode=cancel_mode,
                payload=safe_payload,
                target_session_id=safe_target_session_id,
                idempotency_key=idempotency_key,
                attempt=0,
                max_attempts=max_attempts,
                next_attempt_at=timestamp,
                lease_token=None,
                lease_expires_at=None,
                heartbeat_at=None,
                cancel_requested_at=None,
                terminal_intent=None,
                terminal_intent_at=None,
                progress_current=0,
                progress_total=None,
                progress_message=None,
                progress_updated_at=None,
                result=None,
                result_message=None,
                error_kind=None,
                error_message=None,
                injected_event_seq=None,
                created_at=timestamp,
                updated_at=timestamp,
                started_at=None,
                finished_at=None,
            )
            self._rows[job_id] = record
            self._dedupe[key] = job_id
            return _copy_record(record), True

    async def get(self, job_id: str) -> JobRecord | None:
        safe_job_id = _optional_read_identity(job_id, field="job_id", code="invalid_job_id")
        if safe_job_id is None:
            return None
        async with self._lock:
            record = self._rows.get(safe_job_id)
            return None if record is None else _copy_record(record)

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> builtins.list[JobRecord]:
        _validate_limit(limit)
        if kind is not None:
            kind = _optional_read_identity(kind, field="kind", code="invalid_kind")
            if kind is None:
                return []
        async with self._lock:
            rows = [
                row
                for row in self._rows.values()
                if (status is None or row.status is status) and (kind is None or row.kind == kind)
            ]
            rows.sort(key=lambda row: (row.created_at, row.id), reverse=True)
            return [_copy_record(row) for row in rows[:limit]]

    async def dispatchable(self, now: datetime, limit: int) -> builtins.list[str]:
        _validate_limit(limit)
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            rows = [
                row
                for row in self._rows.values()
                if row.terminal_intent is None
                and row.attempt < row.max_attempts
                and (
                    (row.status is JobStatus.queued and row.next_attempt_at <= now)
                    or (
                        row.status is JobStatus.running
                        and row.lease_expires_at is not None
                        and row.lease_expires_at <= now
                    )
                )
            ]
            rows.sort(key=lambda row: (row.next_attempt_at, row.created_at, row.id))
            return [row.id for row in rows[:limit]]

    async def exhausted(self, now: datetime, limit: int) -> builtins.list[str]:
        _validate_limit(limit)
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            rows = [
                row
                for row in self._rows.values()
                if row.status is JobStatus.running
                and row.lease_expires_at is not None
                and row.lease_expires_at <= now
                and (row.terminal_intent is not None or row.attempt >= row.max_attempts)
            ]
            rows.sort(key=lambda row: (row.lease_expires_at, row.created_at, row.id))
            return [row.id for row in rows[:limit]]

    async def claim(self, job_id: str, now: datetime, lease_seconds: int) -> JobLease | None:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or lease_seconds < 1
        ):
            raise ValueError("lease_seconds must be positive")
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._rows.get(job_id)
            if row is None or row.terminal_intent is not None or row.attempt >= row.max_attempts:
                return None
            due_queued = row.status is JobStatus.queued and row.next_attempt_at <= now
            expired_running = (
                row.status is JobStatus.running
                and row.lease_expires_at is not None
                and row.lease_expires_at <= now
            )
            if not (due_queued or expired_running):
                return None
            token = uuid.uuid4().hex
            claimed = replace(
                row,
                status=JobStatus.running,
                attempt=row.attempt + 1,
                lease_token=token,
                lease_expires_at=now + timedelta(seconds=lease_seconds),
                heartbeat_at=now,
                progress_current=0,
                progress_total=None,
                progress_message=None,
                progress_updated_at=None,
                updated_at=now,
                started_at=row.started_at or now,
            )
            self._rows[job_id] = claimed
            return JobLease(
                job_id=job_id,
                scope_id=self._scope_id,
                token=token,
                kind=claimed.kind,
                payload=copy.deepcopy(claimed.payload),
                attempt=claimed.attempt,
                max_attempts=claimed.max_attempts,
                lease_seconds=lease_seconds,
            )

    async def heartbeat(self, lease: JobLease, now: datetime) -> bool:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._owned(lease, now)
            updated = replace(
                row,
                heartbeat_at=now,
                lease_expires_at=now + timedelta(seconds=lease.lease_seconds),
                updated_at=now,
            )
            self._rows[row.id] = updated
            return updated.cancel_requested_at is not None

    async def progress(
        self,
        lease: JobLease,
        *,
        current: int,
        total: int | None,
        message: str | None,
        now: datetime,
    ) -> JobProgressResult:
        _validate_progress(current, total)
        safe_message = None if message is None else self._limits.progress_message(message)
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._owned(lease, now)
            if current < row.progress_current:
                raise JobValidationError(
                    "progress_regression",
                    "progress current cannot decrease within one attempt",
                )
            updated = replace(
                row,
                progress_current=current,
                progress_total=total,
                progress_message=safe_message,
                progress_updated_at=now,
                heartbeat_at=now,
                lease_expires_at=now + timedelta(seconds=lease.lease_seconds),
                updated_at=now,
            )
            self._rows[row.id] = updated
            return JobProgressResult(
                record=_copy_record(updated),
                cancel_requested=updated.cancel_requested_at is not None,
            )

    async def request_cancel(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        safe_job_id = _optional_read_identity(job_id, field="job_id", code="invalid_job_id")
        if safe_job_id is None:
            return None
        async with self._lock:
            row = self._rows.get(safe_job_id)
            if row is None:
                return None
            if row.status in _TERMINAL_STATUSES:
                return _copy_record(row)
            if row.cancel_mode is CancelMode.disabled:
                raise JobValidationError(
                    "job_not_cancellable",
                    "job does not allow cancellation",
                )
            if row.terminal_intent is JobTerminalIntent.cancelled:
                return _copy_record(row)
            if row.terminal_intent is JobTerminalIntent.failed:
                raise JobValidationError(
                    "job_finalizing",
                    "job is finalizing and cannot be cancelled",
                )
            if _final_attempt_lease_expired(row, now):
                if _cancel_precedes_lease_expiry(row):
                    return _copy_record(row)
                raise JobValidationError(
                    "job_finalizing",
                    "job is finalizing and cannot be cancelled",
                )
            if row.status is JobStatus.queued:
                if row.cancel_mode is CancelMode.cooperative:
                    updated = replace(
                        row,
                        cancel_requested_at=row.cancel_requested_at or now,
                        next_attempt_at=min(row.next_attempt_at, now),
                        updated_at=now,
                    )
                    self._rows[safe_job_id] = updated
                    return _copy_record(updated)
                reserved = self._reserve_terminal_locked(
                    row,
                    JobTerminalIntent.cancelled,
                    now=now,
                    require_cancel_request=False,
                )
                return await self._finalize_reserved_locked(reserved, now)
            if row.status is JobStatus.running:
                updated = replace(
                    row,
                    cancel_requested_at=row.cancel_requested_at or now,
                    updated_at=now,
                )
                self._rows[safe_job_id] = updated
                return _copy_record(updated)
            return _copy_record(row)

    async def reserve_terminal(
        self,
        lease: JobLease,
        intent: JobTerminalIntent,
        *,
        now: datetime,
        error: JobError | None = None,
    ) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        intent = _validated_terminal_intent(intent)
        async with self._lock:
            row = self._owned(lease, now, allow_terminal_intent=True)
            reserved = self._reserve_terminal_locked(
                row,
                intent,
                now=now,
                error=error,
            )
            return _copy_record(reserved)

    async def finalize_terminal(self, lease: JobLease, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._owned(lease, now, allow_terminal_intent=True)
            return await self._finalize_reserved_locked(row, now)

    async def reserve_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._rows.get(job_id)
            if row is None:
                return None
            reserved = self._reserve_exhausted_locked(row, now)
            return None if reserved is None else _copy_record(reserved)

    async def finalize_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._rows.get(job_id)
            if (
                row is None
                or row.status is not JobStatus.running
                or row.lease_expires_at is None
                or row.lease_expires_at > now
                or row.terminal_intent is None
            ):
                return None
            return await self._finalize_reserved_locked(row, now)

    async def requeue(
        self, lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        retry_at = _normalized_utc_timestamp(retry_at, field="retry_at")
        async with self._lock:
            row = self._owned(lease, now)
            if row.attempt >= row.max_attempts:
                raise JobValidationError(
                    "attempts_exhausted", "job has no retry attempts remaining"
                )
            updated = replace(
                row,
                status=JobStatus.queued,
                next_attempt_at=now if row.cancel_requested_at is not None else retry_at,
                lease_token=None,
                lease_expires_at=None,
                heartbeat_at=None,
                error_kind=error.kind,
                error_message=self._limits.error_message(error.message),
                updated_at=now,
            )
            self._rows[row.id] = updated
            return _copy_record(updated)

    async def succeed(self, lease: JobLease, result: JobResult, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._owned(lease, now)
            return await self._finalize_locked(
                row, status=JobStatus.succeeded, now=now, result=result
            )

    async def fail_terminal(self, lease: JobLease, error: JobError, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._owned(lease, now, allow_terminal_intent=True)
            reserved = self._reserve_terminal_locked(
                row,
                JobTerminalIntent.failed,
                now=now,
                error=error,
            )
            return await self._finalize_reserved_locked(reserved, now)

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._owned(lease, now, allow_terminal_intent=True)
            reserved = self._reserve_terminal_locked(
                row,
                JobTerminalIntent.cancelled,
                now=now,
            )
            return await self._finalize_reserved_locked(reserved, now)

    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._rows.get(job_id)
            if row is None:
                return None
            reserved = self._reserve_exhausted_locked(row, now)
            if reserved is None or reserved.terminal_intent is not JobTerminalIntent.failed:
                return None
            return await self._finalize_reserved_locked(reserved, now)

    async def finish_cancelled_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._rows.get(job_id)
            if row is None:
                return None
            reserved = self._reserve_exhausted_locked(row, now)
            if reserved is None or reserved.terminal_intent is not JobTerminalIntent.cancelled:
                return None
            return await self._finalize_reserved_locked(reserved, now)


class PostgresJobStore:
    """Scope-bound durable JobStore over Postgres with RLS defense in depth."""

    def __init__(
        self,
        engine: AsyncEngine,
        scope_id: str,
        *,
        limits: JobLimits | None = None,
    ) -> None:
        try:
            normalized_scope = _validated_identity(
                scope_id, field="scope_id", code="invalid_scope_id"
            )
        except JobValidationError as exc:
            raise ValueError(exc.public_message) from exc
        self._engine = engine
        self._scope_id = normalized_scope
        self._limits = limits or JobLimits()

    @property
    def scope_id(self) -> str:
        return self._scope_id

    async def enqueue_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        cancel_mode: CancelMode = CancelMode.immediate,
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]:
        kind, idempotency_key = _validate_enqueue_fields(kind, idempotency_key, max_attempts)
        cancel_mode = _validated_cancel_mode(cancel_mode)
        timestamp = _normalized_utc_timestamp(now, field="now") if now is not None else _utcnow()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _job_dedupe_lock_id(self._scope_id, kind, idempotency_key)},
            )
            existing_sql = text(
                "SELECT * FROM jobs WHERE scope_id = :scope "
                "AND kind = :kind AND idempotency_key = :key"
            )
            dedupe_params = {
                "scope": self._scope_id,
                "kind": kind,
                "key": idempotency_key,
            }
            existing = (await conn.execute(existing_sql, dedupe_params)).mappings().one_or_none()
            if existing is not None:
                return _to_job_record(existing), False

            safe_payload = self._limits.validate_payload(payload)
            safe_target_session_id = None
            if target_session_id is not None:
                safe_target_session_id = _validated_identity(
                    target_session_id,
                    field="target_session_id",
                    code="invalid_target_session_id",
                )
                exists = (
                    await conn.execute(
                        text("SELECT 1 FROM sessions WHERE id = :session AND scope_id = :scope"),
                        {"session": safe_target_session_id, "scope": self._scope_id},
                    )
                ).one_or_none()
                if exists is None:
                    raise JobValidationError(
                        "target_session_not_found",
                        "target session does not exist in the current scope",
                    )
            job_id = f"job_{uuid.uuid4().hex}"
            inserted = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO jobs "
                            "(id, scope_id, kind, payload, target_session_id, "
                            "idempotency_key, max_attempts, cancel_mode, next_attempt_at, "
                            "created_at, updated_at) VALUES "
                            "(:id, :scope, :kind, CAST(:payload AS jsonb), :target, "
                            ":key, :max_attempts, :cancel_mode, :now, :now, :now) "
                            "ON CONFLICT (scope_id, kind, idempotency_key) DO NOTHING "
                            "RETURNING *"
                        ),
                        {
                            "id": job_id,
                            "scope": self._scope_id,
                            "kind": kind,
                            "payload": json.dumps(safe_payload, ensure_ascii=False),
                            "target": safe_target_session_id,
                            "key": idempotency_key,
                            "max_attempts": max_attempts,
                            "cancel_mode": cancel_mode.value,
                            "now": timestamp,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if inserted is not None:
                return _to_job_record(inserted), True
            existing = (await conn.execute(existing_sql, dedupe_params)).mappings().one()
            return _to_job_record(existing), False

    async def get(self, job_id: str) -> JobRecord | None:
        safe_job_id = _optional_read_identity(job_id, field="job_id", code="invalid_job_id")
        if safe_job_id is None:
            return None
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM jobs WHERE id = :id AND scope_id = :scope"),
                        {"id": safe_job_id, "scope": self._scope_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_job_record(row)

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> builtins.list[JobRecord]:
        _validate_limit(limit)
        if kind is not None:
            kind = _optional_read_identity(kind, field="kind", code="invalid_kind")
            if kind is None:
                return []
        clauses = ["scope_id = :scope"]
        params: dict[str, Any] = {"scope": self._scope_id, "limit": limit}
        if status is not None:
            clauses.append("status = :status")
            params["status"] = status.value
        if kind is not None:
            clauses.append("kind = :kind")
            params["kind"] = kind
        sql = (
            "SELECT * FROM jobs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at DESC, id DESC LIMIT :limit"
        )
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (await conn.execute(text(sql), params)).mappings().all()
        return [_to_job_record(row) for row in rows]

    async def dispatchable(self, now: datetime, limit: int) -> builtins.list[str]:
        _validate_limit(limit)
        now = _normalized_utc_timestamp(now, field="now")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT id FROM jobs WHERE scope_id = :scope "
                            "AND terminal_intent IS NULL "
                            "AND attempt < max_attempts AND ("
                            "  (status = 'queued' AND next_attempt_at <= :now) OR "
                            "  (status = 'running' AND lease_expires_at <= :now)"
                            ") ORDER BY next_attempt_at, created_at, id LIMIT :limit"
                        ),
                        {"scope": self._scope_id, "now": now, "limit": limit},
                    )
                )
                .scalars()
                .all()
            )
        return [str(value) for value in rows]

    async def exhausted(self, now: datetime, limit: int) -> builtins.list[str]:
        _validate_limit(limit)
        now = _normalized_utc_timestamp(now, field="now")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT id FROM jobs WHERE scope_id = :scope "
                            "AND status = 'running' AND lease_expires_at <= :now "
                            "AND (terminal_intent IS NOT NULL OR attempt >= max_attempts) "
                            "ORDER BY lease_expires_at, created_at, id LIMIT :limit"
                        ),
                        {"scope": self._scope_id, "now": now, "limit": limit},
                    )
                )
                .scalars()
                .all()
            )
        return [str(value) for value in rows]

    async def claim(self, job_id: str, now: datetime, lease_seconds: int) -> JobLease | None:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or lease_seconds < 1
        ):
            raise ValueError("lease_seconds must be positive")
        now = _normalized_utc_timestamp(now, field="now")
        token = uuid.uuid4().hex
        expires = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE jobs SET "
                            "status = 'running', attempt = attempt + 1, "
                            "lease_token = :token, lease_expires_at = :expires, "
                            "heartbeat_at = :now, progress_current = 0, "
                            "progress_total = NULL, progress_message = NULL, "
                            "progress_updated_at = NULL, updated_at = :now, "
                            "started_at = COALESCE(started_at, :now) "
                            "WHERE id = :id AND scope_id = :scope "
                            "AND terminal_intent IS NULL "
                            "AND attempt < max_attempts AND ("
                            "  (status = 'queued' AND next_attempt_at <= :now) OR "
                            "  (status = 'running' AND lease_expires_at <= :now)"
                            ") RETURNING *"
                        ),
                        {
                            "token": token,
                            "expires": expires,
                            "now": now,
                            "id": job_id,
                            "scope": self._scope_id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        record = _to_job_record(row)
        return JobLease(
            job_id=record.id,
            scope_id=record.scope_id,
            token=token,
            kind=record.kind,
            payload=record.payload,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            lease_seconds=lease_seconds,
        )

    async def _locked_owned_row(
        self,
        conn: AsyncConnection,
        lease: JobLease,
        now: datetime,
        *,
        allow_terminal_intent: bool = False,
    ) -> Mapping[Any, Any]:
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM jobs WHERE id = :id AND scope_id = :scope FOR UPDATE"),
                    {"id": lease.job_id, "scope": self._scope_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["status"] != JobStatus.running.value
            or row["lease_token"] != lease.token
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= now
            or (row["terminal_intent"] is not None and not allow_terminal_intent)
        ):
            raise JobLeaseLostError(lease.job_id)
        return row

    async def _reserve_terminal_in_transaction(
        self,
        conn: AsyncConnection,
        locked: Mapping[Any, Any],
        intent: JobTerminalIntent,
        *,
        now: datetime,
        error: JobError | None = None,
        require_cancel_request: bool = True,
    ) -> Mapping[Any, Any]:
        current = _to_job_record(locked)
        if current.terminal_intent is not None:
            return locked
        if intent is JobTerminalIntent.cancelled:
            if require_cancel_request and current.cancel_requested_at is None:
                raise JobValidationError(
                    "cancellation_not_requested",
                    "job cancellation was not requested",
                )
            sql = text(
                "UPDATE jobs SET terminal_intent = :intent, "
                "terminal_intent_at = :now, updated_at = :now "
                "WHERE id = :id AND scope_id = :scope RETURNING *"
            )
            params = {
                "intent": intent.value,
                "now": now,
                "id": current.id,
                "scope": self._scope_id,
            }
        else:
            if error is None:
                raise JobValidationError(
                    "terminal_error_required",
                    "failed terminal intent requires a job error",
                )
            safe_error = JobError(error.kind, self._limits.error_message(error.message))
            sql = text(
                "UPDATE jobs SET terminal_intent = :intent, "
                "terminal_intent_at = :now, error_kind = :error_kind, "
                "error_message = :error_message, updated_at = :now "
                "WHERE id = :id AND scope_id = :scope RETURNING *"
            )
            params = {
                "intent": intent.value,
                "error_kind": safe_error.kind,
                "error_message": safe_error.message,
                "now": now,
                "id": current.id,
                "scope": self._scope_id,
            }
        return (await conn.execute(sql, params)).mappings().one()

    async def _reserve_exhausted_in_transaction(
        self,
        conn: AsyncConnection,
        job_id: str,
        now: datetime,
        *,
        locked: Mapping[Any, Any] | None = None,
    ) -> Mapping[Any, Any] | None:
        if locked is None:
            locked = (
                (
                    await conn.execute(
                        text("SELECT * FROM jobs WHERE id = :id AND scope_id = :scope FOR UPDATE"),
                        {"id": job_id, "scope": self._scope_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        if locked is None:
            return None
        current = _to_job_record(locked)
        if (
            current.status is not JobStatus.running
            or current.lease_expires_at is None
            or current.lease_expires_at > now
        ):
            return None
        if current.terminal_intent is not None:
            return locked
        if current.attempt < current.max_attempts:
            return None
        if _cancel_precedes_lease_expiry(current):
            return await self._reserve_terminal_in_transaction(
                conn,
                locked,
                JobTerminalIntent.cancelled,
                now=now,
            )
        return await self._reserve_terminal_in_transaction(
            conn,
            locked,
            JobTerminalIntent.failed,
            now=now,
            error=_attempts_exhausted_error(),
        )

    async def _finalize_reserved_in_transaction(
        self,
        conn: AsyncConnection,
        locked: Mapping[Any, Any],
        *,
        now: datetime,
        lease_token: str | None = None,
        expired_intent: bool = False,
    ) -> JobRecord | None:
        current = _to_job_record(locked)
        if current.terminal_intent is None:
            raise JobValidationError(
                "terminal_intent_missing",
                "job terminal intent has not been reserved",
            )
        error = (
            _reserved_failure_error(current)
            if current.terminal_intent is JobTerminalIntent.failed
            else None
        )
        return await self._finalize_in_transaction(
            conn,
            job_id=current.id,
            status=JobStatus(current.terminal_intent.value),
            now=now,
            lease_token=lease_token,
            error=error,
            expired_intent=expired_intent,
            locked=locked,
        )

    async def heartbeat(self, lease: JobLease, now: datetime) -> bool:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await self._locked_owned_row(conn, lease, now)
            cancel_requested_at = (
                await conn.execute(
                    text(
                        "UPDATE jobs SET heartbeat_at = :now, "
                        "lease_expires_at = :expires, updated_at = :now "
                        "WHERE id = :id AND scope_id = :scope AND lease_token = :token "
                        "RETURNING cancel_requested_at"
                    ),
                    {
                        "now": now,
                        "expires": now + timedelta(seconds=lease.lease_seconds),
                        "id": lease.job_id,
                        "scope": self._scope_id,
                        "token": lease.token,
                    },
                )
            ).scalar_one()
        return cancel_requested_at is not None

    async def progress(
        self,
        lease: JobLease,
        *,
        current: int,
        total: int | None,
        message: str | None,
        now: datetime,
    ) -> JobProgressResult:
        _validate_progress(current, total)
        safe_message = None if message is None else self._limits.progress_message(message)
        now = _normalized_utc_timestamp(now, field="now")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = await self._locked_owned_row(conn, lease, now)
            if current < int(locked["progress_current"]):
                raise JobValidationError(
                    "progress_regression",
                    "progress current cannot decrease within one attempt",
                )
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE jobs SET progress_current = :current, "
                            "progress_total = :total, progress_message = :message, "
                            "progress_updated_at = :now, heartbeat_at = :now, "
                            "lease_expires_at = :expires, updated_at = :now "
                            "WHERE id = :id AND scope_id = :scope "
                            "AND status = 'running' AND lease_token = :token "
                            "RETURNING *"
                        ),
                        {
                            "current": current,
                            "total": total,
                            "message": safe_message,
                            "now": now,
                            "expires": now + timedelta(seconds=lease.lease_seconds),
                            "id": lease.job_id,
                            "scope": self._scope_id,
                            "token": lease.token,
                        },
                    )
                )
                .mappings()
                .one()
            )
        record = _to_job_record(row)
        return JobProgressResult(
            record=record,
            cancel_requested=record.cancel_requested_at is not None,
        )

    async def requeue(
        self, lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        retry_at = _normalized_utc_timestamp(retry_at, field="retry_at")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = await self._locked_owned_row(conn, lease, now)
            if int(locked["attempt"]) >= int(locked["max_attempts"]):
                raise JobValidationError(
                    "attempts_exhausted", "job has no retry attempts remaining"
                )
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE jobs SET status = 'queued', "
                            "next_attempt_at = CASE "
                            "WHEN cancel_requested_at IS NULL THEN :retry_at ELSE :now END, "
                            "lease_token = NULL, "
                            "lease_expires_at = NULL, heartbeat_at = NULL, "
                            "error_kind = :error_kind, error_message = :error_message, "
                            "updated_at = :now "
                            "WHERE id = :id AND scope_id = :scope "
                            "AND status = 'running' AND lease_token = :token "
                            "RETURNING *"
                        ),
                        {
                            "retry_at": retry_at,
                            "error_kind": error.kind,
                            "error_message": self._limits.error_message(error.message),
                            "now": now,
                            "id": lease.job_id,
                            "scope": self._scope_id,
                            "token": lease.token,
                        },
                    )
                )
                .mappings()
                .one()
            )
        return _to_job_record(row)

    async def _finalize_in_transaction(
        self,
        conn: AsyncConnection,
        *,
        job_id: str,
        status: JobStatus,
        now: datetime,
        lease_token: str | None = None,
        result: JobResult | None = None,
        error: JobError | None = None,
        expired_intent: bool = False,
        locked: Mapping[str, Any] | None = None,
    ) -> JobRecord | None:
        if locked is None:
            locked = cast(
                Mapping[str, Any] | None,
                (
                    await conn.execute(
                        text("SELECT * FROM jobs WHERE id = :id AND scope_id = :scope FOR UPDATE"),
                        {"id": job_id, "scope": self._scope_id},
                    )
                )
                .mappings()
                .one_or_none(),
            )
        if locked is None:
            return None
        current = _to_job_record(locked)
        if expired_intent:
            if (
                current.status is not JobStatus.running
                or current.lease_expires_at is None
                or current.lease_expires_at > now
                or current.terminal_intent is None
            ):
                return None
        elif lease_token is not None:
            if (
                current.status is not JobStatus.running
                or current.lease_token != lease_token
                or current.lease_expires_at is None
                or current.lease_expires_at <= now
            ):
                raise JobLeaseLostError(job_id)
        elif current.status is not JobStatus.queued:
            raise JobLeaseLostError(job_id)
        if status in {JobStatus.failed, JobStatus.cancelled}:
            expected_intent = JobTerminalIntent(status.value)
            if current.terminal_intent is not expected_intent:
                raise JobValidationError(
                    "terminal_intent_missing",
                    "job terminal intent has not been reserved",
                )
        elif current.terminal_intent is not None:
            raise JobLeaseLostError(job_id)

        safe_result = None if result is None else self._limits.validate_result(result.data)
        safe_error = (
            None
            if error is None
            else JobError(error.kind, self._limits.error_message(error.message))
        )
        text_value = self._limits.result_message(
            _terminal_message(current, status, result=result, error=safe_error)
        )
        injected_seq = current.injected_event_seq
        if current.target_session_id is not None and injected_seq is None:
            target = (
                await conn.execute(
                    text(
                        "SELECT id FROM sessions WHERE id = :session "
                        "AND scope_id = :scope FOR UPDATE"
                    ),
                    {
                        "session": current.target_session_id,
                        "scope": self._scope_id,
                    },
                )
            ).one_or_none()
            if target is None:
                raise JobValidationError(
                    "target_session_not_found",
                    "target session does not exist in the current scope",
                )
            injected_seq = await append_event_in_transaction(
                conn,
                _injection_event(current, status, text_value, now),
                require_existing_session=True,
            )

        stored_result = safe_result if status is JobStatus.succeeded else None
        result_assignment = "result = NULL"
        params: dict[str, Any] = {
            "status": status.value,
            "result_message": text_value,
            "error_kind": None if safe_error is None else safe_error.kind,
            "error_message": None if safe_error is None else safe_error.message,
            "injected_seq": injected_seq,
            "now": now,
            "id": job_id,
            "scope": self._scope_id,
        }
        if stored_result is not None:
            result_assignment = "result = CAST(:result AS jsonb)"
            params["result"] = json.dumps(stored_result, ensure_ascii=False)
        update_sql = text(
            "UPDATE jobs SET status = :status, "
            "lease_token = NULL, lease_expires_at = NULL, heartbeat_at = NULL, "
            f"{result_assignment}, result_message = :result_message, "
            "error_kind = :error_kind, error_message = :error_message, "
            "injected_event_seq = :injected_seq, updated_at = :now, "
            "finished_at = :now "
            "WHERE id = :id AND scope_id = :scope RETURNING *"
        )
        row = (await conn.execute(update_sql, params)).mappings().one()
        return _to_job_record(row)

    async def request_cancel(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        safe_job_id = _optional_read_identity(job_id, field="job_id", code="invalid_job_id")
        if safe_job_id is None:
            return None
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = cast(
                Mapping[str, Any] | None,
                (
                    await conn.execute(
                        text("SELECT * FROM jobs WHERE id = :id AND scope_id = :scope FOR UPDATE"),
                        {"id": safe_job_id, "scope": self._scope_id},
                    )
                )
                .mappings()
                .one_or_none(),
            )
            if locked is None:
                return None
            record = _to_job_record(locked)
            if record.status in _TERMINAL_STATUSES:
                return record
            if record.cancel_mode is CancelMode.disabled:
                raise JobValidationError(
                    "job_not_cancellable",
                    "job does not allow cancellation",
                )
            if record.terminal_intent is JobTerminalIntent.cancelled:
                return record
            if record.terminal_intent is JobTerminalIntent.failed:
                raise JobValidationError(
                    "job_finalizing",
                    "job is finalizing and cannot be cancelled",
                )
            if _final_attempt_lease_expired(record, now):
                if _cancel_precedes_lease_expiry(record):
                    return record
                raise JobValidationError(
                    "job_finalizing",
                    "job is finalizing and cannot be cancelled",
                )
            if record.status is JobStatus.queued:
                if record.cancel_mode is CancelMode.cooperative:
                    row = (
                        (
                            await conn.execute(
                                text(
                                    "UPDATE jobs SET "
                                    "cancel_requested_at = COALESCE(cancel_requested_at, :now), "
                                    "next_attempt_at = LEAST(next_attempt_at, :now), "
                                    "updated_at = :now "
                                    "WHERE id = :id AND scope_id = :scope RETURNING *"
                                ),
                                {
                                    "now": now,
                                    "id": safe_job_id,
                                    "scope": self._scope_id,
                                },
                            )
                        )
                        .mappings()
                        .one()
                    )
                    return _to_job_record(row)
                reserved = await self._reserve_terminal_in_transaction(
                    conn,
                    locked,
                    JobTerminalIntent.cancelled,
                    now=now,
                    require_cancel_request=False,
                )
                return await self._finalize_reserved_in_transaction(
                    conn,
                    reserved,
                    now=now,
                )
            if record.status is JobStatus.running:
                row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE jobs SET "
                                "cancel_requested_at = COALESCE(cancel_requested_at, :now), "
                                "updated_at = :now "
                                "WHERE id = :id AND scope_id = :scope RETURNING *"
                            ),
                            {
                                "now": now,
                                "id": safe_job_id,
                                "scope": self._scope_id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                return _to_job_record(row)
            return record

    async def reserve_terminal(
        self,
        lease: JobLease,
        intent: JobTerminalIntent,
        *,
        now: datetime,
        error: JobError | None = None,
    ) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        intent = _validated_terminal_intent(intent)
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = await self._locked_owned_row(
                conn,
                lease,
                now,
                allow_terminal_intent=True,
            )
            reserved = await self._reserve_terminal_in_transaction(
                conn,
                locked,
                intent,
                now=now,
                error=error,
            )
        return _to_job_record(reserved)

    async def finalize_terminal(self, lease: JobLease, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = await self._locked_owned_row(
                conn,
                lease,
                now,
                allow_terminal_intent=True,
            )
            row = await self._finalize_reserved_in_transaction(
                conn,
                locked,
                now=now,
                lease_token=lease.token,
            )
        if row is None:
            raise JobLeaseLostError(lease.job_id)
        return row

    async def reserve_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            reserved = await self._reserve_exhausted_in_transaction(conn, job_id, now)
        return None if reserved is None else _to_job_record(reserved)

    async def finalize_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = (
                (
                    await conn.execute(
                        text("SELECT * FROM jobs WHERE id = :id AND scope_id = :scope FOR UPDATE"),
                        {"id": job_id, "scope": self._scope_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if locked is None:
                return None
            current = _to_job_record(locked)
            if (
                current.status is not JobStatus.running
                or current.lease_expires_at is None
                or current.lease_expires_at > now
                or current.terminal_intent is None
            ):
                return None
            return await self._finalize_reserved_in_transaction(
                conn,
                locked,
                now=now,
                expired_intent=True,
            )

    async def succeed(self, lease: JobLease, result: JobResult, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = await self._finalize_in_transaction(
                conn,
                job_id=lease.job_id,
                status=JobStatus.succeeded,
                now=now,
                lease_token=lease.token,
                result=result,
            )
        if row is None:
            raise JobLeaseLostError(lease.job_id)
        return row

    async def fail_terminal(self, lease: JobLease, error: JobError, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = await self._locked_owned_row(
                conn,
                lease,
                now,
                allow_terminal_intent=True,
            )
            reserved = await self._reserve_terminal_in_transaction(
                conn,
                locked,
                JobTerminalIntent.failed,
                now=now,
                error=error,
            )
            row = await self._finalize_reserved_in_transaction(
                conn,
                reserved,
                now=now,
                lease_token=lease.token,
            )
        if row is None:
            raise JobLeaseLostError(lease.job_id)
        return row

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = await self._locked_owned_row(
                conn,
                lease,
                now,
                allow_terminal_intent=True,
            )
            reserved = await self._reserve_terminal_in_transaction(
                conn,
                locked,
                JobTerminalIntent.cancelled,
                now=now,
            )
            row = await self._finalize_reserved_in_transaction(
                conn,
                reserved,
                now=now,
                lease_token=lease.token,
            )
        if row is None:
            raise JobLeaseLostError(lease.job_id)
        return row

    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            reserved = await self._reserve_exhausted_in_transaction(conn, job_id, now)
            if (
                reserved is None
                or _to_job_record(reserved).terminal_intent is not JobTerminalIntent.failed
            ):
                return None
            return await self._finalize_reserved_in_transaction(
                conn,
                reserved,
                now=now,
                expired_intent=True,
            )

    async def finish_cancelled_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            reserved = await self._reserve_exhausted_in_transaction(conn, job_id, now)
            if (
                reserved is None
                or _to_job_record(reserved).terminal_intent is not JobTerminalIntent.cancelled
            ):
                return None
            return await self._finalize_reserved_in_transaction(
                conn,
                reserved,
                now=now,
                expired_intent=True,
            )


def retry_delay_seconds(attempt: int, base_seconds: int, max_seconds: int) -> int:
    values = (attempt, base_seconds, max_seconds)
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in values):
        raise ValueError("attempt, base_seconds and max_seconds must be positive")
    exponent = attempt - 1
    if exponent >= max_seconds.bit_length():
        return max_seconds
    return min(base_seconds * (1 << exponent), max_seconds)
