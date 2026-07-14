"""Durable background-job contracts and stores (ADR-0010)."""

from __future__ import annotations

import asyncio
import builtins
import copy
import json
import math
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol, cast

from keel_core.config import Settings
from keel_core.events import Event, EventType
from keel_core.state import InMemoryEventStore

_MAX_JSON_DEPTH = 100
_MAX_ERROR_CODE_CHARS = 128
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
    result_message_max_chars: int = 8_000
    error_message_max_chars: int = 2_000

    def __post_init__(self) -> None:
        if (
            min(
                self.payload_max_bytes,
                self.result_max_bytes,
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
            result_message_max_chars=settings.job_result_message_max_chars,
            error_message_max_chars=settings.job_error_message_max_chars,
        )

    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _validated_json_object(payload, field="payload", max_bytes=self.payload_max_bytes)

    def validate_result(self, result: dict[str, Any]) -> dict[str, Any]:
        return _validated_json_object(result, field="result", max_bytes=self.result_max_bytes)

    def result_message(self, value: str) -> str:
        return _validated_storage_text(value, field="result_message")[
            : self.result_message_max_chars
        ]

    def error_message(self, value: str) -> str:
        return _validated_storage_text(value, field="error_message")[: self.error_message_max_chars]


@dataclass(frozen=True)
class JobRecord:
    id: str
    scope_id: str
    kind: str
    status: JobStatus
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

    async def requeue(
        self, lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord: ...

    async def succeed(self, lease: JobLease, result: JobResult, now: datetime) -> JobRecord: ...

    async def fail_terminal(self, lease: JobLease, error: JobError, now: datetime) -> JobRecord: ...

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord: ...
    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _validated_identity(value: str, *, field: str, code: str) -> str:
    if not isinstance(value, str):
        raise JobValidationError(code, f"{field} must be storage-safe text")
    normalized = value.strip()
    if not normalized:
        raise JobValidationError(code, f"{field} must not be empty")
    try:
        return _ensure_storage_safe_text(normalized, field=field)
    except ValueError as exc:
        raise JobValidationError(code, f"{field} must be storage-safe UTF-8 text") from exc


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

    def _owned(self, lease: JobLease, now: datetime) -> JobRecord:
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
        return row

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
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]:
        kind = _validated_identity(kind, field="kind", code="invalid_kind")
        idempotency_key = _validated_identity(
            idempotency_key,
            field="idempotency_key",
            code="invalid_idempotency_key",
        )
        if max_attempts < 1:
            raise JobValidationError("invalid_max_attempts", "max_attempts must be at least 1")
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
        async with self._lock:
            record = self._rows.get(job_id)
            return None if record is None else _copy_record(record)

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> builtins.list[JobRecord]:
        _validate_limit(limit)
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
                if row.attempt < row.max_attempts
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
                and row.attempt >= row.max_attempts
            ]
            rows.sort(key=lambda row: (row.lease_expires_at, row.created_at, row.id))
            return [row.id for row in rows[:limit]]

    async def claim(self, job_id: str, now: datetime, lease_seconds: int) -> JobLease | None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._rows.get(job_id)
            if row is None or row.attempt >= row.max_attempts:
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
        safe_message = (
            None if message is None else _validated_storage_text(message, field="progress_message")
        )
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
        async with self._lock:
            row = self._rows.get(job_id)
            if row is None:
                return None
            if row.status is JobStatus.queued:
                return await self._finalize_locked(row, status=JobStatus.cancelled, now=now)
            if row.status is JobStatus.running:
                updated = replace(
                    row,
                    cancel_requested_at=row.cancel_requested_at or now,
                    updated_at=now,
                )
                self._rows[job_id] = updated
                return _copy_record(updated)
            return _copy_record(row)

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
                next_attempt_at=retry_at,
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
            row = self._owned(lease, now)
            return await self._finalize_locked(row, status=JobStatus.failed, now=now, error=error)

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._owned(lease, now)
            return await self._finalize_locked(row, status=JobStatus.cancelled, now=now)

    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        now = _normalized_utc_timestamp(now, field="now")
        async with self._lock:
            row = self._rows.get(job_id)
            if (
                row is None
                or row.status is not JobStatus.running
                or row.lease_expires_at is None
                or row.lease_expires_at > now
                or row.attempt < row.max_attempts
            ):
                return None
            return await self._finalize_locked(
                row,
                status=JobStatus.failed,
                now=now,
                error=JobError(
                    "attempts_exhausted",
                    "job attempts were exhausted after worker lease expiry",
                ),
            )


def retry_delay_seconds(attempt: int, base_seconds: int, max_seconds: int) -> int:
    if attempt < 1 or base_seconds < 1 or max_seconds < 1:
        raise ValueError("attempt, base_seconds and max_seconds must be positive")
    return min(base_seconds * int(2 ** (attempt - 1)), max_seconds)
