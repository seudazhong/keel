"""Durable background-job contracts and stores (ADR-0010)."""

from __future__ import annotations

import builtins
import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from keel_core.config import Settings


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


def _validated_json_object(value: dict[str, Any], *, field: str, max_bytes: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise JobValidationError("json_object_required", f"{field} must be a JSON object")
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise JobValidationError(
            "json_serializable", f"{field} must contain only JSON-serializable values"
        ) from exc
    if len(encoded) > max_bytes:
        raise JobValidationError(
            f"{field}_too_large", f"{field} exceeds {max_bytes} UTF-8 JSON bytes"
        )
    return value


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
        return value[: self.result_message_max_chars]

    def error_message(self, value: str) -> str:
        return value[: self.error_message_max_chars]


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


@dataclass(frozen=True)
class JobError:
    kind: str
    message: str

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("JobError.kind must not be empty")
        if not self.message.strip():
            raise ValueError("JobError.message must not be empty")


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


def retry_delay_seconds(attempt: int, base_seconds: int, max_seconds: int) -> int:
    if attempt < 1 or base_seconds < 1 or max_seconds < 1:
        raise ValueError("attempt, base_seconds and max_seconds must be positive")
    return min(base_seconds * int(2 ** (attempt - 1)), max_seconds)
