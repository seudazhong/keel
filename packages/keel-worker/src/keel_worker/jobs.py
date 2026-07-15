"""Allow-listed durable-job worker orchestration (ADR-0010)."""

from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from time import perf_counter
from typing import Any, TypeVar, cast

from keel_core.jobs import (
    JobCancellationRequested,
    JobError,
    JobLease,
    JobLeaseLostError,
    JobRecord,
    JobResult,
    JobStatus,
    JobStore,
    JobValidationError,
    PermanentJobError,
    RetryableJobError,
    retry_delay_seconds,
)
from keel_core.observability import get_tracer

logger = logging.getLogger("keel.worker.jobs")
_PG_INTEGER_MAX = 2**31 - 1

JobHandler = Callable[["JobContext", dict[str, Any]], Awaitable[JobResult]]
JobCancelledHook = Callable[[JobRecord], Awaitable[None]]
JobFailedHook = Callable[[JobRecord, JobError], Awaitable[None]]
JobClock = Callable[[], datetime]
EnqueueJob = Callable[..., Awaitable[None]]
JobStatusSink = Callable[[JobStatus], None]
T = TypeVar("T")
AsyncOperation = Callable[[], Awaitable[T]]
_MISSING = object()


def _normalized_kind(kind: str) -> str:
    if not isinstance(kind, str):
        raise ValueError("job kind must be storage-safe UTF-8 text")
    normalized = kind.strip()
    if not normalized:
        raise ValueError("job kind must not be empty")
    if "\x00" in normalized:
        raise ValueError("job kind must be storage-safe UTF-8 text without NUL characters")
    try:
        normalized.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("job kind must be storage-safe UTF-8 text") from exc
    return normalized


@dataclass(frozen=True)
class JobDefinition:
    kind: str
    handler: JobHandler
    max_attempts: int = 3
    lease_seconds: int = 300
    on_cancelled: JobCancelledHook | None = None
    on_failed: JobFailedHook | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", _normalized_kind(self.kind))
        if not callable(self.handler):
            raise ValueError("handler must be callable")
        if self.on_cancelled is not None and not callable(self.on_cancelled):
            raise ValueError("on_cancelled must be callable")
        if self.on_failed is not None and not callable(self.on_failed):
            raise ValueError("on_failed must be callable")
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or not 1 <= self.max_attempts <= _PG_INTEGER_MAX
        ):
            raise ValueError(f"max_attempts must be between 1 and {_PG_INTEGER_MAX}")
        if (
            isinstance(self.lease_seconds, bool)
            or not isinstance(self.lease_seconds, int)
            or self.lease_seconds < 1
        ):
            raise ValueError("lease_seconds must be at least 1")


class JobRegistry:
    def __init__(self) -> None:
        self._definitions: dict[str, JobDefinition] = {}

    def register(self, definition: JobDefinition) -> None:
        if definition.kind in self._definitions:
            raise ValueError(f"job kind already registered: {definition.kind}")
        self._definitions[definition.kind] = definition

    def get(self, kind: str) -> JobDefinition | None:
        return self._definitions.get(kind)

    def kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._definitions))


class JobContext:
    def __init__(
        self,
        store: JobStore,
        lease: JobLease,
        *,
        clock: JobClock,
    ) -> None:
        self._store = store
        self._lease = lease
        self._clock = clock
        self.job_id = lease.job_id
        self.scope_id = lease.scope_id
        self.attempt = lease.attempt
        self.max_attempts = lease.max_attempts

    async def progress(
        self,
        current: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        update = await self._store.progress(
            self._lease,
            current=current,
            total=total,
            message=message,
            now=self._clock(),
        )
        if update.cancel_requested:
            raise JobCancellationRequested

    async def checkpoint(self) -> None:
        if await self._store.heartbeat(self._lease, self._clock()):
            raise JobCancellationRequested


def _clock(ctx: dict[str, Any]) -> JobClock:
    configured = ctx.get("job_clock", _MISSING)
    if configured is _MISSING:
        return lambda: datetime.now(UTC)
    if not callable(configured):
        raise TypeError("job_clock must be callable")
    clock = cast(JobClock, configured)

    def checked_clock() -> datetime:
        try:
            value = clock()
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise asyncio.CancelledError from None
            raise exc from None
        if not isinstance(value, datetime):
            raise TypeError("job_clock must return a datetime")
        return value

    return checked_clock


async def _current_status(store: JobStore, job_id: str) -> str:
    row = await store.get(job_id)
    return "missing" if row is None else row.status.value


async def _authoritative_status(store: JobStore, job_id: str) -> tuple[str, JobStatus | None]:
    status_value = await _current_status(store, job_id)
    try:
        return status_value, JobStatus(status_value)
    except ValueError:
        return status_value, None


async def _transition_or_current(
    store: JobStore,
    job_id: str,
    transition: Awaitable[JobRecord],
) -> tuple[str, JobStatus | None]:
    try:
        row = await transition
        return row.status.value, row.status
    except JobLeaseLostError:
        return await _authoritative_status(store, job_id)


async def _lease_hook_record(
    store: JobStore,
    lease: JobLease,
    now: datetime,
) -> JobRecord | None:
    row = await store.get(lease.job_id)
    if (
        row is None
        or row.status is not JobStatus.running
        or row.lease_token != lease.token
        or row.lease_expires_at is None
        or row.lease_expires_at <= now
    ):
        return None
    return row


async def _call_cancelled_hook(
    definition: JobDefinition | None,
    row: JobRecord,
) -> None:
    if definition is not None and definition.on_cancelled is not None:
        await definition.on_cancelled(row)


async def _call_failed_hook(
    definition: JobDefinition | None,
    row: JobRecord,
    error: JobError,
) -> None:
    if definition is not None and definition.on_failed is not None:
        await definition.on_failed(row, error)


async def _finish_cancelled(
    store: JobStore,
    definition: JobDefinition,
    lease: JobLease,
    clock: JobClock,
) -> tuple[str, JobStatus | None]:
    if definition.on_cancelled is not None:
        row = await _lease_hook_record(store, lease, clock())
        if row is not None:
            await _call_cancelled_hook(definition, row)
    return await _transition_or_current(
        store,
        lease.job_id,
        store.finish_cancelled(lease, clock()),
    )


async def _fail_terminal(
    store: JobStore,
    definition: JobDefinition,
    lease: JobLease,
    error: JobError,
    clock: JobClock,
) -> tuple[str, JobStatus | None]:
    if definition.on_failed is not None:
        row = await _lease_hook_record(store, lease, clock())
        if row is not None:
            await _call_failed_hook(definition, row, error)
    return await _transition_or_current(
        store,
        lease.job_id,
        store.fail_terminal(lease, error, clock()),
    )


async def _without_exception_context(  # noqa: UP047 - supports declared mypy>=1.11
    operation: AsyncOperation[T],
) -> T:
    try:
        return await operation()
    except BaseException as exc:
        if isinstance(exc, asyncio.CancelledError):
            raise asyncio.CancelledError from None
        raise exc from None


def _safe_exception_frames(exc: BaseException) -> str:
    frames = traceback.extract_tb(exc.__traceback__, limit=8)
    if not frames:
        return "<no-frame>"
    return " > ".join(
        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}" for frame in frames
    )


async def _retry_or_fail(
    *,
    ctx: dict[str, Any],
    store: JobStore,
    definition: JobDefinition,
    lease: JobLease,
    error: JobError,
    clock: JobClock,
    on_status: JobStatusSink,
) -> tuple[str, JobStatus | None]:
    if lease.attempt >= lease.max_attempts:
        result = await _fail_terminal(
            store,
            definition,
            lease,
            error,
            clock,
        )
        if result[1] is not None:
            on_status(result[1])
        return result
    now = clock()
    settings = ctx["job_settings"]
    retry_at = now + timedelta(
        seconds=retry_delay_seconds(
            lease.attempt,
            settings.job_retry_base_seconds,
            settings.job_retry_max_seconds,
        )
    )
    status_value, status = await _transition_or_current(
        store,
        lease.job_id,
        store.requeue(lease, error, retry_at, now),
    )
    if status is not JobStatus.queued:
        if status is not None:
            on_status(status)
        return status_value, status
    on_status(status)
    try:
        enqueue: EnqueueJob = ctx["enqueue"]
        await enqueue(
            "run_job",
            lease.scope_id,
            lease.job_id,
            _defer_until=retry_at,
        )
    except Exception as exc:
        logger.warning(
            "job retry enqueue failed scope=%s job=%s kind=%s attempt=%d error_type=%s frames=%s",
            lease.scope_id,
            lease.job_id,
            lease.kind,
            lease.attempt,
            type(exc).__name__,
            _safe_exception_frames(exc),
        )
    return status_value, status


async def run_job(ctx: dict[str, Any], scope_id: str, job_id: str) -> str:
    durable_scope = str(ctx["durable_scope"])
    if scope_id != durable_scope:
        logger.warning(
            "job scope mismatch configured=%s requested=%s job=%s",
            durable_scope,
            scope_id,
            job_id,
        )
        return "scope_mismatch"
    store: JobStore = ctx["jobs"]
    if store.scope_id != durable_scope:
        logger.error(
            "job store scope mismatch configured=%s store=%s",
            durable_scope,
            store.scope_id,
        )
        return "scope_mismatch"
    registry: JobRegistry = ctx["job_registry"]
    clock = _clock(ctx)
    before = await store.get(job_id)
    if before is None:
        return "missing"
    if before.status in {
        JobStatus.succeeded,
        JobStatus.failed,
        JobStatus.cancelled,
    }:
        return before.status.value
    definition = registry.get(before.kind)
    lease_seconds = (
        definition.lease_seconds
        if definition is not None
        else ctx["job_settings"].job_lease_seconds
    )
    lease = await store.claim(job_id, clock(), lease_seconds)
    if lease is None:
        return await _current_status(store, job_id)

    tracer = get_tracer("keel.worker.jobs")
    started = perf_counter()
    final_status = JobStatus.running

    def set_final_status(status: JobStatus) -> None:
        nonlocal final_status
        final_status = status

    with tracer.start_as_current_span(
        "job.execute",
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        span.set_attribute("job.id", lease.job_id)
        span.set_attribute("job.kind", lease.kind)
        span.set_attribute("job.scope_id", lease.scope_id)
        span.set_attribute("job.attempt", lease.attempt)

        def finish_observability() -> None:
            span.set_attribute("job.status", final_status.value)
            logger.info(
                "job transition scope=%s job=%s kind=%s attempt=%d status=%s duration_ms=%d",
                lease.scope_id,
                lease.job_id,
                lease.kind,
                lease.attempt,
                final_status.value,
                int((perf_counter() - started) * 1000),
            )

        if definition is None:
            try:
                status_value, status = await _transition_or_current(
                    store,
                    job_id,
                    store.fail_terminal(
                        lease,
                        JobError(
                            "unknown_job_kind",
                            "job kind is not registered on this worker",
                        ),
                        clock(),
                    ),
                )
                if status is not None:
                    final_status = status
                return status_value
            finally:
                finish_observability()

        try:
            context = JobContext(store, lease, clock=clock)
            await context.checkpoint()
            result = await definition.handler(context, lease.payload)
            if not isinstance(result, JobResult):
                raise PermanentJobError(
                    "invalid_job_result",
                    "job handler must return JobResult",
                )
            status_value, status = await _transition_or_current(
                store,
                job_id,
                store.succeed(lease, result, clock()),
            )
            if status is not None:
                final_status = status
            return status_value
        except asyncio.CancelledError:
            raise asyncio.CancelledError from None
        except JobCancellationRequested:
            status_value, status = await _without_exception_context(
                lambda: _finish_cancelled(
                    store,
                    definition,
                    lease,
                    clock,
                )
            )
            if status is not None:
                final_status = status
            return status_value
        except PermanentJobError as exc:
            job_error = JobError(exc.code, exc.public_message)
            status_value, status = await _without_exception_context(
                lambda: _fail_terminal(
                    store,
                    definition,
                    lease,
                    job_error,
                    clock,
                )
            )
            if status is not None:
                final_status = status
            return status_value
        except JobValidationError as exc:
            job_error = JobError(exc.code, exc.public_message)
            status_value, status = await _without_exception_context(
                lambda: _fail_terminal(
                    store,
                    definition,
                    lease,
                    job_error,
                    clock,
                )
            )
            if status is not None:
                final_status = status
            return status_value
        except RetryableJobError as exc:
            job_error = JobError(exc.code, exc.public_message)
            status_value, status = await _without_exception_context(
                lambda: _retry_or_fail(
                    ctx=ctx,
                    store=store,
                    definition=definition,
                    lease=lease,
                    error=job_error,
                    clock=clock,
                    on_status=set_final_status,
                )
            )
            if status is not None:
                final_status = status
            return status_value
        except JobLeaseLostError:
            status_value, status = await _without_exception_context(
                lambda: _authoritative_status(store, job_id)
            )
            if status is not None:
                final_status = status
            return status_value
        except Exception as exc:
            logger.error(
                "job handler raised scope=%s job=%s kind=%s attempt=%d error_type=%s frames=%s",
                lease.scope_id,
                lease.job_id,
                lease.kind,
                lease.attempt,
                type(exc).__name__,
                _safe_exception_frames(exc),
            )
            status_value, status = await _without_exception_context(
                lambda: _retry_or_fail(
                    ctx=ctx,
                    store=store,
                    definition=definition,
                    lease=lease,
                    error=JobError(
                        "internal_error",
                        "job failed with a temporary internal error",
                    ),
                    clock=clock,
                    on_status=set_final_status,
                )
            )
            if status is not None:
                final_status = status
            return status_value
        finally:
            finish_observability()


async def dispatch_jobs(ctx: dict[str, Any]) -> int:
    store: JobStore = ctx["jobs"]
    scope_id = str(ctx["durable_scope"])
    if store.scope_id != scope_id:
        logger.error(
            "job dispatcher scope mismatch configured=%s store=%s",
            scope_id,
            store.scope_id,
        )
        return 0

    settings = ctx["job_settings"]
    registry: JobRegistry = ctx["job_registry"]
    now = _clock(ctx)()
    limit = settings.job_dispatch_limit
    enqueue: EnqueueJob = ctx["enqueue"]
    processed = 0

    dispatchable_ids = await _without_exception_context(lambda: store.dispatchable(now, limit))
    for job_id in dispatchable_ids:
        try:
            await _without_exception_context(partial(enqueue, "run_job", scope_id, job_id))
            processed += 1
        except Exception as exc:
            logger.warning(
                "job dispatch enqueue failed scope=%s job=%s error_type=%s frames=%s",
                scope_id,
                job_id,
                type(exc).__name__,
                _safe_exception_frames(exc),
            )

    exhausted_ids = await _without_exception_context(lambda: store.exhausted(now, limit))
    for job_id in exhausted_ids:
        try:
            row = await _without_exception_context(partial(store.get, job_id))
            if row is None:
                continue
            definition = registry.get(row.kind)
            if row.cancel_requested_at is not None:
                await _without_exception_context(partial(_call_cancelled_hook, definition, row))
                finalized = await _without_exception_context(
                    partial(store.finish_cancelled_exhausted, job_id, now)
                )
            else:
                error = JobError(
                    "attempts_exhausted",
                    "job attempts were exhausted after worker lease expiry",
                )
                await _without_exception_context(partial(_call_failed_hook, definition, row, error))
                finalized = await _without_exception_context(
                    partial(store.fail_exhausted, job_id, now)
                )
            if finalized is not None:
                processed += 1
        except Exception as exc:
            logger.error(
                "job exhaustion finalizer failed scope=%s job=%s error_type=%s frames=%s",
                scope_id,
                job_id,
                type(exc).__name__,
                _safe_exception_frames(exc),
            )
    return processed
