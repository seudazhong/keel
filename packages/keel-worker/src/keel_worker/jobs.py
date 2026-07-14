"""Allow-listed durable-job worker orchestration (ADR-0010)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any, cast

from keel_core.jobs import (
    JobCancellationRequested,
    JobError,
    JobLease,
    JobLeaseLostError,
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

JobHandler = Callable[["JobContext", dict[str, Any]], Awaitable[JobResult]]
JobClock = Callable[[], datetime]
EnqueueJob = Callable[..., Awaitable[None]]


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

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", _normalized_kind(self.kind))
        if not callable(self.handler):
            raise ValueError("handler must be callable")
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise ValueError("max_attempts must be at least 1")
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
    clock = ctx.get("job_clock")
    if callable(clock):
        return cast(JobClock, clock)
    return lambda: datetime.now(UTC)


async def _current_status(store: JobStore, job_id: str) -> str:
    row = await store.get(job_id)
    return "missing" if row is None else row.status.value


async def _retry_or_fail(
    *,
    ctx: dict[str, Any],
    store: JobStore,
    lease: JobLease,
    error: JobError,
    now: datetime,
) -> JobStatus:
    if lease.attempt >= lease.max_attempts:
        row = await store.fail_terminal(lease, error, now)
        return row.status
    settings = ctx["job_settings"]
    retry_at = now + timedelta(
        seconds=retry_delay_seconds(
            lease.attempt,
            settings.job_retry_base_seconds,
            settings.job_retry_max_seconds,
        )
    )
    row = await store.requeue(lease, error, retry_at, now)
    try:
        enqueue: EnqueueJob = ctx["enqueue"]
        await enqueue(
            "run_job",
            lease.scope_id,
            lease.job_id,
            _defer_until=retry_at,
        )
    except Exception:
        logger.warning(
            "job retry enqueue failed scope=%s job=%s kind=%s attempt=%d",
            lease.scope_id,
            lease.job_id,
            lease.kind,
            lease.attempt,
            exc_info=True,
        )
    return row.status


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
    with tracer.start_as_current_span("job.execute") as span:
        span.set_attribute("job.id", lease.job_id)
        span.set_attribute("job.kind", lease.kind)
        span.set_attribute("job.scope_id", lease.scope_id)
        span.set_attribute("job.attempt", lease.attempt)
        try:
            if definition is None:
                row = await store.fail_terminal(
                    lease,
                    JobError(
                        "unknown_job_kind",
                        "job kind is not registered on this worker",
                    ),
                    clock(),
                )
                final_status = row.status
                return row.status.value
            context = JobContext(store, lease, clock=clock)
            await context.checkpoint()
            result = await definition.handler(context, lease.payload)
            if not isinstance(result, JobResult):
                raise PermanentJobError(
                    "invalid_job_result",
                    "job handler must return JobResult",
                )
            row = await store.succeed(lease, result, clock())
            final_status = row.status
            return row.status.value
        except asyncio.CancelledError:
            raise
        except JobCancellationRequested:
            row = await store.finish_cancelled(lease, clock())
            final_status = row.status
            return row.status.value
        except PermanentJobError as exc:
            row = await store.fail_terminal(lease, JobError(exc.code, exc.public_message), clock())
            final_status = row.status
            return row.status.value
        except JobValidationError as exc:
            row = await store.fail_terminal(lease, JobError(exc.code, exc.public_message), clock())
            final_status = row.status
            return row.status.value
        except RetryableJobError as exc:
            final_status = await _retry_or_fail(
                ctx=ctx,
                store=store,
                lease=lease,
                error=JobError(exc.code, exc.public_message),
                now=clock(),
            )
            return final_status.value
        except JobLeaseLostError:
            status_value = await _current_status(store, job_id)
            try:
                final_status = JobStatus(status_value)
            except ValueError:
                pass
            return status_value
        except Exception:
            logger.exception(
                "job handler raised scope=%s job=%s kind=%s attempt=%d",
                lease.scope_id,
                lease.job_id,
                lease.kind,
                lease.attempt,
            )
            final_status = await _retry_or_fail(
                ctx=ctx,
                store=store,
                lease=lease,
                error=JobError(
                    "internal_error",
                    "job failed with a temporary internal error",
                ),
                now=clock(),
            )
            return final_status.value
        finally:
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
