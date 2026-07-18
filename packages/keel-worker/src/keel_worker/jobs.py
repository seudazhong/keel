"""Allow-listed durable-job worker orchestration (ADR-0010)."""

from __future__ import annotations

import asyncio
import logging
import socket
import traceback
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from time import perf_counter
from typing import Any, TypeVar, cast

from keel_core.connector_service import (
    CONNECTOR_RENEW_JOB_KIND,
    CONNECTOR_SYNC_JOB_KIND,
)
from keel_core.job_dispatch import JobDispatchOutbox
from keel_core.jobs import (
    CancelMode,
    JobCancellationRequested,
    JobError,
    JobLease,
    JobLeaseLostError,
    JobLimits,
    JobRecord,
    JobResult,
    JobStatus,
    JobStore,
    JobTerminalIntent,
    JobValidationError,
    PermanentJobError,
    PostgresJobStore,
    RetryableJobError,
    retry_delay_seconds,
)
from keel_core.knowledge.jobs import KNOWLEDGE_DELETE_KIND, KNOWLEDGE_INGEST_KIND
from keel_core.observability import get_tracer
from keel_core.scoping import ScopeValidationError, validate_scope_id

logger = logging.getLogger("keel.worker.jobs")
_PG_INTEGER_MAX = 2**31 - 1

# A stable, per-process reconciler identity for the job-dispatch outbox lease fence.
_WORKER_ID = f"{socket.gethostname()}:{uuid.uuid4().hex[:8]}"

# The durable job kinds that may be dispatched across per-Agent scopes via the global
# job-dispatch outbox. Knowledge indexing/deletion and connector sync/renewal are all created
# under a per-Agent scope; every other durable job (erasure, project sync) stays pinned to the
# process ``durable_scope``. The reconciler and cross-scope ``run_job`` both revalidate an
# intent's kind against this set so a spoofed/foreign job kind can never be dispatched into
# another tenant's scope (findings 1 + 3).
_CROSS_SCOPE_JOB_KINDS = frozenset(
    {
        KNOWLEDGE_INGEST_KIND,
        KNOWLEDGE_DELETE_KIND,
        CONNECTOR_SYNC_JOB_KIND,
        CONNECTOR_RENEW_JOB_KIND,
    }
)

_TERMINAL_JOB_STATUSES = frozenset({JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled})

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
    cancel_mode: CancelMode = CancelMode.immediate

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", _normalized_kind(self.kind))
        if not callable(self.handler):
            raise ValueError("handler must be callable")
        if self.on_cancelled is not None and not callable(self.on_cancelled):
            raise ValueError("on_cancelled must be callable")
        if self.on_failed is not None and not callable(self.on_failed):
            raise ValueError("on_failed must be callable")
        if not isinstance(self.cancel_mode, CancelMode):
            raise ValueError("cancel_mode must be a CancelMode")
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


async def _call_reserved_hook(
    definition: JobDefinition | None,
    row: JobRecord,
) -> None:
    if row.terminal_intent is JobTerminalIntent.cancelled:
        await _call_cancelled_hook(definition, row)
        return
    if (
        row.terminal_intent is JobTerminalIntent.failed
        and row.error_kind is not None
        and row.error_message is not None
    ):
        await _call_failed_hook(
            definition,
            row,
            JobError(row.error_kind, row.error_message),
        )
        return
    raise JobValidationError(
        "terminal_intent_invalid",
        "reserved job terminal intent is incomplete",
    )


async def _finish_cancelled(
    store: JobStore,
    definition: JobDefinition,
    lease: JobLease,
    clock: JobClock,
) -> tuple[str, JobStatus | None]:
    try:
        reserved = await store.reserve_terminal(
            lease,
            JobTerminalIntent.cancelled,
            now=clock(),
        )
    except JobLeaseLostError:
        return await _authoritative_status(store, lease.job_id)
    await _call_reserved_hook(definition, reserved)
    return await _transition_or_current(
        store,
        lease.job_id,
        store.finalize_terminal(lease, clock()),
    )


async def _fail_terminal(
    store: JobStore,
    definition: JobDefinition,
    lease: JobLease,
    error: JobError,
    clock: JobClock,
) -> tuple[str, JobStatus | None]:
    try:
        reserved = await store.reserve_terminal(
            lease,
            JobTerminalIntent.failed,
            now=clock(),
            error=error,
        )
    except JobLeaseLostError:
        return await _authoritative_status(store, lease.job_id)
    await _call_reserved_hook(definition, reserved)
    return await _transition_or_current(
        store,
        lease.job_id,
        store.finalize_terminal(lease, clock()),
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


def _scoped_job_execution(
    ctx: dict[str, Any], scope_id: str
) -> tuple[JobStore, JobRegistry] | None:
    """Resolve the job store + handler registry bound to ``scope_id`` (finding 3).

    The worker dispatches Knowledge indexing/deletion jobs across many per-Agent scopes, so a
    cross-scope ``run_job`` must construct its :class:`JobStore` and Knowledge store/service/
    embedder for the job's *own* ``scope_id`` — never the process-wide ``durable_scope``. The
    pinned ``durable_scope`` reuses the fully-featured registry wired at startup (Knowledge +
    erasure + project sync), preserving existing non-Knowledge job behavior; every other scope
    gets a Knowledge-only registry (the only kinds admitted under a per-Agent scope), so a
    non-Knowledge kind in a per-Agent scope has no handler and fails closed as an unknown kind.

    Returns ``None`` when the scope is malformed or no shared engine is wired (a cross-scope job
    cannot run without Postgres).
    """
    durable_scope = str(ctx["durable_scope"])
    if scope_id == durable_scope:
        # The pinned store/registry wired at startup. A store bound to a *different* scope is a
        # misconfiguration caught by ``run_job``'s ``store.scope_id != scope_id`` guard.
        return ctx["jobs"], ctx["job_registry"]

    engine = ctx.get("engine")
    if engine is None:
        # In-memory test-double path (mirrors the run worker's ``_scoped_stores`` fallback): the
        # single ctx-provided store/registry stands in for whatever scope the test drives.
        double: JobStore | None = ctx.get("jobs")
        registry_double: JobRegistry | None = ctx.get("job_registry")
        if double is None or registry_double is None:
            return None
        return double, registry_double
    settings = ctx["job_settings"]
    embedder = ctx.get("embedder")
    if embedder is None:
        return None
    # Lazy import avoids a worker.knowledge <-> worker.jobs import cycle (worker.knowledge imports
    # JobRegistry from this module).
    from keel_core.knowledge.store import KnowledgeStore, PostgresKnowledgeStore
    from keel_worker.knowledge import knowledge_job_registry

    knowledge_store = PostgresKnowledgeStore(
        engine,
        scope_id,
        document_max_bytes=settings.knowledge_document_max_bytes,
    )
    registry = knowledge_job_registry(cast(KnowledgeStore, knowledge_store), embedder, settings)
    # Register scope-bound connector sync/renew handlers alongside Knowledge so an Agent-scoped
    # ``connector.sync``/``connector.renew`` job dispatched cross-scope actually runs against its
    # own scope's connector service (finding 1) — never the process-wide ``durable_scope`` and
    # never a ``web:local`` fallback. Without a connector factory the registry stays Knowledge-only
    # and a connector kind fails closed as an unknown kind.
    factory = ctx.get("connector_scope_factory")
    if factory is not None:
        # Lazy import avoids a worker.connectors <-> worker.jobs import cycle.
        from keel_worker.connectors import register_connector_jobs

        register_connector_jobs(registry, factory(scope_id), settings)
    job_store = PostgresJobStore(engine, scope_id, limits=JobLimits.from_settings(settings))
    return job_store, registry


async def run_job(ctx: dict[str, Any], scope_id: str, job_id: str) -> str:
    durable_scope = str(ctx["durable_scope"])
    if scope_id != durable_scope:
        # Cross-scope dispatch: an outbox-driven Knowledge job in a *different* scope must be a
        # canonical per-Agent scope (revalidated). The pinned ``durable_scope`` is trusted config
        # and is used as-is (it need not be a canonical ``agent:<org>/<agent>`` value).
        try:
            scope_id = validate_scope_id(scope_id)
        except ScopeValidationError:
            logger.warning("run_job rejected malformed scope=%r job=%s", scope_id, job_id)
            return "invalid_scope"
    scoped = _scoped_job_execution(ctx, scope_id)
    if scoped is None:
        logger.warning("run_job cannot bind scope=%s job=%s (no substrate)", scope_id, job_id)
        return "scope_unavailable"
    store, registry = scoped
    if store.scope_id != scope_id:
        logger.error(
            "job store scope mismatch requested=%s store=%s",
            scope_id,
            store.scope_id,
        )
        return "scope_mismatch"
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
    clock = _clock(ctx)
    now = clock()
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
            reserved = await _without_exception_context(
                partial(store.reserve_exhausted, job_id, clock())
            )
            if reserved is None:
                continue
            definition = registry.get(reserved.kind)
            await _without_exception_context(partial(_call_reserved_hook, definition, reserved))
            finalized = await _without_exception_context(
                partial(store.finalize_exhausted, job_id, clock())
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


def _scoped_job_store(ctx: dict[str, Any], scope_id: str) -> JobStore | None:
    """A :class:`JobStore` bound to ``scope_id`` for the reconciler (reuses the pinned store)."""
    durable_scope = str(ctx["durable_scope"])
    if scope_id == durable_scope:
        pinned: JobStore = ctx["jobs"]
        return pinned if pinned.scope_id == durable_scope else None
    engine = ctx.get("engine")
    if engine is None:
        # In-memory test-double path: the ctx store stands in when its scope matches the intent.
        double: JobStore | None = ctx.get("jobs")
        return double if double is not None and double.scope_id == scope_id else None
    return PostgresJobStore(engine, scope_id, limits=JobLimits.from_settings(ctx["job_settings"]))


async def reconcile_job_dispatch_tick(ctx: dict[str, Any]) -> int:
    """Cross-scope Knowledge job dispatch driven by the global job-dispatch outbox (finding 3).

    The scope-partitioned ``jobs`` table is RLS-forced, so a worker bound to one scope cannot see
    another's jobs, and the pinned ``dispatch_jobs`` reconciler only scans ``durable_scope``. The
    global :class:`~keel_core.job_dispatch.JobDispatchOutbox` is the one cross-scope index:
    Knowledge admission records ``(job_id, scope_id, kind)`` there (atomically with the job
    insert), and this tick leases a batch of due intents (fenced so duplicate workers never both
    process one), re-dispatches ``run_job(scope, job_id)`` for each still-open job, removes the
    intents of jobs that have reached a terminal state, and defers the rest. This makes a document
    created under any per-Agent scope actually indexed after a lost enqueue, rather than orphaned.
    Every step is idempotent (a duplicate ``run_job`` enqueue is deduped by the job claim).
    """
    outbox: JobDispatchOutbox | None = ctx.get("job_dispatch_outbox")
    if outbox is None:
        return 0
    enqueue: EnqueueJob = ctx["enqueue"]
    now = datetime.now(UTC)
    claimed = await outbox.claim_due(worker_id=_WORKER_ID, now=now)
    if not claimed:
        return 0

    dispatched = 0
    for intent in claimed:
        try:
            scope_id = validate_scope_id(intent.scope_id)
        except ScopeValidationError:
            # A malformed scope can never be dispatched — drop its intent (fail closed).
            await outbox.remove(intent.job_id)
            logger.warning("dropped job outbox intent with malformed scope=%r", intent.scope_id)
            continue
        if intent.kind not in _CROSS_SCOPE_JOB_KINDS:
            # A kind that is not cross-scope-dispatchable must never be routed into a per-Agent
            # scope; drop the intent rather than dispatch a foreign/unknown kind (fail closed).
            await outbox.remove(intent.job_id)
            logger.warning(
                "dropped job outbox intent with non-dispatchable kind=%s job=%s",
                intent.kind,
                intent.job_id,
            )
            continue
        store = _scoped_job_store(ctx, scope_id)
        if store is None:
            # No substrate to reconcile against right now; defer for a later tick.
            await outbox.reschedule(intent.job_id, now=now)
            continue
        record = await store.get(intent.job_id)
        if record is None or record.status in _TERMINAL_JOB_STATUSES:
            # Nothing left to dispatch: the job is gone or terminal. Ack (remove) the intent.
            await outbox.remove(intent.job_id)
            continue
        try:
            await enqueue("run_job", scope_id, intent.job_id)
            dispatched += 1
        except Exception as exc:
            logger.warning(
                "job dispatch reconcile enqueue failed scope=%s job=%s error_type=%s",
                scope_id,
                intent.job_id,
                type(exc).__name__,
            )
        # Defer a re-check so a still-queued/running job is retried without spinning; the intent
        # is removed once the job reaches a terminal state (or is purged, via FK cascade).
        await outbox.reschedule(intent.job_id, now=now)
    return dispatched
