"""Durable-job worker registry and cooperative context."""

from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from keel_core.config import Settings
from keel_core.jobs import (
    CancelMode,
    InMemoryJobStore,
    JobCancellationRequested,
    JobError,
    JobLeaseLostError,
    JobRecord,
    JobResult,
    JobStatus,
    JobValidationError,
    PermanentJobError,
    RetryableJobError,
)
from keel_worker.jobs import JobContext, JobDefinition, JobRegistry, dispatch_jobs, run_job

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


async def _handler(context: JobContext, payload: dict[str, object]) -> JobResult:
    return JobResult(data={"attempt": context.attempt, **payload}, message="done")


class _Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


async def _claimed_context(
    *,
    clock: datetime = _NOW + timedelta(seconds=5),
    idempotency_key: str = "context",
) -> tuple[InMemoryJobStore, JobContext]:
    store = InMemoryJobStore("web:local")
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key=idempotency_key,
        max_attempts=3,
        now=_NOW,
    )
    lease = await store.claim(job.id, _NOW, 60)
    assert lease is not None
    return store, JobContext(store, lease, clock=lambda: clock)


async def _enqueued_job(
    store: InMemoryJobStore,
    *,
    key: str,
    kind: str = "test.echo",
    payload: dict[str, Any] | None = None,
    max_attempts: int = 3,
    cancel_mode: CancelMode = CancelMode.immediate,
) -> str:
    job, _ = await store.enqueue_once(
        kind=kind,
        payload=payload or {},
        target_session_id=None,
        idempotency_key=key,
        max_attempts=max_attempts,
        cancel_mode=cancel_mode,
        now=_NOW,
    )
    return job.id


def _ctx(
    store: InMemoryJobStore,
    registry: JobRegistry,
    clock: _Clock,
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]],
) -> dict[str, Any]:
    async def enqueue(name: str, *args: object, **options: object) -> None:
        enqueued.append((name, args, options))

    return {
        "jobs": store,
        "job_registry": registry,
        "durable_scope": "web:local",
        "enqueue": enqueue,
        "job_clock": clock,
        "job_settings": Settings(),
    }


def test_registry_is_empty_by_default_and_uses_injected_handler() -> None:
    registry = JobRegistry()
    assert registry.kinds() == ()

    definition = JobDefinition(kind=" test.echo ", handler=_handler)
    registry.register(definition)

    assert definition.kind == "test.echo"
    assert registry.get("test.echo") is definition
    assert registry.get("test.echo").handler is _handler
    assert registry.kinds() == ("test.echo",)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(JobDefinition(kind="test.echo", handler=_handler))


@pytest.mark.parametrize("kind", ["", " \t ", "bad\x00kind", "bad\ud800kind"])
def test_job_definition_rejects_nonblank_storage_unsafe_kinds(kind: str) -> None:
    with pytest.raises(ValueError, match="job kind"):
        JobDefinition(kind=kind, handler=_handler)


@pytest.mark.parametrize("handler", [None, 42, "not-callable"])
def test_job_definition_rejects_non_callable_handlers(handler: object) -> None:
    with pytest.raises(ValueError, match="handler"):
        JobDefinition(kind="test.bad", handler=handler)  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["on_cancelled", "on_failed"])
def test_job_definition_rejects_non_callable_hooks(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        JobDefinition(kind="test.bad", handler=_handler, **{field: object()})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_attempts", 0),
        ("max_attempts", -1),
        ("max_attempts", True),
        ("max_attempts", 1.5),
        ("max_attempts", float("nan")),
        ("max_attempts", 2**31),
        ("lease_seconds", 0),
        ("lease_seconds", -1),
        ("lease_seconds", True),
        ("lease_seconds", 1.5),
        ("lease_seconds", float("nan")),
    ],
)
def test_job_definition_requires_positive_integer_limits(field: str, value: object) -> None:
    kwargs = {field: value}
    with pytest.raises(ValueError, match=field):
        JobDefinition(kind="test.bad", handler=_handler, **kwargs)  # type: ignore[arg-type]


async def test_job_context_progress_updates_record_and_exposes_identity() -> None:
    store, context = await _claimed_context()

    await context.progress(2, total=10, message="batch 1")

    record = await store.get(context.job_id)
    assert record is not None
    assert (context.scope_id, context.attempt, context.max_attempts) == ("web:local", 1, 3)
    assert (record.progress_current, record.progress_total, record.progress_message) == (
        2,
        10,
        "batch 1",
    )
    assert record.heartbeat_at == _NOW + timedelta(seconds=5)
    assert record.lease_expires_at == _NOW + timedelta(seconds=65)


async def test_job_context_preserves_store_utc_normalization() -> None:
    local_now = datetime(2026, 7, 14, 17, 0, 5, tzinfo=timezone(timedelta(hours=8)))
    store, context = await _claimed_context(clock=local_now, idempotency_key="utc-context")

    await context.checkpoint()

    record = await store.get(context.job_id)
    assert record is not None
    assert record.heartbeat_at == _NOW + timedelta(seconds=5)


async def test_job_context_preserves_current_lease_checks() -> None:
    _, context = await _claimed_context(
        clock=_NOW + timedelta(seconds=60),
        idempotency_key="expired-context",
    )

    with pytest.raises(JobLeaseLostError):
        await context.checkpoint()


async def test_job_context_checkpoint_and_progress_raise_cooperative_cancel() -> None:
    store, context = await _claimed_context(
        clock=_NOW + timedelta(seconds=2),
        idempotency_key="cancel-context",
    )
    await store.request_cancel(context.job_id, _NOW + timedelta(seconds=1))

    with pytest.raises(JobCancellationRequested):
        await context.checkpoint()
    with pytest.raises(JobCancellationRequested):
        await context.progress(1)


async def test_run_job_executes_registered_handler_and_succeeds() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    calls: list[tuple[int, dict[str, Any]]] = []

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        calls.append((context.attempt, payload))
        await context.progress(1, total=1, message="done")
        return JobResult(data={"echo": payload["value"]}, message="completed")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=60))
    job_id = await _enqueued_job(store, key="success", payload={"value": "secret-value"})
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    result = await run_job(_ctx(store, registry, _Clock(_NOW), enqueued), "web:local", job_id)

    assert result == JobStatus.succeeded.value
    assert calls == [(1, {"value": "secret-value"})]
    row = await store.get(job_id)
    assert row is not None and row.result == {"echo": "secret-value"}
    assert enqueued == []


async def test_run_job_rejects_argument_scope_before_store_access() -> None:
    class StoreMustNotBeRead:
        @property
        def scope_id(self) -> str:
            raise AssertionError("store must not be accessed")

    result = await run_job(
        {
            "durable_scope": "web:local",
            "jobs": StoreMustNotBeRead(),
        },
        "scope:other",
        "job_scope",
    )

    assert result == "scope_mismatch"


async def test_run_job_rejects_store_bound_to_another_scope() -> None:
    store = InMemoryJobStore("scope:store")
    job_id = await _enqueued_job(store, key="store-scope")

    result = await run_job(
        _ctx(store, JobRegistry(), _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    assert result == "scope_mismatch"
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]


async def test_run_job_returns_missing_without_claiming() -> None:
    store = InMemoryJobStore("web:local")

    result = await run_job(
        _ctx(store, JobRegistry(), _Clock(_NOW), []),
        "web:local",
        "job_missing",
    )

    assert result == "missing"


async def test_run_job_returns_terminal_status_without_reclaiming() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    called = False

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal called
        called = True
        return JobResult(data={}, message="should not run")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))
    job_id = await _enqueued_job(store, key="terminal")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    await store.succeed(lease, JobResult(data={}, message="already done"), _NOW)

    result = await run_job(
        _ctx(store, registry, _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    assert result == JobStatus.succeeded.value
    assert called is False
    assert (await store.get(job_id)).attempt == 1  # type: ignore[union-attr]


async def test_run_job_treats_claim_as_atomic_authority() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    called = False

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal called
        called = True
        return JobResult(data={}, message="should not run")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))
    job_id = await _enqueued_job(store, key="busy")
    assert await store.claim(job_id, _NOW, 60) is not None

    result = await run_job(
        _ctx(store, registry, _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    assert result == JobStatus.running.value
    assert called is False
    assert (await store.get(job_id)).attempt == 1  # type: ignore[union-attr]


async def test_run_job_unknown_kind_is_permanent_and_never_imported() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _enqueued_job(store, key="unknown", kind="python.module:function")

    result = await run_job(
        _ctx(store, JobRegistry(), _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    row = await store.get(job_id)
    assert result == JobStatus.failed.value
    assert row is not None and row.error_kind == "unknown_job_kind"
    assert row.attempt == 1


async def test_run_job_permanent_error_executes_once() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        raise PermanentJobError("invalid_document", "Document is invalid.")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=60))
    job_id = await _enqueued_job(store, key="permanent")

    result = await run_job(
        _ctx(store, registry, _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    assert result == JobStatus.failed.value
    assert calls == 1
    assert (await store.get(job_id)).error_kind == "invalid_document"  # type: ignore[union-attr]


async def test_failed_hook_runs_before_permanent_terminal_transition() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    observed: list[tuple[JobStatus, str]] = []

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise PermanentJobError("permanent", "Permanent failure.")

    async def on_failed(row: JobRecord, error: JobError) -> None:
        observed.append((row.status, error.kind))

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            lease_seconds=60,
            on_failed=on_failed,
        )
    )
    job_id = await _enqueued_job(store, key="permanent-hook")

    assert (
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []),
            "web:local",
            job_id,
        )
        == JobStatus.failed.value
    )
    assert observed == [(JobStatus.running, "permanent")]


async def test_failed_hook_failure_leaves_job_nonterminal_and_is_retried() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    hook_calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise PermanentJobError("permanent", "Permanent failure.")

    async def on_failed(row: JobRecord, error: JobError) -> None:
        nonlocal hook_calls
        hook_calls += 1
        assert row.status is JobStatus.running
        if hook_calls == 1:
            raise RuntimeError("cleanup unavailable")

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            max_attempts=2,
            lease_seconds=10,
            on_failed=on_failed,
        )
    )
    job_id = await _enqueued_job(store, key="failed-hook-retry", max_attempts=2)
    clock = _Clock(_NOW)
    ctx = _ctx(store, registry, clock, [])

    with pytest.raises(RuntimeError, match="cleanup unavailable"):
        await run_job(ctx, "web:local", job_id)
    first = await store.get(job_id)
    assert first is not None
    assert first.status is JobStatus.running
    assert first.error_kind is None

    clock.value = _NOW + timedelta(seconds=11)
    assert await run_job(ctx, "web:local", job_id) == JobStatus.failed.value
    assert hook_calls == 2


async def test_run_job_validation_error_is_terminal() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise JobValidationError("invalid_payload", "Payload is invalid.")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))
    job_id = await _enqueued_job(store, key="validation")

    result = await run_job(
        _ctx(store, registry, _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    row = await store.get(job_id)
    assert result == JobStatus.failed.value
    assert row is not None and row.error_kind == "invalid_payload"


async def test_run_job_rejects_non_job_result_as_permanent() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> Any:
        return {"not": "a JobResult"}

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))
    job_id = await _enqueued_job(store, key="invalid-result")

    result = await run_job(
        _ctx(store, registry, _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    row = await store.get(job_id)
    assert result == JobStatus.failed.value
    assert row is not None and row.error_kind == "invalid_job_result"


async def test_run_job_retryable_error_requeues_and_defers_delivery() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("provider_timeout", "Provider timed out.")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=60))
    job_id = await _enqueued_job(store, key="retry")
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    clock = _Clock(_NOW)

    result = await run_job(_ctx(store, registry, clock, enqueued), "web:local", job_id)

    row = await store.get(job_id)
    assert result == JobStatus.queued.value
    assert row is not None and row.next_attempt_at == _NOW + timedelta(seconds=5)
    assert enqueued == [
        (
            "run_job",
            ("web:local", job_id),
            {"_defer_until": _NOW + timedelta(seconds=5)},
        )
    ]


async def test_run_job_retryable_error_fails_terminal_on_last_attempt() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("provider_timeout", "Provider timed out.")

    registry.register(JobDefinition("test.echo", handler, max_attempts=1, lease_seconds=60))
    job_id = await _enqueued_job(store, key="retry-final", max_attempts=1)
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    result = await run_job(
        _ctx(store, registry, _Clock(_NOW), enqueued),
        "web:local",
        job_id,
    )

    row = await store.get(job_id)
    assert result == JobStatus.failed.value
    assert row is not None and row.error_kind == "provider_timeout"
    assert row.attempt == 1
    assert enqueued == []


async def test_failed_hook_runs_for_retry_exhaustion_before_terminal_transition() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    observed: list[tuple[JobStatus, str]] = []

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("provider_timeout", "Provider timed out.")

    async def on_failed(row: JobRecord, error: JobError) -> None:
        observed.append((row.status, error.kind))

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            max_attempts=1,
            lease_seconds=60,
            on_failed=on_failed,
        )
    )
    job_id = await _enqueued_job(store, key="retry-hook", max_attempts=1)

    assert (
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []),
            "web:local",
            job_id,
        )
        == JobStatus.failed.value
    )
    assert observed == [(JobStatus.running, "provider_timeout")]


async def test_run_job_retry_enqueue_is_best_effort(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("provider_timeout", "Provider timed out.")

    async def failed_enqueue(name: str, *args: object, **options: object) -> None:
        raise RuntimeError("queue-token=DO-NOT-LOG")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))
    job_id = await _enqueued_job(store, key="retry-enqueue")
    ctx = _ctx(store, registry, _Clock(_NOW), [])
    ctx["enqueue"] = failed_enqueue
    caplog.set_level("WARNING", logger="keel.worker.jobs")

    result = await run_job(ctx, "web:local", job_id)

    row = await store.get(job_id)
    assert result == JobStatus.queued.value
    assert row is not None and row.next_attempt_at == _NOW + timedelta(seconds=5)
    assert "DO-NOT-LOG" not in caplog.text
    assert "RuntimeError" in caplog.text


async def test_run_job_failure_logs_do_not_include_exception_secrets(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RuntimeError(f"token={payload['secret']}")

    registry.register(JobDefinition("test.echo", handler, max_attempts=1, lease_seconds=60))
    job_id = await _enqueued_job(
        store,
        key="secret-error",
        max_attempts=1,
        payload={"secret": "DO-NOT-LOG"},
    )
    caplog.set_level("ERROR", logger="keel.worker.jobs")

    assert (
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []),
            "web:local",
            job_id,
        )
        == JobStatus.failed.value
    )
    assert "DO-NOT-LOG" not in caplog.text
    assert "RuntimeError" in caplog.text


async def test_finalization_error_suppresses_original_handler_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RuntimeError(f"token={payload['secret']}")

    async def broken_finalizer(*args: object, **kwargs: object) -> object:
        raise RuntimeError("database unavailable")

    registry.register(JobDefinition("test.echo", handler, max_attempts=1, lease_seconds=60))
    job_id = await _enqueued_job(
        store,
        key="secret-context",
        max_attempts=1,
        payload={"secret": "DO-NOT-LEAK"},
    )
    monkeypatch.setattr(store, "fail_terminal", broken_finalizer)

    with pytest.raises(RuntimeError) as caught:
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []),
            "web:local",
            job_id,
        )
    formatted = "".join(
        traceback.format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )
    assert "database unavailable" in formatted
    assert "DO-NOT-LEAK" not in formatted


async def test_finalization_clock_error_suppresses_original_handler_secret() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    calls = 0

    def failing_clock() -> datetime:
        nonlocal calls
        calls += 1
        if calls >= 3:
            raise RuntimeError("clock unavailable")
        return _NOW

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RuntimeError(f"token={payload['secret']}")

    registry.register(JobDefinition("test.echo", handler, max_attempts=1, lease_seconds=60))
    job_id = await _enqueued_job(
        store,
        key="secret-clock-context",
        max_attempts=1,
        payload={"secret": "DO-NOT-LEAK"},
    )
    ctx = _ctx(store, registry, _Clock(_NOW), [])
    ctx["job_clock"] = failing_clock

    with pytest.raises(RuntimeError) as caught:
        await run_job(ctx, "web:local", job_id)
    formatted = "".join(
        traceback.format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )
    assert "clock unavailable" in formatted
    assert "DO-NOT-LEAK" not in formatted


@pytest.mark.parametrize(
    "error_kind",
    ["permanent", "validation", "retryable", "unknown", "cancelled"],
)
async def test_run_job_returns_authoritative_status_when_lease_expires_during_error_path(
    error_kind: str,
) -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    clock = _Clock(_NOW)
    job_id = await _enqueued_job(store, key=f"late-{error_kind}")

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        if error_kind == "cancelled":
            await store.request_cancel(job_id, _NOW + timedelta(seconds=1))
        clock.value = _NOW + timedelta(seconds=61)
        if error_kind == "permanent":
            raise PermanentJobError("late", "late")
        if error_kind == "validation":
            raise JobValidationError("late", "late")
        if error_kind == "retryable":
            raise RetryableJobError("late", "late")
        if error_kind == "cancelled":
            raise JobCancellationRequested
        raise RuntimeError("late")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))

    assert (
        await run_job(
            _ctx(store, registry, clock, []),
            "web:local",
            job_id,
        )
        == JobStatus.running.value
    )


async def test_run_job_unknown_exception_retries_then_fails_terminal() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        raise RuntimeError("unexpected")

    registry.register(JobDefinition("test.echo", handler, max_attempts=2, lease_seconds=60))
    job_id = await _enqueued_job(store, key="unknown-error", max_attempts=2)
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    clock = _Clock(_NOW)

    assert (
        await run_job(_ctx(store, registry, clock, enqueued), "web:local", job_id)
        == JobStatus.queued.value
    )
    clock.value = _NOW + timedelta(seconds=5)
    assert (
        await run_job(_ctx(store, registry, clock, enqueued), "web:local", job_id)
        == JobStatus.failed.value
    )

    row = await store.get(job_id)
    assert calls == 2
    assert row is not None and row.error_kind == "internal_error"
    assert row.error_message == "job failed with a temporary internal error"
    assert row.attempt == 2
    assert len(enqueued) == 1


async def test_cancelled_deferred_enqueue_reports_queued_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attributes: dict[str, object] = {}

    class Span:
        def set_attribute(self, name: str, value: object) -> None:
            attributes[name] = value

    class SpanScope:
        def __enter__(self) -> Span:
            return Span()

        def __exit__(
            self,
            exc_type: object,
            exc_value: object,
            traceback_value: object,
        ) -> None:
            return None

    class Tracer:
        def start_as_current_span(self, name: str, **options: object) -> SpanScope:
            assert options == {
                "record_exception": False,
                "set_status_on_exception": False,
            }
            return SpanScope()

    monkeypatch.setattr("keel_worker.jobs.get_tracer", lambda name: Tracer())
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("temporary", "temporary")

    async def cancelled_enqueue(name: str, *args: object, **options: object) -> None:
        raise asyncio.CancelledError("QUEUE-SECRET")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))
    job_id = await _enqueued_job(store, key="cancelled-enqueue")
    ctx = _ctx(store, registry, _Clock(_NOW), [])
    ctx["enqueue"] = cancelled_enqueue

    with pytest.raises(asyncio.CancelledError) as caught:
        await run_job(ctx, "web:local", job_id)
    assert str(caught.value) == ""
    assert "QUEUE-SECRET" not in "".join(
        traceback.format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]
    assert attributes["job.status"] == JobStatus.queued.value


async def test_run_job_observes_persisted_cancel_before_reclaimed_handler() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    called = False

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal called
        called = True
        return JobResult(data={}, message="should not run")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=10))
    job_id = await _enqueued_job(store, key="cancel")
    first = await store.claim(job_id, _NOW, 10)
    assert first is not None
    await store.request_cancel(job_id, _NOW + timedelta(seconds=1))
    clock = _Clock(_NOW + timedelta(seconds=11))

    result = await run_job(_ctx(store, registry, clock, []), "web:local", job_id)

    assert result == JobStatus.cancelled.value
    assert called is False


async def test_cooperative_queued_cancel_runs_hook_before_terminal_transition() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    handler_called = False
    observed: list[tuple[JobStatus, datetime | None]] = []

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal handler_called
        handler_called = True
        return JobResult(data={}, message="should not run")

    async def on_cancelled(row: JobRecord) -> None:
        observed.append((row.status, row.cancel_requested_at))

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            lease_seconds=10,
            on_cancelled=on_cancelled,
        )
    )
    job_id = await _enqueued_job(
        store,
        key="cooperative-cancel-hook",
        cancel_mode=CancelMode.cooperative,
    )
    await store.request_cancel(job_id, _NOW)

    assert (
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []),
            "web:local",
            job_id,
        )
        == JobStatus.cancelled.value
    )
    assert handler_called is False
    assert observed == [(JobStatus.running, _NOW)]


async def test_cancelled_hook_failure_leaves_job_nonterminal_and_retries() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    hook_calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise AssertionError("cancelled job handler must not run")

    async def on_cancelled(row: JobRecord) -> None:
        nonlocal hook_calls
        hook_calls += 1
        assert row.status is JobStatus.running
        if hook_calls == 1:
            raise RuntimeError("cancel cleanup unavailable")

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            max_attempts=2,
            lease_seconds=10,
            on_cancelled=on_cancelled,
        )
    )
    job_id = await _enqueued_job(
        store,
        key="cancel-hook-retry",
        max_attempts=2,
        cancel_mode=CancelMode.cooperative,
    )
    await store.request_cancel(job_id, _NOW)
    clock = _Clock(_NOW)
    ctx = _ctx(store, registry, clock, [])

    with pytest.raises(RuntimeError, match="cancel cleanup unavailable"):
        await run_job(ctx, "web:local", job_id)
    first = await store.get(job_id)
    assert first is not None
    assert first.status is JobStatus.running
    assert first.cancel_requested_at == _NOW

    clock.value = _NOW + timedelta(seconds=11)
    assert await run_job(ctx, "web:local", job_id) == JobStatus.cancelled.value
    assert hook_calls == 2


async def test_cancel_after_retry_becomes_due_and_runs_cleanup_without_handler_retry() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    handler_calls = 0
    hook_calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal handler_calls
        handler_calls += 1
        raise RetryableJobError("temporary", "Temporary failure.")

    async def on_cancelled(row: JobRecord) -> None:
        nonlocal hook_calls
        hook_calls += 1
        assert row.status is JobStatus.running

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            max_attempts=3,
            lease_seconds=10,
            on_cancelled=on_cancelled,
        )
    )
    job_id = await _enqueued_job(
        store,
        key="cancel-after-retry",
        cancel_mode=CancelMode.cooperative,
    )
    clock = _Clock(_NOW)
    ctx = _ctx(store, registry, clock, [])

    assert await run_job(ctx, "web:local", job_id) == JobStatus.queued.value
    await store.request_cancel(job_id, _NOW + timedelta(seconds=1))
    clock.value = _NOW + timedelta(seconds=1)

    assert await run_job(ctx, "web:local", job_id) == JobStatus.cancelled.value
    assert handler_calls == 1
    assert hook_calls == 1


async def test_run_job_returns_current_status_when_lease_is_lost() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    clock = _Clock(_NOW)

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        clock.value = _NOW + timedelta(seconds=61)
        await context.checkpoint()
        return JobResult(data={}, message="should not finish")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))
    job_id = await _enqueued_job(store, key="lease-lost")

    result = await run_job(
        _ctx(store, registry, clock, []),
        "web:local",
        job_id,
    )

    assert result == JobStatus.running.value
    assert (await store.get(job_id)).status is JobStatus.running  # type: ignore[union-attr]


async def test_lease_loss_status_lookup_suppresses_context_and_cancellation_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise JobLeaseLostError("HANDLER-SECRET")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=60))
    job_id = await _enqueued_job(store, key="status-lookup-secret")
    real_get = store.get
    calls = 0

    async def cancelling_get(value: str) -> object:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise asyncio.CancelledError("STATUS-SECRET")
        return await real_get(value)

    monkeypatch.setattr(store, "get", cancelling_get)
    with pytest.raises(asyncio.CancelledError) as caught:
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []),
            "web:local",
            job_id,
        )
    formatted = "".join(
        traceback.format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )
    assert str(caught.value) == ""
    assert "STATUS-SECRET" not in formatted
    assert "HANDLER-SECRET" not in formatted


async def test_run_job_propagates_asyncio_cancelled_error() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise asyncio.CancelledError("HANDLER-SECRET")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=60))
    job_id = await _enqueued_job(store, key="worker-shutdown")

    with pytest.raises(asyncio.CancelledError) as caught:
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []),
            "web:local",
            job_id,
        )
    assert str(caught.value) == ""
    assert "HANDLER-SECRET" not in "".join(
        traceback.format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )

    assert (await store.get(job_id)).status is JobStatus.running  # type: ignore[union-attr]


async def test_run_job_traces_only_safe_execution_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attributes: dict[str, object] = {}
    span_options: dict[str, object] = {}

    class Span:
        def set_attribute(self, name: str, value: object) -> None:
            attributes[name] = value

    class SpanScope:
        def __enter__(self) -> Span:
            return Span()

        def __exit__(
            self,
            exc_type: object,
            exc_value: object,
            traceback: object,
        ) -> None:
            return None

    class Tracer:
        def start_as_current_span(self, name: str, **options: object) -> SpanScope:
            assert name == "job.execute"
            span_options.update(options)
            return SpanScope()

    monkeypatch.setattr("keel_worker.jobs.get_tracer", lambda name: Tracer())
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    registry.register(JobDefinition("test.echo", _handler, lease_seconds=60))
    job_id = await _enqueued_job(
        store,
        key="trace-redaction",
        payload={"secret": "DO-NOT-TRACE"},
    )

    await run_job(
        _ctx(store, registry, _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    assert attributes == {
        "job.id": job_id,
        "job.kind": "test.echo",
        "job.scope_id": "web:local",
        "job.attempt": 1,
        "job.status": JobStatus.succeeded.value,
    }
    assert span_options == {
        "record_exception": False,
        "set_status_on_exception": False,
    }
    assert "DO-NOT-TRACE" not in repr(attributes)
    assert "done" not in repr(attributes)


async def test_run_job_logs_no_payload_or_result(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="keel.worker.jobs")
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    registry.register(JobDefinition("test.echo", _handler, lease_seconds=60))
    job_id = await _enqueued_job(
        store,
        key="redaction",
        payload={"secret": "DO-NOT-LOG"},
    )

    await run_job(
        _ctx(store, registry, _Clock(_NOW), []),
        "web:local",
        job_id,
    )

    assert "DO-NOT-LOG" not in caplog.text
    assert "done" not in caplog.text


async def test_failed_deferred_enqueue_is_recovered_when_retry_becomes_due(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("provider_timeout", "Provider timed out.")

    async def unavailable(name: str, *args: object, **options: object) -> None:
        raise RuntimeError("redis-token=DO-NOT-LOG")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=10))
    job_id = await _enqueued_job(store, key="lost-defer")
    clock = _Clock(_NOW)
    ctx = _ctx(store, registry, clock, [])
    ctx["enqueue"] = unavailable
    caplog.set_level(logging.WARNING, logger="keel.worker.jobs")

    assert await run_job(ctx, "web:local", job_id) == JobStatus.queued.value

    recovered: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    async def enqueue(name: str, *args: object, **options: object) -> None:
        recovered.append((name, args, options))

    ctx["enqueue"] = enqueue
    clock.value = _NOW + timedelta(seconds=4)
    assert await dispatch_jobs(ctx) == 0
    clock.value = _NOW + timedelta(seconds=5)
    assert await dispatch_jobs(ctx) == 1
    assert recovered == [("run_job", ("web:local", job_id), {})]
    assert "DO-NOT-LOG" not in caplog.text
    assert "RuntimeError" in caplog.text


async def test_dispatcher_queries_clock_and_limit_then_enqueues_due_and_reclaimable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryJobStore("web:local")
    due_id = await _enqueued_job(store, key="dispatch-due")
    reclaimable_id = await _enqueued_job(
        store,
        key="dispatch-reclaimable",
        max_attempts=2,
    )
    assert await store.claim(reclaimable_id, _NOW, 10) is not None
    local_now = datetime(
        2026,
        7,
        14,
        17,
        0,
        11,
        tzinfo=timezone(timedelta(hours=8)),
    )
    clock_calls = 0

    def clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        return local_now

    queries: list[tuple[str, datetime, int]] = []
    real_dispatchable = store.dispatchable
    real_exhausted = store.exhausted

    async def dispatchable(now: datetime, limit: int) -> list[str]:
        queries.append(("dispatchable", now, limit))
        return await real_dispatchable(now, limit)

    async def exhausted(now: datetime, limit: int) -> list[str]:
        queries.append(("exhausted", now, limit))
        return await real_exhausted(now, limit)

    monkeypatch.setattr(store, "dispatchable", dispatchable)
    monkeypatch.setattr(store, "exhausted", exhausted)
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    ctx = _ctx(store, JobRegistry(), _Clock(_NOW), enqueued)
    ctx["job_clock"] = clock
    ctx["job_settings"] = Settings(job_dispatch_limit=2)

    assert await dispatch_jobs(ctx) == 2
    assert clock_calls == 1
    assert queries == [
        ("dispatchable", local_now, 2),
        ("exhausted", local_now, 2),
    ]
    assert {str(args[1]) for _, args, _ in enqueued} == {due_id, reclaimable_id}
    assert all(name == "run_job" and options == {} for name, _, options in enqueued)


async def test_dispatcher_finalizes_crash_at_attempt_ceiling() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise asyncio.CancelledError

    registry.register(JobDefinition("test.echo", handler, max_attempts=1, lease_seconds=10))
    job_id = await _enqueued_job(store, key="dispatch-exhausted", max_attempts=1)
    clock = _Clock(_NOW)
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    ctx = _ctx(store, registry, clock, enqueued)

    with pytest.raises(asyncio.CancelledError):
        await run_job(ctx, "web:local", job_id)
    clock.value = _NOW + timedelta(seconds=11)

    assert await dispatch_jobs(ctx) == 1
    assert enqueued == []
    exhausted = await store.get(job_id)
    assert exhausted is not None and exhausted.status is JobStatus.failed
    assert exhausted.error_kind == "attempts_exhausted"


async def test_dispatcher_runs_failed_hook_before_lease_exhaustion_terminal_transition() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    observed: list[tuple[JobStatus, str]] = []

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise asyncio.CancelledError

    async def on_failed(row: JobRecord, error: JobError) -> None:
        observed.append((row.status, error.kind))

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            max_attempts=1,
            lease_seconds=10,
            on_failed=on_failed,
        )
    )
    job_id = await _enqueued_job(store, key="lease-failed-hook", max_attempts=1)
    ctx = _ctx(store, registry, _Clock(_NOW), [])
    with pytest.raises(asyncio.CancelledError):
        await run_job(ctx, "web:local", job_id)

    ctx["job_clock"] = _Clock(_NOW + timedelta(seconds=11))
    assert await dispatch_jobs(ctx) == 1
    assert observed == [(JobStatus.running, "attempts_exhausted")]
    assert (await store.get(job_id)).status is JobStatus.failed  # type: ignore[union-attr]


async def test_dispatcher_cancelled_lease_exhaustion_runs_hook_and_cancels() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    observed: list[JobStatus] = []

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        await store.request_cancel(context.job_id, _NOW + timedelta(seconds=1))
        raise asyncio.CancelledError

    async def on_cancelled(row: JobRecord) -> None:
        observed.append(row.status)

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            max_attempts=1,
            lease_seconds=10,
            on_cancelled=on_cancelled,
        )
    )
    job_id = await _enqueued_job(
        store,
        key="lease-cancel-hook",
        max_attempts=1,
        cancel_mode=CancelMode.cooperative,
    )
    ctx = _ctx(store, registry, _Clock(_NOW), [])
    with pytest.raises(asyncio.CancelledError):
        await run_job(ctx, "web:local", job_id)

    ctx["job_clock"] = _Clock(_NOW + timedelta(seconds=11))
    assert await dispatch_jobs(ctx) == 1
    row = await store.get(job_id)
    assert row is not None
    assert row.status is JobStatus.cancelled
    assert row.error_kind is None
    assert observed == [JobStatus.running]


async def test_dispatcher_hook_failure_leaves_expired_job_nonterminal_for_retry() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    hook_calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise asyncio.CancelledError

    async def on_failed(row: JobRecord, error: JobError) -> None:
        nonlocal hook_calls
        hook_calls += 1
        if hook_calls == 1:
            raise RuntimeError("cleanup unavailable")

    registry.register(
        JobDefinition(
            "test.echo",
            handler,
            max_attempts=1,
            lease_seconds=10,
            on_failed=on_failed,
        )
    )
    job_id = await _enqueued_job(store, key="lease-hook-retry", max_attempts=1)
    ctx = _ctx(store, registry, _Clock(_NOW), [])
    with pytest.raises(asyncio.CancelledError):
        await run_job(ctx, "web:local", job_id)

    ctx["job_clock"] = _Clock(_NOW + timedelta(seconds=11))
    assert await dispatch_jobs(ctx) == 0
    assert (await store.get(job_id)).status is JobStatus.running  # type: ignore[union-attr]
    assert await dispatch_jobs(ctx) == 1
    assert (await store.get(job_id)).status is JobStatus.failed  # type: ignore[union-attr]
    assert hook_calls == 2


async def test_dispatcher_recovers_lost_immediate_enqueue_and_tolerates_duplicates() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return JobResult(data={"calls": calls}, message="done")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=30))
    job_id = await _enqueued_job(store, key="lost-immediate")
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    ctx = _ctx(store, registry, _Clock(_NOW), enqueued)

    assert await dispatch_jobs(ctx) == 1
    assert await dispatch_jobs(ctx) == 1
    assert enqueued == [
        ("run_job", ("web:local", job_id), {}),
        ("run_job", ("web:local", job_id), {}),
    ]

    first, second = await asyncio.gather(
        run_job(ctx, "web:local", job_id),
        run_job(ctx, "web:local", job_id),
    )
    assert {first, second} <= {JobStatus.running.value, JobStatus.succeeded.value}
    assert await run_job(ctx, "web:local", job_id) == JobStatus.succeeded.value
    assert calls == 1


async def test_dispatcher_continues_after_enqueue_failure_without_logging_secret(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = InMemoryJobStore("web:local")
    first = await _enqueued_job(store, key="dispatch-1")
    second = await _enqueued_job(store, key="dispatch-2")
    attempted: list[str] = []

    async def flaky(name: str, *args: object, **options: object) -> None:
        job_id = str(args[1])
        attempted.append(job_id)
        if job_id == first:
            raise RuntimeError("redis-token=DO-NOT-LOG")

    ctx = _ctx(store, JobRegistry(), _Clock(_NOW), [])
    ctx["enqueue"] = flaky
    ctx["job_settings"] = Settings(job_dispatch_limit=100)
    caplog.set_level(logging.WARNING, logger="keel.worker.jobs")

    assert await dispatch_jobs(ctx) == 1
    assert set(attempted) == {first, second}
    assert "DO-NOT-LOG" not in caplog.text
    assert "RuntimeError" in caplog.text


async def test_dispatcher_continues_after_exhaustion_failure_without_logging_secret(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = InMemoryJobStore("web:local")
    first = await _enqueued_job(store, key="exhaustion-1", max_attempts=1)
    second = await _enqueued_job(store, key="exhaustion-2", max_attempts=1)
    assert await store.claim(first, _NOW, 10) is not None
    assert await store.claim(second, _NOW, 10) is not None
    attempted: list[str] = []
    real_fail_exhausted = store.fail_exhausted

    async def flaky(job_id: str, now: datetime) -> object:
        attempted.append(job_id)
        if job_id == first:
            raise RuntimeError("database-token=DO-NOT-LOG")
        return await real_fail_exhausted(job_id, now)

    monkeypatch.setattr(store, "fail_exhausted", flaky)
    caplog.set_level(logging.ERROR, logger="keel.worker.jobs")

    assert (
        await dispatch_jobs(
            _ctx(
                store,
                JobRegistry(),
                _Clock(_NOW + timedelta(seconds=11)),
                [],
            )
        )
        == 1
    )
    assert set(attempted) == {first, second}
    assert (await store.get(first)).status is JobStatus.running  # type: ignore[union-attr]
    assert (await store.get(second)).status is JobStatus.failed  # type: ignore[union-attr]
    assert "DO-NOT-LOG" not in caplog.text
    assert "RuntimeError" in caplog.text


async def test_dispatcher_fails_closed_on_store_scope_mismatch() -> None:
    store = InMemoryJobStore("scope:store")
    job_id = await _enqueued_job(store, key="dispatch-scope")
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    ctx = _ctx(store, JobRegistry(), _Clock(_NOW), enqueued)

    assert await dispatch_jobs(ctx) == 0
    assert enqueued == []
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]


@pytest.mark.parametrize("clock", [None, "not-callable", lambda: "not-a-datetime"])
async def test_dispatcher_rejects_invalid_configured_clocks(clock: object) -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _enqueued_job(store, key=f"invalid-clock-{type(clock).__name__}")
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    ctx = _ctx(store, JobRegistry(), _Clock(_NOW), enqueued)
    ctx["job_clock"] = clock

    with pytest.raises(TypeError, match="job_clock"):
        await dispatch_jobs(ctx)
    assert enqueued == []
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]


async def test_dispatcher_rejects_naive_clock_before_delivery() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _enqueued_job(store, key="naive-dispatch-clock")
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    ctx = _ctx(
        store,
        JobRegistry(),
        _Clock(datetime(2026, 7, 14, 9, 0)),
        enqueued,
    )

    with pytest.raises(JobValidationError) as caught:
        await dispatch_jobs(ctx)
    assert caught.value.code == "timezone_required"
    assert enqueued == []
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]


async def test_dispatcher_redacts_cancelled_enqueue_message() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _enqueued_job(store, key="cancelled-dispatch-enqueue")
    ctx = _ctx(store, JobRegistry(), _Clock(_NOW), [])

    async def cancelled_enqueue(
        name: str,
        *args: object,
        **options: object,
    ) -> None:
        raise asyncio.CancelledError("QUEUE-SECRET")

    ctx["enqueue"] = cancelled_enqueue
    with pytest.raises(asyncio.CancelledError) as caught:
        await dispatch_jobs(ctx)
    formatted = "".join(
        traceback.format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )
    assert str(caught.value) == ""
    assert "QUEUE-SECRET" not in formatted
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]


async def test_dispatcher_redacts_cancelled_finalizer_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _enqueued_job(store, key="cancelled-finalizer", max_attempts=1)
    assert await store.claim(job_id, _NOW, 10) is not None

    async def cancelled_finalizer(value: str, now: datetime) -> None:
        raise asyncio.CancelledError("FINALIZER-SECRET")

    monkeypatch.setattr(store, "fail_exhausted", cancelled_finalizer)
    with pytest.raises(asyncio.CancelledError) as caught:
        await dispatch_jobs(
            _ctx(
                store,
                JobRegistry(),
                _Clock(_NOW + timedelta(seconds=11)),
                [],
            )
        )
    formatted = "".join(
        traceback.format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )
    assert str(caught.value) == ""
    assert "FINALIZER-SECRET" not in formatted
    assert (await store.get(job_id)).status is JobStatus.running  # type: ignore[union-attr]
