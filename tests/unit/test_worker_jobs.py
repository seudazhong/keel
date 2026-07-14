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
    InMemoryJobStore,
    JobCancellationRequested,
    JobLeaseLostError,
    JobResult,
    JobStatus,
    JobValidationError,
    PermanentJobError,
    RetryableJobError,
)
from keel_worker.jobs import JobContext, JobDefinition, JobRegistry, run_job

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
) -> str:
    job, _ = await store.enqueue_once(
        kind=kind,
        payload=payload or {},
        target_session_id=None,
        idempotency_key=key,
        max_attempts=max_attempts,
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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_attempts", 0),
        ("max_attempts", -1),
        ("max_attempts", True),
        ("max_attempts", 1.5),
        ("max_attempts", float("nan")),
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
    assert (context.scope_id, context.attempt) == ("web:local", 1)
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
