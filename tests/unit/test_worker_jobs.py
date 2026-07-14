"""Durable-job worker registry and cooperative context."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from keel_core.jobs import (
    InMemoryJobStore,
    JobCancellationRequested,
    JobLeaseLostError,
    JobResult,
)
from keel_worker.jobs import JobContext, JobDefinition, JobRegistry

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


async def _handler(context: JobContext, payload: dict[str, object]) -> JobResult:
    return JobResult(data={"attempt": context.attempt, **payload}, message="done")


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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_attempts", 0),
        ("max_attempts", -1),
        ("lease_seconds", 0),
        ("lease_seconds", -1),
    ],
)
def test_job_definition_requires_positive_limits(field: str, value: int) -> None:
    kwargs = {field: value}
    with pytest.raises(ValueError, match=field):
        JobDefinition(kind="test.bad", handler=_handler, **kwargs)


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
