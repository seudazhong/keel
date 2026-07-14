"""Acceptance: durable jobs over real Postgres + Redis/arq with injected handlers."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import redis.asyncio as aioredis
from arq.connections import RedisSettings, create_pool
from arq.constants import (
    health_check_key_suffix,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)
from arq.worker import Worker
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.events import Event, EventType
from keel_core.jobs import (
    JobResult,
    JobStatus,
    PermanentJobError,
    PostgresJobStore,
    RetryableJobError,
)
from keel_core.loop import admit
from keel_core.projections import project_messages
from keel_core.state import PostgresEventStore
from keel_worker.jobs import (
    JobContext,
    JobDefinition,
    JobRegistry,
    dispatch_jobs,
    run_job,
)

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


class _Clock:
    def __init__(self, value: datetime = _NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


async def _target(engine: AsyncEngine, scope: str, session_id: str) -> None:
    store = PostgresEventStore(engine, scope)
    await admit(store, session_id, scope, "start")
    await store.append(
        Event(
            type=EventType.message_token,
            seq=0,
            session_id=session_id,
            scope_id=scope,
            ts=_NOW,
            payload={"role": "assistant", "text": "previous answer", "partial": False},
        )
    )


async def _job(
    store: PostgresJobStore,
    *,
    key: str,
    target_session_id: str | None = None,
    max_attempts: int = 3,
    payload: dict[str, Any] | None = None,
) -> str:
    row, _ = await store.enqueue_once(
        kind="test.acceptance",
        payload=payload or {},
        target_session_id=target_session_id,
        idempotency_key=key,
        max_attempts=max_attempts,
        now=_NOW,
    )
    return row.id


def _ctx(
    store: PostgresJobStore,
    registry: JobRegistry,
    clock: _Clock,
    enqueue: Any,
) -> dict[str, Any]:
    return {
        "jobs": store,
        "job_registry": registry,
        "durable_scope": store.scope_id,
        "enqueue": enqueue,
        "job_clock": clock,
        "job_settings": Settings(
            job_retry_base_seconds=5,
            job_retry_max_seconds=300,
            job_dispatch_limit=100,
        ),
    }


async def _events(engine: AsyncEngine, scope: str, session_id: str) -> list[Event]:
    return [event async for event in PostgresEventStore(engine, scope).read(session_id)]


async def _job_events(engine: AsyncEngine, scope: str, session_id: str, job_id: str) -> list[Event]:
    return [
        event
        for event in await _events(engine, scope, session_id)
        if event.payload.get("job_id") == job_id
    ]


def _assert_injection(
    events: list[Event],
    *,
    job_id: str,
    status: JobStatus,
) -> Event:
    assert len(events) == 1
    event = events[0]
    assert event.type is EventType.message_token
    assert event.run_id is None
    assert event.payload["role"] == "assistant"
    assert event.payload["partial"] is False
    assert event.payload["job_id"] == job_id
    assert event.payload["job_kind"] == "test.acceptance"
    assert event.payload["job_status"] == status.value
    return event


def _arq_keys(queue_name: str, arq_job_ids: list[str]) -> list[str]:
    keys = [queue_name, f"{queue_name}{health_check_key_suffix}"]
    for arq_job_id in arq_job_ids:
        keys.extend(
            [
                f"{job_key_prefix}{arq_job_id}",
                f"{result_key_prefix}{arq_job_id}",
                f"{in_progress_key_prefix}{arq_job_id}",
                f"{retry_key_prefix}{arq_job_id}",
            ]
        )
    return keys


async def _noop_enqueue(
    name: str,
    *args: object,
    **options: object,
) -> None:
    return None


async def test_dispatcher_heals_missed_enqueue_and_real_arq_duplicates_execute_once(
    migrated_db: AsyncEngine,
    redis_client: aioredis.Redis,
) -> None:
    scope = f"accept:arq:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        await context.progress(1, total=1, message="done")
        await asyncio.sleep(0.05)
        return JobResult(data={"ok": True}, message="arq completed")

    registry.register(
        JobDefinition(
            "test.acceptance",
            handler,
            max_attempts=3,
            lease_seconds=30,
        )
    )
    job_id = await _job(store, key="arq", target_session_id=session_id)
    redis_url = os.environ.get("KEEL_TEST_REDIS_URL", "redis://localhost:6379/15")
    pool = await create_pool(RedisSettings.from_dsn(redis_url))
    queue_name = f"arq:jobs:{uuid.uuid4().hex}"
    arq_job_ids = [f"acceptance-{uuid.uuid4().hex}" for _ in range(2)]
    keys = _arq_keys(queue_name, arq_job_ids)
    enqueued = 0

    async def enqueue(name: str, *args: object, **options: object) -> None:
        nonlocal enqueued
        arq_job_id = arq_job_ids[enqueued]
        enqueued += 1
        delivery = await pool.enqueue_job(
            name,
            *args,
            _job_id=arq_job_id,
            _queue_name=queue_name,
            **options,
        )
        assert delivery is not None

    try:
        await pool.delete(*keys)
        ctx = _ctx(store, registry, _Clock(), enqueue)

        # enqueue_once() committed only to Postgres. Two dispatcher passes heal the
        # missed immediate enqueue and model at-least-once duplicate delivery.
        assert await pool.zcard(queue_name) == 0
        assert await dispatch_jobs(ctx) == 1
        assert await dispatch_jobs(ctx) == 1
        assert enqueued == 2
        assert await pool.zcard(queue_name) == 2

        worker = Worker(
            functions=[run_job],
            queue_name=queue_name,
            redis_pool=pool,
            burst=True,
            handle_signals=False,
            max_jobs=2,
            keep_result=0,
            poll_delay=0.01,
            ctx=ctx,
        )
        await worker.async_run()

        row = await store.get(job_id)
        assert row is not None
        assert row.status is JobStatus.succeeded
        assert row.attempt == 1
        assert row.result == {"ok": True}
        assert calls == 1
        assert await pool.zcard(queue_name) == 0

        injection = _assert_injection(
            await _job_events(migrated_db, scope, session_id, job_id),
            job_id=job_id,
            status=JobStatus.succeeded,
        )
        assert injection.payload["text"] == "arq completed"
        assert row.injected_event_seq == injection.seq
        assert not any(
            event.type is EventType.run_started
            for event in await _events(migrated_db, scope, session_id)
        )
    finally:
        await pool.delete(*keys)
        assert await redis_client.exists(*keys) == 0
        await pool.aclose()


async def test_concurrent_deliveries_execute_handler_once_and_inject_once(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:duplicate:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return JobResult(data={"calls": calls}, message="duplicate-safe")

    registry.register(JobDefinition("test.acceptance", handler, lease_seconds=30))
    job_id = await _job(store, key="duplicate", target_session_id=session_id)
    ctx = _ctx(store, registry, _Clock(), _noop_enqueue)

    first = asyncio.create_task(run_job(ctx, scope, job_id))
    await asyncio.wait_for(entered.wait(), timeout=5)
    try:
        assert await run_job(ctx, scope, job_id) == JobStatus.running.value
    finally:
        release.set()
    assert await first == JobStatus.succeeded.value
    assert await run_job(ctx, scope, job_id) == JobStatus.succeeded.value

    row = await store.get(job_id)
    assert row is not None
    assert row.status is JobStatus.succeeded
    assert row.attempt == 1
    assert calls == 1
    injection = _assert_injection(
        await _job_events(migrated_db, scope, session_id, job_id),
        job_id=job_id,
        status=JobStatus.succeeded,
    )
    assert row.injected_event_seq == injection.seq


async def test_retryable_handler_fails_twice_then_succeeds_at_deterministic_times(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:retry:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RetryableJobError("embedding_timeout", "Embedding timed out.")
        return JobResult(data={"chunks": 42}, message="indexed 42 chunks")

    registry.register(
        JobDefinition(
            "test.acceptance",
            handler,
            max_attempts=3,
            lease_seconds=30,
        )
    )
    job_id = await _job(
        store,
        key="retry",
        target_session_id=session_id,
        max_attempts=3,
    )
    deferred: list[datetime] = []

    async def enqueue(name: str, *args: object, **options: object) -> None:
        assert name == "run_job"
        assert args == (scope, job_id)
        defer_until = options.get("_defer_until")
        assert isinstance(defer_until, datetime)
        deferred.append(defer_until)

    clock = _Clock()
    ctx = _ctx(store, registry, clock, enqueue)
    assert await run_job(ctx, scope, job_id) == JobStatus.queued.value
    assert await _job_events(migrated_db, scope, session_id, job_id) == []
    clock.advance(5)
    assert await run_job(ctx, scope, job_id) == JobStatus.queued.value
    assert await _job_events(migrated_db, scope, session_id, job_id) == []
    clock.advance(10)
    assert await run_job(ctx, scope, job_id) == JobStatus.succeeded.value

    row = await store.get(job_id)
    assert row is not None
    assert row.status is JobStatus.succeeded
    assert row.attempt == 3
    assert row.result == {"chunks": 42}
    assert calls == 3
    assert deferred == [
        _NOW + timedelta(seconds=5),
        _NOW + timedelta(seconds=15),
    ]
    injection = _assert_injection(
        await _job_events(migrated_db, scope, session_id, job_id),
        job_id=job_id,
        status=JobStatus.succeeded,
    )
    assert injection.payload["text"] == "indexed 42 chunks"
    assert row.injected_event_seq == injection.seq


async def test_permanent_handler_executes_once_and_duplicate_delivery_stays_terminal(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:permanent:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        raise PermanentJobError("invalid_document", "Document is invalid.")

    registry.register(
        JobDefinition(
            "test.acceptance",
            handler,
            max_attempts=3,
            lease_seconds=30,
        )
    )
    job_id = await _job(store, key="permanent", target_session_id=session_id)
    ctx = _ctx(store, registry, _Clock(), _noop_enqueue)

    assert await run_job(ctx, scope, job_id) == JobStatus.failed.value
    assert await run_job(ctx, scope, job_id) == JobStatus.failed.value

    row = await store.get(job_id)
    assert row is not None
    assert row.status is JobStatus.failed
    assert row.error_kind == "invalid_document"
    assert row.attempt == 1
    assert calls == 1
    injection = _assert_injection(
        await _job_events(migrated_db, scope, session_id, job_id),
        job_id=job_id,
        status=JobStatus.failed,
    )
    assert injection.payload["text"] == "后台任务 test.acceptance 失败：invalid_document"
    assert row.injected_event_seq == injection.seq


async def test_worker_cancelled_error_leaves_lease_for_reclaim(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:crash:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise asyncio.CancelledError()
        return JobResult(data={"recovered": True}, message="recovered")

    registry.register(
        JobDefinition(
            "test.acceptance",
            handler,
            max_attempts=2,
            lease_seconds=10,
        )
    )
    job_id = await _job(
        store,
        key="crash",
        target_session_id=session_id,
        max_attempts=2,
    )
    clock = _Clock()
    ctx = _ctx(store, registry, clock, _noop_enqueue)

    with pytest.raises(asyncio.CancelledError):
        await run_job(ctx, scope, job_id)
    after_crash = await store.get(job_id)
    assert after_crash is not None
    assert after_crash.status is JobStatus.running
    assert after_crash.attempt == 1
    assert after_crash.injected_event_seq is None
    assert await _job_events(migrated_db, scope, session_id, job_id) == []

    clock.advance(11)
    assert await run_job(ctx, scope, job_id) == JobStatus.succeeded.value
    recovered = await store.get(job_id)
    assert recovered is not None
    assert recovered.status is JobStatus.succeeded
    assert recovered.attempt == 2
    assert calls == 2
    injection = _assert_injection(
        await _job_events(migrated_db, scope, session_id, job_id),
        job_id=job_id,
        status=JobStatus.succeeded,
    )
    assert recovered.injected_event_seq == injection.seq


async def test_crash_at_attempt_ceiling_is_atomically_failed_and_injected(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:exhaust:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise asyncio.CancelledError()

    registry.register(
        JobDefinition(
            "test.acceptance",
            handler,
            max_attempts=1,
            lease_seconds=10,
        )
    )
    job_id = await _job(
        store,
        key="exhaust",
        target_session_id=session_id,
        max_attempts=1,
    )
    clock = _Clock()
    ctx = _ctx(store, registry, clock, _noop_enqueue)

    with pytest.raises(asyncio.CancelledError):
        await run_job(ctx, scope, job_id)
    crashed = await store.get(job_id)
    assert crashed is not None
    assert crashed.status is JobStatus.running
    assert crashed.attempt == 1
    assert crashed.injected_event_seq is None
    assert await _job_events(migrated_db, scope, session_id, job_id) == []

    clock.advance(11)
    finalized = await asyncio.gather(dispatch_jobs(ctx), dispatch_jobs(ctx))
    assert sum(finalized) == 1

    failed = await store.get(job_id)
    assert failed is not None
    assert failed.status is JobStatus.failed
    assert failed.error_kind == "attempts_exhausted"
    assert failed.attempt == 1
    injection = _assert_injection(
        await _job_events(migrated_db, scope, session_id, job_id),
        job_id=job_id,
        status=JobStatus.failed,
    )
    assert injection.payload["text"] == "后台任务 test.acceptance 失败：attempts_exhausted"
    assert failed.injected_event_seq == injection.seq

    assert await dispatch_jobs(ctx) == 0
    assert await run_job(ctx, scope, job_id) == JobStatus.failed.value
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1


async def test_cooperative_cancel_injects_once_without_provider_run_and_projects_next_turn(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:cancel:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        entered.set()
        await release.wait()
        await context.checkpoint()
        return JobResult(data={}, message="must not win")

    registry.register(JobDefinition("test.acceptance", handler, lease_seconds=30))
    job_id = await _job(store, key="cancel", target_session_id=session_id)
    clock = _Clock()
    ctx = _ctx(store, registry, clock, _noop_enqueue)

    running = asyncio.create_task(run_job(ctx, scope, job_id))
    await asyncio.wait_for(entered.wait(), timeout=5)
    requested = await store.request_cancel(job_id, clock())
    assert requested is not None
    assert requested.status is JobStatus.running
    assert requested.cancel_requested_at == _NOW
    release.set()
    assert await running == JobStatus.cancelled.value
    assert await run_job(ctx, scope, job_id) == JobStatus.cancelled.value

    cancelled = await store.get(job_id)
    assert cancelled is not None
    assert cancelled.status is JobStatus.cancelled
    events_before_next_turn = await _events(migrated_db, scope, session_id)
    assert not any(event.type is EventType.run_started for event in events_before_next_turn)
    injection = _assert_injection(
        [event for event in events_before_next_turn if event.payload.get("job_id") == job_id],
        job_id=job_id,
        status=JobStatus.cancelled,
    )
    assert injection.payload["text"] == "后台任务 test.acceptance 已取消。"
    assert cancelled.injected_event_seq == injection.seq

    await admit(
        PostgresEventStore(migrated_db, scope),
        session_id,
        scope,
        "what happened?",
    )
    events_after_next_turn = await _events(migrated_db, scope, session_id)
    assert not any(event.type is EventType.run_started for event in events_after_next_turn)
    projected = project_messages(events_after_next_turn)
    assert [message["role"] for message in projected] == ["user", "assistant", "user"]
    assert projected[1]["content"] == ("previous answer\n\n后台任务 test.acceptance 已取消。")
