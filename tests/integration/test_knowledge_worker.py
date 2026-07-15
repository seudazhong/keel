"""Acceptance: durable Knowledge jobs over Postgres and isolated Redis/arq."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
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
from keel_core.jobs import (
    CancelMode,
    JobError,
    JobStatus,
    JobTerminalIntent,
    JobValidationError,
    PostgresJobStore,
)
from keel_core.knowledge import (
    KnowledgeBaseCreate,
    KnowledgeChunkReplacement,
    KnowledgeChunkWrite,
    KnowledgeDocumentVersionCreate,
    KnowledgeDocumentVersionRecord,
    KnowledgeJobHandlers,
    KnowledgeSourceType,
    KnowledgeStore,
    KnowledgeVersionStatus,
    PostgresKnowledgeStore,
    content_sha256,
)
from keel_worker.jobs import JobDefinition, JobRegistry, dispatch_jobs, run_job
from keel_worker.knowledge import knowledge_job_definitions, knowledge_job_registry

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 16, 2, 0, tzinfo=UTC)


class _Clock:
    def __init__(self, value: datetime = _NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


class _Embedder:
    model = "fake/embed"
    dim = 3

    def __init__(self) -> None:
        self.calls = 0

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += 1
        return [[1.0, 0.0, 0.0] for _ in texts]


class _FlakyEmbedder(_Embedder):
    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError
        return [[1.0, 0.0, 0.0] for _ in texts]


class _BlockingEmbedder(_Embedder):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return [[1.0, 0.0, 0.0] for _ in texts]


class _ActivateAfterReadPostgresStore(PostgresKnowledgeStore):
    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        super().__init__(engine, scope_id)
        self._race_target: tuple[str, str, str] | None = None
        self._reads_before_race = 0
        self.read_status: KnowledgeVersionStatus | None = None
        self.race_activated = False

    def arm_activation_race(
        self,
        kb_id: str,
        document_id: str,
        version_id: str,
        *,
        reads_before_race: int = 0,
    ) -> None:
        self._race_target = (kb_id, document_id, version_id)
        self._reads_before_race = reads_before_race

    async def get_version(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> KnowledgeDocumentVersionRecord | None:
        version = await super().get_version(kb_id, document_id, document_version_id)
        target = (kb_id, document_id, document_version_id)
        if self._race_target == target:
            if self._reads_before_race > 0:
                self._reads_before_race -= 1
            else:
                self._race_target = None
                self.read_status = None if version is None else version.status
                activation = await super().activate_version(kb_id, document_id, document_version_id)
                self.race_activated = activation.activated
        return version


def _settings() -> Settings:
    return Settings(
        embedding_model="fake/embed",
        embedding_dim=3,
        knowledge_embedding_batch_size=2,
        job_lease_seconds=30,
        job_retry_base_seconds=5,
        job_retry_max_seconds=30,
    )


async def _knowledge_document(
    engine: AsyncEngine,
    scope: str,
    *,
    content: str = "alpha beta gamma delta",
) -> tuple[PostgresKnowledgeStore, str, str, str]:
    store = PostgresKnowledgeStore(engine, scope)
    base = await store.create_base(
        KnowledgeBaseCreate(
            name=f"Docs {uuid.uuid4().hex}",
            description="private description",
            embedding_model="fake/embed",
            embedding_dim=3,
        ),
        now=_NOW,
    )
    created = await store.create_document_version(
        KnowledgeDocumentVersionCreate(
            kb_id=base.id,
            title="Guide",
            source_type=KnowledgeSourceType.text,
            source_uri="https://secret.example/guide",
            content=content,
            mime_type="text/plain",
            chunking_version="keel-char-v1",
            target_chars=6,
            overlap_chars=0,
        ),
        now=_NOW,
    )
    return store, base.id, created.document.id, created.version.id


def _context(
    jobs: PostgresJobStore,
    registry: JobRegistry,
    clock: _Clock,
    enqueue: Any,
) -> dict[str, Any]:
    return {
        "jobs": jobs,
        "job_registry": registry,
        "durable_scope": jobs.scope_id,
        "enqueue": enqueue,
        "job_clock": clock,
        "job_settings": _settings(),
    }


async def _enqueue_knowledge_job(
    jobs: PostgresJobStore,
    definition: JobDefinition,
    payload: dict[str, Any],
    *,
    key: str,
) -> str:
    row, _ = await jobs.enqueue_once(
        kind=definition.kind,
        payload=payload,
        target_session_id=None,
        idempotency_key=key,
        max_attempts=definition.max_attempts,
        cancel_mode=definition.cancel_mode,
        now=_NOW,
    )
    return row.id


def _arq_job_keys(arq_job_id: str) -> list[str]:
    return [
        f"{job_key_prefix}{arq_job_id}",
        f"{result_key_prefix}{arq_job_id}",
        f"{in_progress_key_prefix}{arq_job_id}",
        f"{retry_key_prefix}{arq_job_id}",
    ]


def _arq_keys(queue_name: str, arq_job_ids: list[str]) -> list[str]:
    keys = [queue_name, f"{queue_name}{health_check_key_suffix}"]
    for arq_job_id in arq_job_ids:
        keys.extend(_arq_job_keys(arq_job_id))
    return keys


async def _noop_enqueue(name: str, *args: object, **options: object) -> None:
    del name, args, options


async def test_real_arq_heals_lost_ingest_delivery_and_deduplicates(
    migrated_db: AsyncEngine,
    redis_client: Any,
) -> None:
    del redis_client
    scope = f"knowledge:arq:{uuid.uuid4().hex}"
    knowledge, kb_id, document_id, version_id = await _knowledge_document(migrated_db, scope)
    jobs = PostgresJobStore(migrated_db, scope)
    embedder = _Embedder()
    registry = knowledge_job_registry(cast(KnowledgeStore, knowledge), embedder, _settings())
    assert registry.kinds() == ("knowledge.delete", "knowledge.ingest")
    ingest = registry.get("knowledge.ingest")
    assert ingest is not None
    job_id = await _enqueue_knowledge_job(
        jobs,
        ingest,
        {
            "kb_id": kb_id,
            "document_id": document_id,
            "document_version_id": version_id,
        },
        key=f"ingest:{version_id}",
    )
    await knowledge.attach_version_job(kb_id, document_id, version_id, job_id)

    redis_url = os.environ.get("KEEL_TEST_REDIS_URL", "redis://localhost:6379/15")
    pool = await create_pool(RedisSettings.from_dsn(redis_url))
    queue_name = f"arq:knowledge:{uuid.uuid4().hex}"
    arq_job_ids = [f"knowledge-{uuid.uuid4().hex}" for _ in range(2)]
    keys = _arq_keys(queue_name, arq_job_ids)
    enqueued = 0

    async def enqueue(name: str, *args: object, **options: object) -> None:
        nonlocal enqueued
        arq_job_id = arq_job_ids[enqueued]
        enqueued += 1
        delivery = await cast(Any, pool).enqueue_job(
            name,
            *args,
            _job_id=arq_job_id,
            _queue_name=queue_name,
            **options,
        )
        assert delivery is not None

    try:
        await pool.delete(*keys)
        ctx = _context(jobs, registry, _Clock(), enqueue)

        assert await dispatch_jobs(ctx) == 1
        assert await dispatch_jobs(ctx) == 1
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

        row = await jobs.get(job_id)
        version = await knowledge.get_version(kb_id, document_id, version_id)
        chunks = await knowledge.list_version_chunks(kb_id, document_id, version_id)
        assert row is not None and row.status is JobStatus.succeeded
        assert row.attempt == 1
        assert row.result == {
            "kb_id": kb_id,
            "document_id": document_id,
            "document_version_id": version_id,
            "chunk_count": len(chunks),
        }
        assert version is not None and version.status is KnowledgeVersionStatus.active
        assert chunks
        assert embedder.calls == 2
        assert await pool.zcard(queue_name) == 0
    finally:
        try:
            await pool.delete(*keys)
            assert await pool.exists(*keys) == 0
        finally:
            await pool.aclose()


async def test_ingest_retry_reembeds_and_upserts(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"knowledge:retry:{uuid.uuid4().hex}"
    knowledge, kb_id, document_id, version_id = await _knowledge_document(
        migrated_db,
        scope,
        content="one chunk",
    )
    jobs = PostgresJobStore(migrated_db, scope)
    embedder = _FlakyEmbedder()
    registry = knowledge_job_registry(cast(KnowledgeStore, knowledge), embedder, _settings())
    definition = registry.get("knowledge.ingest")
    assert definition is not None
    job_id = await _enqueue_knowledge_job(
        jobs,
        definition,
        {
            "kb_id": kb_id,
            "document_id": document_id,
            "document_version_id": version_id,
        },
        key=f"retry:{version_id}",
    )
    await knowledge.attach_version_job(kb_id, document_id, version_id, job_id)
    clock = _Clock()
    ctx = _context(jobs, registry, clock, _noop_enqueue)

    assert await run_job(ctx, scope, job_id) == JobStatus.queued.value
    retrying = await jobs.get(job_id)
    assert retrying is not None and retrying.error_kind == "embedding_unavailable"
    clock.advance(5)
    assert await run_job(ctx, scope, job_id) == JobStatus.succeeded.value

    completed = await jobs.get(job_id)
    chunks = await knowledge.list_version_chunks(kb_id, document_id, version_id)
    assert completed is not None and completed.attempt == 2
    assert len(chunks) == 2
    assert embedder.calls == 2


async def test_cooperative_ingest_cancel_cleans_incomplete_version(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"knowledge:cancel:{uuid.uuid4().hex}"
    knowledge, kb_id, document_id, version_id = await _knowledge_document(
        migrated_db,
        scope,
        content="one chunk",
    )
    jobs = PostgresJobStore(migrated_db, scope)
    embedder = _BlockingEmbedder()
    registry = knowledge_job_registry(cast(KnowledgeStore, knowledge), embedder, _settings())
    definition = registry.get("knowledge.ingest")
    assert definition is not None and definition.cancel_mode is CancelMode.cooperative
    job_id = await _enqueue_knowledge_job(
        jobs,
        definition,
        {
            "kb_id": kb_id,
            "document_id": document_id,
            "document_version_id": version_id,
        },
        key=f"cancel:{version_id}",
    )
    await knowledge.attach_version_job(kb_id, document_id, version_id, job_id)
    clock = _Clock()
    ctx = _context(jobs, registry, clock, _noop_enqueue)

    running = asyncio.create_task(run_job(ctx, scope, job_id))
    await asyncio.wait_for(embedder.entered.wait(), timeout=5)
    requested = await jobs.request_cancel(job_id, clock())
    assert requested is not None and requested.status is JobStatus.running
    embedder.release.set()
    assert await running == JobStatus.cancelled.value

    row = await jobs.get(job_id)
    version = await knowledge.get_version(kb_id, document_id, version_id)
    document = await knowledge.get_document(kb_id, document_id)
    assert row is not None and row.status is JobStatus.cancelled
    assert version is not None and version.status is KnowledgeVersionStatus.cancelled
    assert document is not None and document.status.value == "failed"
    assert await knowledge.list_version_chunks(kb_id, document_id, version_id) == []


async def test_queued_retry_cancellation_runs_domain_cleanup(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"knowledge:queued-cancel:{uuid.uuid4().hex}"
    knowledge, kb_id, document_id, version_id = await _knowledge_document(
        migrated_db,
        scope,
        content="one chunk",
    )
    jobs = PostgresJobStore(migrated_db, scope)
    registry = knowledge_job_registry(
        cast(KnowledgeStore, knowledge),
        _FlakyEmbedder(),
        _settings(),
    )
    definition = registry.get("knowledge.ingest")
    assert definition is not None
    job_id = await _enqueue_knowledge_job(
        jobs,
        definition,
        {
            "kb_id": kb_id,
            "document_id": document_id,
            "document_version_id": version_id,
        },
        key=f"queued-cancel:{version_id}",
    )
    await knowledge.attach_version_job(kb_id, document_id, version_id, job_id)
    clock = _Clock()
    ctx = _context(jobs, registry, clock, _noop_enqueue)

    assert await run_job(ctx, scope, job_id) == JobStatus.queued.value
    requested = await jobs.request_cancel(job_id, clock())
    assert requested is not None
    assert requested.status is JobStatus.queued
    assert requested.cancel_requested_at == _NOW
    assert await run_job(ctx, scope, job_id) == JobStatus.cancelled.value

    row = await jobs.get(job_id)
    version = await knowledge.get_version(kb_id, document_id, version_id)
    assert row is not None and row.status is JobStatus.cancelled
    assert version is not None and version.status is KnowledgeVersionStatus.cancelled
    assert await knowledge.list_version_chunks(kb_id, document_id, version_id) == []


async def test_failed_hook_fences_postgres_activation_after_its_version_read(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"knowledge:terminal-race:{uuid.uuid4().hex}"
    knowledge, kb_id, document_id, previous_version_id = await _knowledge_document(
        migrated_db,
        scope,
        content="active",
    )
    await knowledge.mark_indexing(kb_id, document_id, previous_version_id, now=_NOW)
    await knowledge.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=kb_id,
            document_id=document_id,
            document_version_id=previous_version_id,
            chunks=(
                KnowledgeChunkWrite(
                    ordinal=0,
                    text="active",
                    char_start=0,
                    char_end=len("active"),
                    content_hash=content_sha256("active"),
                    heading_path=(),
                    metadata={},
                    model="fake/embed",
                    dim=3,
                    embedding=(1.0, 0.0, 0.0),
                ),
            ),
        ),
        now=_NOW,
    )
    activated = await knowledge.activate_version(
        kb_id,
        document_id,
        previous_version_id,
        now=_NOW,
    )
    assert activated.activated
    target = await knowledge.create_document_version(
        KnowledgeDocumentVersionCreate(
            kb_id=kb_id,
            document_id=document_id,
            title="Guide",
            source_type=KnowledgeSourceType.text,
            source_uri=None,
            content="replacement",
            mime_type="text/plain",
            chunking_version="keel-char-v1",
            target_chars=100,
            overlap_chars=0,
        ),
        now=_NOW + timedelta(seconds=1),
    )
    raced_store = _ActivateAfterReadPostgresStore(migrated_db, scope)
    jobs = PostgresJobStore(migrated_db, scope)
    settings = _settings()
    wrong_embedder = _Embedder()
    wrong_embedder.model = "wrong/model"
    registry = knowledge_job_registry(cast(KnowledgeStore, raced_store), wrong_embedder, settings)
    definition = registry.get("knowledge.ingest")
    assert definition is not None
    job_id = await _enqueue_knowledge_job(
        jobs,
        definition,
        {
            "kb_id": kb_id,
            "document_id": document_id,
            "document_version_id": target.version.id,
        },
        key=f"terminal-race:{target.version.id}",
    )
    await knowledge.attach_version_job(kb_id, document_id, target.version.id, job_id)
    await knowledge.mark_indexing(kb_id, document_id, target.version.id, now=_NOW)
    await knowledge.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=kb_id,
            document_id=document_id,
            document_version_id=target.version.id,
            chunks=(
                KnowledgeChunkWrite(
                    ordinal=0,
                    text="replacement",
                    char_start=0,
                    char_end=len("replacement"),
                    content_hash=content_sha256("replacement"),
                    heading_path=(),
                    metadata={},
                    model="fake/embed",
                    dim=3,
                    embedding=(1.0, 0.0, 0.0),
                ),
            ),
        ),
        now=_NOW,
    )
    raced_store.arm_activation_race(
        kb_id,
        document_id,
        target.version.id,
        reads_before_race=1,
    )
    clock = _Clock()
    ctx = _context(jobs, registry, clock, _noop_enqueue)

    assert await run_job(ctx, scope, job_id) == JobStatus.failed.value

    row = await jobs.get(job_id)
    document = await knowledge.get_document(kb_id, document_id)
    failed = await knowledge.get_version(kb_id, document_id, target.version.id)
    restored = await knowledge.get_version(kb_id, document_id, previous_version_id)
    assert raced_store.read_status is KnowledgeVersionStatus.indexing
    assert raced_store.race_activated
    assert row is not None and row.status is JobStatus.failed
    assert row.terminal_intent is JobTerminalIntent.failed
    assert failed is not None and failed.status is KnowledgeVersionStatus.failed
    assert await knowledge.list_version_chunks(kb_id, document_id, target.version.id) == []
    assert restored is not None and restored.status is KnowledgeVersionStatus.active
    assert document is not None
    assert document.status.value == "active"
    assert document.active_version_id == previous_version_id
    assert document.desired_version_id == previous_version_id


async def test_failed_hook_crash_replays_same_terminal_intent(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"knowledge:hook:{uuid.uuid4().hex}"
    knowledge, kb_id, document_id, version_id = await _knowledge_document(migrated_db, scope)
    jobs = PostgresJobStore(migrated_db, scope)
    settings = _settings()
    wrong_embedder = _Embedder()
    wrong_embedder.model = "wrong/model"
    handlers = KnowledgeJobHandlers(cast(KnowledgeStore, knowledge), wrong_embedder, settings)
    registry = JobRegistry()
    crashed = False

    async def crash_after_domain_commit(row: Any, error: JobError) -> None:
        nonlocal crashed
        await handlers.ingest_failed(row, error)
        if not crashed:
            crashed = True
            raise RuntimeError("simulated hook crash")

    registry.register(
        JobDefinition(
            kind="knowledge.ingest",
            handler=handlers.ingest,
            lease_seconds=settings.job_lease_seconds,
            cancel_mode=CancelMode.cooperative,
            on_cancelled=handlers.ingest_cancelled,
            on_failed=crash_after_domain_commit,
        )
    )
    definition = registry.get("knowledge.ingest")
    assert definition is not None
    job_id = await _enqueue_knowledge_job(
        jobs,
        definition,
        {
            "kb_id": kb_id,
            "document_id": document_id,
            "document_version_id": version_id,
        },
        key=f"hook:{version_id}",
    )
    await knowledge.attach_version_job(kb_id, document_id, version_id, job_id)
    clock = _Clock()
    ctx = _context(jobs, registry, clock, _noop_enqueue)

    with pytest.raises(RuntimeError, match="simulated hook crash"):
        await run_job(ctx, scope, job_id)
    reserved = await jobs.get(job_id)
    version = await knowledge.get_version(kb_id, document_id, version_id)
    assert reserved is not None and reserved.terminal_intent is JobTerminalIntent.failed
    assert version is not None and version.status is KnowledgeVersionStatus.failed

    clock.advance(settings.job_lease_seconds + 1)
    assert await dispatch_jobs(ctx) == 1
    finalized = await jobs.get(job_id)
    replayed = await knowledge.get_version(kb_id, document_id, version_id)
    assert finalized is not None and finalized.status is JobStatus.failed
    assert finalized.terminal_intent is JobTerminalIntent.failed
    assert replayed is not None and replayed.status is KnowledgeVersionStatus.failed
    assert replayed.error_kind == "embedding_configuration_mismatch"


async def test_delete_is_not_cancellable_and_purge_is_idempotent(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"knowledge:delete:{uuid.uuid4().hex}"
    knowledge, kb_id, document_id, version_id = await _knowledge_document(
        migrated_db,
        scope,
        content="secret",
    )
    jobs = PostgresJobStore(migrated_db, scope)
    embedder = _Embedder()
    settings = _settings()
    registry = knowledge_job_registry(cast(KnowledgeStore, knowledge), embedder, settings)
    ingest, delete = knowledge_job_definitions(
        cast(KnowledgeStore, knowledge),
        embedder,
        settings,
    )
    assert ingest.kind == "knowledge.ingest"
    ingest_job_id = await _enqueue_knowledge_job(
        jobs,
        ingest,
        {
            "kb_id": kb_id,
            "document_id": document_id,
            "document_version_id": version_id,
        },
        key=f"active:{version_id}",
    )
    await knowledge.attach_version_job(kb_id, document_id, version_id, ingest_job_id)
    ctx = _context(jobs, registry, _Clock(), _noop_enqueue)
    assert await run_job(ctx, scope, ingest_job_id) == JobStatus.succeeded.value

    await knowledge.tombstone_base(kb_id)
    delete_job_id = await _enqueue_knowledge_job(
        jobs,
        delete,
        {"kb_id": kb_id},
        key=f"delete:{kb_id}",
    )
    with pytest.raises(JobValidationError, match="job_not_cancellable"):
        await jobs.request_cancel(delete_job_id, _NOW)

    assert await run_job(ctx, scope, delete_job_id) == JobStatus.succeeded.value
    assert await run_job(ctx, scope, delete_job_id) == JobStatus.succeeded.value
    row = await jobs.get(delete_job_id)
    base = await knowledge.get_base(kb_id)
    document = await knowledge.get_document(kb_id, document_id)
    version = await knowledge.get_version(kb_id, document_id, version_id)
    assert row is not None and row.result is not None
    assert row.result["documents_purged"] == 1
    assert base is not None and base.description is None
    assert document is not None and document.source_uri is None
    assert version is not None and version.content is None
    assert await knowledge.list_version_chunks(kb_id, document_id, version_id) == []
