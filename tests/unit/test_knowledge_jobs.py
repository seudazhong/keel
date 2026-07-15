"""Worker-agnostic Knowledge ingest/delete handlers and terminal hooks."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from keel_core.config import Settings
from keel_core.jobs import (
    CancelMode,
    JobCancellationRequested,
    JobError,
    JobRecord,
    JobStatus,
    JobTerminalIntent,
    PermanentJobError,
    RetryableJobError,
)
from keel_core.knowledge import (
    InMemoryKnowledgeStore,
    KnowledgeBaseCreate,
    KnowledgeBaseStatus,
    KnowledgeChunkReplacement,
    KnowledgeChunkWrite,
    KnowledgeDeletePayload,
    KnowledgeDocumentTombstone,
    KnowledgeDocumentVersionCreate,
    KnowledgeDocumentVersionRecord,
    KnowledgeIngestPayload,
    KnowledgeJobContext,
    KnowledgeJobHandlers,
    KnowledgeSourceType,
    KnowledgeStorageError,
    KnowledgeVersionStatus,
    content_sha256,
    new_knowledge_base_id,
    new_knowledge_document_id,
    new_knowledge_version_id,
)
from keel_core.knowledge.chunking import chunk_document

_NOW = datetime(2026, 7, 16, 2, 0, tzinfo=UTC)


@dataclass
class _Context:
    job_id: str = "job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    scope_id: str = "web:local"
    attempt: int = 1
    max_attempts: int = 3
    progress_updates: list[tuple[int, int | None, str | None]] = field(default_factory=list)
    checkpoints: int = 0

    async def progress(
        self,
        current: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        self.progress_updates.append((current, total, message))

    async def checkpoint(self) -> None:
        self.checkpoints += 1


class _Embedder:
    def __init__(self, *, model: str = "fake/embed", dim: int = 3) -> None:
        self.model = model
        self.dim = dim
        self.calls: list[list[str]] = []
        self.response_override: list[list[float]] | None = None

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        batch = list(texts)
        self.calls.append(batch)
        if self.response_override is not None:
            return self.response_override
        return [[float(index + 1), 0.0, 0.0] for index, _ in enumerate(batch)]


class _ActivateAfterReadStore(InMemoryKnowledgeStore):
    def __init__(self, scope_id: str) -> None:
        super().__init__(scope_id)
        self._race_target: tuple[str, str, str] | None = None
        self.read_status: KnowledgeVersionStatus | None = None
        self.race_activated = False

    def arm_activation_race(self, kb_id: str, document_id: str, version_id: str) -> None:
        self._race_target = (kb_id, document_id, version_id)

    async def get_version(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
    ) -> KnowledgeDocumentVersionRecord | None:
        version = await super().get_version(kb_id, document_id, document_version_id)
        target = (kb_id, document_id, document_version_id)
        if self._race_target == target:
            self._race_target = None
            self.read_status = None if version is None else version.status
            activation = await super().activate_version(kb_id, document_id, document_version_id)
            self.race_activated = activation.activated
        return version


class _ClaimStorageFailureStore(InMemoryKnowledgeStore):
    async def attach_version_job(
        self,
        kb_id: str,
        document_id: str,
        document_version_id: str,
        job_id: str,
        *,
        now: datetime | None = None,
    ) -> KnowledgeDocumentVersionRecord:
        del kb_id, document_id, document_version_id, job_id, now
        raise KnowledgeStorageError


async def _document(
    store: InMemoryKnowledgeStore,
    *,
    content: str = "alpha beta gamma delta",
    target_chars: int = 6,
    overlap_chars: int = 0,
    document_id: str | None = None,
) -> tuple[str, str, str]:
    base = await store.create_base(
        KnowledgeBaseCreate(
            name="Docs",
            description="private description",
            embedding_model="fake/embed",
            embedding_dim=3,
        ),
        now=_NOW,
    )
    created = await store.create_document_version(
        KnowledgeDocumentVersionCreate(
            kb_id=base.id,
            document_id=document_id,
            title="Guide",
            source_type=KnowledgeSourceType.text,
            source_uri="https://secret.example/guide",
            content=content,
            mime_type="text/plain",
            chunking_version="persisted-v1",
            target_chars=target_chars,
            overlap_chars=overlap_chars,
        ),
        now=_NOW,
    )
    return base.id, created.document.id, created.version.id


def _payload(kb_id: str, document_id: str, version_id: str) -> dict[str, str]:
    return {
        "kb_id": kb_id,
        "document_id": document_id,
        "document_version_id": version_id,
    }


def _row(
    *,
    job_id: str,
    kind: str,
    payload: dict[str, Any],
    scope_id: str = "web:local",
    terminal_intent: JobTerminalIntent | None = None,
) -> JobRecord:
    return JobRecord(
        id=job_id,
        scope_id=scope_id,
        kind=kind,
        status=JobStatus.running,
        cancel_mode=CancelMode.cooperative,
        payload=payload,
        target_session_id=None,
        idempotency_key=f"key:{job_id}",
        attempt=1,
        max_attempts=3,
        next_attempt_at=_NOW,
        lease_token="lease",
        lease_expires_at=_NOW,
        heartbeat_at=_NOW,
        cancel_requested_at=None,
        terminal_intent=terminal_intent,
        terminal_intent_at=_NOW if terminal_intent is not None else None,
        progress_current=0,
        progress_total=None,
        progress_message=None,
        progress_updated_at=None,
        result=None,
        result_message=None,
        error_kind=None,
        error_message=None,
        injected_event_seq=None,
        created_at=_NOW,
        updated_at=_NOW,
        started_at=_NOW,
        finished_at=None,
    )


def test_payloads_are_strict_extra_forbid_and_storage_safe() -> None:
    kb_id = new_knowledge_base_id()
    document_id = new_knowledge_document_id()
    version_id = new_knowledge_version_id()
    assert KnowledgeIngestPayload.model_validate(_payload(kb_id, document_id, version_id)) == (
        KnowledgeIngestPayload(
            kb_id=kb_id,
            document_id=document_id,
            document_version_id=version_id,
        )
    )
    assert KnowledgeDeletePayload(kb_id=kb_id).document_id is None

    with pytest.raises(ValidationError):
        KnowledgeIngestPayload.model_validate(
            {**_payload(kb_id, document_id, version_id), "content": "must not travel"}
        )
    with pytest.raises(ValidationError):
        KnowledgeDeletePayload.model_validate({"kb_id": kb_id, "document_id": 7})
    with pytest.raises(ValidationError):
        KnowledgeIngestPayload.model_validate(_payload("kb_bad\x00", document_id, version_id))


def test_worker_context_matches_core_protocol_structurally() -> None:
    assert isinstance(_Context(), KnowledgeJobContext)


async def test_ingest_uses_persisted_settings_batches_checkpoints_and_exact_result() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(store)
    context = _Context()
    embedder = _Embedder()
    handlers = KnowledgeJobHandlers(
        store,
        embedder,
        Settings(
            knowledge_chunk_target_chars=1_600,
            knowledge_chunk_overlap_chars=200,
            knowledge_embedding_batch_size=2,
        ),
    )

    result = await handlers.ingest(context, _payload(kb_id, document_id, version_id))

    expected_drafts = chunk_document(
        "alpha beta gamma delta",
        KnowledgeSourceType.text,
        target_chars=6,
        overlap_chars=0,
    )
    assert result.data == {
        "kb_id": kb_id,
        "document_id": document_id,
        "document_version_id": version_id,
        "chunk_count": len(expected_drafts),
    }
    assert result.message == "Knowledge indexing completed."
    assert context.checkpoints == math.ceil(len(expected_drafts) / 2)
    assert [len(batch) for batch in embedder.calls] == [2, 2]
    assert {message for _, _, message in context.progress_updates} == {
        "chunking",
        "embedding",
        "writing",
        "activating",
    }
    assert "alpha" not in repr(context.progress_updates)

    version = await store.get_version(kb_id, document_id, version_id)
    chunks = await store.list_version_chunks(kb_id, document_id, version_id)
    assert version is not None and version.status is KnowledgeVersionStatus.active
    assert version.ingest_job_id == context.job_id
    assert [chunk.text for chunk in chunks] == [draft.text for draft in expected_drafts]
    assert {chunk.model for chunk in chunks} == {"fake/embed"}
    assert {chunk.dim for chunk in chunks} == {3}


async def test_ingest_pin_mismatch_is_permanent_before_embedding_or_chunk_write() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(store)
    context = _Context()
    embedder = _Embedder(model="wrong/model")
    handlers = KnowledgeJobHandlers(store, embedder, Settings())

    with pytest.raises(PermanentJobError) as caught:
        await handlers.ingest(context, _payload(kb_id, document_id, version_id))

    assert caught.value.code == "embedding_configuration_mismatch"
    assert embedder.calls == []
    assert await store.list_version_chunks(kb_id, document_id, version_id) == []
    claimed = await store.get_version(kb_id, document_id, version_id)
    assert claimed is not None
    assert claimed.ingest_job_id == context.job_id
    assert claimed.status is KnowledgeVersionStatus.indexing

    await handlers.ingest_failed(
        _row(
            job_id=context.job_id,
            kind="knowledge.ingest",
            payload=_payload(kb_id, document_id, version_id),
            terminal_intent=JobTerminalIntent.failed,
        ),
        JobError(caught.value.code, caught.value.public_message),
    )
    failed = await store.get_version(kb_id, document_id, version_id)
    assert failed is not None and failed.status is KnowledgeVersionStatus.failed
    assert await store.list_version_chunks(kb_id, document_id, version_id) == []


async def test_ingest_claim_storage_failure_is_retryable_without_mutation() -> None:
    store = _ClaimStorageFailureStore("web:local")
    kb_id, document_id, version_id = await _document(store)
    handlers = KnowledgeJobHandlers(store, _Embedder(), Settings())

    with pytest.raises(RetryableJobError) as caught:
        await handlers.ingest(_Context(), _payload(kb_id, document_id, version_id))

    version = await store.get_version(kb_id, document_id, version_id)
    assert caught.value.code == "knowledge_storage_failure"
    assert version is not None
    assert version.ingest_job_id is None
    assert version.status is KnowledgeVersionStatus.pending


@pytest.mark.parametrize(
    "invalid_vector",
    [
        [float("nan"), 0.0, 0.0],
        [0.0, 0.0, 0.0],
        [1e-50, -1e-50, 0.0],
    ],
)
async def test_ingest_rejects_invalid_vectors_with_bounded_retryable_error(
    invalid_vector: list[float],
) -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(
        store,
        content="one chunk",
        target_chars=100,
    )
    embedder = _Embedder()
    embedder.response_override = [invalid_vector]
    handlers = KnowledgeJobHandlers(store, embedder, Settings())

    with pytest.raises(RetryableJobError) as caught:
        await handlers.ingest(_Context(), _payload(kb_id, document_id, version_id))

    assert caught.value.code == "embedding_response_invalid"
    assert "one chunk" not in caught.value.public_message
    assert await store.list_version_chunks(kb_id, document_id, version_id) == []


async def test_ingest_duplicate_delivery_returns_active_without_reembedding() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(store)
    embedder = _Embedder()
    handlers = KnowledgeJobHandlers(
        store,
        embedder,
        Settings(knowledge_embedding_batch_size=2),
    )

    first = await handlers.ingest(_Context(), _payload(kb_id, document_id, version_id))
    first_chunks = await store.list_version_chunks(kb_id, document_id, version_id)
    call_count = len(embedder.calls)
    replay = await handlers.ingest(_Context(), _payload(kb_id, document_id, version_id))
    replay_chunks = await store.list_version_chunks(kb_id, document_id, version_id)

    assert replay == first
    assert len(embedder.calls) == call_count
    assert [chunk.id for chunk in replay_chunks] == [chunk.id for chunk in first_chunks]


async def test_concurrent_ingests_claim_one_job_and_reject_the_other_without_cleanup() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(
        store,
        content="one chunk",
        target_chars=100,
    )
    handlers = KnowledgeJobHandlers(store, _Embedder(), Settings())
    contexts = [
        _Context(job_id="job_first"),
        _Context(job_id="job_second"),
    ]

    outcomes = await asyncio.gather(
        *(
            handlers.ingest(context, _payload(kb_id, document_id, version_id))
            for context in contexts
        ),
        return_exceptions=True,
    )

    conflicts = [outcome for outcome in outcomes if isinstance(outcome, PermanentJobError)]
    assert len(conflicts) == 1
    assert conflicts[0].code == "idempotency_job_conflict"
    assert sum(not isinstance(outcome, BaseException) for outcome in outcomes) == 1

    version = await store.get_version(kb_id, document_id, version_id)
    assert version is not None and version.status is KnowledgeVersionStatus.active
    assert version.ingest_job_id in {context.job_id for context in contexts}
    losing_context = next(
        context for context in contexts if context.job_id != version.ingest_job_id
    )
    await handlers.ingest_failed(
        _row(
            job_id=losing_context.job_id,
            kind="knowledge.ingest",
            payload=_payload(kb_id, document_id, version_id),
            terminal_intent=JobTerminalIntent.failed,
        ),
        JobError("idempotency_job_conflict", "The other job owns this version."),
    )

    unchanged = await store.get_version(kb_id, document_id, version_id)
    assert unchanged is not None
    assert unchanged.status is KnowledgeVersionStatus.active
    assert unchanged.ingest_job_id == version.ingest_job_id


async def test_stale_desired_version_is_superseded_without_embedding() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, first_version_id = await _document(store, content="first")
    updated = await store.create_document_version(
        KnowledgeDocumentVersionCreate(
            kb_id=kb_id,
            document_id=document_id,
            title="Guide",
            source_type=KnowledgeSourceType.text,
            source_uri=None,
            content="second",
            mime_type="text/plain",
            chunking_version="persisted-v1",
            target_chars=10,
            overlap_chars=0,
        ),
        now=_NOW,
    )
    embedder = _Embedder()
    handlers = KnowledgeJobHandlers(store, embedder, Settings())

    result = await handlers.ingest(
        _Context(),
        _payload(kb_id, document_id, first_version_id),
    )

    first = await store.get_version(kb_id, document_id, first_version_id)
    assert first is not None and first.status is KnowledgeVersionStatus.superseded
    assert updated.version.status is KnowledgeVersionStatus.pending
    assert result.data["chunk_count"] == 0
    assert embedder.calls == []


async def test_terminal_hooks_preserve_old_active_and_replay_idempotently() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, active_version_id = await _document(store, content="active")
    handlers = KnowledgeJobHandlers(store, _Embedder(), Settings())
    await handlers.ingest(_Context(), _payload(kb_id, document_id, active_version_id))
    update = await store.create_document_version(
        KnowledgeDocumentVersionCreate(
            kb_id=kb_id,
            document_id=document_id,
            title="Guide",
            source_type=KnowledgeSourceType.text,
            source_uri=None,
            content="replacement",
            mime_type="text/plain",
            chunking_version="persisted-v1",
            target_chars=100,
            overlap_chars=0,
        ),
        now=_NOW,
    )
    job_id = "job_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    await store.attach_version_job(kb_id, document_id, update.version.id, job_id)
    await store.mark_indexing(kb_id, document_id, update.version.id)
    row = _row(
        job_id=job_id,
        kind="knowledge.ingest",
        payload=_payload(kb_id, document_id, update.version.id),
    )

    await handlers.ingest_failed(
        row,
        JobError("provider_failed", "A" * 2_000),
    )
    await handlers.ingest_failed(
        row,
        JobError("different_error", "Must not replace terminal intent."),
    )

    document = await store.get_document(kb_id, document_id)
    failed = await store.get_version(kb_id, document_id, update.version.id)
    active = await store.get_version(kb_id, document_id, active_version_id)
    assert document is not None and document.active_version_id == active_version_id
    assert document.status.value == "active"
    assert active is not None and active.status is KnowledgeVersionStatus.active
    assert failed is not None and failed.status is KnowledgeVersionStatus.failed
    assert failed.error_kind == "provider_failed"
    assert failed.error_message == "A" * 512

    await handlers.ingest_cancelled(row)
    unchanged = await store.get_version(kb_id, document_id, update.version.id)
    assert unchanged is not None and unchanged.status is KnowledgeVersionStatus.failed


async def test_terminal_hooks_never_switch_a_reserved_intent() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(store, content="replacement")
    job_id = "job_ffffffffffffffffffffffffffffffff"
    await store.attach_version_job(kb_id, document_id, version_id, job_id)
    await store.mark_indexing(kb_id, document_id, version_id)
    handlers = KnowledgeJobHandlers(store, _Embedder(), Settings())
    payload = _payload(kb_id, document_id, version_id)

    await handlers.ingest_failed(
        _row(
            job_id=job_id,
            kind="knowledge.ingest",
            payload=payload,
            terminal_intent=JobTerminalIntent.cancelled,
        ),
        JobError("wrong_intent", "must not switch"),
    )
    still_indexing = await store.get_version(kb_id, document_id, version_id)
    assert still_indexing is not None
    assert still_indexing.status is KnowledgeVersionStatus.indexing

    await handlers.ingest_cancelled(
        _row(
            job_id=job_id,
            kind="knowledge.ingest",
            payload=payload,
            terminal_intent=JobTerminalIntent.cancelled,
        )
    )
    cancelled = await store.get_version(kb_id, document_id, version_id)
    assert cancelled is not None
    assert cancelled.status is KnowledgeVersionStatus.cancelled


async def test_failed_hook_fences_activation_after_its_version_read() -> None:
    store = _ActivateAfterReadStore("web:local")
    kb_id, document_id, previous_version_id = await _document(store, content="active")
    handlers = KnowledgeJobHandlers(store, _Embedder(), Settings())
    await handlers.ingest(
        _Context(job_id="job_previous"),
        _payload(kb_id, document_id, previous_version_id),
    )
    target = await store.create_document_version(
        KnowledgeDocumentVersionCreate(
            kb_id=kb_id,
            document_id=document_id,
            title="Guide",
            source_type=KnowledgeSourceType.text,
            source_uri=None,
            content="replacement",
            mime_type="text/plain",
            chunking_version="persisted-v1",
            target_chars=100,
            overlap_chars=0,
        ),
        now=_NOW,
    )
    job_id = "job_raced_terminal_fence"
    await store.attach_version_job(kb_id, document_id, target.version.id, job_id)
    await store.mark_indexing(kb_id, document_id, target.version.id)
    await store.replace_version_chunks(
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
        )
    )
    store.arm_activation_race(kb_id, document_id, target.version.id)

    await handlers.ingest_failed(
        _row(
            job_id=job_id,
            kind="knowledge.ingest",
            payload=_payload(kb_id, document_id, target.version.id),
            terminal_intent=JobTerminalIntent.failed,
        ),
        JobError("provider_failed", "Embedding failed."),
    )

    document = await store.get_document(kb_id, document_id)
    failed = await store.get_version(kb_id, document_id, target.version.id)
    restored = await store.get_version(kb_id, document_id, previous_version_id)
    assert store.read_status is KnowledgeVersionStatus.indexing
    assert store.race_activated
    assert failed is not None and failed.status is KnowledgeVersionStatus.failed
    assert await store.list_version_chunks(kb_id, document_id, target.version.id) == []
    assert restored is not None and restored.status is KnowledgeVersionStatus.active
    assert document is not None
    assert document.status.value == "active"
    assert document.active_version_id == previous_version_id
    assert document.desired_version_id == previous_version_id


async def test_cancel_hook_wins_before_zombie_chunk_write() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(store, content="one chunk", target_chars=100)
    job_id = "job_cccccccccccccccccccccccccccccccc"
    await store.attach_version_job(kb_id, document_id, version_id, job_id)
    handlers: KnowledgeJobHandlers

    class CancellingEmbedder(_Embedder):
        async def embed(self, texts: Sequence[str]) -> list[list[float]]:
            await handlers.ingest_cancelled(
                _row(
                    job_id=job_id,
                    kind="knowledge.ingest",
                    payload=_payload(kb_id, document_id, version_id),
                )
            )
            return await super().embed(texts)

    handlers = KnowledgeJobHandlers(store, CancellingEmbedder(), Settings())

    with pytest.raises(JobCancellationRequested):
        await handlers.ingest(
            _Context(job_id=job_id),
            _payload(kb_id, document_id, version_id),
        )

    version = await store.get_version(kb_id, document_id, version_id)
    assert version is not None and version.status is KnowledgeVersionStatus.cancelled
    assert await store.list_version_chunks(kb_id, document_id, version_id) == []


async def test_delete_purge_wins_before_zombie_ingest_write_or_activation() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(store, content="one chunk", target_chars=100)
    handlers: KnowledgeJobHandlers

    class PurgingEmbedder(_Embedder):
        async def embed(self, texts: Sequence[str]) -> list[list[float]]:
            await store.tombstone_document(kb_id, document_id)
            await store.purge_document(kb_id, document_id)
            return await super().embed(texts)

    handlers = KnowledgeJobHandlers(store, PurgingEmbedder(), Settings())
    result = await handlers.ingest(
        _Context(),
        _payload(kb_id, document_id, version_id),
    )

    document = await store.get_document(kb_id, document_id)
    version = await store.get_version(kb_id, document_id, version_id)
    assert result.data["chunk_count"] == 0
    assert document is not None and document.status.value == "deleted"
    assert version is not None and version.status is KnowledgeVersionStatus.purged
    assert version.content is None
    assert await store.list_version_chunks(kb_id, document_id, version_id) == []


async def test_delete_purges_document_and_replays_with_zero_counts() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(store, content="secret", target_chars=100)
    handlers = KnowledgeJobHandlers(store, _Embedder(), Settings())
    await handlers.ingest(_Context(), _payload(kb_id, document_id, version_id))
    await store.tombstone_document(
        KnowledgeDocumentTombstone(kb_id=kb_id, document_id=document_id).kb_id,
        document_id,
    )
    payload = {"kb_id": kb_id, "document_id": document_id}

    first = await handlers.delete(_Context(), payload)
    replay = await handlers.delete(_Context(), payload)

    assert first.data["documents_purged"] == 1
    assert first.data["versions_purged"] == 1
    assert first.data["chunks_removed"] > 0
    assert replay.data == {
        "kb_id": kb_id,
        "document_id": document_id,
        "documents_purged": 0,
        "versions_purged": 0,
        "chunks_removed": 0,
    }
    document = await store.get_document(kb_id, document_id)
    version = await store.get_version(kb_id, document_id, version_id)
    assert document is not None and document.source_uri is None
    assert version is not None and version.content is None
    assert await store.list_version_chunks(kb_id, document_id, version_id) == []


async def test_delete_failed_replays_purge_and_ignores_unsafe_hook_rows() -> None:
    store = InMemoryKnowledgeStore("web:local")
    kb_id, document_id, version_id = await _document(store, content="secret", target_chars=100)
    handlers = KnowledgeJobHandlers(store, _Embedder(), Settings())
    await store.tombstone_base(kb_id)
    row = _row(
        job_id="job_dddddddddddddddddddddddddddddddd",
        kind="knowledge.delete",
        payload={"kb_id": kb_id},
    )

    await handlers.delete_failed(row, JobError("internal_error", "temporary"))
    await handlers.delete_failed(row, JobError("internal_error", "temporary"))
    await handlers.delete_failed(
        _row(
            job_id="job_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            kind="knowledge.delete",
            payload={"kb_id": kb_id, "extra": "unsafe"},
        ),
        JobError("internal_error", "temporary"),
    )

    base = await store.get_base(kb_id)
    version = await store.get_version(kb_id, document_id, version_id)
    assert base is not None and base.status is KnowledgeBaseStatus.deleted
    assert base.description is None
    assert version is not None and version.status is KnowledgeVersionStatus.purged


async def test_payload_parsing_and_scope_checks_happen_before_store_side_effects() -> None:
    store = InMemoryKnowledgeStore("web:local")
    handlers = KnowledgeJobHandlers(store, _Embedder(), Settings())

    with pytest.raises(PermanentJobError, match="Knowledge job payload is invalid"):
        await handlers.ingest(
            _Context(scope_id="wrong"),
            {"kb_id": "not-an-id", "content": "secret"},
        )
    with pytest.raises(PermanentJobError, match="scope"):
        await handlers.delete(
            _Context(scope_id="wrong"),
            {"kb_id": new_knowledge_base_id()},
        )
