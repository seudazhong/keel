"""In-memory Knowledge lifecycle, deletion, and idempotency invariants."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from keel_core.knowledge import (
    InMemoryKnowledgeStore,
    KnowledgeBaseCreate,
    KnowledgeBaseStatus,
    KnowledgeChunkReplacement,
    KnowledgeChunkWrite,
    KnowledgeConflict,
    KnowledgeDocumentReindex,
    KnowledgeDocumentStatus,
    KnowledgeDocumentVersionCreate,
    KnowledgeEmbeddingMismatch,
    KnowledgeIdempotencyAttach,
    KnowledgeIdempotencyBegin,
    KnowledgeOperation,
    KnowledgeResourceKind,
    KnowledgeSourceType,
    KnowledgeStore,
    KnowledgeVersionCancellation,
    KnowledgeVersionFailure,
    KnowledgeVersionStatus,
    content_sha256,
    request_fingerprint,
)

_NOW = datetime(2026, 7, 15, 8, 0, tzinfo=UTC)


async def _base(
    store: InMemoryKnowledgeStore,
    *,
    name: str = "Docs",
    now: datetime = _NOW,
):
    return await store.create_base(
        KnowledgeBaseCreate(
            name=name,
            description="Product documentation",
            embedding_model="fake/embed",
            embedding_dim=3,
        ),
        now=now,
    )


def _version_command(
    kb_id: str,
    content: str,
    *,
    document_id: str | None = None,
    title: str = "Guide.md",
    target_chars: int = 1600,
    overlap_chars: int = 200,
) -> KnowledgeDocumentVersionCreate:
    return KnowledgeDocumentVersionCreate(
        kb_id=kb_id,
        document_id=document_id,
        title=title,
        source_type=KnowledgeSourceType.markdown,
        source_uri="https://example.test/guide",
        content=content,
        mime_type="text/markdown",
        chunking_version="keel-char-v1",
        target_chars=target_chars,
        overlap_chars=overlap_chars,
    )


def _chunk(text: str = "Install Keel.", *, metadata: dict[str, object] | None = None):
    return KnowledgeChunkWrite(
        ordinal=0,
        text=text,
        char_start=0,
        char_end=len(text),
        content_hash=content_sha256(text),
        heading_path=("Guide",),
        metadata={} if metadata is None else metadata,
        model="fake/embed",
        dim=3,
        embedding=(1.0, 0.0, 0.0),
    )


async def _index_and_activate(
    store: InMemoryKnowledgeStore,
    kb_id: str,
    document_id: str,
    version_id: str,
    *,
    now: datetime,
) -> None:
    indexed = await store.mark_indexing(kb_id, document_id, version_id, now=now)
    assert indexed.version.status is KnowledgeVersionStatus.indexing
    assert indexed.version.content is not None
    await store.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=kb_id,
            document_id=document_id,
            document_version_id=version_id,
            chunks=(_chunk(indexed.version.content),),
        ),
        now=now,
    )
    activated = await store.activate_version(kb_id, document_id, version_id, now=now)
    assert activated.activated


async def test_store_is_scope_bound_and_active_names_are_unique() -> None:
    store = InMemoryKnowledgeStore("web:local")
    other = InMemoryKnowledgeStore("web:other")
    assert isinstance(store, KnowledgeStore)
    first = await _base(store)
    assert store.scope_id == "web:local"
    assert await other.get_base(first.id) is None

    with pytest.raises(KnowledgeConflict, match="knowledge_base_name_conflict"):
        await _base(store)

    deleted = await store.tombstone_base(first.id, now=_NOW + timedelta(seconds=1))
    assert deleted.status is KnowledgeBaseStatus.deleted
    replacement = await _base(store, now=_NOW + timedelta(seconds=2))
    assert replacement.name == first.name
    assert replacement.id != first.id


async def test_versions_are_monotonic_and_reuse_only_current_live_fingerprints() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    first = await store.create_document_version(_version_command(base.id, "A"), now=_NOW)
    assert first.version.version == 1
    assert not first.reused

    pending_replay = await store.create_document_version(
        _version_command(base.id, "A", document_id=first.document.id),
        now=_NOW + timedelta(seconds=1),
    )
    assert pending_replay.reused
    assert pending_replay.version.id == first.version.id

    await store.mark_indexing(
        base.id,
        first.document.id,
        first.version.id,
        now=_NOW + timedelta(seconds=2),
    )
    indexing_replay = await store.create_document_version(
        _version_command(base.id, "A", document_id=first.document.id),
        now=_NOW + timedelta(seconds=3),
    )
    assert indexing_replay.reused
    assert indexing_replay.version.id == first.version.id

    await _index_and_activate(
        store,
        base.id,
        first.document.id,
        first.version.id,
        now=_NOW + timedelta(seconds=4),
    )
    active_replay = await store.create_document_version(
        _version_command(base.id, "A", document_id=first.document.id),
        now=_NOW + timedelta(seconds=5),
    )
    assert active_replay.reused
    assert active_replay.version.id == first.version.id

    second = await store.create_document_version(
        _version_command(base.id, "B", document_id=first.document.id),
        now=_NOW + timedelta(seconds=6),
    )
    await _index_and_activate(
        store,
        base.id,
        first.document.id,
        second.version.id,
        now=_NOW + timedelta(seconds=7),
    )
    reverted = await store.create_document_version(
        _version_command(base.id, "A", document_id=first.document.id),
        now=_NOW + timedelta(seconds=8),
    )
    assert not reverted.reused
    assert reverted.version.version == 3

    failed = await store.create_document_version(
        _version_command(base.id, "C", document_id=first.document.id),
        now=_NOW + timedelta(seconds=9),
    )
    await store.mark_version_failed(
        KnowledgeVersionFailure(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=failed.version.id,
            error_kind="embedding_unavailable",
            error_message="Embedding is unavailable.",
        ),
        now=_NOW + timedelta(seconds=10),
    )
    failed_retry = await store.create_document_version(
        _version_command(base.id, "C", document_id=first.document.id),
        now=_NOW + timedelta(seconds=11),
    )
    assert not failed_retry.reused
    assert failed_retry.version.version == 5

    cancelled = await store.create_document_version(
        _version_command(base.id, "D", document_id=first.document.id),
        now=_NOW + timedelta(seconds=12),
    )
    await store.mark_version_cancelled(
        KnowledgeVersionCancellation(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=cancelled.version.id,
        ),
        now=_NOW + timedelta(seconds=13),
    )
    cancelled_retry = await store.create_document_version(
        _version_command(base.id, "D", document_id=first.document.id),
        now=_NOW + timedelta(seconds=14),
    )
    assert not cancelled_retry.reused
    assert cancelled_retry.version.version == 7


async def test_concurrent_version_allocation_is_monotonic() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    first = await store.create_document_version(_version_command(base.id, "seed"), now=_NOW)

    results = await asyncio.gather(
        *(
            store.create_document_version(
                _version_command(base.id, f"content-{index}", document_id=first.document.id),
                now=_NOW + timedelta(seconds=index + 1),
            )
            for index in range(10)
        )
    )
    assert sorted(result.version.version for result in results) == list(range(2, 12))
    versions = await store.list_versions(base.id, first.document.id)
    assert [version.version for version in versions] == list(range(1, 12))


async def test_new_desired_version_blocks_stale_activation_and_cleans_chunks() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    first = await store.create_document_version(_version_command(base.id, "A"), now=_NOW)
    await store.mark_indexing(base.id, first.document.id, first.version.id, now=_NOW)
    await store.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=first.version.id,
            chunks=(_chunk("A"),),
        ),
        now=_NOW,
    )
    second = await store.create_document_version(
        _version_command(base.id, "B", document_id=first.document.id),
        now=_NOW + timedelta(seconds=1),
    )

    stale = await store.activate_version(
        base.id,
        first.document.id,
        first.version.id,
        now=_NOW + timedelta(seconds=2),
    )
    assert stale.stale
    assert not stale.activated
    assert stale.version.status is KnowledgeVersionStatus.superseded
    assert await store.list_version_chunks(base.id, first.document.id, first.version.id) == []

    await _index_and_activate(
        store,
        base.id,
        first.document.id,
        second.version.id,
        now=_NOW + timedelta(seconds=3),
    )
    document = await store.get_document(base.id, first.document.id)
    assert document is not None
    assert document.active_version_id == second.version.id


@pytest.mark.parametrize("terminal", ["failed", "cancelled"])
async def test_failed_or_cancelled_update_preserves_old_active_version(terminal: str) -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    first = await store.create_document_version(_version_command(base.id, "A"), now=_NOW)
    await _index_and_activate(store, base.id, first.document.id, first.version.id, now=_NOW)
    update = await store.create_document_version(
        _version_command(base.id, "B", document_id=first.document.id),
        now=_NOW + timedelta(seconds=1),
    )
    await store.mark_indexing(
        base.id,
        first.document.id,
        update.version.id,
        now=_NOW + timedelta(seconds=2),
    )

    if terminal == "failed":
        version = await store.mark_version_failed(
            KnowledgeVersionFailure(
                kb_id=base.id,
                document_id=first.document.id,
                document_version_id=update.version.id,
                error_kind="provider_failed",
                error_message="Embedding failed.",
            ),
            now=_NOW + timedelta(seconds=3),
        )
        assert version.status is KnowledgeVersionStatus.failed
    else:
        version = await store.mark_version_cancelled(
            KnowledgeVersionCancellation(
                kb_id=base.id,
                document_id=first.document.id,
                document_version_id=update.version.id,
            ),
            now=_NOW + timedelta(seconds=3),
        )
        assert version.status is KnowledgeVersionStatus.cancelled

    document = await store.get_document(base.id, first.document.id)
    assert document is not None
    assert document.status is KnowledgeDocumentStatus.active
    assert document.active_version_id == first.version.id


async def test_terminal_and_delete_transitions_never_downgrade_authoritative_state() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    created = await store.create_document_version(_version_command(base.id, "secret"), now=_NOW)
    await store.mark_indexing(base.id, created.document.id, created.version.id, now=_NOW)
    failed = await store.mark_version_failed(
        KnowledgeVersionFailure(
            kb_id=base.id,
            document_id=created.document.id,
            document_version_id=created.version.id,
            error_kind="failed",
            error_message="Indexing failed.",
        ),
        now=_NOW + timedelta(seconds=1),
    )
    cancelled_after_failure = await store.mark_version_cancelled(
        KnowledgeVersionCancellation(
            kb_id=base.id,
            document_id=created.document.id,
            document_version_id=created.version.id,
        ),
        now=_NOW + timedelta(seconds=2),
    )
    assert cancelled_after_failure.status is failed.status is KnowledgeVersionStatus.failed

    await store.tombstone_document(
        base.id,
        created.document.id,
        now=_NOW + timedelta(seconds=3),
    )
    after_delete = await store.mark_version_failed(
        KnowledgeVersionFailure(
            kb_id=base.id,
            document_id=created.document.id,
            document_version_id=created.version.id,
            error_kind="late_failure",
            error_message="Late failure.",
        ),
        now=_NOW + timedelta(seconds=4),
    )
    assert after_delete.status is KnowledgeVersionStatus.purged
    assert after_delete.content is None


async def test_chunk_replacement_is_guarded_pinned_and_copy_isolated() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    created = await store.create_document_version(_version_command(base.id, "A"), now=_NOW)
    replacement = KnowledgeChunkReplacement(
        kb_id=base.id,
        document_id=created.document.id,
        document_version_id=created.version.id,
        chunks=(_chunk("A", metadata={"nested": {"value": 1}}),),
    )

    with pytest.raises(KnowledgeConflict, match="chunk_write_rejected"):
        await store.replace_version_chunks(replacement, now=_NOW)

    await store.mark_indexing(base.id, created.document.id, created.version.id, now=_NOW)
    wrong_pin = replace(
        replacement,
        chunks=(replace(replacement.chunks[0], model="client/chosen"),),
    )
    with pytest.raises(KnowledgeEmbeddingMismatch):
        await store.replace_version_chunks(wrong_pin, now=_NOW)

    result = await store.replace_version_chunks(replacement, now=_NOW)
    assert result.chunk_count == 1
    replacement.chunks[0].metadata["nested"]["value"] = 9  # type: ignore[index]
    first_read = await store.list_version_chunks(base.id, created.document.id, created.version.id)
    assert first_read[0].metadata == {"nested": {"value": 1}}
    first_read[0].metadata["nested"]["value"] = 7  # type: ignore[index]
    second_read = await store.list_version_chunks(base.id, created.document.id, created.version.id)
    assert second_read[0].metadata == {"nested": {"value": 1}}

    await store.tombstone_document(base.id, created.document.id, now=_NOW)
    with pytest.raises(KnowledgeConflict, match="chunk_write_rejected"):
        await store.replace_version_chunks(replacement, now=_NOW)


async def test_reindex_uses_active_content_and_current_server_pin() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    first = await store.create_document_version(_version_command(base.id, "A"), now=_NOW)
    await _index_and_activate(store, base.id, first.document.id, first.version.id, now=_NOW)

    same = await store.reindex_document(
        KnowledgeDocumentReindex(
            kb_id=base.id,
            document_id=first.document.id,
            chunking_version="keel-char-v1",
            target_chars=1600,
            overlap_chars=200,
        ),
        now=_NOW + timedelta(seconds=1),
    )
    assert same.reused
    assert same.version.id == first.version.id

    changed = await store.reindex_document(
        KnowledgeDocumentReindex(
            kb_id=base.id,
            document_id=first.document.id,
            chunking_version="keel-char-v2",
            target_chars=1600,
            overlap_chars=200,
        ),
        now=_NOW + timedelta(seconds=2),
    )
    assert not changed.reused
    assert changed.version.content == "A"
    assert changed.version.version == 2


async def test_reindex_requires_an_active_version() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    created = await store.create_document_version(_version_command(base.id, "A"), now=_NOW)

    with pytest.raises(KnowledgeConflict, match="no_active_version"):
        await store.reindex_document(
            KnowledgeDocumentReindex(
                kb_id=base.id,
                document_id=created.document.id,
                chunking_version="keel-char-v1",
                target_chars=1600,
                overlap_chars=200,
            ),
            now=_NOW,
        )


async def test_purge_retains_minimal_tombstones_and_removes_sensitive_data() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    created = await store.create_document_version(
        _version_command(base.id, "secret content"),
        now=_NOW,
    )
    await _index_and_activate(store, base.id, created.document.id, created.version.id, now=_NOW)
    await store.tombstone_base(base.id, now=_NOW + timedelta(seconds=1))
    purged = await store.purge_base(base.id, now=_NOW + timedelta(seconds=2))
    assert purged.documents_purged == 1
    assert purged.versions_purged == 1
    assert purged.chunks_removed == 1

    base_tombstone = await store.get_base(base.id)
    document_tombstone = await store.get_document(base.id, created.document.id)
    version_tombstone = await store.get_version(
        base.id,
        created.document.id,
        created.version.id,
    )
    assert base_tombstone is not None and base_tombstone.description is None
    assert document_tombstone is not None
    assert document_tombstone.status is KnowledgeDocumentStatus.deleted
    assert document_tombstone.source_uri is None
    assert document_tombstone.active_version_id is None
    assert document_tombstone.desired_version_id is None
    assert version_tombstone is not None
    assert version_tombstone.status is KnowledgeVersionStatus.purged
    assert version_tombstone.content is None
    assert (
        await store.list_version_chunks(
            base.id,
            created.document.id,
            created.version.id,
        )
        == []
    )


async def test_idempotency_replays_same_ids_conflicts_on_new_input_and_attaches_job() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    created = await store.create_document_version(_version_command(base.id, "A"), now=_NOW)
    fingerprint = request_fingerprint(
        "POST",
        KnowledgeOperation.create_document,
        {"kb_id": base.id},
        {"content_sha256": created.version.content_sha256},
    )
    begin = KnowledgeIdempotencyBegin(
        operation=KnowledgeOperation.create_document,
        idempotency_key="request-1",
        request_fingerprint=fingerprint,
        resource_kind=KnowledgeResourceKind.document,
        resource_id=created.document.id,
        document_version_id=created.version.id,
    )
    first = await store.begin_idempotent_request(begin, now=_NOW)
    replay = await store.begin_idempotent_request(
        replace(
            begin,
            resource_id="doc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            document_version_id="kbv_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
        now=_NOW + timedelta(seconds=1),
    )
    assert not first.replayed
    assert replay.replayed
    assert replay.record.id == first.record.id
    assert replay.record.resource_id == created.document.id
    assert replay.record.document_version_id == created.version.id

    with pytest.raises(KnowledgeConflict, match="idempotency_key_reused"):
        await store.begin_idempotent_request(
            replace(begin, request_fingerprint="f" * 64),
            now=_NOW + timedelta(seconds=2),
        )

    attached = await store.attach_idempotent_job(
        KnowledgeIdempotencyAttach(
            operation=begin.operation,
            idempotency_key=begin.idempotency_key,
            request_fingerprint=fingerprint,
            job_id="job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        now=_NOW + timedelta(seconds=3),
    )
    assert attached.job_id == "job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    version = await store.get_version(base.id, created.document.id, created.version.id)
    assert version is not None
    assert version.ingest_job_id == attached.job_id
    assert (
        await store.attach_idempotent_job(
            KnowledgeIdempotencyAttach(
                operation=begin.operation,
                idempotency_key=begin.idempotency_key,
                request_fingerprint=fingerprint,
                job_id=attached.job_id,
            ),
            now=_NOW + timedelta(seconds=4),
        )
    ).job_id == attached.job_id
    attached_replay = await store.begin_idempotent_request(
        begin,
        now=_NOW + timedelta(seconds=5),
    )
    assert attached_replay.replayed
    assert attached_replay.record.job_id == attached.job_id


async def test_first_ingest_failure_is_safe_and_marks_document_failed() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    secret = "do-not-echo-this-content"
    created = await store.create_document_version(_version_command(base.id, secret), now=_NOW)
    await store.mark_version_failed(
        KnowledgeVersionFailure(
            kb_id=base.id,
            document_id=created.document.id,
            document_version_id=created.version.id,
            error_kind="embedding_unavailable",
            error_message="Embedding is unavailable.",
        ),
        now=_NOW,
    )
    document = await store.get_document(base.id, created.document.id)
    assert document is not None
    assert document.status is KnowledgeDocumentStatus.failed
    assert secret not in (document.last_error_message or "")
