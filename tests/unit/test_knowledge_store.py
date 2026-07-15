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
    KnowledgeBaseTombstone,
    KnowledgeChunkReplacement,
    KnowledgeChunkWrite,
    KnowledgeConflict,
    KnowledgeDocumentReindex,
    KnowledgeDocumentStatus,
    KnowledgeDocumentTombstone,
    KnowledgeDocumentVersionCreate,
    KnowledgeEmbeddingMismatch,
    KnowledgeIdempotencyAttach,
    KnowledgeIdempotencyBegin,
    KnowledgeOperation,
    KnowledgeSourceType,
    KnowledgeStore,
    KnowledgeValidationError,
    KnowledgeVersionCancellation,
    KnowledgeVersionFailure,
    KnowledgeVersionStatus,
    content_sha256,
    new_knowledge_base_id,
    new_knowledge_document_id,
    new_knowledge_idempotency_id,
    new_knowledge_version_id,
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
    source_type: KnowledgeSourceType = KnowledgeSourceType.markdown,
    target_chars: int = 1600,
    overlap_chars: int = 200,
) -> KnowledgeDocumentVersionCreate:
    mime_type = "text/markdown" if source_type is KnowledgeSourceType.markdown else "text/plain"
    return KnowledgeDocumentVersionCreate(
        kb_id=kb_id,
        document_id=document_id,
        title=title,
        source_type=source_type,
        source_uri="https://example.test/guide",
        content=content,
        mime_type=mime_type,
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


async def test_source_type_change_with_identical_bytes_creates_new_version() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    markdown = await store.create_document_version(_version_command(base.id, "same"), now=_NOW)

    text = await store.create_document_version(
        _version_command(
            base.id,
            "same",
            document_id=markdown.document.id,
            source_type=KnowledgeSourceType.text,
        ),
        now=_NOW + timedelta(seconds=1),
    )

    assert not text.reused
    assert text.version.version == 2
    assert text.version.id != markdown.version.id
    assert text.version.mime_type == "text/plain"
    assert text.document.source_type is KnowledgeSourceType.text
    stored = await store.get_document(base.id, markdown.document.id)
    assert stored is not None
    assert stored.source_type is KnowledgeSourceType.text
    assert stored.desired_version_id == text.version.id


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


def _idempotency(
    operation: KnowledgeOperation,
    key: str,
    *,
    method: str,
    path_ids: dict[str, str],
    body: dict[str, object] | None,
) -> KnowledgeIdempotencyBegin:
    return KnowledgeIdempotencyBegin(
        operation=operation,
        idempotency_key=key,
        request_fingerprint=request_fingerprint(method, operation, path_ids, body),
    )


async def test_atomic_idempotent_mutations_replay_exact_resources_and_ledgers() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base_command = KnowledgeBaseCreate(
        name="Docs",
        description="Product documentation",
        embedding_model="fake/embed",
        embedding_dim=3,
        base_id=new_knowledge_base_id(),
    )
    base_begin = _idempotency(
        KnowledgeOperation.create_base,
        "create-base",
        method="POST",
        path_ids={},
        body={"name": base_command.name, "description": base_command.description},
    )
    base_first = await store.create_base_idempotent(base_command, base_begin, now=_NOW)
    base_replay = await store.create_base_idempotent(
        replace(base_command, base_id=new_knowledge_base_id()),
        replace(base_begin, ledger_id=new_knowledge_idempotency_id()),
        now=_NOW + timedelta(seconds=1),
    )
    assert not base_first.replayed
    assert base_replay.replayed
    assert base_replay.resource.id == base_first.resource.id
    assert base_replay.ledger.id == base_first.ledger.id
    assert base_first.ledger.resource_id == base_first.resource.id
    assert (
        await store.get_idempotent_request(KnowledgeOperation.create_base, "create-base")
        == base_first.ledger
    )

    create_command = replace(
        _version_command(base_first.resource.id, "A"),
        new_document_id=new_knowledge_document_id(),
        document_version_id=new_knowledge_version_id(),
    )
    create_begin = _idempotency(
        KnowledgeOperation.create_document,
        "create-document",
        method="POST",
        path_ids={"kb_id": base_first.resource.id},
        body={"content_sha256": content_sha256(create_command.content)},
    )
    create_first = await store.create_document_version_idempotent(
        create_command,
        create_begin,
        now=_NOW + timedelta(seconds=2),
    )
    create_replay = await store.create_document_version_idempotent(
        replace(
            create_command,
            new_document_id=new_knowledge_document_id(),
            document_version_id=new_knowledge_version_id(),
        ),
        create_begin,
        now=_NOW + timedelta(seconds=3),
    )
    assert not create_first.replayed
    assert create_replay.replayed
    assert create_replay.resource.document.id == create_first.resource.document.id
    assert create_replay.resource.version.id == create_first.resource.version.id
    assert create_replay.ledger.id == create_first.ledger.id
    assert create_first.ledger.resource_id == create_first.resource.document.id
    assert create_first.ledger.document_version_id == create_first.resource.version.id

    update_command = replace(
        _version_command(
            base_first.resource.id,
            "B",
            document_id=create_first.resource.document.id,
        ),
        document_version_id=new_knowledge_version_id(),
    )
    update_begin = _idempotency(
        KnowledgeOperation.update_document,
        "update-document",
        method="PUT",
        path_ids={
            "kb_id": base_first.resource.id,
            "document_id": create_first.resource.document.id,
        },
        body={"content_sha256": content_sha256(update_command.content)},
    )
    update_first = await store.update_document_version_idempotent(
        update_command,
        update_begin,
        now=_NOW + timedelta(seconds=4),
    )
    update_replay = await store.update_document_version_idempotent(
        replace(update_command, document_version_id=new_knowledge_version_id()),
        update_begin,
        now=_NOW + timedelta(seconds=5),
    )
    assert not update_first.replayed
    assert update_replay.replayed
    assert update_replay.resource.version.id == update_first.resource.version.id
    assert update_replay.ledger.id == update_first.ledger.id
    assert update_first.ledger.resource_id == update_first.resource.document.id
    assert update_first.ledger.document_version_id == update_first.resource.version.id

    await _index_and_activate(
        store,
        base_first.resource.id,
        create_first.resource.document.id,
        update_first.resource.version.id,
        now=_NOW + timedelta(seconds=6),
    )
    reindex_command = KnowledgeDocumentReindex(
        kb_id=base_first.resource.id,
        document_id=create_first.resource.document.id,
        chunking_version="keel-char-v2",
        target_chars=1600,
        overlap_chars=200,
        document_version_id=new_knowledge_version_id(),
    )
    reindex_begin = _idempotency(
        KnowledgeOperation.reindex_document,
        "reindex-document",
        method="POST",
        path_ids={
            "kb_id": base_first.resource.id,
            "document_id": create_first.resource.document.id,
        },
        body={"chunking_version": "keel-char-v2"},
    )
    reindex_first = await store.reindex_document_idempotent(
        reindex_command,
        reindex_begin,
        now=_NOW + timedelta(seconds=7),
    )
    reindex_replay = await store.reindex_document_idempotent(
        replace(reindex_command, document_version_id=new_knowledge_version_id()),
        reindex_begin,
        now=_NOW + timedelta(seconds=8),
    )
    assert not reindex_first.replayed
    assert reindex_replay.replayed
    assert reindex_replay.resource.version.id == reindex_first.resource.version.id
    assert reindex_replay.ledger.id == reindex_first.ledger.id
    assert reindex_first.ledger.resource_id == reindex_first.resource.document.id
    assert reindex_first.ledger.document_version_id == reindex_first.resource.version.id

    document_tombstone = KnowledgeDocumentTombstone(
        kb_id=base_first.resource.id,
        document_id=create_first.resource.document.id,
    )
    document_delete_begin = _idempotency(
        KnowledgeOperation.delete_document,
        "delete-document",
        method="DELETE",
        path_ids={
            "kb_id": base_first.resource.id,
            "document_id": create_first.resource.document.id,
        },
        body=None,
    )
    document_delete_first = await store.tombstone_document_idempotent(
        document_tombstone,
        document_delete_begin,
        now=_NOW + timedelta(seconds=9),
    )
    document_delete_replay = await store.tombstone_document_idempotent(
        document_tombstone,
        document_delete_begin,
        now=_NOW + timedelta(seconds=10),
    )
    assert not document_delete_first.replayed
    assert document_delete_replay.replayed
    assert document_delete_replay.resource.id == document_delete_first.resource.id
    assert document_delete_replay.ledger.id == document_delete_first.ledger.id
    assert document_delete_first.ledger.resource_id == document_delete_first.resource.id
    assert document_delete_first.ledger.document_version_id is None

    base_tombstone = KnowledgeBaseTombstone(kb_id=base_first.resource.id)
    base_delete_begin = _idempotency(
        KnowledgeOperation.delete_base,
        "delete-base",
        method="DELETE",
        path_ids={"kb_id": base_first.resource.id},
        body=None,
    )
    base_delete_first = await store.tombstone_base_idempotent(
        base_tombstone,
        base_delete_begin,
        now=_NOW + timedelta(seconds=11),
    )
    base_delete_replay = await store.tombstone_base_idempotent(
        base_tombstone,
        base_delete_begin,
        now=_NOW + timedelta(seconds=12),
    )
    assert not base_delete_first.replayed
    assert base_delete_replay.replayed
    assert base_delete_replay.resource.id == base_delete_first.resource.id
    assert base_delete_replay.ledger.id == base_delete_first.ledger.id
    assert base_delete_first.ledger.resource_id == base_delete_first.resource.id
    assert base_delete_first.ledger.document_version_id is None


async def test_failed_atomic_resource_mutation_does_not_create_ledger() -> None:
    store = InMemoryKnowledgeStore("web:local")
    await _base(store)
    begin = _idempotency(
        KnowledgeOperation.create_base,
        "failed-create-base",
        method="POST",
        path_ids={},
        body={"name": "Docs"},
    )

    with pytest.raises(KnowledgeConflict, match="knowledge_base_name_conflict"):
        await store.create_base_idempotent(
            KnowledgeBaseCreate(
                name="Docs",
                description=None,
                embedding_model="fake/embed",
                embedding_dim=3,
            ),
            begin,
            now=_NOW + timedelta(seconds=1),
        )

    assert await store.get_idempotent_request(begin.operation, begin.idempotency_key) is None
    assert len(await store.list_bases()) == 1


async def test_concurrent_atomic_create_replays_one_resource_and_ledger() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    begin = _idempotency(
        KnowledgeOperation.create_document,
        "concurrent-create",
        method="POST",
        path_ids={"kb_id": base.id},
        body={"content_sha256": content_sha256("same")},
    )
    commands = [
        replace(
            _version_command(base.id, "same"),
            new_document_id=new_knowledge_document_id(),
            document_version_id=new_knowledge_version_id(),
        )
        for _ in range(2)
    ]

    first, second = await asyncio.gather(
        *(
            store.create_document_version_idempotent(command, begin, now=_NOW)
            for command in commands
        )
    )

    assert sorted((first.replayed, second.replayed)) == [False, True]
    assert first.resource.document.id == second.resource.document.id
    assert first.resource.version.id == second.resource.version.id
    assert first.ledger.id == second.ledger.id
    assert len(await store.list_documents(base.id)) == 1
    assert len(await store.list_versions(base.id, first.resource.document.id)) == 1


async def test_idempotency_conflicts_never_partially_mutate_resources() -> None:
    store = InMemoryKnowledgeStore("web:local")
    assert store.document_max_bytes == 1_048_576
    base = await _base(store)
    created = await store.create_document_version(_version_command(base.id, "A"), now=_NOW)

    update_begin = _idempotency(
        KnowledgeOperation.update_document,
        "conflicting-update",
        method="PUT",
        path_ids={"kb_id": base.id, "document_id": created.document.id},
        body={"content_sha256": content_sha256("B")},
    )
    await store.update_document_version_idempotent(
        _version_command(base.id, "B", document_id=created.document.id),
        update_begin,
        now=_NOW + timedelta(seconds=1),
    )
    before_versions = await store.list_versions(base.id, created.document.id)
    before_document = await store.get_document(base.id, created.document.id)

    with pytest.raises(KnowledgeConflict, match="idempotency_key_reused"):
        await store.update_document_version_idempotent(
            _version_command(
                base.id,
                "C",
                document_id=created.document.id,
                title="Mutated title",
            ),
            replace(update_begin, request_fingerprint="f" * 64),
            now=_NOW + timedelta(seconds=2),
        )

    assert await store.list_versions(base.id, created.document.id) == before_versions
    assert await store.get_document(base.id, created.document.id) == before_document

    other = await store.create_document_version(
        _version_command(base.id, "other", title="Other"),
        now=_NOW + timedelta(seconds=3),
    )
    delete_begin = _idempotency(
        KnowledgeOperation.delete_document,
        "conflicting-delete",
        method="DELETE",
        path_ids={"kb_id": base.id, "document_id": created.document.id},
        body=None,
    )
    await store.tombstone_document_idempotent(
        KnowledgeDocumentTombstone(kb_id=base.id, document_id=created.document.id),
        delete_begin,
        now=_NOW + timedelta(seconds=4),
    )
    with pytest.raises(KnowledgeConflict, match="idempotency_key_reused"):
        await store.tombstone_document_idempotent(
            KnowledgeDocumentTombstone(kb_id=base.id, document_id=other.document.id),
            replace(delete_begin, request_fingerprint="e" * 64),
            now=_NOW + timedelta(seconds=5),
        )
    untouched = await store.get_document(base.id, other.document.id)
    assert untouched is not None
    assert untouched.status is KnowledgeDocumentStatus.pending

    other_base = await _base(store, name="Other base", now=_NOW + timedelta(seconds=6))
    base_delete_begin = _idempotency(
        KnowledgeOperation.delete_base,
        "conflicting-base-delete",
        method="DELETE",
        path_ids={"kb_id": base.id},
        body=None,
    )
    await store.tombstone_base_idempotent(
        KnowledgeBaseTombstone(kb_id=base.id),
        base_delete_begin,
        now=_NOW + timedelta(seconds=7),
    )
    with pytest.raises(KnowledgeConflict, match="idempotency_key_reused"):
        await store.tombstone_base_idempotent(
            KnowledgeBaseTombstone(kb_id=other_base.id),
            replace(base_delete_begin, request_fingerprint="d" * 64),
            now=_NOW + timedelta(seconds=8),
        )
    untouched_base = await store.get_base(other_base.id)
    assert untouched_base is not None
    assert untouched_base.status is KnowledgeBaseStatus.active


async def test_atomic_version_job_attachment_recovers_after_commit() -> None:
    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    command = _version_command(base.id, "A")
    begin = _idempotency(
        KnowledgeOperation.create_document,
        "request-1",
        method="POST",
        path_ids={"kb_id": base.id},
        body={"content_sha256": content_sha256(command.content)},
    )
    first = await store.create_document_version_idempotent(command, begin, now=_NOW)
    assert first.ledger.job_id is None

    attached = await store.attach_idempotent_job(
        KnowledgeIdempotencyAttach(
            operation=begin.operation,
            idempotency_key=begin.idempotency_key,
            request_fingerprint=begin.request_fingerprint,
            job_id="job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        now=_NOW + timedelta(seconds=1),
    )
    assert attached.job_id == "job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    version = await store.get_version(
        base.id,
        first.resource.document.id,
        first.resource.version.id,
    )
    assert version is not None
    assert version.ingest_job_id == attached.job_id
    assert (
        await store.attach_idempotent_job(
            KnowledgeIdempotencyAttach(
                operation=begin.operation,
                idempotency_key=begin.idempotency_key,
                request_fingerprint=begin.request_fingerprint,
                job_id=attached.job_id,
            ),
            now=_NOW + timedelta(seconds=2),
        )
    ).job_id == attached.job_id
    attached_replay = await store.create_document_version_idempotent(
        command,
        begin,
        now=_NOW + timedelta(seconds=3),
    )
    assert attached_replay.replayed
    assert attached_replay.ledger.job_id == attached.job_id


async def test_document_byte_limit_is_strict_bounded_and_reindex_safe() -> None:
    for invalid in (True, 1.0, 0, -1):
        with pytest.raises((KnowledgeValidationError, ValueError), match="positive integer"):
            InMemoryKnowledgeStore(  # type: ignore[arg-type]
                "web:local",
                document_max_bytes=invalid,
            )

    store = InMemoryKnowledgeStore("web:local")
    base = await _base(store)
    boundary = "é" * 524_288
    accepted = await store.create_document_version(
        _version_command(base.id, boundary),
        now=_NOW,
    )
    assert accepted.version.content == boundary

    oversized = boundary + "x"
    with pytest.raises(KnowledgeValidationError, match="content_too_large") as caught:
        await store.create_document_version(
            _version_command(base.id, oversized),
            now=_NOW + timedelta(seconds=1),
        )
    assert oversized[:100] not in caught.value.public_message
    assert len(await store.list_documents(base.id)) == 1

    await _index_and_activate(
        store,
        base.id,
        accepted.document.id,
        accepted.version.id,
        now=_NOW + timedelta(seconds=2),
    )
    store._document_max_bytes = 1
    reindexed = await store.reindex_document(
        KnowledgeDocumentReindex(
            kb_id=base.id,
            document_id=accepted.document.id,
            chunking_version="keel-char-v2",
            target_chars=1600,
            overlap_chars=200,
        ),
        now=_NOW + timedelta(seconds=3),
    )
    assert reindexed.version.content == boundary


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
