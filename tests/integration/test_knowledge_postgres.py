"""Postgres Knowledge lifecycle, concurrency, isolation, and recovery invariants."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from sqlalchemy import event
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from keel_core.knowledge import (
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
    KnowledgeNotFound,
    KnowledgeOperation,
    KnowledgeSourceType,
    KnowledgeStorageError,
    KnowledgeStore,
    KnowledgeValidationError,
    KnowledgeVersionCancellation,
    KnowledgeVersionFailure,
    KnowledgeVersionStatus,
    PostgresKnowledgeStore,
    content_sha256,
    index_fingerprint,
    new_knowledge_base_id,
    new_knowledge_document_id,
    new_knowledge_version_id,
    request_fingerprint,
)

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 15, 8, 0, tzinfo=UTC)


async def _base(
    store: PostgresKnowledgeStore,
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


def _version(
    kb_id: str,
    content: str,
    *,
    document_id: str | None = None,
    title: str = "Guide",
    source_type: KnowledgeSourceType = KnowledgeSourceType.markdown,
    target_chars: int = 1600,
    overlap_chars: int = 200,
) -> KnowledgeDocumentVersionCreate:
    return KnowledgeDocumentVersionCreate(
        kb_id=kb_id,
        document_id=document_id,
        title=title,
        source_type=source_type,
        source_uri="https://example.test/guide",
        content=content,
        mime_type=(
            "text/markdown" if source_type is KnowledgeSourceType.markdown else "text/plain"
        ),
        chunking_version="keel-char-v1",
        target_chars=target_chars,
        overlap_chars=overlap_chars,
    )


def _chunk(
    text: str,
    *,
    ordinal: int = 0,
    char_start: int = 0,
    metadata: dict[str, object] | None = None,
    model: str = "fake/embed",
) -> KnowledgeChunkWrite:
    return KnowledgeChunkWrite(
        ordinal=ordinal,
        text=text,
        char_start=char_start,
        char_end=char_start + len(text),
        content_hash=content_sha256(text),
        heading_path=("Guide",),
        metadata={} if metadata is None else metadata,
        model=model,
        dim=3,
        embedding=(1.0, 0.0, 0.0),
    )


def _begin(
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


async def _index_and_activate(
    store: PostgresKnowledgeStore,
    kb_id: str,
    document_id: str,
    version_id: str,
    content: str,
    *,
    now: datetime,
) -> None:
    indexed = await store.mark_indexing(kb_id, document_id, version_id, now=now)
    assert indexed.version.status is KnowledgeVersionStatus.indexing
    await store.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=kb_id,
            document_id=document_id,
            document_version_id=version_id,
            chunks=(_chunk(content),),
        ),
        now=now,
    )
    activated = await store.activate_version(kb_id, document_id, version_id, now=now)
    assert activated.activated


async def test_constructor_scope_guards_cross_kb_non_disclosure_and_byte_limit(
    migrated_db: AsyncEngine,
) -> None:
    with pytest.raises(KnowledgeValidationError, match="database engine"):
        PostgresKnowledgeStore(object(), "scope:a")  # type: ignore[arg-type]
    for invalid in (True, 1.0, 0, -1):
        with pytest.raises(KnowledgeValidationError, match="positive integer"):
            PostgresKnowledgeStore(
                migrated_db,
                "scope:a",
                document_max_bytes=invalid,  # type: ignore[arg-type]
            )

    first_scope = PostgresKnowledgeStore(migrated_db, "scope:a", document_max_bytes=4)
    other_scope = PostgresKnowledgeStore(migrated_db, "scope:b")
    assert isinstance(first_scope, KnowledgeStore)
    first_base = await _base(first_scope)
    second_base = await _base(first_scope, name="Other")
    accepted = await first_scope.create_document_version(
        _version(first_base.id, "éé"),
        now=_NOW,
    )
    assert accepted.version.content == "éé"

    with pytest.raises(KnowledgeValidationError, match="content_too_large") as caught:
        await first_scope.create_document_version(
            _version(first_base.id, "ééx", title="Oversized"),
            now=_NOW + timedelta(seconds=1),
        )
    assert "ééx" not in caught.value.public_message
    assert len(await first_scope.list_documents(first_base.id)) == 1

    assert await other_scope.get_base(first_base.id) is None
    assert await other_scope.list_bases() == []
    assert await other_scope.get_document(first_base.id, accepted.document.id) is None
    assert (
        await other_scope.get_version(
            first_base.id,
            accepted.document.id,
            accepted.version.id,
        )
        is None
    )
    assert await first_scope.get_document(second_base.id, accepted.document.id) is None
    assert (
        await first_scope.get_version(
            second_base.id,
            accepted.document.id,
            accepted.version.id,
        )
        is None
    )
    with pytest.raises(KnowledgeNotFound, match="knowledge_document_not_found"):
        await first_scope.list_versions(second_base.id, accepted.document.id)


async def test_debug_echo_cannot_log_successful_knowledge_rows(
    migrated_db: AsyncEngine,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="sqlalchemy.engine")
    logger = logging.getLogger("sqlalchemy.engine.Engine")
    original_handlers = tuple(logger.handlers)
    original_level = logger.level
    engine = create_async_engine(
        migrated_db.url,
        echo="debug",
        hide_parameters=False,
    )
    description = "sentinel-knowledge-description"
    source_uri = "https://sentinel-source-uri.example.test/private"
    raw_content = "sentinel-knowledge-raw-content"
    chunk_text = "sentinel-knowledge-chunk-text"
    try:
        store = PostgresKnowledgeStore(engine, "logging:scope")
        assert engine.echo is False
        assert engine.sync_engine.echo is False
        assert engine.sync_engine.hide_parameters is True
        assert not engine.sync_engine.logger.isEnabledFor(logging.DEBUG)
        assert not engine.sync_engine.logger.isEnabledFor(logging.INFO)

        base = await store.create_base(
            KnowledgeBaseCreate(
                name="Logging secrecy",
                description=description,
                embedding_model="fake/embed",
                embedding_dim=3,
            ),
            now=_NOW,
        )
        created = await store.create_document_version(
            replace(
                _version(base.id, raw_content),
                source_uri=source_uri,
            ),
            now=_NOW,
        )
        updated_content = f"{chunk_text}\n{raw_content}"
        updated = await store.create_document_version(
            replace(
                _version(
                    base.id,
                    updated_content,
                    document_id=created.document.id,
                    title="Updated guide",
                ),
                source_uri=f"{source_uri}/updated",
            ),
            now=_NOW + timedelta(seconds=1),
        )
        await store.mark_indexing(
            base.id,
            updated.document.id,
            updated.version.id,
            now=_NOW + timedelta(seconds=2),
        )
        await store.replace_version_chunks(
            KnowledgeChunkReplacement(
                kb_id=base.id,
                document_id=updated.document.id,
                document_version_id=updated.version.id,
                chunks=(_chunk(chunk_text),),
            ),
            now=_NOW + timedelta(seconds=3),
        )

        loaded_base = await store.get_base(base.id)
        assert loaded_base is not None
        assert loaded_base.description == description
        assert [item.id for item in await store.list_bases()] == [base.id]
        loaded_document = await store.get_document(base.id, updated.document.id)
        assert loaded_document is not None
        assert loaded_document.source_uri == f"{source_uri}/updated"
        assert [item.id for item in await store.list_documents(base.id)] == [updated.document.id]
        loaded_version = await store.get_version(
            base.id,
            updated.document.id,
            updated.version.id,
        )
        assert loaded_version is not None
        assert loaded_version.content == updated_content
        assert [item.id for item in await store.list_versions(base.id, updated.document.id)] == [
            created.version.id,
            updated.version.id,
        ]
        assert [
            item.text
            for item in await store.list_version_chunks(
                base.id,
                updated.document.id,
                updated.version.id,
            )
        ] == [chunk_text]
    finally:
        await engine.dispose()
        logger.setLevel(original_level)
        for handler in tuple(logger.handlers):
            if handler not in original_handlers:
                logger.removeHandler(handler)
                handler.close()

    logging.getLogger(__name__).debug("unrelated application debug remains enabled")
    captured = capsys.readouterr()
    assert "unrelated application debug remains enabled" in caplog.text
    for sentinel in (description, source_uri, raw_content, chunk_text):
        assert sentinel not in caplog.text
        assert sentinel not in captured.out
        assert sentinel not in captured.err


async def test_dbapi_failures_are_bounded_redacted_and_retryable(
    migrated_db: AsyncEngine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    migrated_db.sync_engine.hide_parameters = False
    store = PostgresKnowledgeStore(migrated_db, "storage:scope")
    assert migrated_db.sync_engine.hide_parameters is True
    base = await _base(store)
    secret = "sentinel-storage-secret"
    command = replace(
        _version(base.id, f"document {secret}"),
        source_uri=f"https://example.test/{secret}",
    )

    def fail_on_secret(
        _cursor: object,
        _statement: str,
        parameters: object,
        _context: object,
    ) -> None:
        if secret in repr(parameters):
            raise psycopg.OperationalError("forced storage outage")

    dialect = migrated_db.sync_engine.dialect
    event.listen(dialect, "do_execute", fail_on_secret)
    caplog.set_level(logging.DEBUG)
    try:
        with pytest.raises(KnowledgeStorageError) as caught:
            await store.create_document_version(command, now=_NOW)
    finally:
        event.remove(dialect, "do_execute", fail_on_secret)

    error = caught.value
    assert error.code == "knowledge_storage_failure"
    assert error.public_message == "Knowledge storage is temporarily unavailable."
    assert error.retryable is True
    assert secret not in str(error)
    assert secret not in repr(error)
    assert error.__cause__ is None
    assert error.__suppress_context__ is True
    dbapi_error = error.__context__
    assert isinstance(dbapi_error, SQLAlchemyOperationalError)
    assert isinstance(dbapi_error.orig, psycopg.OperationalError)
    assert secret in repr(dbapi_error.params)
    assert dbapi_error.hide_parameters is True
    assert "SQL parameters hidden due to hide_parameters=True" in str(dbapi_error)
    assert secret not in str(dbapi_error)
    assert secret not in repr(dbapi_error)
    assert secret not in caplog.text


async def test_integrity_errors_keep_specific_conflict_mapping(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "integrity:scope")
    await _base(store)

    with pytest.raises(KnowledgeConflict) as caught:
        await _base(store, now=_NOW + timedelta(seconds=1))

    assert type(caught.value) is KnowledgeConflict
    assert caught.value.code == "knowledge_base_name_conflict"


async def test_all_idempotent_mutations_are_atomic_replayable_and_recover_jobs(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "ledger:scope")
    base_begin = _begin(
        KnowledgeOperation.create_base,
        "create-base",
        method="POST",
        path_ids={},
        body={"name": "Docs"},
    )
    base_commands = [
        KnowledgeBaseCreate(
            name="Docs",
            description="Product documentation",
            embedding_model="fake/embed",
            embedding_dim=3,
            base_id=new_knowledge_base_id(),
        )
        for _ in range(2)
    ]
    first_base, second_base = await asyncio.gather(
        *(store.create_base_idempotent(command, base_begin, now=_NOW) for command in base_commands)
    )
    assert sorted((first_base.replayed, second_base.replayed)) == [False, True]
    assert first_base.resource.id == second_base.resource.id
    assert first_base.ledger.id == second_base.ledger.id
    base = first_base.resource

    failed_begin = _begin(
        KnowledgeOperation.create_base,
        "failed-base",
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
            failed_begin,
            now=_NOW,
        )
    assert (
        await store.get_idempotent_request(
            failed_begin.operation,
            failed_begin.idempotency_key,
        )
        is None
    )

    create_command = replace(
        _version(base.id, "A"),
        new_document_id=new_knowledge_document_id(),
        document_version_id=new_knowledge_version_id(),
    )
    create_begin = _begin(
        KnowledgeOperation.create_document,
        "create-document",
        method="POST",
        path_ids={"kb_id": base.id},
        body={"content_sha256": content_sha256(create_command.content)},
    )
    created = await store.create_document_version_idempotent(
        create_command,
        create_begin,
        now=_NOW + timedelta(seconds=1),
    )
    create_replay = await store.create_document_version_idempotent(
        replace(
            create_command,
            new_document_id=new_knowledge_document_id(),
            document_version_id=new_knowledge_version_id(),
        ),
        create_begin,
        now=_NOW + timedelta(seconds=2),
    )
    assert create_replay.replayed
    assert create_replay.resource.document.id == created.resource.document.id
    assert create_replay.resource.version.id == created.resource.version.id

    job_id = "job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    attached = await store.attach_idempotent_job(
        KnowledgeIdempotencyAttach(
            operation=create_begin.operation,
            idempotency_key=create_begin.idempotency_key,
            request_fingerprint=create_begin.request_fingerprint,
            job_id=job_id,
        ),
        now=_NOW + timedelta(seconds=3),
    )
    assert attached.job_id == job_id
    with pytest.raises(KnowledgeConflict, match="idempotency_job_conflict"):
        await store.attach_idempotent_job(
            KnowledgeIdempotencyAttach(
                operation=create_begin.operation,
                idempotency_key=create_begin.idempotency_key,
                request_fingerprint=create_begin.request_fingerprint,
                job_id="job_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ),
            now=_NOW + timedelta(seconds=4),
        )
    attached_version = await store.attach_version_job(
        base.id,
        created.resource.document.id,
        created.resource.version.id,
        job_id,
        now=_NOW + timedelta(seconds=5),
    )
    assert attached_version.ingest_job_id == job_id
    with pytest.raises(KnowledgeConflict, match="idempotency_job_conflict"):
        await store.attach_version_job(
            base.id,
            created.resource.document.id,
            created.resource.version.id,
            "job_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            now=_NOW + timedelta(seconds=6),
        )

    update_command = replace(
        _version(
            base.id,
            "B",
            document_id=created.resource.document.id,
        ),
        document_version_id=new_knowledge_version_id(),
    )
    update_begin = _begin(
        KnowledgeOperation.update_document,
        "update-document",
        method="PUT",
        path_ids={
            "kb_id": base.id,
            "document_id": created.resource.document.id,
        },
        body={"content_sha256": content_sha256("B")},
    )
    updated = await store.update_document_version_idempotent(
        update_command,
        update_begin,
        now=_NOW + timedelta(seconds=7),
    )
    update_replay = await store.update_document_version_idempotent(
        replace(update_command, document_version_id=new_knowledge_version_id()),
        update_begin,
        now=_NOW + timedelta(seconds=8),
    )
    assert update_replay.replayed
    assert update_replay.resource.version.id == updated.resource.version.id
    versions_before_conflict = await store.list_versions(
        base.id,
        created.resource.document.id,
    )
    with pytest.raises(KnowledgeConflict, match="idempotency_key_reused"):
        await store.update_document_version_idempotent(
            replace(update_command, content="C"),
            replace(update_begin, request_fingerprint="f" * 64),
            now=_NOW + timedelta(seconds=9),
        )
    assert (
        await store.list_versions(base.id, created.resource.document.id) == versions_before_conflict
    )

    await _index_and_activate(
        store,
        base.id,
        created.resource.document.id,
        updated.resource.version.id,
        "B",
        now=_NOW + timedelta(seconds=10),
    )
    reindex_command = KnowledgeDocumentReindex(
        kb_id=base.id,
        document_id=created.resource.document.id,
        chunking_version="keel-char-v2",
        target_chars=1200,
        overlap_chars=120,
        document_version_id=new_knowledge_version_id(),
    )
    reindex_begin = _begin(
        KnowledgeOperation.reindex_document,
        "reindex-document",
        method="POST",
        path_ids={
            "kb_id": base.id,
            "document_id": created.resource.document.id,
        },
        body={"chunking_version": "keel-char-v2"},
    )
    reindexed = await store.reindex_document_idempotent(
        reindex_command,
        reindex_begin,
        now=_NOW + timedelta(seconds=11),
    )
    reindex_replay = await store.reindex_document_idempotent(
        replace(reindex_command, document_version_id=new_knowledge_version_id()),
        reindex_begin,
        now=_NOW + timedelta(seconds=12),
    )
    assert reindex_replay.replayed
    assert reindex_replay.resource.version.id == reindexed.resource.version.id

    delete_document = KnowledgeDocumentTombstone(
        kb_id=base.id,
        document_id=created.resource.document.id,
    )
    delete_document_begin = _begin(
        KnowledgeOperation.delete_document,
        "delete-document",
        method="DELETE",
        path_ids={
            "kb_id": base.id,
            "document_id": created.resource.document.id,
        },
        body=None,
    )
    deleted_document = await store.tombstone_document_idempotent(
        delete_document,
        delete_document_begin,
        now=_NOW + timedelta(seconds=13),
    )
    deleted_document_replay = await store.tombstone_document_idempotent(
        delete_document,
        delete_document_begin,
        now=_NOW + timedelta(seconds=14),
    )
    assert deleted_document.resource.status is KnowledgeDocumentStatus.deleted
    assert deleted_document_replay.replayed
    assert deleted_document_replay.ledger.id == deleted_document.ledger.id

    delete_base = KnowledgeBaseTombstone(kb_id=base.id)
    delete_base_begin = _begin(
        KnowledgeOperation.delete_base,
        "delete-base",
        method="DELETE",
        path_ids={"kb_id": base.id},
        body=None,
    )
    deleted_base = await store.tombstone_base_idempotent(
        delete_base,
        delete_base_begin,
        now=_NOW + timedelta(seconds=15),
    )
    deleted_base_replay = await store.tombstone_base_idempotent(
        delete_base,
        delete_base_begin,
        now=_NOW + timedelta(seconds=16),
    )
    assert deleted_base.resource.status is KnowledgeBaseStatus.deleted
    assert deleted_base_replay.replayed
    assert deleted_base_replay.ledger.id == deleted_base.ledger.id


async def test_document_lock_serializes_versions_and_source_type_changes_fingerprint(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "versions:scope")
    base = await _base(store)
    markdown = await store.create_document_version(_version(base.id, "same"), now=_NOW)
    text_version = await store.create_document_version(
        _version(
            base.id,
            "same",
            document_id=markdown.document.id,
            source_type=KnowledgeSourceType.text,
        ),
        now=_NOW + timedelta(seconds=1),
    )
    assert text_version.version.version == 2
    assert text_version.version.mime_type == "text/plain"
    assert text_version.version.index_fingerprint == index_fingerprint(
        content_sha256("same"),
        KnowledgeSourceType.text,
        "keel-char-v1",
        base.embedding_model,
        base.embedding_dim,
        1600,
        200,
    )

    concurrent = await asyncio.gather(
        *(
            store.create_document_version(
                _version(
                    base.id,
                    f"content-{index}",
                    document_id=markdown.document.id,
                ),
                now=_NOW + timedelta(seconds=index + 2),
            )
            for index in range(8)
        )
    )
    assert sorted(result.version.version for result in concurrent) == list(range(3, 11))
    assert [
        record.version for record in await store.list_versions(base.id, markdown.document.id)
    ] == list(range(1, 11))


async def test_guarded_chunk_retry_stale_activation_and_active_preservation(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "lifecycle:scope")
    base = await _base(store)
    first = await store.create_document_version(_version(base.id, "AlphaBeta"), now=_NOW)
    await store.mark_indexing(base.id, first.document.id, first.version.id, now=_NOW)
    nested_metadata: dict[str, object] = {"nested": {"value": 1}}
    initial_replacement = KnowledgeChunkReplacement(
        kb_id=base.id,
        document_id=first.document.id,
        document_version_id=first.version.id,
        chunks=(
            _chunk("Alpha", ordinal=0, metadata=nested_metadata),
            _chunk("Beta", ordinal=1, char_start=5),
        ),
    )
    with pytest.raises(KnowledgeEmbeddingMismatch):
        await store.replace_version_chunks(
            replace(
                initial_replacement,
                chunks=(replace(initial_replacement.chunks[0], model="wrong/embed"),),
            ),
            now=_NOW,
        )
    await store.replace_version_chunks(initial_replacement, now=_NOW)
    nested_metadata["nested"]["value"] = 9  # type: ignore[index]
    initial_chunks = await store.list_version_chunks(
        base.id,
        first.document.id,
        first.version.id,
    )
    assert initial_chunks[0].metadata == {"nested": {"value": 1}}
    first_chunk_id = initial_chunks[0].id
    first_created_at = initial_chunks[0].created_at

    await store.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=first.version.id,
            chunks=(_chunk("AlphaBeta", metadata={"retry": True}),),
        ),
        now=_NOW + timedelta(seconds=1),
    )
    retried_chunks = await store.list_version_chunks(
        base.id,
        first.document.id,
        first.version.id,
    )
    assert len(retried_chunks) == 1
    assert retried_chunks[0].id == first_chunk_id
    assert retried_chunks[0].created_at == first_created_at
    assert retried_chunks[0].metadata == {"retry": True}
    retried_chunks[0].metadata["retry"] = False
    assert (await store.list_version_chunks(base.id, first.document.id, first.version.id))[
        0
    ].metadata == {"retry": True}

    activated_first = await store.activate_version(
        base.id,
        first.document.id,
        first.version.id,
        now=_NOW + timedelta(seconds=2),
    )
    assert activated_first.activated

    second = await store.create_document_version(
        _version(base.id, "Second", document_id=first.document.id),
        now=_NOW + timedelta(seconds=3),
    )
    await store.mark_indexing(
        base.id,
        first.document.id,
        second.version.id,
        now=_NOW + timedelta(seconds=4),
    )
    await store.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=second.version.id,
            chunks=(_chunk("Second"),),
        ),
        now=_NOW + timedelta(seconds=4),
    )
    third = await store.create_document_version(
        _version(base.id, "Third", document_id=first.document.id),
        now=_NOW + timedelta(seconds=5),
    )
    stale = await store.activate_version(
        base.id,
        first.document.id,
        second.version.id,
        now=_NOW + timedelta(seconds=6),
    )
    assert stale.stale
    assert stale.version.status is KnowledgeVersionStatus.superseded
    assert await store.list_version_chunks(base.id, first.document.id, second.version.id) == []

    await store.mark_indexing(
        base.id,
        first.document.id,
        third.version.id,
        now=_NOW + timedelta(seconds=7),
    )
    failed = await store.mark_version_failed(
        KnowledgeVersionFailure(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=third.version.id,
            error_kind="embedding_unavailable",
            error_message="Embedding is unavailable.",
        ),
        now=_NOW + timedelta(seconds=8),
    )
    assert failed.status is KnowledgeVersionStatus.failed
    still_failed = await store.mark_version_cancelled(
        KnowledgeVersionCancellation(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=third.version.id,
        ),
        now=_NOW + timedelta(seconds=9),
    )
    assert still_failed.status is KnowledgeVersionStatus.failed
    document = await store.get_document(base.id, first.document.id)
    assert document is not None
    assert document.status is KnowledgeDocumentStatus.active
    assert document.active_version_id == first.version.id

    cancelled = await store.create_document_version(
        _version(base.id, "Cancelled", document_id=first.document.id),
        now=_NOW + timedelta(seconds=10),
    )
    await store.mark_indexing(
        base.id,
        first.document.id,
        cancelled.version.id,
        now=_NOW + timedelta(seconds=11),
    )
    cancelled_version = await store.mark_version_cancelled(
        KnowledgeVersionCancellation(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=cancelled.version.id,
        ),
        now=_NOW + timedelta(seconds=12),
    )
    assert cancelled_version.status is KnowledgeVersionStatus.cancelled
    after_cancel = await store.get_document(base.id, first.document.id)
    assert after_cancel is not None
    assert after_cancel.active_version_id == first.version.id
    assert after_cancel.status is KnowledgeDocumentStatus.active

    replacement = await store.create_document_version(
        _version(base.id, "Fourth", document_id=first.document.id),
        now=_NOW + timedelta(seconds=13),
    )
    await _index_and_activate(
        store,
        base.id,
        first.document.id,
        replacement.version.id,
        "Fourth",
        now=_NOW + timedelta(seconds=14),
    )
    old_active = await store.get_version(
        base.id,
        first.document.id,
        first.version.id,
    )
    assert old_active is not None
    assert old_active.status is KnowledgeVersionStatus.superseded
    with pytest.raises(KnowledgeConflict, match="chunk_write_rejected"):
        await store.replace_version_chunks(
            KnowledgeChunkReplacement(
                kb_id=base.id,
                document_id=first.document.id,
                document_version_id=replacement.version.id,
                chunks=(_chunk("Fourth"),),
            ),
            now=_NOW + timedelta(seconds=15),
        )


async def test_delete_hooks_purge_without_resurrection_and_purge_is_idempotent(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "delete:scope")
    base = await _base(store)
    first = await store.create_document_version(_version(base.id, "Active"), now=_NOW)
    await _index_and_activate(
        store,
        base.id,
        first.document.id,
        first.version.id,
        "Active",
        now=_NOW,
    )
    pending = await store.create_document_version(
        _version(base.id, "Pending", document_id=first.document.id),
        now=_NOW + timedelta(seconds=1),
    )
    await store.mark_indexing(
        base.id,
        first.document.id,
        pending.version.id,
        now=_NOW + timedelta(seconds=2),
    )
    await store.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=base.id,
            document_id=first.document.id,
            document_version_id=pending.version.id,
            chunks=(_chunk("Pending"),),
        ),
        now=_NOW + timedelta(seconds=2),
    )

    await asyncio.gather(
        store.tombstone_document(
            base.id,
            first.document.id,
            now=_NOW + timedelta(seconds=3),
        ),
        store.activate_version(
            base.id,
            first.document.id,
            pending.version.id,
            now=_NOW + timedelta(seconds=3),
        ),
    )
    deleted_document = await store.get_document(base.id, first.document.id)
    assert deleted_document is not None
    assert deleted_document.status is KnowledgeDocumentStatus.deleted
    assert deleted_document.desired_version_id is None
    assert deleted_document.active_version_id is None

    late = await store.activate_version(
        base.id,
        first.document.id,
        pending.version.id,
        now=_NOW + timedelta(seconds=4),
    )
    assert not late.activated
    assert late.version.status is KnowledgeVersionStatus.purged
    assert late.version.content is None
    assert await store.list_version_chunks(base.id, first.document.id, pending.version.id) == []

    purged_document = await store.purge_document(
        base.id,
        first.document.id,
        now=_NOW + timedelta(seconds=5),
    )
    assert purged_document.documents_purged == 1
    assert purged_document.versions_purged >= 1
    repeated_document = await store.purge_document(
        base.id,
        first.document.id,
        now=_NOW + timedelta(seconds=6),
    )
    assert repeated_document.documents_purged == 0
    assert repeated_document.versions_purged == 0
    assert repeated_document.chunks_removed == 0

    await store.tombstone_base(base.id, now=_NOW + timedelta(seconds=7))
    await store.purge_base(base.id, now=_NOW + timedelta(seconds=8))
    base_tombstone = await store.get_base(base.id)
    document_tombstone = await store.get_document(base.id, first.document.id)
    version_tombstone = await store.get_version(
        base.id,
        first.document.id,
        first.version.id,
    )
    assert base_tombstone is not None and base_tombstone.description is None
    assert document_tombstone is not None and document_tombstone.source_uri is None
    assert version_tombstone is not None and version_tombstone.content is None
    assert version_tombstone.status is KnowledgeVersionStatus.purged
    repeated_base = await store.purge_base(
        base.id,
        now=_NOW + timedelta(seconds=9),
    )
    assert repeated_base.documents_purged == 0
    assert repeated_base.versions_purged == 0
    assert repeated_base.chunks_removed == 0


async def test_cross_scope_idempotency_and_same_scope_cross_kb_are_not_disclosed(
    migrated_db: AsyncEngine,
) -> None:
    first = PostgresKnowledgeStore(migrated_db, "isolation:a")
    second = PostgresKnowledgeStore(migrated_db, "isolation:b")
    base = await _base(first)
    other_base = await _base(first, name="Other")
    begin = _begin(
        KnowledgeOperation.create_document,
        "isolated-request",
        method="POST",
        path_ids={"kb_id": base.id},
        body={"content_sha256": content_sha256("secret")},
    )
    created = await first.create_document_version_idempotent(
        _version(base.id, "secret"),
        begin,
        now=_NOW,
    )

    assert await second.get_base(base.id) is None
    assert await second.get_document(base.id, created.resource.document.id) is None
    assert await second.get_idempotent_request(begin.operation, begin.idempotency_key) is None
    with pytest.raises(KnowledgeNotFound, match="idempotency_not_found"):
        await second.attach_idempotent_job(
            KnowledgeIdempotencyAttach(
                operation=begin.operation,
                idempotency_key=begin.idempotency_key,
                request_fingerprint=begin.request_fingerprint,
                job_id="job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            now=_NOW,
        )

    assert await first.get_document(other_base.id, created.resource.document.id) is None
    assert (
        await first.get_version(
            other_base.id,
            created.resource.document.id,
            created.resource.version.id,
        )
        is None
    )
    with pytest.raises(KnowledgeNotFound):
        await first.attach_version_job(
            other_base.id,
            created.resource.document.id,
            created.resource.version.id,
            "job_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            now=_NOW,
        )
