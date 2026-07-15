"""Postgres Knowledge retrieval, isolation, degradation, and citation invariants."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from sqlalchemy import event
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

import keel_core.knowledge.search as knowledge_search
from keel_core.knowledge import (
    KnowledgeBaseCreate,
    KnowledgeChunkReplacement,
    KnowledgeChunkWrite,
    KnowledgeDocumentVersionCreate,
    KnowledgeSearcher,
    KnowledgeSearchMode,
    KnowledgeSourceType,
    KnowledgeStorageError,
    KnowledgeValidationError,
    PostgresKnowledgeStore,
    content_sha256,
    new_knowledge_base_id,
)
from keel_core.knowledge.search import _fuse_chunk_ids

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 16, 1, 0, tzinfo=UTC)
_MODEL = "fake/knowledge-search"
_DIM = 2


@dataclass(frozen=True, slots=True)
class _Chunk:
    text: str
    embedding: tuple[float, float]
    heading_path: tuple[str, ...] = ("Guide",)


@dataclass(frozen=True, slots=True)
class _IndexedDocument:
    document_id: str
    version_id: str
    chunk_ids: tuple[str, ...]
    starts: tuple[int, ...]


class _MeaningEmbedder:
    model = _MODEL
    dim = _DIM

    def __init__(self, vector: tuple[float, float] = (1.0, 0.0)) -> None:
        self.vector = vector
        self.calls: list[tuple[str, ...]] = []

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls.append(tuple(texts))
        return [list(self.vector) for _ in texts]


class _FailingEmbedder:
    model = _MODEL
    dim = _DIM

    def __init__(self) -> None:
        self.calls = 0

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += 1
        raise RuntimeError(f"provider-secret-message for {len(texts)} inputs")


class _CountingEmbedder:
    def __init__(self, *, model: str, dim: int) -> None:
        self.model = model
        self.dim = dim
        self.calls = 0

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += 1
        return [[1.0] + [0.0] * (self.dim - 1) for _ in texts]


def _assert_detached_storage_error(error: BaseException, *forbidden_text: str) -> None:
    assert error.__cause__ is None
    assert error.__context__ is None

    pending: list[object] = [error]
    seen: set[int] = set()
    while pending:
        value = pending.pop()
        identity = id(value)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(value, str):
            for forbidden in forbidden_text:
                assert forbidden not in value
        elif isinstance(value, BaseException):
            assert not isinstance(value, SQLAlchemyError)
            for forbidden in forbidden_text:
                assert forbidden not in str(value)
                assert forbidden not in repr(value)
            pending.extend(value.args)
            pending.extend(vars(value).values())
            pending.extend(
                linked for linked in (value.__cause__, value.__context__) if linked is not None
            )
        elif isinstance(value, dict):
            pending.extend(value.keys())
            pending.extend(value.values())
        elif isinstance(value, list | tuple | set | frozenset):
            pending.extend(value)

    boundary_frame_found = False
    traceback = error.__traceback__
    while traceback is not None:
        frame_locals = traceback.tb_frame.f_locals
        if "storage_failure" in frame_locals and "conflict_code" in frame_locals:
            boundary_frame_found = True
            assert "args" not in frame_locals
            assert "kwargs" not in frame_locals
            for forbidden in forbidden_text:
                assert forbidden not in repr(frame_locals)
        traceback = traceback.tb_next
    assert boundary_frame_found


async def _create_base(
    store: PostgresKnowledgeStore,
    *,
    name: str,
    model: str = _MODEL,
    dim: int = _DIM,
) -> str:
    base = await store.create_base(
        KnowledgeBaseCreate(
            name=name,
            description="Search test Knowledge Base",
            embedding_model=model,
            embedding_dim=dim,
        ),
        now=_NOW,
    )
    return base.id


async def _index_document(
    store: PostgresKnowledgeStore,
    kb_id: str,
    chunks: Sequence[_Chunk],
    *,
    title: str = "Guide.md",
    source_uri: str | None = "https://example.test/guide",
    document_id: str | None = None,
    now: datetime = _NOW,
    activate: bool = True,
) -> _IndexedDocument:
    content_parts: list[str] = []
    starts: list[int] = []
    offset = 0
    for index, chunk in enumerate(chunks):
        if index:
            content_parts.append("\n")
            offset += 1
        starts.append(offset)
        content_parts.append(chunk.text)
        offset += len(chunk.text)
    content = "".join(content_parts)
    created = await store.create_document_version(
        KnowledgeDocumentVersionCreate(
            kb_id=kb_id,
            document_id=document_id,
            title=title,
            source_type=KnowledgeSourceType.markdown,
            source_uri=source_uri,
            content=content,
            mime_type="text/markdown",
            chunking_version="keel-char-v1",
            target_chars=max(len(content) + 1, 2),
            overlap_chars=0,
        ),
        now=now,
    )
    await store.mark_indexing(
        kb_id,
        created.document.id,
        created.version.id,
        now=now + timedelta(milliseconds=1),
    )
    replaced = await store.replace_version_chunks(
        KnowledgeChunkReplacement(
            kb_id=kb_id,
            document_id=created.document.id,
            document_version_id=created.version.id,
            chunks=tuple(
                KnowledgeChunkWrite(
                    ordinal=ordinal,
                    text=chunk.text,
                    char_start=starts[ordinal],
                    char_end=starts[ordinal] + len(chunk.text),
                    content_hash=content_sha256(chunk.text),
                    heading_path=chunk.heading_path,
                    metadata={"test": True},
                    model=_MODEL,
                    dim=_DIM,
                    embedding=chunk.embedding,
                )
                for ordinal, chunk in enumerate(chunks)
            ),
        ),
        now=now + timedelta(milliseconds=2),
    )
    assert replaced.chunk_count == len(chunks)
    if activate:
        activated = await store.activate_version(
            kb_id,
            created.document.id,
            created.version.id,
            now=now + timedelta(milliseconds=3),
        )
        assert activated.activated
    records = await store.list_version_chunks(
        kb_id,
        created.document.id,
        created.version.id,
    )
    return _IndexedDocument(
        document_id=created.document.id,
        version_id=created.version.id,
        chunk_ids=tuple(record.id for record in records),
        starts=tuple(starts),
    )


async def test_active_version_switch_deleted_and_purged_content_never_leaks(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "search:lifecycle")
    kb_id = await _create_base(store, name="Lifecycle")
    first = await _index_document(
        store,
        kb_id,
        [_Chunk("legacy installation alpha", (1.0, 0.0))],
    )
    searcher = KnowledgeSearcher(migrated_db, store.scope_id)

    hits, _ = await searcher.search(kb_id, "legacy installation", k=5)
    assert [hit.citation.document_version_id for hit in hits] == [first.version_id]

    second = await _index_document(
        store,
        kb_id,
        [_Chunk("current upgrade beta", (0.0, 1.0))],
        document_id=first.document_id,
        now=_NOW + timedelta(seconds=1),
    )
    stale = await _index_document(
        store,
        kb_id,
        [_Chunk("staged release gamma", (1.0, 0.0))],
        document_id=first.document_id,
        now=_NOW + timedelta(seconds=2),
        activate=False,
    )

    old_hits, _ = await searcher.search(kb_id, "legacy installation", k=5)
    staged_hits, _ = await searcher.search(kb_id, "staged release", k=5)
    current_hits, _ = await searcher.search(kb_id, "current upgrade", k=5)
    assert old_hits == []
    assert staged_hits == []
    assert [hit.citation.document_version_id for hit in current_hits] == [second.version_id]
    assert stale.version_id not in {hit.citation.document_version_id for hit in current_hits}

    await store.tombstone_document(
        kb_id,
        first.document_id,
        now=_NOW + timedelta(seconds=3),
    )
    deleted_hits, _ = await searcher.search(kb_id, "current upgrade", k=5)
    assert deleted_hits == []

    purged = await store.purge_document(
        kb_id,
        first.document_id,
        now=_NOW + timedelta(seconds=4),
    )
    assert purged.chunks_removed >= 2
    purged_hits, _ = await searcher.search(kb_id, "current upgrade", k=5)
    assert purged_hits == []

    deleted_kb_id = await _create_base(store, name="Deleted base")
    await _index_document(
        store,
        deleted_kb_id,
        [_Chunk("base tombstone marker", (1.0, 0.0))],
        now=_NOW + timedelta(seconds=5),
    )
    await store.tombstone_base(deleted_kb_id, now=_NOW + timedelta(seconds=6))
    deleted_base_hits, _ = await searcher.search(deleted_kb_id, "base tombstone", k=5)
    assert deleted_base_hits == []


async def test_scope_and_same_scope_cross_kb_content_is_not_disclosed(
    migrated_db: AsyncEngine,
) -> None:
    first_scope = PostgresKnowledgeStore(migrated_db, "search:scope-a")
    second_scope = PostgresKnowledgeStore(migrated_db, "search:scope-b")
    first_kb = await _create_base(first_scope, name="First")
    second_kb = await _create_base(first_scope, name="Second")
    other_scope_kb = await _create_base(second_scope, name="Other scope")
    first_doc = await _index_document(
        first_scope,
        first_kb,
        [_Chunk("scope alpha private marker", (1.0, 0.0))],
    )
    second_doc = await _index_document(
        first_scope,
        second_kb,
        [_Chunk("scope beta private marker", (0.0, 1.0))],
    )
    other_doc = await _index_document(
        second_scope,
        other_scope_kb,
        [_Chunk("scope gamma private marker", (1.0, 0.0))],
    )

    first_searcher = KnowledgeSearcher(migrated_db, first_scope.scope_id)
    second_searcher = KnowledgeSearcher(migrated_db, second_scope.scope_id)
    first_hits, _ = await first_searcher.search(first_kb, "private marker", k=5)
    second_hits, _ = await first_searcher.search(second_kb, "private marker", k=5)
    wrong_scope_hits, _ = await second_searcher.search(first_kb, "private marker", k=5)
    reverse_scope_hits, _ = await first_searcher.search(other_scope_kb, "private marker", k=5)

    assert [hit.citation.chunk_id for hit in first_hits] == list(first_doc.chunk_ids)
    assert [hit.citation.chunk_id for hit in second_hits] == list(second_doc.chunk_ids)
    assert wrong_scope_hits == []
    assert reverse_scope_hits == []
    assert other_doc.chunk_ids[0] not in {
        hit.citation.chunk_id for hit in [*first_hits, *second_hits]
    }


async def test_lexical_cjk_search_returns_bounded_exact_citation_data(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "search:lexical")
    kb_id = await _create_base(store, name="Lexical")
    long_text = "在 Windows 上安装 Keel 的步骤。" + "补充说明" * 600
    indexed = await _index_document(
        store,
        kb_id,
        [
            _Chunk(
                long_text,
                (1.0, 0.0),
                heading_path=("指南", "Windows"),
            )
        ],
        title="安装指南.md",
        source_uri="https://example.test/安装",
    )

    hits, status = await KnowledgeSearcher(migrated_db, store.scope_id).search(
        kb_id,
        "安装",
        k=5,
    )

    assert status.mode is KnowledgeSearchMode.lexical
    assert status.semantic_error is None
    assert len(hits) == 1
    hit = hits[0]
    assert hit.rank == 1
    assert hit.snippet == long_text[:2_000]
    assert hit.heading_path == ["指南", "Windows"]
    assert hit.citation.id == "cite_1"
    assert hit.citation.kb_id == kb_id
    assert hit.citation.document_id == indexed.document_id
    assert hit.citation.document_version_id == indexed.version_id
    assert hit.citation.chunk_id == indexed.chunk_ids[0]
    assert hit.citation.title == "安装指南.md"
    assert hit.citation.source_uri == "https://example.test/安装"
    assert hit.citation.ordinal == 0
    assert hit.citation.char_start == indexed.starts[0]
    assert hit.citation.char_end == indexed.starts[0] + len(long_text)
    assert hit.citation.label == "安装指南.md#chunk-1"


async def test_hybrid_search_uses_exact_cosine_candidates(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "search:hybrid")
    kb_id = await _create_base(store, name="Hybrid")
    indexed = await _index_document(
        store,
        kb_id,
        [
            _Chunk("A sleeping cat rests quietly.", (1.0, 0.0)),
            _Chunk("Database migration instructions.", (0.0, 1.0)),
        ],
    )
    embedder = _MeaningEmbedder((1.0, 0.0))

    hits, status = await KnowledgeSearcher(
        migrated_db,
        store.scope_id,
        embedder,
    ).search(kb_id, "qsemantic", k=1)

    assert status.mode is KnowledgeSearchMode.hybrid
    assert status.semantic_error is None
    assert embedder.calls == [("qsemantic",)]
    assert [hit.citation.chunk_id for hit in hits] == [indexed.chunk_ids[0]]


async def test_embedding_runtime_failure_preserves_lexical_hits_without_message_leak(
    migrated_db: AsyncEngine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "search:degraded")
    kb_id = await _create_base(store, name="Degraded")
    indexed = await _index_document(
        store,
        kb_id,
        [_Chunk("runtime degradation marker", (1.0, 0.0))],
    )
    embedder = _FailingEmbedder()
    caplog.set_level(logging.DEBUG)

    hits, status = await KnowledgeSearcher(
        migrated_db,
        store.scope_id,
        embedder,
    ).search(kb_id, "runtime degradation", k=5)

    assert [hit.citation.chunk_id for hit in hits] == list(indexed.chunk_ids)
    assert embedder.calls == 1
    assert status.mode is KnowledgeSearchMode.lexical_degraded
    assert status.semantic_error == "embedding_unavailable"
    assert "provider-secret-message" not in caplog.text


@pytest.mark.parametrize(
    ("model", "dim"),
    [
        ("fake/wrong-model", _DIM),
        (_MODEL, _DIM + 1),
    ],
)
async def test_embedder_pin_mismatch_degrades_without_calling_embed(
    migrated_db: AsyncEngine,
    model: str,
    dim: int,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, f"search:mismatch:{dim}:{model}")
    kb_id = await _create_base(store, name=f"Mismatch {dim} {model}")
    indexed = await _index_document(
        store,
        kb_id,
        [_Chunk("configuration mismatch marker", (1.0, 0.0))],
    )
    embedder = _CountingEmbedder(model=model, dim=dim)

    hits, status = await KnowledgeSearcher(
        migrated_db,
        store.scope_id,
        embedder,
    ).search(kb_id, "configuration mismatch", k=5)

    assert [hit.citation.chunk_id for hit in hits] == list(indexed.chunk_ids)
    assert embedder.calls == 0
    assert status.mode is KnowledgeSearchMode.lexical_degraded
    assert status.semantic_error == "embedding_configuration_mismatch"


async def test_rrf_uses_local_integers_and_has_stable_ties(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _fuse_chunk_ids([["chunk-a", "chunk-b"], ["chunk-b", "chunk-a"]]) == [
        "chunk-a",
        "chunk-b",
    ]

    captured: list[list[int]] = []

    def fake_rrf(ranked_lists: Sequence[Sequence[int]], *, k: int = 60) -> list[int]:
        assert k == 60
        captured.extend([list(ranked) for ranked in ranked_lists])
        assert all(type(item) is int for ranked in ranked_lists for item in ranked)
        return [3, 2, 1]

    monkeypatch.setattr(knowledge_search, "rrf_fuse", fake_rrf)
    assert _fuse_chunk_ids([["chunk-a", "chunk-b"], ["chunk-b", "chunk-c"]]) == [
        "chunk-c",
        "chunk-b",
        "chunk-a",
    ]
    assert captured == [[1, 2], [2, 3]]


async def test_search_order_is_stable_for_equal_scores(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "search:ties")
    kb_id = await _create_base(store, name="Ties")
    indexed = await _index_document(
        store,
        kb_id,
        [
            _Chunk("identical score marker", (1.0, 0.0)),
            _Chunk("identical score marker", (1.0, 0.0)),
        ],
    )
    searcher = KnowledgeSearcher(migrated_db, store.scope_id)

    first, _ = await searcher.search(kb_id, "identical score marker", k=2)
    second, _ = await searcher.search(kb_id, "identical score marker", k=2)
    expected = list(indexed.chunk_ids)
    assert [hit.citation.chunk_id for hit in first] == expected
    assert [hit.citation.chunk_id for hit in second] == expected
    assert [hit.rank for hit in first] == [1, 2]
    assert [hit.citation.id for hit in first] == ["cite_1", "cite_2"]


async def test_constructor_and_search_validate_limits_before_sql(
    migrated_db: AsyncEngine,
) -> None:
    with pytest.raises(KnowledgeValidationError, match="database engine"):
        KnowledgeSearcher(object(), "search:validation")  # type: ignore[arg-type]
    for invalid_scope in ("", " ", "bad\x00scope", 3):
        with pytest.raises(KnowledgeValidationError):
            KnowledgeSearcher(migrated_db, invalid_scope)  # type: ignore[arg-type]
    for invalid_limit in (True, 0, -1):
        with pytest.raises(KnowledgeValidationError, match="query limit"):
            KnowledgeSearcher(
                migrated_db,
                "search:validation",
                query_max_chars=invalid_limit,  # type: ignore[arg-type]
            )
        with pytest.raises(KnowledgeValidationError, match="result limit"):
            KnowledgeSearcher(
                migrated_db,
                "search:validation",
                k_max=invalid_limit,  # type: ignore[arg-type]
            )
        with pytest.raises(KnowledgeValidationError, match="candidate multiplier"):
            KnowledgeSearcher(
                migrated_db,
                "search:validation",
                candidate_multiplier=invalid_limit,  # type: ignore[arg-type]
            )
    with pytest.raises(KnowledgeValidationError, match="result limit"):
        KnowledgeSearcher(migrated_db, "search:validation", k_max=11)

    searcher = KnowledgeSearcher(
        migrated_db,
        "search:validation",
        query_max_chars=8,
        k_max=3,
    )
    statements = 0

    def count_sql(
        _conn: object,
        _cursor: object,
        _statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        nonlocal statements
        statements += 1

    event.listen(migrated_db.sync_engine, "before_cursor_execute", count_sql)
    try:
        invalid_queries: tuple[object, ...] = (
            "",
            "   ",
            "bad\x00query",
            "\ud800",
            "123456789",
            3,
        )
        for invalid_query in invalid_queries:
            with pytest.raises(KnowledgeValidationError, match="query"):
                await searcher.search(
                    new_knowledge_base_id(),
                    invalid_query,  # type: ignore[arg-type]
                )
        for invalid_k in (True, 0, 4, 1.0, "1"):
            with pytest.raises(KnowledgeValidationError, match="search k"):
                await searcher.search(
                    new_knowledge_base_id(),
                    "valid",
                    k=invalid_k,  # type: ignore[arg-type]
                )
        with pytest.raises(KnowledgeValidationError, match="identifier"):
            await searcher.search("not-a-kb-id", "valid")
    finally:
        event.remove(migrated_db.sync_engine, "before_cursor_execute", count_sql)
    assert statements == 0


async def test_no_answer_is_empty_with_explicit_mode(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresKnowledgeStore(migrated_db, "search:no-answer")
    kb_id = await _create_base(store, name="No answer")
    await _index_document(
        store,
        kb_id,
        [_Chunk("installation guide for the desktop application", (1.0, 0.0))],
    )
    empty_kb_id = await _create_base(store, name="Empty hybrid")

    lexical_hits, lexical_status = await KnowledgeSearcher(
        migrated_db,
        store.scope_id,
    ).search(kb_id, "qzxv-no-answer", k=5)
    embedder = _MeaningEmbedder()
    hybrid_hits, hybrid_status = await KnowledgeSearcher(
        migrated_db,
        store.scope_id,
        embedder,
    ).search(empty_kb_id, "qzxv-no-answer", k=5)

    assert lexical_hits == []
    assert lexical_status.mode is KnowledgeSearchMode.lexical
    assert hybrid_hits == []
    assert hybrid_status.mode is KnowledgeSearchMode.hybrid
    assert embedder.calls == [("qzxv-no-answer",)]


async def test_sql_failures_are_bounded_and_debug_echo_cannot_leak_search_data(
    migrated_db: AsyncEngine,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    scope_id = "search:logging"
    store = PostgresKnowledgeStore(migrated_db, scope_id)
    kb_id = await _create_base(store, name="Logging")
    query = "sentinel-search-query"
    chunk_text = f"{query} sentinel-chunk-text"
    source_uri = "https://sentinel-source.example.test/private"
    await _index_document(
        store,
        kb_id,
        [_Chunk(chunk_text, (1.0, 0.0))],
        source_uri=source_uri,
    )

    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="sqlalchemy.engine")
    base_logger = logging.getLogger("sqlalchemy.engine.Engine")
    original_handlers = tuple(base_logger.handlers)
    original_level = base_logger.level
    engine = create_async_engine(
        migrated_db.url,
        echo="debug",
        hide_parameters=False,
    )
    outage = "sentinel-forced-sql-outage"
    try:
        searcher = KnowledgeSearcher(engine, scope_id, _MeaningEmbedder())
        assert engine.echo is False
        assert engine.sync_engine.echo is False
        assert engine.sync_engine.hide_parameters is True
        assert not engine.sync_engine.logger.isEnabledFor(logging.DEBUG)
        assert not engine.sync_engine.logger.isEnabledFor(logging.INFO)

        hits, status = await searcher.search(kb_id, query, k=5)
        assert hits
        assert status.mode is KnowledgeSearchMode.hybrid

        def fail_on_query(
            _cursor: object,
            _statement: str,
            parameters: object,
            _context: object,
        ) -> None:
            if query in repr(parameters):
                raise psycopg.OperationalError(outage)

        dialect = engine.sync_engine.dialect
        event.listen(dialect, "do_execute", fail_on_query)
        try:
            with pytest.raises(KnowledgeStorageError) as caught:
                await searcher.search(kb_id, query, k=5)
        finally:
            event.remove(dialect, "do_execute", fail_on_query)
    finally:
        await engine.dispose()
        base_logger.setLevel(original_level)
        for handler in tuple(base_logger.handlers):
            if handler not in original_handlers:
                base_logger.removeHandler(handler)
                handler.close()

    error = caught.value
    assert error.code == "knowledge_storage_failure"
    assert error.retryable is True
    _assert_detached_storage_error(error, query, chunk_text, source_uri, outage)
    captured = capsys.readouterr()
    for secret in (query, chunk_text, source_uri, outage, "[1.0,0.0]"):
        assert secret not in caplog.text
        assert secret not in captured.out
        assert secret not in captured.err
