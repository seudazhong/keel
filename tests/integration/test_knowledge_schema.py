"""Integration: complete scope-bound Knowledge Base schema."""

from __future__ import annotations

import uuid
from collections.abc import Mapping

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

_KNOWLEDGE_TABLES = (
    "knowledge_bases",
    "kb_documents",
    "kb_document_versions",
    "kb_chunks",
    "knowledge_idempotency",
)

_EXPECTED_COLUMNS = {
    "knowledge_bases": {
        "id",
        "scope_id",
        "name",
        "description",
        "embedding_model",
        "embedding_dim",
        "status",
        "created_at",
        "updated_at",
        "deleted_at",
    },
    "kb_documents": {
        "id",
        "scope_id",
        "kb_id",
        "title",
        "source_type",
        "source_uri",
        "status",
        "desired_version_id",
        "active_version_id",
        "last_error_kind",
        "last_error_message",
        "created_at",
        "updated_at",
        "deleted_at",
    },
    "kb_document_versions": {
        "id",
        "scope_id",
        "kb_id",
        "document_id",
        "version",
        "content",
        "content_sha256",
        "index_fingerprint",
        "mime_type",
        "chunking_version",
        "target_chars",
        "overlap_chars",
        "ingest_job_id",
        "status",
        "error_kind",
        "error_message",
        "created_at",
        "activated_at",
        "deleted_at",
        "purged_at",
    },
    "kb_chunks": {
        "id",
        "scope_id",
        "kb_id",
        "document_id",
        "document_version_id",
        "ordinal",
        "text",
        "char_start",
        "char_end",
        "content_hash",
        "heading_path",
        "metadata",
        "model",
        "dim",
        "embedding",
        "fts",
        "created_at",
    },
    "knowledge_idempotency": {
        "id",
        "scope_id",
        "operation",
        "idempotency_key",
        "request_fingerprint",
        "resource_kind",
        "resource_id",
        "document_version_id",
        "job_id",
        "created_at",
        "updated_at",
    },
}


async def _columns(
    engine: AsyncEngine,
    table: str,
) -> dict[str, Mapping[str, object]]:
    async with engine.connect() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        "SELECT column_name, data_type, udt_name, is_nullable, "
                        "column_default, is_generated, generation_expression "
                        "FROM information_schema.columns "
                        "WHERE table_schema = current_schema() AND table_name = :table"
                    ),
                    {"table": table},
                )
            )
            .mappings()
            .all()
        )
    return {str(row["column_name"]): row for row in rows}


async def _constraint_definitions(engine: AsyncEngine, table: str) -> dict[str, str]:
    async with engine.connect() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        "SELECT conname, pg_get_constraintdef(oid) AS definition "
                        "FROM pg_constraint WHERE conrelid = CAST(:table AS regclass)"
                    ),
                    {"table": table},
                )
            )
            .mappings()
            .all()
        )
    return {str(row["conname"]): str(row["definition"]) for row in rows}


async def _index_definitions(engine: AsyncEngine, table: str) -> dict[str, str]:
    async with engine.connect() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        "SELECT indexname, indexdef FROM pg_indexes "
                        "WHERE schemaname = current_schema() AND tablename = :table"
                    ),
                    {"table": table},
                )
            )
            .mappings()
            .all()
        )
    return {str(row["indexname"]): str(row["indexdef"]) for row in rows}


async def _insert_base(
    engine: AsyncEngine,
    *,
    kb_id: str,
    scope_id: str,
    name: str,
    status: str = "active",
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO knowledge_bases "
                "(id, scope_id, name, embedding_model, embedding_dim, status) "
                "VALUES (:id, :scope, :name, 'test/embed', 2, :status)"
            ),
            {"id": kb_id, "scope": scope_id, "name": name, "status": status},
        )


async def _seed_graph(engine: AsyncEngine, *, scope: str, suffix: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO knowledge_bases "
                "(id, scope_id, name, embedding_model, embedding_dim) "
                "VALUES (:kb, :scope, :name, 'test/embed', 2)"
            ),
            {"kb": f"kb_{suffix}", "scope": scope, "name": f"Knowledge {suffix}"},
        )
        await conn.execute(
            text(
                "INSERT INTO kb_documents "
                "(id, scope_id, kb_id, title, source_type) "
                "VALUES (:doc, :scope, :kb, :title, 'text')"
            ),
            {
                "doc": f"doc_{suffix}",
                "scope": scope,
                "kb": f"kb_{suffix}",
                "title": f"Document {suffix}",
            },
        )
        await conn.execute(
            text(
                "INSERT INTO kb_document_versions "
                "(id, scope_id, kb_id, document_id, version, content, "
                "content_sha256, index_fingerprint, mime_type, chunking_version, "
                "target_chars, overlap_chars) "
                "VALUES (:version_id, :scope, :kb, :doc, 1, 'content', "
                ":content_hash, :fingerprint, 'text/plain', 'keel-char-v1', 1600, 200)"
            ),
            {
                "version_id": f"kbv_{suffix}",
                "scope": scope,
                "kb": f"kb_{suffix}",
                "doc": f"doc_{suffix}",
                "content_hash": "a" * 64,
                "fingerprint": "b" * 64,
            },
        )
        await conn.execute(
            text(
                "UPDATE kb_documents SET desired_version_id = :version_id, "
                "active_version_id = :version_id, status = 'active' "
                "WHERE id = :doc AND scope_id = :scope"
            ),
            {
                "version_id": f"kbv_{suffix}",
                "doc": f"doc_{suffix}",
                "scope": scope,
            },
        )
        await conn.execute(
            text(
                "INSERT INTO kb_chunks "
                "(id, scope_id, kb_id, document_id, document_version_id, ordinal, "
                "text, char_start, char_end, content_hash, model, dim, embedding) "
                "VALUES (:chunk, :scope, :kb, :doc, :version_id, 0, 'content', "
                "0, 7, :content_hash, 'test/embed', 2, '[0.1,0.2]'::vector)"
            ),
            {
                "chunk": f"kbc_{suffix}",
                "scope": scope,
                "kb": f"kb_{suffix}",
                "doc": f"doc_{suffix}",
                "version_id": f"kbv_{suffix}",
                "content_hash": "c" * 64,
            },
        )
        await conn.execute(
            text(
                "INSERT INTO knowledge_idempotency "
                "(id, scope_id, operation, idempotency_key, request_fingerprint, "
                "resource_kind, resource_id, document_version_id) "
                "VALUES (:id, :scope, 'create_document', :key, :fingerprint, "
                "'document', :doc, :version_id)"
            ),
            {
                "id": f"kbi_{suffix}",
                "scope": scope,
                "key": f"request-{suffix}",
                "fingerprint": "d" * 64,
                "doc": f"doc_{suffix}",
                "version_id": f"kbv_{suffix}",
            },
        )


async def test_knowledge_schema_has_exact_columns_checks_fks_and_indexes(
    migrated_db: AsyncEngine,
) -> None:
    async with migrated_db.connect() as conn:
        revision = await conn.scalar(text("SELECT version_num FROM alembic_version"))
    assert revision == "0017_web_routing_isolation"

    for table, expected in _EXPECTED_COLUMNS.items():
        assert set(await _columns(migrated_db, table)) == expected

    chunk_columns = await _columns(migrated_db, "kb_chunks")
    assert chunk_columns["embedding"]["udt_name"] == "vector"
    assert chunk_columns["fts"]["udt_name"] == "tsvector"
    assert chunk_columns["fts"]["is_generated"] == "ALWAYS"
    assert "to_tsvector" in str(chunk_columns["fts"]["generation_expression"])
    assert chunk_columns["heading_path"]["is_nullable"] == "NO"
    assert chunk_columns["heading_path"]["column_default"] is not None
    assert chunk_columns["metadata"]["is_nullable"] == "NO"
    assert chunk_columns["metadata"]["column_default"] is not None
    version_columns = await _columns(migrated_db, "kb_document_versions")
    assert version_columns["target_chars"]["is_nullable"] == "NO"
    assert version_columns["overlap_chars"]["is_nullable"] == "NO"

    definitions = {
        table: "\n".join((await _constraint_definitions(migrated_db, table)).values())
        for table in _KNOWLEDGE_TABLES
    }
    assert "embedding_dim > 0" in definitions["knowledge_bases"]
    assert all(value in definitions["knowledge_bases"] for value in ("active", "deleted"))
    assert all(value in definitions["kb_documents"] for value in ("text", "markdown"))
    assert all(
        value in definitions["kb_documents"] for value in ("pending", "active", "failed", "deleted")
    )
    assert "version >= 1" in definitions["kb_document_versions"]
    assert "target_chars > 0" in definitions["kb_document_versions"]
    assert "overlap_chars >= 0" in definitions["kb_document_versions"]
    assert "overlap_chars < target_chars" in definitions["kb_document_versions"]
    assert all(
        value in definitions["kb_document_versions"]
        for value in (
            "pending",
            "indexing",
            "active",
            "superseded",
            "failed",
            "cancelled",
            "deleted",
            "purged",
        )
    )
    assert "ordinal >= 0" in definitions["kb_chunks"]
    assert "char_start >= 0" in definitions["kb_chunks"]
    assert "char_end >= char_start" in definitions["kb_chunks"]
    assert "dim > 0" in definitions["kb_chunks"]
    assert "char_length(request_fingerprint) = 64" in definitions["knowledge_idempotency"]
    assert all(
        value in definitions["knowledge_idempotency"]
        for value in (
            "create_base",
            "delete_base",
            "create_document",
            "update_document",
            "reindex_document",
            "delete_document",
            "base",
            "document",
        )
    )

    document_constraints = await _constraint_definitions(migrated_db, "kb_documents")
    assert (
        "FOREIGN KEY (scope_id, kb_id) REFERENCES knowledge_bases(scope_id, id)"
        in (definitions["kb_documents"])
    )
    assert document_constraints["fk_kb_documents_desired_version"] == (
        "FOREIGN KEY (scope_id, kb_id, id, desired_version_id) "
        "REFERENCES kb_document_versions(scope_id, kb_id, document_id, id)"
    )
    assert document_constraints["fk_kb_documents_active_version"] == (
        "FOREIGN KEY (scope_id, kb_id, id, active_version_id) "
        "REFERENCES kb_document_versions(scope_id, kb_id, document_id, id)"
    )
    assert (
        "FOREIGN KEY (scope_id, kb_id, document_id) REFERENCES kb_documents(scope_id, kb_id, id)"
    ) in definitions["kb_document_versions"]
    assert (
        "FOREIGN KEY (scope_id, kb_id, document_id, document_version_id) "
        "REFERENCES kb_document_versions(scope_id, kb_id, document_id, id)"
    ) in definitions["kb_chunks"]

    expected_indexes = {
        "knowledge_bases": {
            "ix_kb_scope_status",
            "uq_kb_scope_active_name",
        },
        "kb_documents": {"ix_kb_documents_scope_kb_status"},
        "kb_document_versions": {
            "ix_kb_versions_scope_document_status",
            "ix_kb_versions_scope_document_fingerprint",
        },
        "kb_chunks": {
            "ix_kb_chunks_scope_collection",
            "ix_kb_chunks_fts",
            "ix_kb_chunks_trgm",
        },
    }
    for table, names in expected_indexes.items():
        indexes = await _index_definitions(migrated_db, table)
        assert names <= indexes.keys()
    base_indexes = await _index_definitions(migrated_db, "knowledge_bases")
    assert "UNIQUE INDEX uq_kb_scope_active_name" in base_indexes["uq_kb_scope_active_name"]
    assert "WHERE (status = 'active'::text)" in base_indexes["uq_kb_scope_active_name"]
    chunk_indexes = await _index_definitions(migrated_db, "kb_chunks")
    assert "USING gin (fts)" in chunk_indexes["ix_kb_chunks_fts"]
    assert "USING gin (text gin_trgm_ops)" in chunk_indexes["ix_kb_chunks_trgm"]


@pytest.mark.parametrize(
    "assignment",
    [
        "target_chars = NULL",
        "overlap_chars = NULL",
        "target_chars = 0",
        "overlap_chars = -1",
        "overlap_chars = target_chars",
    ],
)
async def test_document_version_chunk_settings_are_database_enforced(
    migrated_db: AsyncEngine,
    assignment: str,
) -> None:
    await _seed_graph(migrated_db, scope="schema:chunk-settings", suffix="chunk_settings")

    with pytest.raises(IntegrityError):
        async with migrated_db.begin() as conn:
            await conn.execute(
                text(
                    f"UPDATE kb_document_versions SET {assignment} WHERE id = 'kbv_chunk_settings'"
                )
            )


async def test_jobs_cancel_mode_migration_defaults_existing_behavior_and_checks_values(
    migrated_db: AsyncEngine,
) -> None:
    columns = await _columns(migrated_db, "jobs")
    assert columns["cancel_mode"]["is_nullable"] == "NO"
    assert columns["cancel_mode"]["column_default"] == "'immediate'::text"

    async with migrated_db.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO jobs "
                "(id, scope_id, kind, idempotency_key, max_attempts) "
                "VALUES ('job_cancel_default', 'schema:jobs', 'test.echo', 'default', 3)"
            )
        )
        mode = await conn.scalar(
            text("SELECT cancel_mode FROM jobs WHERE id = 'job_cancel_default'")
        )
    assert mode == "immediate"

    with pytest.raises(IntegrityError):
        async with migrated_db.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO jobs "
                    "(id, scope_id, kind, idempotency_key, max_attempts, cancel_mode) "
                    "VALUES ('job_cancel_invalid', 'schema:jobs', 'test.echo', "
                    "'invalid', 3, 'unknown')"
                )
            )


async def test_active_knowledge_base_name_is_unique_only_within_active_scope(
    migrated_db: AsyncEngine,
) -> None:
    await _insert_base(
        migrated_db,
        kb_id="kb_name_active",
        scope_id="schema:name:a",
        name="Docs",
    )
    await _insert_base(
        migrated_db,
        kb_id="kb_name_deleted",
        scope_id="schema:name:a",
        name="Docs",
        status="deleted",
    )
    await _insert_base(
        migrated_db,
        kb_id="kb_name_other_scope",
        scope_id="schema:name:b",
        name="Docs",
    )

    with pytest.raises(IntegrityError):
        await _insert_base(
            migrated_db,
            kb_id="kb_name_duplicate",
            scope_id="schema:name:a",
            name="Docs",
        )


async def test_composite_foreign_keys_reject_same_scope_cross_kb_and_cross_document_links(
    migrated_db: AsyncEngine,
) -> None:
    await _seed_graph(migrated_db, scope="schema:fk", suffix="fk_a")
    await _seed_graph(migrated_db, scope="schema:fk", suffix="fk_b")
    async with migrated_db.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO kb_documents "
                "(id, scope_id, kb_id, title, source_type) "
                "VALUES ('doc_fk_a2', 'schema:fk', 'kb_fk_a', 'Second A', 'markdown')"
            )
        )
        await conn.execute(
            text(
                "INSERT INTO kb_document_versions "
                "(id, scope_id, kb_id, document_id, version, content, "
                "content_sha256, index_fingerprint, mime_type, chunking_version, "
                "target_chars, overlap_chars) "
                "VALUES ('kbv_fk_a2', 'schema:fk', 'kb_fk_a', 'doc_fk_a2', 1, "
                "'content', :content_hash, :fingerprint, 'text/markdown', "
                "'keel-char-v1', 1600, 200)"
            ),
            {"content_hash": "e" * 64, "fingerprint": "f" * 64},
        )

    with pytest.raises(IntegrityError):
        async with migrated_db.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO kb_document_versions "
                    "(id, scope_id, kb_id, document_id, version, content, "
                    "content_sha256, index_fingerprint, mime_type, chunking_version, "
                    "target_chars, overlap_chars) "
                    "VALUES ('kbv_wrong_kb', 'schema:fk', 'kb_fk_b', 'doc_fk_a', 2, "
                    "'content', :content_hash, :fingerprint, 'text/plain', "
                    "'keel-char-v1', 1600, 200)"
                ),
                {"content_hash": "1" * 64, "fingerprint": "2" * 64},
            )

    for column, version_id in (
        ("desired_version_id", "kbv_fk_a2"),
        ("active_version_id", "kbv_fk_b"),
    ):
        with pytest.raises(IntegrityError):
            async with migrated_db.begin() as conn:
                await conn.execute(
                    text(
                        f"UPDATE kb_documents SET {column} = :version_id "
                        "WHERE id = 'doc_fk_a' AND scope_id = 'schema:fk'"
                    ),
                    {"version_id": version_id},
                )

    invalid_chunks = (
        ("kbc_wrong_kb", "kb_fk_b", "doc_fk_a", "kbv_fk_a"),
        ("kbc_wrong_doc", "kb_fk_a", "doc_fk_a2", "kbv_fk_a"),
    )
    for chunk_id, kb_id, document_id, version_id in invalid_chunks:
        with pytest.raises(IntegrityError):
            async with migrated_db.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO kb_chunks "
                        "(id, scope_id, kb_id, document_id, document_version_id, "
                        "ordinal, text, char_start, char_end, content_hash, model, dim, "
                        "embedding) VALUES (:id, 'schema:fk', :kb, :doc, :version_id, "
                        "1, 'bad', 0, 3, :content_hash, 'test/embed', 2, "
                        "'[0.1,0.2]'::vector)"
                    ),
                    {
                        "id": chunk_id,
                        "kb": kb_id,
                        "doc": document_id,
                        "version_id": version_id,
                        "content_hash": "3" * 64,
                    },
                )


async def test_knowledge_rls_is_enabled_policy_complete_and_fails_closed(
    migrated_db: AsyncEngine,
) -> None:
    await _seed_graph(migrated_db, scope="schema:rls:a", suffix="rls_a")
    await _seed_graph(migrated_db, scope="schema:rls:b", suffix="rls_b")
    role = f"knowledge_rls_{uuid.uuid4().hex}"
    table_list = ", ".join(_KNOWLEDGE_TABLES)

    async with migrated_db.begin() as conn:
        await conn.execute(text(f'CREATE ROLE "{role}" NOSUPERUSER'))
        await conn.execute(text(f'GRANT SELECT ON {table_list} TO "{role}"'))

    try:
        async with migrated_db.connect() as conn:
            policies = (
                await conn.execute(
                    text(
                        "SELECT tablename, policyname, qual, with_check "
                        "FROM pg_policies WHERE schemaname = current_schema() "
                        "AND tablename = ANY(:tables)"
                    ),
                    {"tables": list(_KNOWLEDGE_TABLES)},
                )
            ).mappings()
            by_table = {str(row["tablename"]): row for row in policies}
            assert set(by_table) == set(_KNOWLEDGE_TABLES)
            for table in _KNOWLEDGE_TABLES:
                row = by_table[table]
                assert row["policyname"] == "scope_isolation"
                assert "current_setting('app.scope_id'::text, true)" in str(row["qual"])
                assert "current_setting('app.scope_id'::text, true)" in str(row["with_check"])

            await conn.execute(text(f'SET ROLE "{role}"'))
            await conn.execute(text("RESET app.scope_id"))
            for table in _KNOWLEDGE_TABLES:
                assert await conn.scalar(text(f"SELECT count(*) FROM {table}")) == 0

            await conn.execute(text("SELECT set_config('app.scope_id', 'schema:rls:a', false)"))
            for table in _KNOWLEDGE_TABLES:
                scopes = (
                    await conn.execute(text(f"SELECT DISTINCT scope_id FROM {table}"))
                ).scalars()
                assert scopes.all() == ["schema:rls:a"]
            await conn.execute(text("RESET ROLE"))
    finally:
        async with migrated_db.begin() as conn:
            await conn.execute(text(f'REVOKE ALL PRIVILEGES ON {table_list} FROM "{role}"'))
            await conn.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
