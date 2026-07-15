"""knowledge base schema and durable-job cancellation modes.

Revision ID: 0010_knowledge_base
Revises: 0009_background_jobs
Create Date: 2026-07-15
"""

from __future__ import annotations

from alembic import op

revision = "0010_knowledge_base"
down_revision = "0009_background_jobs"
branch_labels = None
depends_on = None

_KNOWLEDGE_TABLES = (
    "knowledge_bases",
    "kb_documents",
    "kb_document_versions",
    "kb_chunks",
    "knowledge_idempotency",
)


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE jobs
        ADD COLUMN cancel_mode text NOT NULL DEFAULT 'immediate'
            CHECK (cancel_mode IN ('immediate', 'cooperative', 'disabled'))
        """
    )

    op.execute(
        """
        CREATE TABLE knowledge_bases (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            name text NOT NULL,
            description text,
            embedding_model text NOT NULL,
            embedding_dim integer NOT NULL CHECK (embedding_dim > 0),
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'deleted')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            deleted_at timestamptz,
            UNIQUE (scope_id, id)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE kb_documents (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            kb_id text NOT NULL,
            title text NOT NULL,
            source_type text NOT NULL
                CHECK (source_type IN ('text', 'markdown')),
            source_uri text,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'active', 'failed', 'deleted')),
            desired_version_id text,
            active_version_id text,
            last_error_kind text,
            last_error_message text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            deleted_at timestamptz,
            UNIQUE (scope_id, id),
            UNIQUE (scope_id, kb_id, id),
            FOREIGN KEY (scope_id, kb_id)
                REFERENCES knowledge_bases(scope_id, id)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE kb_document_versions (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            kb_id text NOT NULL,
            document_id text NOT NULL,
            version integer NOT NULL CHECK (version >= 1),
            content text,
            content_sha256 text NOT NULL,
            index_fingerprint text NOT NULL,
            mime_type text NOT NULL,
            chunking_version text NOT NULL,
            ingest_job_id text,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN (
                    'pending', 'indexing', 'active', 'superseded',
                    'failed', 'cancelled', 'deleted', 'purged'
                )),
            error_kind text,
            error_message text,
            created_at timestamptz NOT NULL DEFAULT now(),
            activated_at timestamptz,
            deleted_at timestamptz,
            purged_at timestamptz,
            UNIQUE (scope_id, id),
            UNIQUE (scope_id, kb_id, document_id, id),
            UNIQUE (scope_id, document_id, version),
            FOREIGN KEY (scope_id, kb_id, document_id)
                REFERENCES kb_documents(scope_id, kb_id, id)
        )
        """
    )

    op.execute(
        """
        ALTER TABLE kb_documents
        ADD CONSTRAINT fk_kb_documents_desired_version
        FOREIGN KEY (scope_id, kb_id, id, desired_version_id)
        REFERENCES kb_document_versions(scope_id, kb_id, document_id, id)
        """
    )
    op.execute(
        """
        ALTER TABLE kb_documents
        ADD CONSTRAINT fk_kb_documents_active_version
        FOREIGN KEY (scope_id, kb_id, id, active_version_id)
        REFERENCES kb_document_versions(scope_id, kb_id, document_id, id)
        """
    )

    op.execute(
        """
        CREATE TABLE kb_chunks (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            kb_id text NOT NULL,
            document_id text NOT NULL,
            document_version_id text NOT NULL,
            ordinal integer NOT NULL CHECK (ordinal >= 0),
            text text NOT NULL,
            char_start integer NOT NULL CHECK (char_start >= 0),
            char_end integer NOT NULL CHECK (char_end >= char_start),
            content_hash text NOT NULL,
            heading_path jsonb NOT NULL DEFAULT '[]'::jsonb,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            model text NOT NULL,
            dim integer NOT NULL CHECK (dim > 0),
            embedding vector NOT NULL,
            fts tsvector GENERATED ALWAYS AS
                (to_tsvector('simple', text)) STORED,
            created_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (scope_id, id),
            UNIQUE (scope_id, document_version_id, ordinal),
            FOREIGN KEY (
                scope_id, kb_id, document_id, document_version_id
            ) REFERENCES kb_document_versions(
                scope_id, kb_id, document_id, id
            )
        )
        """
    )

    op.execute(
        """
        CREATE TABLE knowledge_idempotency (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            operation text NOT NULL CHECK (operation IN (
                'create_base', 'delete_base', 'create_document',
                'update_document', 'reindex_document', 'delete_document'
            )),
            idempotency_key text NOT NULL,
            request_fingerprint text NOT NULL
                CHECK (char_length(request_fingerprint) = 64),
            resource_kind text NOT NULL
                CHECK (resource_kind IN ('base', 'document')),
            resource_id text NOT NULL,
            document_version_id text,
            job_id text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (scope_id, operation, idempotency_key)
        )
        """
    )

    op.execute("CREATE INDEX ix_kb_scope_status ON knowledge_bases (scope_id, status)")
    op.execute(
        "CREATE UNIQUE INDEX uq_kb_scope_active_name "
        "ON knowledge_bases (scope_id, name) WHERE status = 'active'"
    )
    op.execute(
        "CREATE INDEX ix_kb_documents_scope_kb_status ON kb_documents (scope_id, kb_id, status)"
    )
    op.execute(
        "CREATE INDEX ix_kb_versions_scope_document_status "
        "ON kb_document_versions (scope_id, document_id, status)"
    )
    op.execute(
        "CREATE INDEX ix_kb_versions_scope_document_fingerprint "
        "ON kb_document_versions (scope_id, document_id, index_fingerprint)"
    )
    op.execute(
        "CREATE INDEX ix_kb_chunks_scope_collection ON kb_chunks (scope_id, kb_id, model, dim)"
    )
    op.execute("CREATE INDEX ix_kb_chunks_fts ON kb_chunks USING gin (fts)")
    op.execute("CREATE INDEX ix_kb_chunks_trgm ON kb_chunks USING gin (text gin_trgm_ops)")

    for table in _KNOWLEDGE_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY scope_isolation ON {table} "
            "USING (scope_id = current_setting('app.scope_id', true)) "
            "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
        )


def downgrade() -> None:
    op.execute("ALTER TABLE kb_documents DROP CONSTRAINT IF EXISTS fk_kb_documents_active_version")
    op.execute("ALTER TABLE kb_documents DROP CONSTRAINT IF EXISTS fk_kb_documents_desired_version")
    op.execute("DROP TABLE IF EXISTS kb_chunks")
    op.execute("DROP TABLE IF EXISTS knowledge_idempotency")
    op.execute("DROP TABLE IF EXISTS kb_document_versions")
    op.execute("DROP TABLE IF EXISTS kb_documents")
    op.execute("DROP TABLE IF EXISTS knowledge_bases")
    op.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS cancel_mode")
