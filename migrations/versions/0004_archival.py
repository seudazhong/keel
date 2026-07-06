"""search: archival memory with hybrid FTS + vector retrieval (scoped + RLS)

Revision ID: 0004_archival
Revises: 0003_connectors
Create Date: 2026-07-07

Archival passages (WS-D) for ``archival_search``: hybrid retrieval combines
``pg_trgm`` (CJK-safe lexical) + ``tsvector`` FTS with pgvector KNN, fused by RRF.
The ``embedding`` column is an unconstrained ``vector`` so any pinned model's
dimension fits; ``(model, dim)`` are recorded so a search only compares embeddings
produced by the same pinned model (ADR-0007). Scoped + RLS like every other table.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004_archival"
down_revision: str | None = "0003_connectors"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE archival (
            id bigserial PRIMARY KEY,
            scope_id text NOT NULL,
            content text NOT NULL,
            model text NOT NULL,
            dim int NOT NULL,
            embedding vector NOT NULL,
            fts tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ix_archival_scope ON archival (scope_id)")
    op.execute("CREATE INDEX ix_archival_fts ON archival USING gin (fts)")
    op.execute("CREATE INDEX ix_archival_trgm ON archival USING gin (content gin_trgm_ops)")

    op.execute("ALTER TABLE archival ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON archival "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS archival CASCADE")
