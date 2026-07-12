"""semantic session recall: message embedding projection

Revision ID: 0007_message_embeddings
Revises: 0006_session_search_idx
Create Date: 2026-07-12
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0007_message_embeddings"
down_revision: str | None = "0006_session_search_idx"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE message_embeddings (
            event_id bigint NOT NULL REFERENCES events(id) ON DELETE CASCADE,
            scope_id text NOT NULL,
            session_id text NOT NULL,
            seq bigint NOT NULL,
            role text NOT NULL CHECK (role IN ('user', 'assistant')),
            content text NOT NULL,
            model text NOT NULL,
            dim int NOT NULL,
            embedding vector NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (event_id, model, dim)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_message_embeddings_scope_session "
        "ON message_embeddings (scope_id, session_id)"
    )
    op.execute(
        "CREATE INDEX ix_message_embeddings_scope_model_dim "
        "ON message_embeddings (scope_id, model, dim)"
    )
    op.execute("ALTER TABLE message_embeddings ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON message_embeddings "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS message_embeddings CASCADE")
