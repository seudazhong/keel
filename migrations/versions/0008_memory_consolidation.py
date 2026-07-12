"""memory consolidation: cursors, proposals, archival provenance.

Revision ID: 0008_memory_consolidation
Revises: 0007_message_embeddings
Create Date: 2026-07-12
"""

from __future__ import annotations

from alembic import op

revision = "0008_memory_consolidation"
down_revision = "0007_message_embeddings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE consolidation_cursors (
            scope_id text PRIMARY KEY,
            last_event_id bigint NOT NULL DEFAULT 0,
            last_run_at timestamptz,
            last_status text,
            lease_token text,
            lease_expires_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("ALTER TABLE consolidation_cursors ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON consolidation_cursors "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )

    op.execute(
        """
        CREATE TABLE memory_proposals (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            block text NOT NULL CHECK (block IN ('persona', 'human')),
            expected_version integer NOT NULL,
            proposed_value text NOT NULL,
            reason text NOT NULL,
            confidence double precision NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
            source_event_ids bigint[] NOT NULL,
            idempotency_key text NOT NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'applied', 'rejected', 'stale')),
            created_at timestamptz NOT NULL DEFAULT now(),
            resolved_at timestamptz,
            resolved_by text,
            UNIQUE (scope_id, idempotency_key)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_memory_proposals_scope_status ON memory_proposals (scope_id, status)"
    )
    op.execute("ALTER TABLE memory_proposals ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON memory_proposals "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )

    op.execute("ALTER TABLE archival ADD COLUMN origin text NOT NULL DEFAULT 'agent'")
    op.execute("ALTER TABLE archival ADD COLUMN content_hash text")
    op.execute("ALTER TABLE archival ADD COLUMN source_event_ids bigint[]")
    op.execute(
        "CREATE UNIQUE INDEX ix_archival_scope_content_hash "
        "ON archival (scope_id, content_hash) WHERE content_hash IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_archival_scope_content_hash")
    op.execute("ALTER TABLE archival DROP COLUMN IF EXISTS source_event_ids")
    op.execute("ALTER TABLE archival DROP COLUMN IF EXISTS content_hash")
    op.execute("ALTER TABLE archival DROP COLUMN IF EXISTS origin")
    op.execute("DROP TABLE IF EXISTS memory_proposals")
    op.execute("DROP TABLE IF EXISTS consolidation_cursors")
