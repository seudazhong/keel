"""durable background jobs.

Revision ID: 0009_background_jobs
Revises: 0008_memory_consolidation
Create Date: 2026-07-14
"""

from __future__ import annotations

from alembic import op

revision = "0009_background_jobs"
down_revision = "0008_memory_consolidation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE jobs (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            kind text NOT NULL,
            status text NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            target_session_id text,
            idempotency_key text NOT NULL,
            attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
            max_attempts integer NOT NULL CHECK (max_attempts >= 1),
            next_attempt_at timestamptz NOT NULL DEFAULT now(),
            lease_token text,
            lease_expires_at timestamptz,
            heartbeat_at timestamptz,
            cancel_requested_at timestamptz,
            progress_current bigint NOT NULL DEFAULT 0 CHECK (progress_current >= 0),
            progress_total bigint CHECK (progress_total IS NULL OR progress_total >= 0),
            progress_message text,
            progress_updated_at timestamptz,
            result jsonb,
            result_message text,
            error_kind text,
            error_message text,
            injected_event_seq bigint,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz,
            finished_at timestamptz,
            UNIQUE (scope_id, kind, idempotency_key)
        )
        """
    )
    op.execute("CREATE INDEX ix_jobs_dispatch ON jobs (scope_id, status, next_attempt_at)")
    op.execute(
        "CREATE INDEX ix_jobs_lease_expiry ON jobs (scope_id, lease_expires_at) "
        "WHERE status = 'running'"
    )
    op.execute(
        "CREATE INDEX ix_jobs_target_session ON jobs (scope_id, target_session_id) "
        "WHERE target_session_id IS NOT NULL"
    )
    op.execute("ALTER TABLE jobs ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON jobs "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS jobs")
