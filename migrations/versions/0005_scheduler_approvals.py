"""scheduler + durable approvals (scoped + RLS)

Revision ID: 0005_scheduler_approvals
Revises: 0004_archival
Create Date: 2026-07-07

M2 autonomy slice: persistent ``schedules`` (an at-most-once cursor in ``next_run_at``)
and durable tool ``approvals`` (G5 — cross-surface suspend/resume). Both carry
``scope_id`` and have Row-Level Security keyed by the ``app.scope_id`` GUC (G16), behind
the application-layer scope binding — mirroring ``connector_tokens`` (0003).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0005_scheduler_approvals"
down_revision: str | None = "0004_archival"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE schedules (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            agent_id text NOT NULL,
            session_id text NOT NULL,
            trigger_kind text NOT NULL,
            spec text NOT NULL,
            next_run_at timestamptz NOT NULL,
            interval_s integer NOT NULL DEFAULT 0,
            enabled boolean NOT NULL DEFAULT true,
            last_run_at timestamptz,
            last_status text
        )
        """
    )
    op.execute("CREATE INDEX ix_schedules_due ON schedules (enabled, next_run_at)")
    op.execute("ALTER TABLE schedules ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON schedules "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )

    op.execute(
        """
        CREATE TABLE approvals (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            run_id text NOT NULL,
            session_id text NOT NULL,
            tool text NOT NULL,
            args jsonb NOT NULL,
            call_id text NOT NULL,
            idempotency_key text NOT NULL,
            reason text NOT NULL,
            status text NOT NULL DEFAULT 'pending',
            created_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz NOT NULL,
            resolved_at timestamptz,
            resolved_by text
        )
        """
    )
    op.execute("CREATE INDEX ix_approvals_scope_status ON approvals (scope_id, status)")
    op.execute("ALTER TABLE approvals ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON approvals "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS approvals CASCADE")
    op.execute("DROP TABLE IF EXISTS schedules CASCADE")
