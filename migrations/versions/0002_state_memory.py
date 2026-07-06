"""state + memory schema: sessions, events, memory blocks (scoped + RLS)

Revision ID: 0002_state_memory
Revises: 0001_baseline
Create Date: 2026-07-06

Event-sourced state and editable memory (WS-D). Every scoped table carries
``scope_id`` and has Row-Level Security keyed by the ``app.scope_id`` GUC
(ADR-0009 / DESIGN-REVIEW G16, defense-in-depth behind the app-layer scope guard).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002_state_memory"
down_revision: str | None = "0001_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCOPED_TABLES = ("sessions", "events", "memory_blocks", "memory_block_versions")


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE sessions (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            title text,
            next_seq bigint NOT NULL DEFAULT 1,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ix_sessions_scope ON sessions (scope_id)")

    op.execute(
        """
        CREATE TABLE events (
            id bigserial PRIMARY KEY,
            session_id text NOT NULL,
            scope_id text NOT NULL,
            seq bigint NOT NULL,
            type text NOT NULL,
            version int NOT NULL DEFAULT 1,
            run_id text,
            ts timestamptz NOT NULL,
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE (session_id, seq)
        )
        """
    )
    op.execute("CREATE INDEX ix_events_session_seq ON events (session_id, seq)")
    op.execute("CREATE INDEX ix_events_scope ON events (scope_id)")

    op.execute(
        """
        CREATE TABLE memory_blocks (
            id bigserial PRIMARY KEY,
            scope_id text NOT NULL,
            key text NOT NULL,
            value text NOT NULL DEFAULT '',
            version int NOT NULL DEFAULT 1,
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (scope_id, key)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE memory_block_versions (
            id bigserial PRIMARY KEY,
            scope_id text NOT NULL,
            key text NOT NULL,
            version int NOT NULL,
            value text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_memblock_versions ON memory_block_versions (scope_id, key, version)"
    )

    # Row-Level Security: a session sees only rows for its app.scope_id GUC.
    for table in _SCOPED_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY scope_isolation ON {table} "
            "USING (scope_id = current_setting('app.scope_id', true)) "
            "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
        )


def downgrade() -> None:
    for table in reversed(_SCOPED_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
