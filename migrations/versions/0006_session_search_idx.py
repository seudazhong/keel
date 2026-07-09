"""events message trigram index for session search

Revision ID: 0006_session_search_idx
Revises: 0005_scheduler_approvals
Create Date: 2026-07-09

A partial GIN trigram index on ``message.token`` text so ``session_search`` (pg_trgm
similarity, CJK-safe) stays fast as the event log grows. The lexical arm also blends a
``tsvector`` FTS rank at query time (no stored column needed for the ``simple`` config).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0006_session_search_idx"
down_revision: str | None = "0005_scheduler_approvals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX ix_events_msg_trgm ON events "
        "USING gin ((payload->>'text') gin_trgm_ops) "
        "WHERE type = 'message.token'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_events_msg_trgm")
