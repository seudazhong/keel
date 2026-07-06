"""baseline: enable required Postgres extensions

Revision ID: 0001_baseline
Revises:
Create Date: 2026-07-06

Enables pgvector (semantic search) and pg_trgm (CJK-safe trigram FTS) per
ADR-0002 / ARCHITECTURE §8.2. No tables yet — the event store and projections
land in M1 (WS-D).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0001_baseline"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")


def downgrade() -> None:
    op.execute("DROP EXTENSION IF EXISTS pg_trgm")
    op.execute("DROP EXTENSION IF EXISTS vector")
