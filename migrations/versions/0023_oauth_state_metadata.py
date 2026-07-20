"""persist provider-private OAuth authorization metadata

Revision ID: 0023_oauth_state_metadata
Revises: 0022_patch_generation_requests
Create Date: 2026-07-20
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0023_oauth_state_metadata"
down_revision: str | None = "0022_patch_generation_requests"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE oauth_states ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb")


def downgrade() -> None:
    op.execute("ALTER TABLE oauth_states DROP COLUMN IF EXISTS metadata")
