"""connectors: encrypted OAuth token store (scoped + RLS)

Revision ID: 0003_connectors
Revises: 0002_state_memory
Create Date: 2026-07-07

Connector tokens are envelope-encrypted and keyed by ``(scope_id, connector_id)``
(WS-G / DESIGN-REVIEW G18). The table carries ``scope_id`` and has Row-Level
Security keyed by the ``app.scope_id`` GUC (G16, defense-in-depth behind the
application-layer scope binding). Only ciphertext is ever stored.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003_connectors"
down_revision: str | None = "0002_state_memory"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE connector_tokens (
            scope_id text NOT NULL,
            connector_id text NOT NULL,
            ciphertext text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (scope_id, connector_id)
        )
        """
    )
    op.execute("ALTER TABLE connector_tokens ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON connector_tokens "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS connector_tokens CASCADE")
