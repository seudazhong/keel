"""shared connector bindings, resources, cursors, and delivery replay ledger.

Revision ID: 0015_connector_foundation
Revises: 0014_durable_runs
Create Date: 2026-07-18
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0015_connector_foundation"
down_revision: str | None = "0014_durable_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCOPED_TABLES = (
    "connector_bindings",
    "connector_resources",
    "connector_cursors",
    "connector_deliveries",
)
_NO_SECRET_KEYS = (
    "secret",
    "token",
    "password",
    "client_secret",
    "private_key",
    "api_key",
    "access_token",
    "refresh_token",
)


def _metadata_check(column: str) -> str:
    keys = ",".join(f"'{key}'" for key in _NO_SECRET_KEYS)
    return f"CHECK (NOT ({column} ?| ARRAY[{keys}]))"


def upgrade() -> None:
    op.execute(
        f"""
        CREATE TABLE connector_bindings (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            connector_id text NOT NULL,
            status text NOT NULL
                CHECK (status IN ('configured', 'connected', 'error', 'revoked')),
            display_name text,
            external_account_id text,
            external_tenant_id text,
            metadata jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            last_success_at timestamptz,
            error_code text,
            error_summary text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (scope_id, connector_id),
            UNIQUE (scope_id, id),
            {_metadata_check("metadata")}
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_connector_bindings_status "
        "ON connector_bindings (scope_id, status, updated_at)"
    )

    op.execute(
        f"""
        CREATE TABLE connector_resources (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            connector_id text NOT NULL,
            binding_id text NOT NULL,
            external_id text NOT NULL,
            kind text NOT NULL,
            display_name text NOT NULL,
            url text,
            selected boolean NOT NULL DEFAULT false,
            config jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (scope_id, id),
            UNIQUE (scope_id, binding_id, external_id),
            FOREIGN KEY (scope_id, binding_id)
                REFERENCES connector_bindings (scope_id, id) ON DELETE CASCADE,
            {_metadata_check("config")}
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_connector_resources_selected "
        "ON connector_resources (scope_id, connector_id, binding_id, selected)"
    )

    op.execute(
        """
        CREATE TABLE connector_cursors (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            connector_id text NOT NULL,
            binding_id text NOT NULL,
            resource_id text,
            resource_key text NOT NULL DEFAULT '',
            stream text NOT NULL DEFAULT 'default',
            cursor_value text NOT NULL,
            etag text,
            last_modified text,
            revision text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (scope_id, binding_id, resource_key, stream),
            FOREIGN KEY (scope_id, binding_id)
                REFERENCES connector_bindings (scope_id, id) ON DELETE CASCADE,
            FOREIGN KEY (scope_id, resource_id)
                REFERENCES connector_resources (scope_id, id) ON DELETE CASCADE,
            CHECK ((resource_id IS NULL AND resource_key = '')
                OR (resource_id IS NOT NULL AND resource_key = resource_id))
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_connector_cursors_lookup "
        "ON connector_cursors (scope_id, connector_id, binding_id, stream)"
    )

    op.execute(
        """
        CREATE TABLE connector_deliveries (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            connector_id text NOT NULL,
            binding_id text NOT NULL,
            delivery_id text NOT NULL,
            payload_hash text NOT NULL,
            status text NOT NULL DEFAULT 'received'
                CHECK (status IN ('received', 'processing', 'processed', 'failed')),
            event_id text,
            error_code text,
            error_summary text,
            received_at timestamptz NOT NULL DEFAULT now(),
            processed_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (scope_id, connector_id, delivery_id),
            FOREIGN KEY (scope_id, binding_id)
                REFERENCES connector_bindings (scope_id, id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_connector_deliveries_status "
        "ON connector_deliveries (scope_id, connector_id, status, received_at)"
    )
    op.execute(
        "CREATE INDEX ix_connector_deliveries_payload_hash "
        "ON connector_deliveries (scope_id, connector_id, payload_hash)"
    )

    # Existing encrypted Gmail (and any early connector) token rows become bindings without
    # decrypting or rewriting credentials. The deterministic id keeps retries/idempotent
    # migration replays stable. connector_tokens already has FORCE RLS; temporarily relax it
    # inside this migration transaction so the table owner can backfill every scope, then
    # restore FORCE before commit.
    op.execute("ALTER TABLE connector_tokens NO FORCE ROW LEVEL SECURITY")
    op.execute(
        """
        INSERT INTO connector_bindings
            (id, scope_id, connector_id, status, display_name, created_at, updated_at)
        SELECT
            'legacy-' || md5(scope_id || ':' || connector_id),
            scope_id,
            connector_id,
            'connected',
            connector_id,
            created_at,
            updated_at
        FROM connector_tokens
        ON CONFLICT (scope_id, connector_id) DO NOTHING
        """
    )
    op.execute("ALTER TABLE connector_tokens FORCE ROW LEVEL SECURITY")

    for table in _SCOPED_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY scope_isolation ON {table} "
            "USING (scope_id = current_setting('app.scope_id', true)) "
            "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
        )
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE
                        ON connector_bindings, connector_resources,
                           connector_cursors, connector_deliveries
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime connector grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS connector_deliveries CASCADE")
    op.execute("DROP TABLE IF EXISTS connector_cursors CASCADE")
    op.execute("DROP TABLE IF EXISTS connector_resources CASCADE")
    op.execute("DROP TABLE IF EXISTS connector_bindings CASCADE")
