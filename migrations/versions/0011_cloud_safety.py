"""cloud-safety: durable oauth state, webhook replay, outbound idempotency, key versions.

Revision ID: 0011_cloud_safety
Revises: 0010_knowledge_base
Create Date: 2026-07-16

M3.3 cloud-safety controls (WS-E/G/J):

* ``oauth_states`` — durable, expiring, one-time OAuth CSRF ``state`` (survives restart /
  a second replica). Keyed by the random state (itself the capability), so not
  scope-partitioned.
* ``webhook_deliveries`` — durable IM webhook replay records; the ``(provider,
  delivery_id)`` unique key drops a replayed delivery. Global (the gateway is not
  scope-bound).
* ``connector_outbox`` — durable at-most-once claim for outbound connector actions,
  scope-bound + RLS like ``connector_tokens``.
* ``connector_tokens.key_id`` — records which envelope key encrypted each token so
  versioned decrypt / online key rotation is safe.

Runtime DB role strategy: a **non-owner, non-bypass** ``keel_runtime`` role (deployment-safe:
skipped where role creation is restricted, e.g. managed Postgres), plus
``FORCE ROW LEVEL SECURITY`` on the scope-bound token/outbox tables so even the table owner
is subject to the scope policy (defense-in-depth). Grants are applied only when the role
exists.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0011_cloud_safety"
down_revision: str | None = "0010_knowledge_base"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Scope-bound tables that get FORCE RLS + runtime grants in this migration. Restricted to
# tables whose access path always sets ``app.scope_id`` (connector token/outbox stores),
# so forcing RLS on the owner connection can never hide a legitimately cross-scope scan.
_SCOPED_TABLES = ("connector_tokens", "connector_outbox")


def upgrade() -> None:
    # --- Durable OAuth CSRF state (one-time, expiring) --------------------------------
    op.execute(
        """
        CREATE TABLE oauth_states (
            state text PRIMARY KEY,
            scope_id text NOT NULL,
            connector_id text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz NOT NULL
        )
        """
    )
    op.execute("CREATE INDEX ix_oauth_states_expiry ON oauth_states (expires_at)")

    # --- Durable webhook replay/delivery records --------------------------------------
    op.execute(
        """
        CREATE TABLE webhook_deliveries (
            provider text NOT NULL,
            delivery_id text NOT NULL,
            received_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz NOT NULL,
            PRIMARY KEY (provider, delivery_id)
        )
        """
    )
    op.execute("CREATE INDEX ix_webhook_deliveries_expiry ON webhook_deliveries (expires_at)")

    # --- Durable outbound-connector idempotency (scope-bound + RLS) --------------------
    op.execute(
        """
        CREATE TABLE connector_outbox (
            scope_id text NOT NULL,
            connector_id text NOT NULL,
            idempotency_key text NOT NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'done')),
            result text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (scope_id, connector_id, idempotency_key)
        )
        """
    )
    op.execute("ALTER TABLE connector_outbox ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON connector_outbox "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )

    # --- Key-version metadata for envelope rotation -----------------------------------
    op.execute("ALTER TABLE connector_tokens ADD COLUMN key_id text NOT NULL DEFAULT 'v1'")

    # --- FORCE RLS so even the table owner is subject to the scope policy --------------
    for table in _SCOPED_TABLES:
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")

    # --- Non-owner, non-bypass runtime role (deployment-safe) -------------------------
    # Wrapped so a managed Postgres that forbids CREATE ROLE / GRANT does not fail the
    # migration; operators grant these manually there. The role is NOLOGIN (a group role
    # the login user is granted into) and NOBYPASSRLS so it can never see across scopes.
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    EXECUTE 'CREATE ROLE keel_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS';
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime not created (insufficient privilege)';
                END;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT USAGE ON SCHEMA public TO keel_runtime;
                    GRANT SELECT, INSERT, UPDATE, DELETE
                        ON ALL TABLES IN SCHEMA public TO keel_runtime;
                    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO keel_runtime;
                    ALTER DEFAULT PRIVILEGES IN SCHEMA public
                        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO keel_runtime;
                    ALTER DEFAULT PRIVILEGES IN SCHEMA public
                        GRANT USAGE, SELECT ON SEQUENCES TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    for table in _SCOPED_TABLES:
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE connector_tokens DROP COLUMN IF EXISTS key_id")
    op.execute("DROP TABLE IF EXISTS connector_outbox CASCADE")
    op.execute("DROP TABLE IF EXISTS webhook_deliveries CASCADE")
    op.execute("DROP TABLE IF EXISTS oauth_states CASCADE")
    # The keel_runtime role is intentionally left in place: other databases/tables may
    # depend on it, and dropping a role that still owns grants would fail. Operators drop
    # it manually if desired.
