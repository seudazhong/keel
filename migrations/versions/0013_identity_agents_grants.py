"""identity: durable users, organizations, memberships, persisted Agents, resource grants.

Revision ID: 0013_identity_agents_grants
Revises: 0012_data_lifecycle
Create Date: 2026-07-17

M3.6 identity / persisted Agent / grants foundation (WS-L). Durable, multi-tenant
identity primitives that later durable-run integration binds against:

* ``users`` — global human identity (one row per person). Soft-deletable. A user may
  belong to many organizations, so this table is deliberately **not** org-partitioned.
* ``oidc_identities`` — global ``(issuer, subject) -> user`` links from an external OIDC
  provider, used for JIT provisioning / explicit account linking. Also global.
* ``organizations`` — the tenant root. Global lookup table (a user must be able to list
  the orgs it belongs to), archivable rather than hard-deleted.
* ``memberships`` — the user<->org RBAC edge (owner/admin/member/viewer). **Tenant-owned**
  (carries ``org_id``) and RLS-protected.
* ``agents`` — persisted personal/team Agents owned by a user inside an org. Tenant-owned,
  RLS-protected, optimistic-concurrency ``version``.
* ``resource_grants`` — explicit ``(org, agent, resource_type, resource_id, capability)``
  grants. Tenant-owned, RLS-protected. A composite FK to ``agents(id, org_id)`` makes a
  cross-org grant structurally impossible (confused-deputy defense).

Row-Level Security (ADR-0009 / DESIGN-REVIEW G16): the three tenant-owned tables carry a
policy keyed by the ``app.org_id`` GUC (mirroring the ``app.scope_id`` pattern of the
event core). ``memberships`` additionally permits a data-subject self-read keyed by
``app.user_id`` so a user can enumerate its own memberships across orgs before an org is
selected; writes are still confined to the operating org. ``FORCE ROW LEVEL SECURITY`` +
the non-owner, non-bypass ``keel_runtime`` role (created in 0011) keep the owner connection
subject to the policy in production. Grants are (re)applied here guarded so a managed
Postgres that forbids GRANT does not fail the migration.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0013_identity_agents_grants"
down_revision: str | None = "0012_data_lifecycle"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Tenant-owned tables that get RLS + FORCE RLS + runtime grants. Every access path sets
# ``app.org_id`` (and, for the membership self-read, ``app.user_id``).
_ORG_TABLES = ("memberships", "agents", "resource_grants")


def upgrade() -> None:
    # --- Global human identity -------------------------------------------------------
    op.execute(
        """
        CREATE TABLE users (
            id text PRIMARY KEY,
            email text,
            display_name text NOT NULL,
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'suspended', 'deleted')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            deleted_at timestamptz,
            CHECK (email IS NULL OR email = lower(email))
        )
        """
    )
    # Email is unique among live (non-deleted) users only, so an erased address can be
    # re-provisioned later without colliding with the tombstoned row.
    op.execute(
        "CREATE UNIQUE INDEX ux_users_email_live ON users (email) "
        "WHERE email IS NOT NULL AND deleted_at IS NULL"
    )

    # --- Global external OIDC identity links -----------------------------------------
    op.execute(
        """
        CREATE TABLE oidc_identities (
            id text PRIMARY KEY,
            user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            issuer text NOT NULL,
            subject text NOT NULL,
            email text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            last_login_at timestamptz,
            UNIQUE (issuer, subject)
        )
        """
    )
    op.execute("CREATE INDEX ix_oidc_identities_user ON oidc_identities (user_id)")

    # --- Tenant root (global lookup) -------------------------------------------------
    op.execute(
        """
        CREATE TABLE organizations (
            id text PRIMARY KEY,
            slug text NOT NULL UNIQUE,
            display_name text NOT NULL,
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'archived')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            archived_at timestamptz,
            UNIQUE (id, status)
        )
        """
    )

    # --- Membership / RBAC edge (tenant-owned) ---------------------------------------
    op.execute(
        """
        CREATE TABLE memberships (
            id text PRIMARY KEY,
            org_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            role text NOT NULL
                CHECK (role IN ('owner', 'admin', 'member', 'viewer')),
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'revoked')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            revoked_at timestamptz,
            UNIQUE (org_id, user_id)
        )
        """
    )
    op.execute("CREATE INDEX ix_memberships_user ON memberships (user_id)")
    op.execute("CREATE INDEX ix_memberships_org_role ON memberships (org_id, role)")
    # At least one active owner per org: a partial unique-less guard is enforced in the
    # application (last-owner protection); this partial index accelerates the owner count.
    op.execute(
        "CREATE INDEX ix_memberships_active_owner ON memberships (org_id) "
        "WHERE role = 'owner' AND status = 'active'"
    )

    # --- Persisted Agents (tenant-owned) ---------------------------------------------
    op.execute(
        """
        CREATE TABLE agents (
            id text PRIMARY KEY,
            org_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            kind text NOT NULL CHECK (kind IN ('personal', 'team')),
            owner_user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            name text NOT NULL,
            persona text NOT NULL DEFAULT '',
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'archived')),
            version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            archived_at timestamptz,
            UNIQUE (id, org_id)
        )
        """
    )
    # A live Agent name is unique within an org (archived rows free the name).
    op.execute(
        "CREATE UNIQUE INDEX ux_agents_org_name_live ON agents (org_id, lower(name)) "
        "WHERE status = 'active'"
    )
    op.execute("CREATE INDEX ix_agents_owner ON agents (org_id, owner_user_id)")

    # --- Explicit resource grants (tenant-owned) -------------------------------------
    op.execute(
        """
        CREATE TABLE resource_grants (
            id text PRIMARY KEY,
            org_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            agent_id text NOT NULL,
            resource_type text NOT NULL,
            resource_id text NOT NULL,
            capability text NOT NULL
                CHECK (capability IN ('read', 'use', 'write', 'manage')),
            grantor_user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'revoked')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            revoked_at timestamptz,
            -- Composite FK pins the grant's agent to the SAME org: a cross-org grant
            -- (agent from org A, grant row claiming org B) cannot be inserted.
            FOREIGN KEY (agent_id, org_id) REFERENCES agents(id, org_id) ON DELETE CASCADE,
            UNIQUE (org_id, agent_id, resource_type, resource_id, capability)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_resource_grants_lookup "
        "ON resource_grants (org_id, resource_type, resource_id)"
    )
    op.execute("CREATE INDEX ix_resource_grants_agent ON resource_grants (org_id, agent_id)")

    # --- Row-Level Security on the tenant-owned tables -------------------------------
    for table in _ORG_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")

    # agents / resource_grants: strict single-org isolation.
    for table in ("agents", "resource_grants"):
        op.execute(
            f"CREATE POLICY org_isolation ON {table} "
            "USING (org_id = current_setting('app.org_id', true)) "
            "WITH CHECK (org_id = current_setting('app.org_id', true))"
        )

    # memberships: org isolation, plus a data-subject self-read (list my own orgs). Writes
    # remain confined to the operating org (WITH CHECK omits the self-read branch).
    op.execute(
        "CREATE POLICY org_isolation ON memberships "
        "USING ("
        "  org_id = current_setting('app.org_id', true) "
        "  OR user_id = current_setting('app.user_id', true)"
        ") "
        "WITH CHECK (org_id = current_setting('app.org_id', true))"
    )

    for table in _ORG_TABLES:
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")

    # --- (Re)apply non-owner runtime grants for the new tables (guarded) -------------
    # 0011 set ALTER DEFAULT PRIVILEGES so owner-created tables inherit these grants, but
    # applying them explicitly is idempotent and robust to a differing table owner.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        users, oidc_identities, organizations,
                        memberships, agents, resource_grants
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime identity grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    for table in reversed(_ORG_TABLES):
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
    op.execute("DROP TABLE IF EXISTS resource_grants CASCADE")
    op.execute("DROP TABLE IF EXISTS agents CASCADE")
    op.execute("DROP TABLE IF EXISTS memberships CASCADE")
    op.execute("DROP TABLE IF EXISTS organizations CASCADE")
    op.execute("DROP TABLE IF EXISTS oidc_identities CASCADE")
    op.execute("DROP TABLE IF EXISTS users CASCADE")
