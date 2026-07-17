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
event core). ``memberships`` additionally layers a **SELECT-only** data-subject self-read
keyed by ``app.user_id`` so a user can enumerate its own memberships across orgs before an
org is selected; that self-read never applies to UPDATE/DELETE, so mutations always require
the correct operating ``app.org_id``. ``FORCE ROW LEVEL SECURITY`` +
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

    # memberships: strict org isolation for EVERY command (SELECT/INSERT/UPDATE/DELETE) —
    # a mutation is only ever visible/allowed inside the operating org.
    op.execute(
        "CREATE POLICY org_isolation ON memberships "
        "USING (org_id = current_setting('app.org_id', true)) "
        "WITH CHECK (org_id = current_setting('app.org_id', true))"
    )
    # A data-subject self-read (list my own orgs) is layered as a **SELECT-only**
    # permissive policy. Permissive policies are OR-combined per command, so this widens
    # only reads to the caller's own membership rows; it deliberately does NOT apply to
    # UPDATE/DELETE, so a caller holding just ``app.user_id`` (no operating ``app.org_id``)
    # can never mutate a cross-org membership row — mutations still require the correct
    # ``app.org_id`` above (defense-in-depth behind the service's authorization).
    op.execute(
        "CREATE POLICY self_read ON memberships FOR SELECT "
        "USING ("
        "  org_id = current_setting('app.org_id', true) "
        "  OR user_id = current_setting('app.user_id', true)"
        ")"
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

    # --- Maintenance-only erasure (works under keel_runtime without RLS bypass) -------
    # User erasure is inherently CROSS-tenant (a user belongs to many orgs) and must delete
    # the global ``users`` row (whose ON DELETE CASCADE would otherwise silently orphan an
    # active org). Under production RLS (``keel_runtime`` is NOBYPASSRLS + FORCE RLS), a plain
    # cross-org enumeration returns nothing, so the sole-owner block/archive guard would be
    # bypassed and a live tenant left ownerless. We therefore centralize erasure in
    # ``SECURITY DEFINER`` functions with a locked-down ``search_path`` and ownership, owned by
    # a dedicated ``keel_maintenance`` role. Normal runtime principals cannot invoke arbitrary
    # cross-tenant deletion: DELETE on the three global identity tables is revoked from
    # ``keel_runtime`` and EXECUTE on these functions is revoked from PUBLIC (granted only to
    # ``keel_maintenance``). The functions enumerate + FOR UPDATE-lock the affected orgs,
    # enforce sole-owner block/archive semantics, and purge every identity row atomically.
    #
    # Column names deliberately avoid the identity table names so plpgsql does not shadow the
    # tables the function DELETEs from.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION keel_erase_organization(p_org_id text)
        RETURNS TABLE (
            deleted_grants integer,
            deleted_agents integer,
            deleted_memberships integer,
            deleted_org integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $fn$
        DECLARE
            v_grants integer := 0;
            v_agents integer := 0;
            v_members integer := 0;
            v_org integer := 0;
        BEGIN
            IF p_org_id IS NULL OR length(p_org_id) = 0 THEN
                RAISE EXCEPTION 'keel_erase_organization: org id is required';
            END IF;
            -- Org-first lock (matches keel_erase_user + the normal membership mutations):
            -- take the organization row's FOR UPDATE lock before touching any child row.
            PERFORM 1 FROM organizations WHERE id = p_org_id FOR UPDATE;
            DELETE FROM resource_grants WHERE org_id = p_org_id;
            GET DIAGNOSTICS v_grants = ROW_COUNT;
            DELETE FROM agents WHERE org_id = p_org_id;
            GET DIAGNOSTICS v_agents = ROW_COUNT;
            DELETE FROM memberships WHERE org_id = p_org_id;
            GET DIAGNOSTICS v_members = ROW_COUNT;
            DELETE FROM organizations WHERE id = p_org_id;
            GET DIAGNOSTICS v_org = ROW_COUNT;
            deleted_grants := v_grants;
            deleted_agents := v_agents;
            deleted_memberships := v_members;
            deleted_org := v_org;
            RETURN NEXT;
            RETURN;
        END;
        $fn$;
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION keel_erase_user(p_user_id text)
        RETURNS TABLE (
            blocked_org_ids text[],
            deleted_oidc integer,
            deleted_agents integer,
            deleted_memberships integer,
            deleted_grants integer,
            deleted_users integer,
            archived_orgs integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $fn$
        DECLARE
            v_blocked text[] := ARRAY[]::text[];
            v_archivable text[] := ARRAY[]::text[];
            v_oidc integer := 0;
            v_agents integer := 0;
            v_members integer := 0;
            v_grants integer := 0;
            v_user integer := 0;
            v_archived integer := 0;
            r record;
        BEGIN
            IF p_user_id IS NULL OR length(p_user_id) = 0 THEN
                RAISE EXCEPTION 'keel_erase_user: user id is required';
            END IF;
            -- Org-first, deterministic lock order (identical to the normal membership
            -- mutations, which FOR UPDATE-lock the organization row BEFORE any membership
            -- row). FOR UPDATE-lock every organization the user is an active member of, in
            -- ascending id order, so this cross-tenant erasure can neither deadlock with, nor
            -- race the owner-count invariant of, a concurrent invite/promotion/demotion/
            -- removal on those orgs. Every affected org row is held for the rest of the txn.
            FOR r IN
                SELECT DISTINCT m.org_id AS org_id
                FROM memberships m
                WHERE m.user_id = p_user_id AND m.status = 'active'
                ORDER BY m.org_id
            LOOP
                PERFORM 1 FROM organizations WHERE id = r.org_id FOR UPDATE;
            END LOOP;
            -- Now, under those org locks, classify the active orgs the user SOLELY owns (no
            -- other active owner) by whether any OTHER active member remains: block (other
            -- members present) vs archive (user is the only active member). The org locks —
            -- not a membership-row lock — freeze the block/archive decision, so the reads here
            -- are consistent with what any concurrent membership mutation will see next.
            FOR r IN
                SELECT m.org_id AS org_id,
                       (SELECT count(*) FROM memberships mm
                          WHERE mm.org_id = m.org_id AND mm.status = 'active'
                            AND mm.user_id <> p_user_id) AS other_members
                FROM memberships m
                JOIN organizations o ON o.id = m.org_id
                WHERE m.user_id = p_user_id AND m.status = 'active' AND m.role = 'owner'
                  AND o.status = 'active'
                  AND NOT EXISTS (
                      SELECT 1 FROM memberships o2
                      WHERE o2.org_id = m.org_id AND o2.status = 'active'
                        AND o2.role = 'owner' AND o2.user_id <> p_user_id)
            LOOP
                IF r.other_members > 0 THEN
                    v_blocked := array_append(v_blocked, r.org_id);
                ELSE
                    v_archivable := array_append(v_archivable, r.org_id);
                END IF;
            END LOOP;

            IF array_length(v_blocked, 1) IS NOT NULL THEN
                -- Blocked: delete nothing, surface the blocking org ids (atomic no-op).
                blocked_org_ids := v_blocked;
                deleted_oidc := 0;
                deleted_agents := 0;
                deleted_memberships := 0;
                deleted_grants := 0;
                deleted_users := 0;
                archived_orgs := 0;
                RETURN NEXT;
                RETURN;
            END IF;

            IF array_length(v_archivable, 1) IS NOT NULL THEN
                UPDATE organizations
                   SET status = 'archived', archived_at = now(), updated_at = now()
                 WHERE id = ANY(v_archivable) AND status = 'active';
                GET DIAGNOSTICS v_archived = ROW_COUNT;
            END IF;

            DELETE FROM resource_grants g
             WHERE g.grantor_user_id = p_user_id
                OR g.agent_id IN (SELECT id FROM agents WHERE owner_user_id = p_user_id);
            GET DIAGNOSTICS v_grants = ROW_COUNT;
            DELETE FROM agents WHERE owner_user_id = p_user_id;
            GET DIAGNOSTICS v_agents = ROW_COUNT;
            DELETE FROM memberships WHERE user_id = p_user_id;
            GET DIAGNOSTICS v_members = ROW_COUNT;
            DELETE FROM oidc_identities WHERE user_id = p_user_id;
            GET DIAGNOSTICS v_oidc = ROW_COUNT;
            DELETE FROM users WHERE id = p_user_id;
            GET DIAGNOSTICS v_user = ROW_COUNT;

            blocked_org_ids := ARRAY[]::text[];
            deleted_oidc := v_oidc;
            deleted_agents := v_agents;
            deleted_memberships := v_members;
            deleted_grants := v_grants;
            deleted_users := v_user;
            archived_orgs := v_archived;
            RETURN NEXT;
            RETURN;
        END;
        $fn$;
        """
    )
    # Never executable by PUBLIC (which would include keel_runtime). EXECUTE is granted only
    # to the dedicated maintenance EXECUTOR role below; the definer owns the functions.
    op.execute("REVOKE ALL ON FUNCTION keel_erase_organization(text) FROM PUBLIC")
    op.execute("REVOKE ALL ON FUNCTION keel_erase_user(text) FROM PUBLIC")

    # Two-role split (least privilege). The erasure privilege is deliberately separated into a
    # *definer* role and an *executor* role so that the principal an operator/app login is
    # granted into is never the one that holds direct cross-tenant table DML + BYPASSRLS:
    #
    #  * ``keel_maintenance`` (DEFINER) — NOLOGIN + BYPASSRLS. Owns the SECURITY DEFINER
    #    functions and holds the table DML they need. Because the functions run with the
    #    definer's rights, this role is the only one that ever touches identity tables across
    #    tenants. NOTHING logs in as it and NO login is granted membership in it.
    #  * ``keel_maintenance_exec`` (EXECUTOR) — NOLOGIN + NOBYPASSRLS, granted ONLY EXECUTE on
    #    the two functions (no table privileges, not a member of the definer). A least-
    #    privilege maintenance LOGIN is made a member of THIS role only, so it can invoke the
    #    audited erasure entrypoints but can neither read/delete identity tables directly nor
    #    ``SET ROLE`` into the definer.
    #
    # Guarded so a managed Postgres that forbids CREATE ROLE / ALTER OWNER / GRANT does not
    # fail the migration; there erasure fails closed until an operator provisions both roles
    # manually (see docs/OPERATIONS.md).
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance') THEN
                BEGIN
                    EXECUTE 'CREATE ROLE keel_maintenance NOLOGIN NOSUPERUSER BYPASSRLS';
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_maintenance not created (insufficient privilege)';
                END;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        users, oidc_identities, organizations,
                        memberships, agents, resource_grants
                        TO keel_maintenance;
                    ALTER FUNCTION keel_erase_organization(text) OWNER TO keel_maintenance;
                    ALTER FUNCTION keel_erase_user(text) OWNER TO keel_maintenance;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_maintenance erasure grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )

    # Distinct maintenance EXECUTOR role (the principal a maintenance login is a member of).
    # NOLOGIN + NOBYPASSRLS, granted ONLY EXECUTE — never any table DML and never membership
    # in keel_maintenance (so it cannot SET ROLE into the definer). ``ALTER ROLE ... NOINHERIT``
    # is intentionally NOT applied: EXECUTE is used directly, and it is never a member of a
    # privileged role.
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance_exec') THEN
                BEGIN
                    EXECUTE 'CREATE ROLE keel_maintenance_exec NOLOGIN NOSUPERUSER NOBYPASSRLS';
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_maintenance_exec not created (insufficient privilege)';
                END;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance_exec') THEN
                BEGIN
                    -- Defensive: strip any table DML that a prior/manual provisioning may
                    -- have granted, so the executor is EXECUTE-only regardless of history.
                    REVOKE ALL ON
                        users, oidc_identities, organizations,
                        memberships, agents, resource_grants
                        FROM keel_maintenance_exec;
                    GRANT USAGE ON SCHEMA public TO keel_maintenance_exec;
                    GRANT EXECUTE ON FUNCTION keel_erase_organization(text)
                        TO keel_maintenance_exec;
                    GRANT EXECUTE ON FUNCTION keel_erase_user(text)
                        TO keel_maintenance_exec;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_maintenance_exec grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )

    # Normal runtime principals must not be able to perform cross-tenant deletion: revoke
    # DELETE on the three GLOBAL identity tables (deleting them cascades across orgs) and
    # EXECUTE on the erasure functions from keel_runtime. Single-tenant deletes on the
    # RLS-scoped tables (memberships/agents/resource_grants, always bound to app.org_id) stay
    # available for ordinary org-scoped operations.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    REVOKE DELETE ON users, oidc_identities, organizations FROM keel_runtime;
                    REVOKE EXECUTE ON FUNCTION keel_erase_organization(text) FROM keel_runtime;
                    REVOKE EXECUTE ON FUNCTION keel_erase_user(text) FROM keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime erasure revokes skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    # Restore the broad keel_runtime DELETE grant (0011/0013 baseline) and drop the erasure
    # functions. The keel_maintenance and keel_maintenance_exec roles are intentionally left
    # in place (like keel_runtime): dropping a role that may still own objects or back a
    # maintenance login elsewhere would fail; operators drop them explicitly if desired.
    # Dropping the functions cascades the executor's EXECUTE grants, so downgrade fails closed
    # (erasure is unavailable) rather than leaving a dangling privileged path.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT DELETE ON users, oidc_identities, organizations TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime DELETE re-grant skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )
    op.execute("DROP FUNCTION IF EXISTS keel_erase_user(text)")
    op.execute("DROP FUNCTION IF EXISTS keel_erase_organization(text)")
    for table in reversed(_ORG_TABLES):
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
    op.execute("DROP TABLE IF EXISTS resource_grants CASCADE")
    op.execute("DROP TABLE IF EXISTS agents CASCADE")
    op.execute("DROP TABLE IF EXISTS memberships CASCADE")
    op.execute("DROP TABLE IF EXISTS organizations CASCADE")
    op.execute("DROP TABLE IF EXISTS oidc_identities CASCADE")
    op.execute("DROP TABLE IF EXISTS users CASCADE")
