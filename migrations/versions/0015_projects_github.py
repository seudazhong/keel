"""managed projects + GitHub synchronization foundations.

Revision ID: 0015_projects_github
Revises: 0014_durable_runs
Create Date: 2026-07-17

M3.7 managed Project + GitHub synchronization foundations (WS-P). Org-owned managed
projects, their durable Git storage handles, run-scoped worktree records, a repo sync
ledger, per-org project quotas, and the GitHub App binding tables (installations,
repositories, webhook deliveries, and sync state).

Tenant-owned tables (``org_id``-partitioned, RLS + FORCE RLS keyed by the ``app.org_id``
GUC exactly like migration 0013):

* ``projects`` — one managed project per row. Blank/local or GitHub-sourced, archivable /
  soft-deletable, optimistic ``version``, ``default_branch`` + ``visibility``. Carries the
  durable active-Git storage handle (``active_git_handle`` / ``storage_backend``) that the
  control plane owns; the sandbox never receives a writable authoritative repo path.
  ``UNIQUE (id, org_id)`` backs the composite FKs below so a child row can never reference a
  project in a *different* org (confused-deputy / cross-org defense).
* ``project_worktrees`` — an ephemeral, run-scoped materialized worktree bound to a durable
  run id. Composite FK ``(project_id, org_id) -> projects(id, org_id)`` makes a cross-org
  project/worktree link structurally impossible.
* ``project_runs`` — the project<->durable-run association (a run belongs to exactly one
  project). Composite FK to ``projects(id, org_id)`` prevents a cross-org project/run link.
* ``repo_sync_ledger`` — an append-only ledger of every import / fetch / webhook-driven
  sync attempt, for durable idempotent reconciliation + audit. Composite FK to the project.
* ``project_quotas`` — per-org project quotas (max projects / active worktrees / repo bytes).
* ``github_installations`` — the GitHub App installation <-> org binding. ``UNIQUE
  (installation_id, org_id)`` backs a composite FK from repositories so a repo can never be
  bound to an installation in another org, and a partial unique index binds each live GitHub
  installation to exactly one org (a cross-installation hijack fails closed).
* ``github_repositories`` — a repository visible through an installation, optionally linked to
  a managed project. Composite FK to ``github_installations(installation_id, org_id)``.
* ``github_sync_state`` — per-repository durable sync cursor (last delivery / sha / time).

Global (not org-partitioned, authenticated independently by webhook HMAC + installation
binding, mirroring ``users`` / ``oidc_identities``):

* ``github_webhook_deliveries`` — one row per received ``X-GitHub-Delivery`` id. The PK makes
  a replayed delivery a no-op (durable idempotent processing + replay protection).

Lifecycle: ``keel_erase_organization`` is re-created here to additionally purge the new
tenant-owned project + GitHub tables, so org erasure stays complete.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0015_projects_github"
down_revision: str | None = "0014_durable_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Tenant-owned tables that get RLS + FORCE RLS + runtime grants (keyed by ``app.org_id``).
_ORG_TABLES = (
    "projects",
    "project_worktrees",
    "project_runs",
    "repo_sync_ledger",
    "project_quotas",
    "github_repositories",
    "github_sync_state",
)

# Global tables (no org partition). ``github_installations`` is a global installation<->org
# binding (like ``oidc_identities``) so the HMAC-authenticated webhook path can resolve a
# delivery's installation to its org BEFORE any tenant context exists;
# ``github_webhook_deliveries`` is the durable delivery-id replay ledger.
_GLOBAL_TABLES = ("github_installations", "github_webhook_deliveries")


def upgrade() -> None:
    # --- Managed projects (tenant-owned) ---------------------------------------------
    op.execute(
        """
        CREATE TABLE projects (
            id text PRIMARY KEY,
            org_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            slug text NOT NULL,
            display_name text NOT NULL,
            source text NOT NULL DEFAULT 'blank'
                CHECK (source IN ('blank', 'github')),
            visibility text NOT NULL DEFAULT 'private'
                CHECK (visibility IN ('private', 'internal')),
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'archived', 'deleted')),
            default_branch text NOT NULL DEFAULT 'main',
            -- Durable, control-plane-owned active Git storage handle (opaque to the sandbox).
            active_git_handle text,
            storage_backend text NOT NULL DEFAULT 'local',
            -- Optional GitHub repository (numeric GitHub id) this project mirrors.
            github_repository_id bigint,
            version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            archived_at timestamptz,
            deleted_at timestamptz,
            -- Backs the composite FKs from child rows (cross-org link is impossible).
            UNIQUE (id, org_id)
        )
        """
    )
    # A live project slug is unique within an org (archived/deleted rows free the slug).
    op.execute(
        "CREATE UNIQUE INDEX ux_projects_org_slug_live ON projects (org_id, lower(slug)) "
        "WHERE status = 'active'"
    )
    op.execute("CREATE INDEX ix_projects_org_status ON projects (org_id, status)")
    op.execute(
        "CREATE INDEX ix_projects_github_repo ON projects (org_id, github_repository_id) "
        "WHERE github_repository_id IS NOT NULL"
    )

    # --- Run-scoped worktree records (tenant-owned) ----------------------------------
    op.execute(
        """
        CREATE TABLE project_worktrees (
            id text PRIMARY KEY,
            org_id text NOT NULL,
            project_id text NOT NULL,
            run_id text NOT NULL,
            coding_run_id text NOT NULL,
            git_ref text NOT NULL DEFAULT 'HEAD',
            commit_sha text,
            handle_path text NOT NULL,
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'reclaimed')),
            created_at timestamptz NOT NULL DEFAULT now(),
            reclaimed_at timestamptz,
            -- Cross-org project/worktree link is structurally impossible.
            FOREIGN KEY (project_id, org_id) REFERENCES projects(id, org_id) ON DELETE CASCADE,
            -- At most one worktree row per durable run within a project.
            UNIQUE (org_id, project_id, run_id)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_project_worktrees_project ON project_worktrees (org_id, project_id)"
    )
    op.execute(
        "CREATE INDEX ix_project_worktrees_active ON project_worktrees (org_id, status) "
        "WHERE status = 'active'"
    )

    # --- Project <-> durable-run association (tenant-owned) --------------------------
    op.execute(
        """
        CREATE TABLE project_runs (
            id text PRIMARY KEY,
            org_id text NOT NULL,
            project_id text NOT NULL,
            run_id text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (project_id, org_id) REFERENCES projects(id, org_id) ON DELETE CASCADE,
            -- A run belongs to exactly one project (org-scoped idempotent association).
            UNIQUE (org_id, run_id)
        )
        """
    )
    op.execute("CREATE INDEX ix_project_runs_project ON project_runs (org_id, project_id)")

    # --- Repo sync ledger (tenant-owned, append-only) --------------------------------
    op.execute(
        """
        CREATE TABLE repo_sync_ledger (
            id text PRIMARY KEY,
            org_id text NOT NULL,
            project_id text NOT NULL,
            kind text NOT NULL
                CHECK (kind IN ('import', 'fetch', 'webhook_push', 'default_branch')),
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'succeeded', 'failed')),
            git_ref text,
            before_sha text,
            after_sha text,
            delivery_id text,
            detail jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (project_id, org_id) REFERENCES projects(id, org_id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_repo_sync_ledger_project "
        "ON repo_sync_ledger (org_id, project_id, created_at)"
    )
    # A GitHub delivery drives at most one durable sync ledger row per project (idempotency).
    op.execute(
        "CREATE UNIQUE INDEX ux_repo_sync_ledger_delivery ON repo_sync_ledger "
        "(org_id, project_id, delivery_id) WHERE delivery_id IS NOT NULL"
    )

    # --- Per-org project quotas (tenant-owned) ---------------------------------------
    op.execute(
        """
        CREATE TABLE project_quotas (
            org_id text PRIMARY KEY REFERENCES organizations(id) ON DELETE CASCADE,
            max_projects integer NOT NULL DEFAULT 100 CHECK (max_projects >= 0),
            max_active_worktrees integer NOT NULL DEFAULT 64 CHECK (max_active_worktrees >= 0),
            max_repository_bytes bigint NOT NULL DEFAULT 2147483648
                CHECK (max_repository_bytes >= 0),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )

    # --- GitHub App installations (tenant-owned) -------------------------------------
    op.execute(
        """
        CREATE TABLE github_installations (
            id text PRIMARY KEY,
            org_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            installation_id bigint NOT NULL,
            app_id bigint NOT NULL,
            account_login text NOT NULL,
            account_type text NOT NULL DEFAULT 'Organization',
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'suspended', 'deleted')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            suspended_at timestamptz,
            deleted_at timestamptz,
            -- Backs the composite FK from repositories (installation pinned to its org).
            UNIQUE (installation_id, org_id)
        )
        """
    )
    # A live GitHub installation binds to exactly one org (cross-installation hijack fails
    # closed): a second org claiming the same installation_id while one is live is rejected.
    op.execute(
        "CREATE UNIQUE INDEX ux_github_installations_live ON github_installations "
        "(installation_id) WHERE status <> 'deleted'"
    )
    op.execute("CREATE INDEX ix_github_installations_org ON github_installations (org_id)")

    # --- GitHub repositories (tenant-owned) ------------------------------------------
    op.execute(
        """
        CREATE TABLE github_repositories (
            id text PRIMARY KEY,
            org_id text NOT NULL,
            installation_id bigint NOT NULL,
            repo_id bigint NOT NULL,
            full_name text NOT NULL,
            default_branch text NOT NULL DEFAULT 'main',
            is_private boolean NOT NULL DEFAULT true,
            clone_url text NOT NULL,
            project_id text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            -- Repo pinned to an installation IN THE SAME ORG (composite FK): a repo row can
            -- never reference an installation belonging to a different org.
            FOREIGN KEY (installation_id, org_id)
                REFERENCES github_installations(installation_id, org_id) ON DELETE CASCADE,
            -- Optional managed-project link, pinned to the same org.
            FOREIGN KEY (project_id, org_id) REFERENCES projects(id, org_id) ON DELETE SET NULL,
            -- A GitHub repo id maps to at most one row per org.
            UNIQUE (org_id, repo_id)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_github_repositories_installation "
        "ON github_repositories (org_id, installation_id)"
    )

    # --- GitHub per-repository sync state (tenant-owned) ------------------------------
    op.execute(
        """
        CREATE TABLE github_sync_state (
            id text PRIMARY KEY,
            org_id text NOT NULL,
            repository_id text NOT NULL,
            last_delivery_id text,
            last_synced_sha text,
            last_synced_ref text,
            last_synced_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (org_id, repository_id)
        )
        """
    )

    # --- GitHub webhook deliveries (global, webhook-authenticated) --------------------
    # PK on the GitHub delivery id makes a replayed delivery a durable no-op. This table is
    # intentionally NOT org-partitioned: the endpoint is authenticated by HMAC over the raw
    # body and the installation->org binding BEFORE any tenant context exists.
    op.execute(
        """
        CREATE TABLE github_webhook_deliveries (
            delivery_id text PRIMARY KEY,
            event text NOT NULL,
            installation_id bigint,
            action text,
            status text NOT NULL DEFAULT 'received'
                CHECK (status IN ('received', 'processed', 'skipped', 'failed')),
            received_at timestamptz NOT NULL DEFAULT now(),
            processed_at timestamptz
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_github_webhook_deliveries_received "
        "ON github_webhook_deliveries (received_at)"
    )

    # --- Row-Level Security on the tenant-owned tables -------------------------------
    for table in _ORG_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY org_isolation ON {table} "
            "USING (org_id = current_setting('app.org_id', true)) "
            "WITH CHECK (org_id = current_setting('app.org_id', true))"
        )
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")

    # --- (Re)apply non-owner runtime grants (guarded) --------------------------------
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        projects, project_worktrees, project_runs, repo_sync_ledger,
                        project_quotas, github_installations, github_repositories,
                        github_sync_state, github_webhook_deliveries
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime project grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )

    # --- Extend org erasure to purge the new tenant-owned tables ----------------------
    # Re-create the SECURITY DEFINER org-erasure function (defined in 0013) so it also purges
    # the managed-project + GitHub tables. Preserves the org-first FOR UPDATE lock order.
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
            PERFORM 1 FROM organizations WHERE id = p_org_id FOR UPDATE;
            -- Managed project + GitHub tables first (children before the org row). Child rows
            -- (worktrees / runs / ledger / repos / sync state) cascade from their parents.
            DELETE FROM github_webhook_deliveries d
             WHERE d.installation_id IN (
                 SELECT installation_id FROM github_installations WHERE org_id = p_org_id);
            DELETE FROM github_sync_state WHERE org_id = p_org_id;
            DELETE FROM github_repositories WHERE org_id = p_org_id;
            DELETE FROM github_installations WHERE org_id = p_org_id;
            DELETE FROM repo_sync_ledger WHERE org_id = p_org_id;
            DELETE FROM project_runs WHERE org_id = p_org_id;
            DELETE FROM project_worktrees WHERE org_id = p_org_id;
            DELETE FROM project_quotas WHERE org_id = p_org_id;
            DELETE FROM projects WHERE org_id = p_org_id;
            -- Identity rows (unchanged from 0013).
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
    # Re-apply the maintenance-role table grants for the new tables (guarded).
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        projects, project_worktrees, project_runs, repo_sync_ledger,
                        project_quotas, github_installations, github_repositories,
                        github_sync_state, github_webhook_deliveries
                        TO keel_maintenance;
                    ALTER FUNCTION keel_erase_organization(text) OWNER TO keel_maintenance;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_maintenance project grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS github_webhook_deliveries CASCADE")
    op.execute("DROP TABLE IF EXISTS github_sync_state CASCADE")
    op.execute("DROP TABLE IF EXISTS github_repositories CASCADE")
    op.execute("DROP TABLE IF EXISTS github_installations CASCADE")
    op.execute("DROP TABLE IF EXISTS project_quotas CASCADE")
    op.execute("DROP TABLE IF EXISTS repo_sync_ledger CASCADE")
    op.execute("DROP TABLE IF EXISTS project_runs CASCADE")
    op.execute("DROP TABLE IF EXISTS project_worktrees CASCADE")
    op.execute("DROP TABLE IF EXISTS projects CASCADE")
    # Restore the 0013 org-erasure function body (without the project/GitHub tables).
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
