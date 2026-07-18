"""durable controlled patch proposals + human-approved GitHub Draft PR writeback ledger.

Revision ID: 0019_patch_proposals
Revises: 0018_im_routing
Create Date: 2026-07-18

Adds the durable data plane for the *controlled development -> immutable proposal ->
human-approved remote branch + GitHub Draft PR* phase (WS-PP):

* ``patch_proposals`` — the **org-owned** immutable-proposal record binding one controlled
  patch-generation run to its org, project, durable run + attempt, executing Agent, requesting
  actor, approved base ref/SHA, resulting head SHA, the **content-addressed proposal bundle**
  hash (``bundle_sha256`` — the diff/commits/tree/test-results live in immutable artifacts, never
  as a mutable DB blob), the ``changed_path_digest`` an approval is bound to, the durable
  approval id, and the writeback refs (dedicated remote branch + Draft PR number/url/node id).
  Org-partitioned under ``FORCE ROW LEVEL SECURITY`` on ``app.org_id`` with a composite FK
  ``(project_id, org_id) -> projects(id, org_id)`` so a proposal can only ever bind a project in
  its *own* org (cross-org/project binding is structurally impossible), plus ``ON DELETE CASCADE``
  from the project (and, transitively, the org) so project/org erasure removes every proposal.
  ``status`` is a fail-closed state-machine column; ``version`` is the optimistic/fenced
  transition counter; ``UNIQUE (org_id, idempotency_key)`` makes a re-submitted trigger a no-op;
  a partial unique index reserves at most one live proposal per dedicated remote branch per
  project (branch-collision defense).

* ``patch_writeback_ledger`` — the **org-owned, append-only** repo-sync/writeback audit ledger
  for a proposal's control-plane writeback: one row per step (``verify_base`` / ``push_branch`` /
  ``create_pr`` / ``reconcile``) with a status and the before/after SHAs, dedicated remote branch
  and PR number. Composite FKs ``(project_id, org_id) -> projects(id, org_id)`` and
  ``(proposal_id, org_id) -> patch_proposals(id, org_id)`` (both ``ON DELETE CASCADE``) keep every
  ledger row inside its proposal's org and remove it when the proposal/project/org is erased.
  ``FORCE ROW LEVEL SECURITY`` on ``app.org_id``.

Both tables are reversible; the downgrade drops them (children before parents via CASCADE). No
change is made to the ``keel_erase_organization`` SECURITY DEFINER function: both tables cascade
from ``projects`` (and the org), so org/project erasure already purges them.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0019_patch_proposals"
down_revision: str | None = "0018_im_routing"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Legal proposal lifecycle states (fail closed). Mirrors keel_core.patch.models.PatchStatus.
_STATUSES = (
    "generating",
    "ready",
    "approval_pending",
    "approved",
    "denied",
    "expired",
    "writing",
    "draft_pr_created",
    "failed",
    "stale",
    "cancelled",
)

_TEST_STATUSES = ("unknown", "passed", "failed", "skipped")

_LEDGER_STEPS = ("verify_base", "push_branch", "create_pr", "reconcile")
_LEDGER_STATUSES = ("pending", "succeeded", "failed", "stale")


def _in_list(column: str, values: Sequence[str]) -> str:
    joined = ", ".join(f"'{value}'" for value in values)
    return f"CHECK ({column} IN ({joined}))"


def upgrade() -> None:
    # --- org-owned immutable proposal record (RLS on app.org_id) ----------------------
    op.execute(
        f"""
        CREATE TABLE patch_proposals (
            id text PRIMARY KEY,
            org_id text NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
            project_id text NOT NULL,
            run_id text NOT NULL,
            run_attempt integer NOT NULL DEFAULT 0 CHECK (run_attempt >= 0),
            agent_id text NOT NULL,
            actor text NOT NULL,
            -- Optional linkage to the originating review finding / report (audit only).
            source_ref text NOT NULL DEFAULT '',
            -- SHA-256 of the (tainted) development task text; the plaintext task never lands here.
            task_digest text NOT NULL DEFAULT '',
            -- Approved base: the requested ref and the EXACT resolved base commit.
            base_ref text NOT NULL,
            base_sha text NOT NULL DEFAULT '',
            head_sha text NOT NULL DEFAULT '',
            -- Content-addressed proposal bundle (diff/commits/tree/tests) hash; never a DB blob.
            bundle_sha256 text NOT NULL DEFAULT '',
            diff_sha256 text NOT NULL DEFAULT '',
            -- Digest over the sorted changed (path, blob-sha) pairs an approval is bound to.
            changed_path_digest text NOT NULL DEFAULT '',
            changed_files integer NOT NULL DEFAULT 0 CHECK (changed_files >= 0),
            test_status text NOT NULL DEFAULT 'unknown' {_in_list("test_status", _TEST_STATUSES)},
            status text NOT NULL DEFAULT 'generating' {_in_list("status", _STATUSES)},
            version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
            -- Durable approval binding (empty until approval requested).
            approval_id text NOT NULL DEFAULT '',
            -- Writeback refs: dedicated safe run branch + idempotent Draft PR.
            remote_branch text NOT NULL DEFAULT '',
            pr_number integer,
            pr_url text NOT NULL DEFAULT '',
            pr_node_id text NOT NULL DEFAULT '',
            idempotency_key text NOT NULL,
            fingerprint text NOT NULL DEFAULT '',
            cost_usd double precision NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
            error_kind text NOT NULL DEFAULT '',
            error_message text NOT NULL DEFAULT '',
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            ready_at timestamptz,
            decided_at timestamptz,
            written_at timestamptz,
            expires_at timestamptz NOT NULL,
            -- Backs the composite FK from the writeback ledger (cross-org link impossible).
            UNIQUE (id, org_id),
            -- A re-submitted trigger (same idempotency key) is a durable no-op within the org.
            UNIQUE (org_id, idempotency_key),
            FOREIGN KEY (project_id, org_id) REFERENCES projects (id, org_id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_patch_proposals_project_status "
        "ON patch_proposals (org_id, project_id, status)"
    )
    op.execute("CREATE INDEX ix_patch_proposals_run ON patch_proposals (org_id, run_id)")
    op.execute(
        "CREATE INDEX ix_patch_proposals_expires ON patch_proposals (expires_at) "
        "WHERE status IN ('generating', 'ready', 'approval_pending', 'approved', 'writing')"
    )
    # At most one *live* proposal may reserve a given dedicated remote branch per project — a
    # writeback branch is never shared or silently reused (branch-collision defense). Terminal
    # rows (denied/expired/failed/stale/cancelled) free the name; draft_pr_created keeps it.
    op.execute(
        "CREATE UNIQUE INDEX ux_patch_proposals_branch ON patch_proposals "
        "(org_id, project_id, remote_branch) WHERE remote_branch <> '' "
        "AND status IN ('approved', 'writing', 'draft_pr_created')"
    )

    op.execute("ALTER TABLE patch_proposals ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY org_isolation ON patch_proposals "
        "USING (org_id = current_setting('app.org_id', true)) "
        "WITH CHECK (org_id = current_setting('app.org_id', true))"
    )
    op.execute("ALTER TABLE patch_proposals FORCE ROW LEVEL SECURITY")

    # --- org-owned append-only writeback ledger (RLS on app.org_id) -------------------
    op.execute(
        f"""
        CREATE TABLE patch_writeback_ledger (
            id text PRIMARY KEY,
            org_id text NOT NULL,
            project_id text NOT NULL,
            proposal_id text NOT NULL,
            step text NOT NULL {_in_list("step", _LEDGER_STEPS)},
            status text NOT NULL DEFAULT 'pending' {_in_list("status", _LEDGER_STATUSES)},
            git_ref text,
            before_sha text,
            after_sha text,
            remote_branch text NOT NULL DEFAULT '',
            pr_number integer,
            detail jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (project_id, org_id) REFERENCES projects (id, org_id) ON DELETE CASCADE,
            FOREIGN KEY (proposal_id, org_id)
                REFERENCES patch_proposals (id, org_id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_patch_writeback_ledger_proposal "
        "ON patch_writeback_ledger (org_id, proposal_id, created_at)"
    )

    op.execute("ALTER TABLE patch_writeback_ledger ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY org_isolation ON patch_writeback_ledger "
        "USING (org_id = current_setting('app.org_id', true)) "
        "WITH CHECK (org_id = current_setting('app.org_id', true))"
    )
    op.execute("ALTER TABLE patch_writeback_ledger FORCE ROW LEVEL SECURITY")

    # --- non-owner runtime / maintenance grants (guarded) -----------------------------
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        patch_proposals, patch_writeback_ledger
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime patch grants skipped';
                END;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        patch_proposals, patch_writeback_ledger
                        TO keel_maintenance;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_maintenance patch grants skipped';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    # Drop the ledger (child) before the proposals (parent); CASCADE clears dependent objects.
    op.execute("DROP TABLE IF EXISTS patch_writeback_ledger CASCADE")
    op.execute("DROP TABLE IF EXISTS patch_proposals CASCADE")
