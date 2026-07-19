"""global patch-proposal dispatch outbox (non-RLS pointer/index) for cross-org reconciliation.

Revision ID: 0021_patch_proposal_outbox
Revises: 0020_runtime_role_hardening
Create Date: 2026-07-19

M4 controlled-patch phase (WS-PP, P1). The org-partitioned ``patch_proposals`` table (0019) is
under ``FORCE ROW LEVEL SECURITY`` keyed by ``app.org_id``, so a worker bound to one org can only
*see* that org's proposals. To reconcile controlled-patch work (drive generation, then trusted
writeback after a human approval) across **every** org from a single worker process — without a
per-org cron and without a privileged BYPASSRLS scan — proposal admission records a minimal,
non-sensitive dispatch intent in a **global** index:

* ``patch_proposal_outbox`` — a per-proposal pointer carrying only the routing keys a worker needs
  to discover which proposals currently have open background work: the ``proposal_id`` it points
  at, the owning ``org_id`` + validated per-Agent ``scope_id`` (so the worker can re-enter that
  scope's RLS context), a coarse ``status_hint`` (``generating`` -> ``ready`` -> ``approved``, NOT
  the proposal's authoritative status, which stays RLS-protected on ``patch_proposals``), an
  optional ``job_id`` back-reference, a ``next_attempt_at`` retry hint, and a **fenced reconciler
  lease** (``lease_owner`` + a random ``lease_token`` + ``lease_expires_at``). It carries **no**
  diff, task text, bundle bytes, token, or any other sensitive payload — the content-addressed
  bundle and the tainted task digest stay behind RLS on ``patch_proposals`` — so, exactly like the
  0017 dispatch outboxes, it is safe for the non-owner ``keel_runtime`` role to read/lease/update/
  delete even though it is deliberately **not** under RLS (it is read across orgs).

The composite FK ``(proposal_id, org_id) -> patch_proposals(id, org_id) ON DELETE CASCADE`` keeps a
pointer's ``org_id`` structurally consistent with the proposal it names and cascades the pointer
away when the proposal (and, transitively, its project/org) is erased — so a purge never leaves an
orphaned pointer and no pointer can outlive the proposal it references. The ``lease_token`` is a
fencing token: a worker that claimed a stale lease can never ack, reschedule, or delete a row that
a newer worker has since re-leased. The three lease columns are all-or-nothing (a
``num_nonnulls(...) IN (0, 3)`` CHECK) so a row is never left half-leased.

Both due-scan indexes are ordinary btree indexes with **no** ``now()`` in a predicate (a volatile
function is not allowed in an index predicate/expression and would silently freeze the plan at
build time); the reconciler passes the current instant as a bound parameter instead.

Reversible: the downgrade drops the table (its pointers cascade from ``patch_proposals`` already).
No change is made to ``keel_erase_organization``: the table cascades from ``patch_proposals`` (and
the org), so org/project/proposal erasure already purges it.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0021_patch_proposal_outbox"
down_revision: str | None = "0020_runtime_role_hardening"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Coarse routing hints, NOT the proposal's authoritative status (which lives, RLS-protected, on
# ``patch_proposals``). Mirrors keel_core.patch.outbox.PatchOutboxStatus.
_STATUS_HINTS = ("generating", "ready", "approved")


def upgrade() -> None:
    op.execute(
        f"""
        CREATE TABLE patch_proposal_outbox (
            -- The dispatch pointer is owned by its proposal: deleting a proposal (project/org
            -- erasure, lifecycle purge) cascades the pointer away, so purge leaves no orphaned
            -- metadata and no pointer can outlive (or dangle past) the proposal it names. The
            -- composite FK also pins the pointer's org_id to the proposal's own org.
            proposal_id text PRIMARY KEY,
            org_id text NOT NULL,
            -- The validated per-Agent scope the worker re-enters to drive this proposal's work
            -- under RLS (canonical ``agent:<org>/<agent>`` / ``web:local``; validated by the
            -- application's scoping helper before it is ever written here).
            scope_id text NOT NULL,
            -- Coarse dispatch hint (generating -> ready -> approved); the reconciler uses it to
            -- pick the phase of work, never as the authoritative proposal status.
            status_hint text NOT NULL DEFAULT 'generating'
                CHECK (status_hint IN ({", ".join(f"'{value}'" for value in _STATUS_HINTS)})),
            -- Optional back-reference to the durable worker job currently servicing the pointer.
            job_id text NOT NULL DEFAULT '',
            attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            -- Retry hint: a leased/deferred pointer is retried no earlier than this.
            next_attempt_at timestamptz NOT NULL DEFAULT now(),
            -- Fenced reconciler lease so duplicate workers never both process one pointer, and a
            -- stale worker can never ack/delete a row a newer worker has since re-leased.
            lease_owner text,
            lease_token text,
            lease_expires_at timestamptz,
            -- TTL for the pointer itself (a maintenance reconciler retires a pointer whose
            -- proposal has expired). Mirrors the proposal's own ``expires_at``.
            expires_at timestamptz NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            -- Lease is all-or-nothing: never half-set (owner without token, token without expiry).
            CONSTRAINT ck_patch_proposal_outbox_lease
                CHECK (num_nonnulls(lease_owner, lease_token, lease_expires_at) IN (0, 3)),
            -- Structural org consistency + cascade: the pointer can only ever name a proposal in
            -- its own org, and proposal/project/org erasure removes it.
            FOREIGN KEY (proposal_id, org_id)
                REFERENCES patch_proposals (id, org_id) ON DELETE CASCADE
        )
        """
    )
    # The reconciler's due-scan: pending or lease-expired pointers, oldest first. Ordinary btree
    # index (NO now() predicate -- a volatile function is illegal in an index predicate and would
    # freeze the plan at build time); the reconciler compares against a bound :now parameter.
    op.execute(
        "CREATE INDEX ix_patch_proposal_outbox_due "
        "ON patch_proposal_outbox (next_attempt_at, lease_expires_at)"
    )
    # Enumerate distinct active scopes cheaply.
    op.execute("CREATE INDEX ix_patch_proposal_outbox_scope ON patch_proposal_outbox (scope_id)")

    # The outbox is deliberately global (no RLS): it is the cross-org dispatch index, carrying no
    # sensitive payload. Grant the non-owner runtime + maintenance roles the minimal DML they need
    # to read/lease/update/delete pointers (guarded so a managed Postgres that forbids GRANT does
    # not fail the migration). No DDL / ownership / BYPASSRLS is granted -- the runtime role's
    # least-privilege posture (0020) is preserved.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON patch_proposal_outbox
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime patch outbox grants skipped (insufficient priv)';
                END;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON patch_proposal_outbox
                        TO keel_maintenance;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_maintenance patch outbox grants skipped (insufficient priv)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS patch_proposal_outbox CASCADE")
