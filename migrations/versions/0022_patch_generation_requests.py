"""scope-partitioned durable patch generation-request payloads (atomic re-dispatch seam).

Revision ID: 0022_patch_generation_requests
Revises: 0021_patch_proposal_outbox
Create Date: 2026-07-20

M4 controlled-patch phase (WS-PP, P4a). A ``patch.generate`` job carries the full authorized
request (the tainted dev ``task`` text + build params + budgets), but the ``patch_proposals``
row (0019) persists only a ``task_digest`` and the global dispatch pointer (0021) deliberately
carries **no** sensitive payload. So a proposal admitted into ``generating`` needs somewhere durable
to keep the exact request the fenced reconciler re-dispatches after a lost enqueue.

Earlier (P3b-1) that request lived on the run's append-only ``events`` log, recorded by a *separate*
write from the proposal ``create`` — a two-phase seam a crash between the two could strand (a
``generating`` proposal whose request can never be rebuilt). This migration replaces that seam with
a dedicated table written in the **same transaction** as the proposal + pointer, so a committed
``generating`` proposal always implies a committed, reconstructable request (all three commit or
roll back together):

* ``patch_generation_requests`` — one immutable row per proposal (``proposal_id`` PK) holding the
  bounded request ``payload`` (JSONB) and an immutable ``fingerprint`` (a SHA-256 over the request's
  identity fields) an idempotent re-submission is verified against. Unlike the global 0021 pointer
  this row DOES carry the tainted task text, so — like ``patch_proposals`` — it is partitioned under
  ``FORCE ROW LEVEL SECURITY``, but on the runtime data-plane ``app.scope_id`` key (the canonical
  per-Agent scope the worker re-enters, mirroring ``runs`` / ``approvals`` / ``jobs``), so a worker
  bound to one scope can never read another scope's request payload. The composite FK
  ``(proposal_id, org_id) -> patch_proposals(id, org_id) ON DELETE CASCADE`` pins the row's org to
  the proposal it names and cascades it away when the proposal (transitively project/org) is erased,
  so a purge never leaves an orphaned payload and no payload can outlive its proposal. There is no
  cross-scope/global index — the reconciler loads one row by its PK under the row's own scope
  RLS.

Fail-closed bounds at the schema (defense-in-depth behind the strict application payload contract):
the ``payload`` must be a JSON object under a byte ceiling, and ``fingerprint`` must be a 64-char
lowercase hex digest.

Runtime/maintenance get the minimal SELECT/INSERT/UPDATE/DELETE DML (guarded so a managed Postgres
that forbids GRANT does not fail the migration); no DDL / ownership / BYPASSRLS is granted — the M3A
least-privilege posture (0020) is preserved. Reversible: the downgrade drops the table (its rows
already cascade from ``patch_proposals``). No change is made to ``keel_erase_organization``: the
table cascades from ``patch_proposals`` (and the org), so org/project/proposal erasure purges
it.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0022_patch_generation_requests"
down_revision: str | None = "0021_patch_proposal_outbox"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Defense-in-depth byte ceiling on the stored payload (the strict application contract already
# bounds every field: a <=20k-char task + <=20 bounded test commands + refs/model/budgets).
_PAYLOAD_MAX_BYTES = 262_144


def upgrade() -> None:
    op.execute(
        f"""
        CREATE TABLE patch_generation_requests (
            -- The request is owned by its proposal: deleting a proposal (project/org erasure,
            -- lifecycle purge) cascades the payload away, so purge leaves no orphaned request and
            -- no request can outlive the proposal it names. The composite FK pins the request's
            -- org_id to the proposal's own org.
            proposal_id text PRIMARY KEY,
            org_id text NOT NULL,
            -- The canonical per-Agent scope this request executes under (validated by the
            -- application's scoping helper before it is ever written). RLS is keyed on it.
            scope_id text NOT NULL,
            -- The bounded, validated generation request (raw tainted task text + base/model/
            -- agent/source/test commands/budgets). Validated field-by-field by the application
            -- payload contract on read; a JSON object under a byte ceiling here as a backstop.
            payload jsonb NOT NULL,
            -- SHA-256 (64-char lowercase hex) over the request-identity fields (the payload minus
            -- the per-attempt proposal_id/run_id): an idempotent re-submission is accepted when
            -- its identity fingerprint matches this immutable digest.
            fingerprint text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT ck_patch_generation_requests_payload_object
                CHECK (jsonb_typeof(payload) = 'object'),
            CONSTRAINT ck_patch_generation_requests_payload_bytes
                CHECK (pg_column_size(payload) <= {_PAYLOAD_MAX_BYTES}),
            CONSTRAINT ck_patch_generation_requests_fingerprint
                CHECK (fingerprint ~ '^[0-9a-f]{{64}}$'),
            -- Structural org consistency + cascade: the request can only name a proposal in its
            -- own org, and proposal/project/org erasure removes it.
            FOREIGN KEY (proposal_id, org_id)
                REFERENCES patch_proposals (id, org_id) ON DELETE CASCADE
        )
        """
    )

    # Scope-partitioned RLS (mirrors runs/approvals/jobs): a worker bound to a scope can never read
    # another scope's tainted request payload. FORCE so even the table owner is bound by the policy.
    op.execute("ALTER TABLE patch_generation_requests ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON patch_generation_requests "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )
    op.execute("ALTER TABLE patch_generation_requests FORCE ROW LEVEL SECURITY")

    # Minimal non-owner runtime + maintenance DML (guarded). No DDL / ownership / BYPASSRLS — the
    # runtime role's least-privilege posture (0020) is preserved.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON patch_generation_requests
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime patch generation-request grants skipped';
                END;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON patch_generation_requests
                        TO keel_maintenance;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_maintenance patch generation-request grants skipped';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS patch_generation_requests CASCADE")
