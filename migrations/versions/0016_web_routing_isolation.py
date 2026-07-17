"""global run-dispatch outbox for cross-scope reconciliation.

Revision ID: 0016_web_routing_isolation
Revises: 0015_projects_github
Create Date: 2026-07-18

M3.6 durable Web routing review findings 2 (composite session tenant identity) and 4
(cross-scope dispatch/reconciliation).

The ``runs`` table (0014) is scope-partitioned under ``FORCE ROW LEVEL SECURITY`` keyed by
``app.scope_id``, so a worker that binds one scope can only *see* that scope's runs. Before
this migration the worker's reconciler was pinned to the single ``web:local`` scope, so a
durable run admitted into any per-Agent scope (``agent:<org>/<agent>``) was never redispatched
after a lost enqueue, and its expired lease / approval expiry / stuck resume were never
recovered. There was no per-process, all-scopes reconciliation.

``run_dispatch_outbox`` is a **minimal, non-sensitive, global** index of runs with an open
dispatch intent: a ``run_id`` + its canonical ``scope_id`` + a coarse ``state`` + a lease and
a ``next_attempt_at`` retry hint. It deliberately carries **no** prompt/content/tool/argument
payload — only the routing key a worker needs to discover *which* scopes currently have work,
so a single reconciler can enumerate every active scope, bind each in turn, and drive its runs
(redispatch, reclaim expired leases, expire approvals, repair stuck resumes) without a
per-scope cron. Admission writes the intent as part of the queued transition; the reconciler
removes it once the run reaches a terminal state.

Unlike the scope-partitioned run tables this index is intentionally **not** under row-level
security: it is the one table a worker reads *across* scopes. It exposes nothing beyond the
run id + scope routing key, so it is safe for the non-owner ``keel_runtime`` role to read,
lease, and delete. The scope-bound authority (the actual run row, its events, its approvals)
stays behind RLS; the outbox is only a dispatch pointer.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0016_web_routing_isolation"
down_revision: str | None = "0015_projects_github"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE run_dispatch_outbox (
            run_id text PRIMARY KEY,
            scope_id text NOT NULL,
            -- Coarse dispatch state, NOT the run's authoritative status (which lives, RLS-
            -- protected, on ``runs``). 'pending' means "a worker should look at this run in
            -- this scope"; the reconciler removes the row once the run is terminal.
            state text NOT NULL DEFAULT 'pending'
                CHECK (state IN ('pending', 'leased')),
            attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            -- Retry hint: a leased/failed intent is retried no earlier than this.
            next_attempt_at timestamptz NOT NULL DEFAULT now(),
            -- Fenced reconciler lease so duplicate workers never both process one intent.
            lease_owner text,
            lease_expires_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))
        )
        """
    )
    # The reconciler's due-scan: pending or lease-expired intents, oldest first.
    op.execute(
        "CREATE INDEX ix_run_dispatch_outbox_due ON run_dispatch_outbox (next_attempt_at, scope_id)"
    )
    # Enumerate distinct active scopes cheaply.
    op.execute("CREATE INDEX ix_run_dispatch_outbox_scope ON run_dispatch_outbox (scope_id)")

    # The outbox is deliberately global (no RLS): it is the cross-scope dispatch index. Grant
    # the non-owner runtime role read/lease/delete (guarded so a managed Postgres that forbids
    # GRANT does not fail the migration).
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON run_dispatch_outbox
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime outbox grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )

    # --- Composite session tenant identity (review finding 2) -----------------------------
    # Before this migration ``sessions.id`` was a *global* primary key and ``events`` was
    # unique on ``(session_id, seq)``, so a session id could exist in only one scope — two orgs
    # could never reuse the same external session id, and admission denied the second org's
    # session id as a cross-scope conflict. The authority is now the composite ``(scope_id,
    # id)``: each scope owns its own session namespace, so identical external ids in two orgs
    # coexist as two fully isolated sessions (RLS already partitions every read/write by
    # ``app.scope_id``). Existing rows upgrade safely — every current session id is globally
    # unique, so ``(scope_id, id)`` and ``(scope_id, session_id, seq)`` remain unique for them.
    op.execute("ALTER TABLE sessions DROP CONSTRAINT sessions_pkey")
    op.execute("ALTER TABLE sessions ADD PRIMARY KEY (scope_id, id)")
    op.execute("ALTER TABLE events DROP CONSTRAINT events_session_id_seq_key")
    op.execute(
        "ALTER TABLE events ADD CONSTRAINT events_scope_session_seq_key "
        "UNIQUE (scope_id, session_id, seq)"
    )
    # Scope-prefixed read index for the per-scope session event log.
    op.execute("CREATE INDEX ix_events_scope_session_seq ON events (scope_id, session_id, seq)")


def downgrade() -> None:
    # Restore the global session-id namespace. Safe only when no two scopes share a session id
    # (i.e. the composite-identity feature was not exercised); a genuine cross-scope id
    # collision cannot be represented by the old global constraints and will surface here.
    op.execute("DROP INDEX IF EXISTS ix_events_scope_session_seq")
    op.execute("ALTER TABLE events DROP CONSTRAINT IF EXISTS events_scope_session_seq_key")
    op.execute(
        "ALTER TABLE events ADD CONSTRAINT events_session_id_seq_key UNIQUE (session_id, seq)"
    )
    op.execute("ALTER TABLE sessions DROP CONSTRAINT sessions_pkey")
    op.execute("ALTER TABLE sessions ADD PRIMARY KEY (id)")

    op.execute("DROP TABLE IF EXISTS run_dispatch_outbox CASCADE")
