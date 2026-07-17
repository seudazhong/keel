"""global run-dispatch outbox for cross-scope reconciliation.

Revision ID: 0016_web_routing_isolation
Revises: 0015_projects_github
Create Date: 2026-07-18

M3.6 durable Web routing review findings 2 (composite session tenant identity), 3 (per-scope
Knowledge + cross-scope Knowledge job dispatch) and 4 (cross-scope run dispatch/reconciliation).

Two global, non-RLS dispatch indices are created here: ``run_dispatch_outbox`` (runs) and
``job_dispatch_outbox`` (durable jobs, e.g. Knowledge ingest/delete). Both carry only a resource
id + canonical scope routing key (no content/payload), so a single worker reconciler can drive
work across *every* per-Agent scope without a per-scope cron.

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
            -- The dispatch pointer is owned by its run: deleting a run (lifecycle purge,
            -- scope erasure) cascades the intent away, so purge leaves no orphaned metadata
            -- and no intent can outlive (or dangle past) the run it points at (finding 4).
            run_id text PRIMARY KEY
                REFERENCES runs (id) ON DELETE CASCADE,
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

    # --- Global Knowledge job-dispatch outbox (review finding 3) --------------------------
    # The scope-partitioned ``jobs`` table (0009) is under ``ENABLE ROW LEVEL SECURITY`` keyed
    # by ``app.scope_id``, so a worker bound to one scope cannot *see* another scope's jobs.
    # Before this migration the server built a single ``web:local``-bound Knowledge service and
    # the worker's ``dispatch_jobs`` reconciler was pinned to that one scope, so a Knowledge
    # document created under a per-Agent scope (``agent:<org>/<agent>``) enqueued an ingest job
    # that no worker ever dispatched — the document stayed orphaned/unindexed.
    #
    # ``job_dispatch_outbox`` is the Knowledge/durable-job analogue of ``run_dispatch_outbox``:
    # a **minimal, non-sensitive, global** index of durable jobs with an open dispatch intent —
    # a ``job_id`` + its canonical ``scope_id`` + the job ``kind`` + a coarse ``state`` + a fenced
    # lease and a ``next_attempt_at`` retry hint. It carries **no** document content, payload,
    # prompt, or embedding — only the routing key a worker needs to discover *which* scopes have
    # dispatchable jobs, so a single reconciler can enumerate every active scope, bind each in
    # turn, and dispatch ``run_job(scope, job_id)`` without a per-scope cron. Admission writes the
    # intent atomically with the job insert; the reconciler removes it once the job is terminal.
    op.execute(
        """
        CREATE TABLE job_dispatch_outbox (
            -- The dispatch pointer is owned by its job: deleting a job (lifecycle purge, scope
            -- erasure) cascades the intent away, so purge leaves no orphaned metadata and no
            -- intent can outlive (or dangle past) the job it points at (finding 3).
            job_id text PRIMARY KEY
                REFERENCES jobs (id) ON DELETE CASCADE,
            scope_id text NOT NULL,
            -- The job kind (e.g. 'knowledge.ingest'); lets the reconciler revalidate that the
            -- intent points at a cross-scope-dispatchable kind before enqueuing.
            kind text NOT NULL,
            -- Coarse dispatch state, NOT the job's authoritative status (which lives, RLS-
            -- protected, on ``jobs``). 'pending' means "a worker should dispatch this job in
            -- this scope"; the reconciler removes the row once the job is terminal.
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
        "CREATE INDEX ix_job_dispatch_outbox_due ON job_dispatch_outbox (next_attempt_at, scope_id)"
    )
    # Enumerate distinct active scopes cheaply.
    op.execute("CREATE INDEX ix_job_dispatch_outbox_scope ON job_dispatch_outbox (scope_id)")

    # The outbox is deliberately global (no RLS): it is the cross-scope dispatch index. Grant
    # the non-owner runtime role read/lease/delete (guarded so a managed Postgres that forbids
    # GRANT does not fail the migration).
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON job_dispatch_outbox
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime job outbox grants skipped (insufficient privilege)';
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
    # Restore the global session-id namespace. Under the composite ``(scope_id, id)`` identity
    # two different scopes may legitimately own the *same* external session id; the pre-0016
    # global ``sessions_pkey (id)`` / ``events_session_id_seq_key (session_id, seq)`` cannot
    # represent that, so before restoring them we must deterministically collapse every
    # cross-scope collision down to a single global namespace. One scope keeps the original id
    # (the canonical holder — the lexicographically smallest ``scope_id`` for that id, a stable
    # choice); every other scope's same-id session is renamed to a stable, collision-free id
    # derived from its original id + scope, and *all* rows that reference it (events, runs,
    # approvals, schedules, jobs, message embeddings, tombstones, session-scoped erasure
    # requests) are repointed in lock-step so no event is ever detached from its session and no
    # reference is left dangling. There are no session FKs, so ON UPDATE CASCADE is unavailable
    # — we repoint each referencing table explicitly, children before the ``sessions`` row.
    #
    # ``runs`` (and only ``runs`` among the session-referencing tables) is under FORCE ROW LEVEL
    # SECURITY, so even the table owner is filtered by ``app.scope_id`` — which is unset during
    # a migration. We drop FORCE for the duration of the cross-scope remap so the repoint reaches
    # every scope's runs, then restore it. The derivation is deterministic and only touches rows
    # that still collide, so a repeated/partial downgrade converges (idempotent) and the
    # no-collision case is a pure no-op.
    op.execute("ALTER TABLE runs NO FORCE ROW LEVEL SECURITY")
    op.execute(
        """
        DO $$
        DECLARE
            collision RECORD;
            new_id text;
            base text;
            digest text;
            bump int;
        BEGIN
            -- Materialize the collision set up front (every non-canonical scoped session for a
            -- globally-duplicated id) so repointing ``sessions`` mid-loop cannot disturb the
            -- iteration.
            CREATE TEMP TABLE _session_id_collisions ON COMMIT DROP AS
            SELECT s.scope_id AS scope_id, s.id AS old_id
            FROM sessions s
            JOIN (
                SELECT id, min(scope_id) AS canonical_scope
                FROM sessions
                GROUP BY id
                HAVING count(*) > 1
            ) dup ON dup.id = s.id
            WHERE s.scope_id <> dup.canonical_scope;

            FOR collision IN SELECT scope_id, old_id FROM _session_id_collisions LOOP
                -- Deterministic, stable rename derived from the original id + owning scope.
                -- ``(scope_id, id)`` is the composite PK, so md5(scope||id) is unique per row;
                -- the base is truncated to keep the derived id bounded in length/format.
                base := left(collision.old_id, 180);
                digest := md5(collision.scope_id || '|' || collision.old_id);
                new_id := base || '.scope-' || digest;
                -- Guard the (astronomically unlikely) digest collision with an existing id or
                -- one already assigned in this pass: extend deterministically until free.
                bump := 0;
                WHILE EXISTS (SELECT 1 FROM sessions WHERE id = new_id) LOOP
                    bump := bump + 1;
                    new_id := base || '.scope-' || digest || '-' || bump;
                END LOOP;

                -- Repoint every referencing row for this scoped session, then the row itself.
                UPDATE events SET session_id = new_id
                    WHERE scope_id = collision.scope_id AND session_id = collision.old_id;
                UPDATE runs SET session_id = new_id
                    WHERE scope_id = collision.scope_id AND session_id = collision.old_id;
                UPDATE approvals SET session_id = new_id
                    WHERE scope_id = collision.scope_id AND session_id = collision.old_id;
                UPDATE schedules SET session_id = new_id
                    WHERE scope_id = collision.scope_id AND session_id = collision.old_id;
                UPDATE message_embeddings SET session_id = new_id
                    WHERE scope_id = collision.scope_id AND session_id = collision.old_id;
                UPDATE jobs SET target_session_id = new_id
                    WHERE scope_id = collision.scope_id AND target_session_id = collision.old_id;
                UPDATE event_tombstones SET session_id = new_id
                    WHERE scope_id = collision.scope_id AND session_id = collision.old_id;
                UPDATE erasure_requests SET target_id = new_id
                    WHERE scope_id = collision.scope_id
                        AND target_kind = 'session'
                        AND target_id = collision.old_id;
                UPDATE sessions SET id = new_id
                    WHERE scope_id = collision.scope_id AND id = collision.old_id;
            END LOOP;
        END $$;
        """
    )
    op.execute("ALTER TABLE runs FORCE ROW LEVEL SECURITY")

    # Every cross-scope collision is now collapsed, so the global constraints are representable.
    op.execute("DROP INDEX IF EXISTS ix_events_scope_session_seq")
    op.execute("ALTER TABLE events DROP CONSTRAINT IF EXISTS events_scope_session_seq_key")
    op.execute(
        "ALTER TABLE events ADD CONSTRAINT events_session_id_seq_key UNIQUE (session_id, seq)"
    )
    op.execute("ALTER TABLE sessions DROP CONSTRAINT sessions_pkey")
    op.execute("ALTER TABLE sessions ADD PRIMARY KEY (id)")

    op.execute("DROP TABLE IF EXISTS job_dispatch_outbox CASCADE")
    op.execute("DROP TABLE IF EXISTS run_dispatch_outbox CASCADE")
