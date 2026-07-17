"""durable interactive runs + cross-surface approval binding.

Revision ID: 0014_durable_runs
Revises: 0013_identity_agents_grants
Create Date: 2026-07-17

M3.6 worker-owned durable interactive runs (WS-M). An interactive run stops being a
server-local ``asyncio.Task`` and becomes a durable, leased row a worker claims and drives
to a named terminal state, so admission / interrupt / steering / approval-suspension all
survive a server **and** worker restart and are safe under N racing workers.

* ``runs`` — one row per interactive run. Scope-partitioned (``scope_id``, the runtime
  data-plane tenant key used by ``events`` / ``approvals`` / ``jobs``) and additionally
  bound to a persisted-identity ``org_id`` + ``actor`` + selected ``agent_id`` for authz /
  audit / trace tags. Carries the fenced lease (``lease_token`` + ``lease_expires_at`` +
  ``worker_id``), optimistic ``version``, ``attempt`` counter, budget + running cost
  summary, idempotency key, and result/error references. A partial unique index makes a
  **second live owner impossible**; ``UNIQUE (scope_id, idempotency_key)`` makes a
  duplicate admission impossible.
* ``run_control`` — durable interrupt / cancel / steering requests consumed by the owning
  worker (restart- and N-worker-safe: a request outlives the process that issued it).
* ``approvals`` gains binding columns (``org_id``, ``actor``, ``action_hash``,
  ``run_attempt``) so a cross-surface decision is bound to the exact run attempt + tool +
  argument set — a stale/replayed approval for different args (or an earlier attempt) is
  rejected.

Row-Level Security (ADR-0009 / DESIGN-REVIEW G16): ``runs`` and ``run_control`` carry the
``app.scope_id`` policy (mirroring ``jobs`` / ``approvals``) plus ``FORCE ROW LEVEL
SECURITY`` and the non-owner ``keel_runtime`` grants (guarded so a managed Postgres that
forbids GRANT does not fail the migration).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0014_durable_runs"
down_revision: str | None = "0013_identity_agents_grants"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCOPED_TABLES = ("runs", "run_control")


def upgrade() -> None:
    # --- Durable interactive runs ----------------------------------------------------
    op.execute(
        """
        CREATE TABLE runs (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            org_id text NOT NULL,
            actor text NOT NULL,
            agent_id text NOT NULL,
            session_id text NOT NULL,
            surface text NOT NULL,
            idempotency_key text NOT NULL,
            status text NOT NULL DEFAULT 'admitted'
                CHECK (status IN ('admitted', 'queued', 'running', 'waiting_approval',
                    'completed', 'failed', 'cancelled', 'interrupted', 'expired')),
            stop_reason text,
            attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
            version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
            worker_id text,
            lease_token text,
            lease_expires_at timestamptz,
            heartbeat_at timestamptz,
            max_iterations integer NOT NULL DEFAULT 20 CHECK (max_iterations >= 1),
            token_budget integer CHECK (token_budget IS NULL OR token_budget >= 0),
            prompt_tokens bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
            completion_tokens bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
            cost_usd double precision NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
            result_ref text,
            error_kind text,
            error_message text,
            -- Explicit durable resume marker: set when an approval resolves and the run is
            -- requeued so the claiming worker calls loop.resume() (never inferred from a
            -- pre-claim status that requeue overwrote).
            resume_requested boolean NOT NULL DEFAULT false,
            -- Durable admission progress: true once the user turn is persisted. Reconciliation
            -- must never dispatch a run whose prompt was not durably admitted.
            prompt_persisted boolean NOT NULL DEFAULT false,
            -- Cumulative agent-loop iterations consumed across claim/suspend/resume attempts,
            -- so max_iterations bounds the whole run (a resume cannot reset the budget).
            iterations integer NOT NULL DEFAULT 0 CHECK (iterations >= 0),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz,
            finished_at timestamptz,
            expires_at timestamptz NOT NULL,
            -- A leased row must always name its owner (fencing sanity).
            CHECK ((lease_token IS NULL) = (worker_id IS NULL)),
            UNIQUE (scope_id, idempotency_key)
        )
        """
    )
    # Dispatcher scan (queued/admitted, oldest first) + lease-expiry reconciliation scan.
    op.execute("CREATE INDEX ix_runs_dispatch ON runs (scope_id, status, created_at)")
    op.execute(
        "CREATE INDEX ix_runs_lease_expiry ON runs (scope_id, lease_expires_at) "
        "WHERE status IN ('running', 'waiting_approval')"
    )
    op.execute("CREATE INDEX ix_runs_session ON runs (scope_id, session_id)")
    op.execute("CREATE INDEX ix_runs_org_agent ON runs (scope_id, org_id, agent_id)")
    # At most one live (leased/running) owner per run id — structurally prevents two
    # workers from concurrently owning the same run. (The PK already makes id unique; this
    # partial index documents + enforces the single-active-owner invariant on lease_token.)
    op.execute(
        "CREATE UNIQUE INDEX ux_runs_single_owner ON runs (id) "
        "WHERE status = 'running' AND lease_token IS NOT NULL"
    )

    # --- Durable interrupt / cancel / steering requests -------------------------------
    op.execute(
        """
        CREATE TABLE run_control (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            run_id text NOT NULL,
            kind text NOT NULL CHECK (kind IN ('interrupt', 'cancel', 'steer')),
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            requested_by text NOT NULL,
            requested_at timestamptz NOT NULL DEFAULT now(),
            consumed_at timestamptz
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_run_control_pending ON run_control (scope_id, run_id) "
        "WHERE consumed_at IS NULL"
    )

    # --- Durably-unique admission / steering turns ------------------------------------
    # A partial-unique index on the ``dedup_key`` marker makes the admission user turn and
    # each steering user turn append **exactly once** even under N racing admitters/workers:
    # a concurrent or replayed append loses the race here (surfaced as DuplicateEventError)
    # instead of duplicating the prompt/steer message. Only the winner appends + dispatches.
    op.execute(
        "CREATE UNIQUE INDEX ux_events_dedup ON events (scope_id, (payload->>'dedup_key')) "
        "WHERE payload ? 'dedup_key'"
    )

    # --- Cross-surface approval binding (bind a decision to run attempt + action) ------
    # Additive columns; existing rows default so the migration is safe on populated DBs.
    op.execute("ALTER TABLE approvals ADD COLUMN org_id text NOT NULL DEFAULT ''")
    op.execute("ALTER TABLE approvals ADD COLUMN actor text NOT NULL DEFAULT ''")
    op.execute("ALTER TABLE approvals ADD COLUMN action_hash text NOT NULL DEFAULT ''")
    op.execute("ALTER TABLE approvals ADD COLUMN run_attempt integer NOT NULL DEFAULT 0")

    # --- Row-Level Security on the scope-bound run tables -----------------------------
    for table in _SCOPED_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY scope_isolation ON {table} "
            "USING (scope_id = current_setting('app.scope_id', true)) "
            "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
        )
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")

    # --- (Re)apply non-owner runtime grants (guarded) ---------------------------------
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON runs, run_control
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime run grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ux_events_dedup")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS run_attempt")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS action_hash")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS actor")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS org_id")
    op.execute("DROP TABLE IF EXISTS run_control CASCADE")
    op.execute("DROP TABLE IF EXISTS runs CASCADE")
