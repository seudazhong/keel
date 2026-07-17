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
  ``run_attempt``, ``batch_id``) so a cross-surface decision is bound to the exact run
  attempt + tool + argument set — a stale/replayed approval for different args (or an
  earlier attempt) is rejected — and every approval raised by one suspended tool batch
  shares a ``batch_id`` so the run resumes only once *all* its decisions are terminal.
* ``runs`` additionally persists an immutable admission ``fingerprint`` and namespaces its
  admission-identity uniqueness by ``(scope_id, org_id, actor, idempotency_key)`` so one
  tenant/actor cannot collide with or hijack another's run via a shared idempotency key.
* ``runs`` carries a durable suspension checkpoint (``suspend_checkpoint`` +
  ``checkpoint_attempt`` + ``checkpoint_batch_id``): set — fenced by the active lease — in the
  SAME transaction that persists an approval batch's ``tool.call`` events, approval rows, and
  ``approval.requested`` events (never a separate ``mark_checkpoint`` commit before it), so a
  worker crash before the ``running -> waiting_approval`` release either rolls the whole unit
  back (reclaim restarts fresh, nothing lost) or finds a complete durable batch bound to the
  checkpoint (reclaim *resumes*, honouring the approval) — never a partial in-between.
* A suspended tool batch persists its ``tool.call`` events, its approval rows, its
  ``approval.requested`` events, **and** its run checkpoint (batch id + source attempt) as one
  atomic unit (single transaction when the run/event/approval stores share an engine), so a
  crash can never leave an approval row without its events (or a partially-raised batch), nor a
  checkpoint without its batch. ``ux_approvals_run_call`` (partial, interactive rows only)
  makes at most one approval row per (run, call, source attempt), backing resume's
  reconstruction repair that rebuilds the call -> approval association from the durable rows
  when an ``approval.requested`` event was lost to an older-build crash — adopting a row only
  when its run/session/call/action-hash **and** the checkpoint's exact source attempt + batch
  id all match, so a granted approval is never silently denied and a foreign/injected row is
  never adopted.

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
            -- Immutable admission fingerprint: a stable hash over the tenant/actor/agent/
            -- session/surface binding + the normalized admission content. A retried admission
            -- that presents the same idempotency identity but a *different* binding/content is
            -- rejected as a conflict rather than silently repaired with attacker-supplied
            -- values (M3.6 blocker 3). Defaults empty so pre-existing rows upgrade safely.
            fingerprint text NOT NULL DEFAULT '',
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
            -- Durable suspension checkpoint (M3.6 crash boundary): set — fenced by the active
            -- lease — the instant a worker begins persisting an approval batch, BEFORE the
            -- running -> waiting_approval release. If the worker crashes in that window the row
            -- is still 'running' with a durable approval bound to its attempt; the marker makes
            -- the lease-expiry reclaim *resume* (honouring the approval) instead of restarting
            -- fresh and silently discarding it. checkpoint_attempt records the batch's source
            -- attempt for approval binding, preserved across a reclaim (the lease attempt
            -- advances for fencing, this does not). checkpoint_batch_id records the exact
            -- suspended batch id — persisted, fenced by the lease, in the SAME transaction as
            -- the batch's approval rows + events (never a separate commit), so resume's
            -- reconstruction repair adopts a durable approval row only when its batch_id AND
            -- source attempt match the checkpoint exactly (a foreign/older/newer attempt or
            -- batch fails closed). Defaults so pre-existing rows upgrade safely.
            suspend_checkpoint boolean NOT NULL DEFAULT false,
            checkpoint_attempt integer NOT NULL DEFAULT 0 CHECK (checkpoint_attempt >= 0),
            checkpoint_batch_id text NOT NULL DEFAULT '',
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz,
            finished_at timestamptz,
            expires_at timestamptz NOT NULL,
            -- A leased row must always name its owner (fencing sanity).
            CHECK ((lease_token IS NULL) = (worker_id IS NULL)),
            -- Admission identity is namespaced by tenant (org) AND admitting actor, not by a
            -- shared (scope, idempotency_key) alone: one tenant/actor cannot collide with — or
            -- hijack — another's run by reusing an idempotency key (M3.6 blocker 3).
            UNIQUE (scope_id, org_id, actor, idempotency_key)
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
    # Batch identity: every approval raised by one suspended tool batch shares a batch_id so a
    # run resumes only once *every* decision in the batch is terminal (M3.6 blocker 5). A
    # single-approval suspension is just a batch of one. Defaults empty for legacy rows.
    op.execute("ALTER TABLE approvals ADD COLUMN batch_id text NOT NULL DEFAULT ''")
    op.execute(
        "CREATE INDEX ix_approvals_batch ON approvals (scope_id, run_id, batch_id) "
        "WHERE status = 'pending'"
    )
    # At most one durable-run approval row per (run, call, source attempt): every interactive
    # approval carries a ``batch_id`` (legacy scheduled/digest approvals do not), so this
    # partial unique index structurally prevents a duplicate or **injected** approval row for
    # the same tool call within a run attempt. It backs resume's reconstruction repair — which
    # rebuilds the call -> approval association from the durable rows when an
    # ``approval.requested`` event was lost to a crash before it committed — so a reconstructed
    # match is always unambiguous (M3.6 approval-event atomicity). Excludes legacy rows
    # (empty ``batch_id``) so the migration is safe on populated DBs.
    op.execute(
        "CREATE UNIQUE INDEX ux_approvals_run_call ON approvals "
        "(scope_id, run_id, call_id, run_attempt) WHERE batch_id <> ''"
    )

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
    op.execute("DROP INDEX IF EXISTS ux_approvals_run_call")
    op.execute("DROP INDEX IF EXISTS ix_approvals_batch")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS batch_id")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS run_attempt")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS action_hash")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS actor")
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS org_id")
    op.execute("DROP TABLE IF EXISTS run_control CASCADE")
    op.execute("DROP TABLE IF EXISTS runs CASCADE")
