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

import hashlib
import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016_web_routing_isolation"
down_revision: str | None = "0015_projects_github"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# The exact list of session-referencing repoints applied when a colliding scoped session is
# renamed on downgrade (children before the ``sessions`` row itself). ``runs`` is remapped
# separately because its immutable admission fingerprint must be recomputed for the new id.
_SESSION_REPOINTS: tuple[str, ...] = (
    "UPDATE events SET session_id = :new WHERE scope_id = :scope AND session_id = :old",
    "UPDATE approvals SET session_id = :new WHERE scope_id = :scope AND session_id = :old",
    "UPDATE schedules SET session_id = :new WHERE scope_id = :scope AND session_id = :old",
    "UPDATE message_embeddings SET session_id = :new WHERE scope_id = :scope AND session_id = :old",
    "UPDATE jobs SET target_session_id = :new WHERE scope_id = :scope AND target_session_id = :old",
    "UPDATE event_tombstones SET session_id = :new WHERE scope_id = :scope AND session_id = :old",
    "UPDATE erasure_requests SET target_id = :new "
    "WHERE scope_id = :scope AND target_kind = 'session' AND target_id = :old",
    "UPDATE sessions SET id = :new WHERE scope_id = :scope AND id = :old",
)


def _legacy_admission_fingerprint(
    *,
    org_id: str,
    actor: str,
    agent_id: str,
    session_id: str,
    surface: str,
    content: str,
) -> str:
    """Recompute the *pre-0016* (pre-model) admission fingerprint in pure Python.

    Byte-for-byte identical to :func:`keel_core.runs.legacy_admission_fingerprint`: canonical
    JSON (sorted keys, compact separators) over the tenant/actor/agent/session/surface binding
    plus a SHA-256 of the immutable admission content, hashed with SHA-256. No ``model`` key —
    the form the code running *after* a rollback below 0016 computes — and no optional DB
    extension (pgcrypto is not required). Downgrade rewrites every remapped run's fingerprint to
    exactly this value for its NEW session id so a retry after rollback is recognized as the same
    admission instead of being falsely rejected as a conflict, while a changed
    content/actor/agent/surface still produces a different hash (a real conflict).
    """
    payload = {
        "org_id": org_id,
        "actor": actor,
        "agent_id": agent_id,
        "session_id": session_id,
        "surface": surface,
        "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _derive_remapped_session_id(
    bind: sa.engine.Connection, scope_id: str, old_id: str, taken: set[str]
) -> str:
    """Deterministic, collision-free rename derived from the original id + owning scope.

    Mirrors the id derivation this migration used before fingerprint recomputation moved to
    Python: ``left(old_id, 180) || '.scope-' || md5(scope || '|' || old_id)`` with a
    deterministic ``-N`` bump extending it until free (guarding the astronomically unlikely
    digest collision against an existing id or one already assigned in this pass).
    """
    base = old_id[:180]
    digest = hashlib.md5(f"{scope_id}|{old_id}".encode()).hexdigest()  # noqa: S324
    new_id = f"{base}.scope-{digest}"
    bump = 0
    while (
        new_id in taken
        or bind.execute(sa.text("SELECT 1 FROM sessions WHERE id = :nid"), {"nid": new_id}).first()
        is not None
    ):
        bump += 1
        new_id = f"{base}.scope-{digest}-{bump}"
    return new_id


def _admission_content(bind: sa.engine.Connection, scope_id: str, run_id: str) -> str:
    """Recover a run's immutable admission content from its unique admission user event.

    The admission user turn (see :func:`keel_core.loop.admit_run`) carries an ``admission_run``
    payload marker equal to the run id and a ``dedup_key`` of ``admit:<run_id>`` guarded by a
    per-scope partial-unique index, so there is at most one matching event per run. We require
    **exactly one** unambiguous match with a string ``text`` field; anything else (a half-admitted
    run with no prompt, a truncated payload) means the fingerprint cannot be reconstructed safely,
    so we raise and fail the downgrade *before* any constraint changes — never clearing/wildcarding
    the fingerprint.
    """
    rows = bind.execute(
        sa.text(
            "SELECT payload FROM events "
            "WHERE scope_id = :scope AND payload->>'admission_run' = :run_id"
        ),
        {"scope": scope_id, "run_id": run_id},
    ).all()
    if len(rows) != 1:
        raise RuntimeError(
            "0016 downgrade: refusing to remap run fingerprint for run "
            f"{run_id!r} in scope {scope_id!r}: expected exactly one admission event, "
            f"found {len(rows)}"
        )
    raw = rows[0][0]
    payload = json.loads(raw) if isinstance(raw, str) else raw
    text_value = payload.get("text") if isinstance(payload, dict) else None
    if not isinstance(text_value, str):
        raise RuntimeError(
            "0016 downgrade: refusing to remap run fingerprint for run "
            f"{run_id!r} in scope {scope_id!r}: admission event has no immutable 'text' content"
        )
    return text_value


def _remap_run_fingerprints(
    bind: sa.engine.Connection, scope_id: str, old_id: str, new_id: str
) -> None:
    """Repoint a scoped session's runs to ``new_id`` and rebind each immutable fingerprint.

    The admission fingerprint binds the session id, so a bare ``session_id`` repoint would leave
    every remapped run carrying a hash that no longer matches its own row — a retry after rollback
    would be falsely rejected as a conflict. For each affected run we recompute the exact pre-0016
    fingerprint for ``new_id`` (org/actor/agent/surface/content preserved; only the session id
    moves), recovering the content from the run's unique admission event. A run that never carried
    a fingerprint (``''``) stays unverifiable/empty (no reconstruction, safe to leave). We update
    ``session_id`` and ``fingerprint`` together so the row is never observed in a bound-mismatched
    state.
    """
    runs = bind.execute(
        sa.text(
            "SELECT id, org_id, actor, agent_id, surface, fingerprint "
            "FROM runs WHERE scope_id = :scope AND session_id = :old"
        ),
        {"scope": scope_id, "old": old_id},
    ).all()
    for run in runs:
        new_fingerprint = run.fingerprint
        if run.fingerprint:
            new_fingerprint = _legacy_admission_fingerprint(
                org_id=run.org_id,
                actor=run.actor,
                agent_id=run.agent_id,
                session_id=new_id,
                surface=run.surface,
                content=_admission_content(bind, scope_id, run.id),
            )
        bind.execute(
            sa.text(
                "UPDATE runs SET session_id = :new, fingerprint = :fp "
                "WHERE scope_id = :scope AND id = :run_id"
            ),
            {"new": new_id, "fp": new_fingerprint, "scope": scope_id, "run_id": run.id},
        )


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
    # A run additionally carries an immutable admission ``fingerprint`` that binds the session
    # id, so a remapped run cannot be repointed by session id alone: we recompute the exact
    # pre-0016 fingerprint for the NEW id (:func:`_remap_run_fingerprints`), recovering the
    # immutable content from the run's unique admission event and failing the downgrade if it
    # cannot be reconstructed safely — never wildcarding/clearing it. The remap + fingerprint
    # recomputation run in Python (this migration's bind) rather than plpgsql so the SHA-256
    # fingerprint matches the application algorithm byte-for-byte without an optional DB
    # extension (pgcrypto).
    #
    # ``runs`` (and only ``runs`` among the session-referencing tables) is under FORCE ROW LEVEL
    # SECURITY, so even the table owner is filtered by ``app.scope_id`` — which is unset during
    # a migration. We drop FORCE for the duration of the cross-scope remap so the repoint reaches
    # every scope's runs, then restore it. The derivation is deterministic and only touches rows
    # that still collide, so a repeated/partial downgrade converges (idempotent) and the
    # no-collision case is a pure no-op.
    bind = op.get_bind()
    op.execute("ALTER TABLE runs NO FORCE ROW LEVEL SECURITY")

    # Materialize the collision set up front (every non-canonical scoped session for a
    # globally-duplicated id) so repointing ``sessions`` mid-loop cannot disturb the iteration.
    collisions = bind.execute(
        sa.text(
            """
            SELECT s.scope_id AS scope_id, s.id AS old_id
            FROM sessions s
            JOIN (
                SELECT id, min(scope_id) AS canonical_scope
                FROM sessions
                GROUP BY id
                HAVING count(*) > 1
            ) dup ON dup.id = s.id
            WHERE s.scope_id <> dup.canonical_scope
            ORDER BY s.id, s.scope_id
            """
        )
    ).all()

    taken: set[str] = set()
    for collision in collisions:
        scope_id = collision.scope_id
        old_id = collision.old_id
        new_id = _derive_remapped_session_id(bind, scope_id, old_id, taken)
        taken.add(new_id)

        # Repoint the runs (with their recomputed fingerprints) before the generic repoints so a
        # failed/unsafe fingerprint reconstruction aborts the downgrade before any constraint or
        # id changes for this collision.
        _remap_run_fingerprints(bind, scope_id, old_id, new_id)
        for statement in _SESSION_REPOINTS:
            bind.execute(sa.text(statement), {"new": new_id, "old": old_id, "scope": scope_id})

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
