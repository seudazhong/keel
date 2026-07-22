"""R1B generic durable Effect ledger + cross-scope reconciliation pointer (C4/C5).

Revision ID: 0026_effect_ledger
Revises: 0025_agent_access_sessions
Create Date: 2026-07-22

Introduces the typed, durable Effect state machine every outbound connector action goes
through instead of the narrower ``connector_outbox`` claim/finalize/release triple
(migration ``0011_cloud_safety``): ``reserved -> executing -> {confirmed, unknown,
failed}``, with ``unknown`` reachable only from ``executing`` and leaving only through
provider reconciliation (``reconciled_confirmed`` / ``reconciled_absent``). See
``keel_core.effects`` for the complete legal-transition graph and
``keel_core.effect_store``/``keel_core.effect_outbox`` for the store + cross-scope
reconciliation-pointer implementations this schema backs.

* ``effects`` — the authoritative, scope-partitioned (RLS + FORCE RLS, matching
  ``connector_tokens``/``connector_outbox``) ledger row. Identity is ``(scope_id,
  provider, action_name, idempotency_key)`` (``UNIQUE``), so a duplicate logical send
  always resolves to the *same* row and can never issue a second provider mutation. The
  immutable ``action_hash`` binds the exact tool + argument set an approval authorized
  (C5); ``canonical_args`` is either the sorted-key canonical JSON of the call's
  arguments or a ``sha256:<hex>`` digest when the arguments contain a secret-shaped key
  (never the raw secret — mirrors the ``_NO_SECRET_KEYS`` guard in migration
  ``0016_connector_foundation``). The three ``lease_*`` columns are the fenced execution
  lease (``num_nonnulls(...) IN (0, 3)`` — never half-set) that makes
  ``begin_execution`` an atomic single-winner compare-and-set.
* ``effect_reconciliation_outbox`` — a global (non-RLS) cross-scope dispatch pointer,
  exactly the ``patch_proposal_outbox`` (migration ``0021``) pattern: the RLS'd
  ``effects`` table cannot be scanned across every scope by one worker process without
  either a per-scope cron or a privileged ``BYPASSRLS`` scan, so admission/execution
  records a minimal, non-sensitive pointer (``effect_id``, ``scope_id``, ``org_id``,
  ``provider``, a coarse ``kind`` — ``lease_watch`` while an execution lease is
  outstanding, ``reconcile`` once the Effect is ``unknown`` — plus a fenced reconciler
  lease). It carries **no** action args, provider ref, or result. The composite FK
  ``(effect_id, scope_id) -> effects (id, scope_id) ON DELETE CASCADE`` keeps a pointer
  structurally bound to its Effect's own scope and purges it automatically when the
  Effect (and, transitively, its scope) is erased.

Retention/lifecycle: both tables are ``standard``-class operational ledger data (see
``keel_core.lifecycle.policies.EFFECT`` / ``EFFECT_RECONCILIATION_OUTBOX`` and
``docs/DATA-LIFECYCLE.md``) — retained for audit, purged on scope erasure
(``keel_core.effect_store.purge_scope``), never permanent user content.

Reversible: the downgrade drops both tables (the outbox already cascades from
``effects``, so dropping ``effects`` first would already remove it; both are dropped
explicitly here for clarity and to tolerate a partial upgrade).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0026_effect_ledger"
down_revision: str | None = "0025_agent_access_sessions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_EFFECT_STATUSES = (
    "reserved",
    "executing",
    "confirmed",
    "unknown",
    "reconciled_confirmed",
    "reconciled_absent",
    "failed",
)
_OUTBOX_KINDS = ("lease_watch", "reconcile")


def upgrade() -> None:
    op.execute(
        f"""
        CREATE TABLE effects (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            -- Audit/traceability columns (R1B requirement 2). Best-effort: blank for a
            -- legacy/binding-less run or the local-preview scope rather than a hard
            -- failure — the ledger's correctness (identity/transitions/isolation) never
            -- depends on any of these being populated.
            org_id text NOT NULL DEFAULT '',
            agent_id text NOT NULL DEFAULT '',
            actor_id text NOT NULL DEFAULT '',
            run_id text NOT NULL DEFAULT '',
            tool_name text NOT NULL,
            provider text NOT NULL,
            resource_id text NOT NULL DEFAULT '',
            action_name text NOT NULL,
            -- Immutable action-hash binding (C5): keel_core.runs.action_hash(tool, args).
            action_hash text NOT NULL,
            idempotency_key text NOT NULL,
            -- Canonical sorted-key JSON of the call arguments, OR 'sha256:<hex>' when the
            -- arguments contained a secret-shaped key (never the raw secret). Immutable
            -- once written (only ever set at INSERT).
            canonical_args text NOT NULL DEFAULT '',
            status text NOT NULL DEFAULT 'reserved'
                CHECK (status IN ({", ".join(f"'{value}'" for value in _EFFECT_STATUSES)})),
            attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
            -- Fenced execution lease (all-or-nothing): begin_execution's atomic
            -- compare-and-set single-winner claim.
            lease_owner text,
            lease_token text,
            lease_expires_at timestamptz,
            provider_ref text NOT NULL DEFAULT '',
            result text NOT NULL DEFAULT '',
            error text NOT NULL DEFAULT '',
            reconciliation_attempts integer NOT NULL DEFAULT 0
                CHECK (reconciliation_attempts >= 0),
            next_reconciliation_at timestamptz,
            reconciled_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            -- Effect identity: a duplicate logical send always resolves to this exact
            -- row (idempotent create_or_get) and can never issue a second mutation.
            UNIQUE (scope_id, provider, action_name, idempotency_key),
            -- Referenced by the reconciliation outbox's composite FK below.
            UNIQUE (id, scope_id),
            CONSTRAINT ck_effects_lease
                CHECK (num_nonnulls(lease_owner, lease_token, lease_expires_at) IN (0, 3))
        )
        """
    )
    op.execute("CREATE INDEX ix_effects_scope_status ON effects (scope_id, status)")
    op.execute("CREATE INDEX ix_effects_scope_created ON effects (scope_id, created_at DESC)")

    op.execute("ALTER TABLE effects ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON effects "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )
    op.execute("ALTER TABLE effects FORCE ROW LEVEL SECURITY")

    # --- Global (non-RLS) cross-scope reconciliation pointer --------------------------
    op.execute(
        f"""
        CREATE TABLE effect_reconciliation_outbox (
            effect_id text PRIMARY KEY,
            scope_id text NOT NULL,
            org_id text NOT NULL DEFAULT '',
            provider text NOT NULL,
            kind text NOT NULL
                CHECK (kind IN ({", ".join(f"'{value}'" for value in _OUTBOX_KINDS)})),
            due_at timestamptz NOT NULL DEFAULT now(),
            attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            lease_owner text,
            lease_token text,
            lease_expires_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT ck_effect_reconciliation_outbox_lease
                CHECK (num_nonnulls(lease_owner, lease_token, lease_expires_at) IN (0, 3)),
            FOREIGN KEY (effect_id, scope_id)
                REFERENCES effects (id, scope_id) ON DELETE CASCADE
        )
        """
    )
    # Due-scan index (no now() predicate: a volatile function is illegal in an index
    # predicate/expression and would freeze the plan at build time). The reconciler
    # binds the current instant as a parameter instead.
    op.execute(
        "CREATE INDEX ix_effect_reconciliation_outbox_due "
        "ON effect_reconciliation_outbox (due_at, lease_expires_at)"
    )
    op.execute(
        "CREATE INDEX ix_effect_reconciliation_outbox_scope "
        "ON effect_reconciliation_outbox (scope_id)"
    )

    # --- Least-privilege runtime/maintenance grants (deployment-safe) -----------------
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE
                        ON effects, effect_reconciliation_outbox
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime effect ledger grants skipped (insufficient priv)';
                END;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE
                        ON effects, effect_reconciliation_outbox
                        TO keel_maintenance;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE
                        'keel_maintenance effect ledger grants skipped (insufficient priv)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE effects NO FORCE ROW LEVEL SECURITY")
    op.execute("DROP TABLE IF EXISTS effect_reconciliation_outbox CASCADE")
    op.execute("DROP TABLE IF EXISTS effects CASCADE")
