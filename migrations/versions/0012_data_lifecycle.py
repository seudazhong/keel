"""data lifecycle: durable erasure requests/steps, event tombstones, retention.

Revision ID: 0012_data_lifecycle
Revises: 0011_cloud_safety
Create Date: 2026-07-17

M3.5 retention + erasure (WS-K). Durable, restart-safe, idempotent data erasure and
retention metadata:

* ``erasure_requests`` — one durable row per erasure request (scope / session / project
  granularity). Deduped by ``(scope_id, idempotency_key)`` so a repeated request resolves
  to the same job. ``external_incomplete`` records that an external provider/telemetry
  step could not be verified (the request then finishes ``partial``, never ``completed``).
* ``erasure_steps`` — per-step ledger for an in-flight erasure so a crash/resume skips
  already-``done`` steps and every store cleanup is idempotent + observable.
* ``event_tombstones`` — a scope/session marker recorded when a session's events are
  erased, so a projection rebuild (``keel_core.rebuild``) withholds any residual event and
  erased content can never be resurrected from a stale event source (Redis backlog, etc).
* ``retention_policies`` — durable per-scope retention overrides (class + TTL) layered
  over the typed defaults in ``keel_core.lifecycle.policies``.

Every table here is scope-bound and carries Row-Level Security keyed by the
``app.scope_id`` GUC (ADR-0009 / DESIGN-REVIEW G16), mirroring ``sessions``/``events``:
the erasure coordinator always sets ``app.scope_id`` before touching them. Global dedup
tables (``webhook_deliveries``) are deliberately left out of scope erasure — they hold no
personal content and are preserved as shared configuration.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0012_data_lifecycle"
down_revision: str | None = "0011_cloud_safety"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCOPED_TABLES = (
    "erasure_requests",
    "erasure_steps",
    "event_tombstones",
    "retention_policies",
)


def upgrade() -> None:
    # --- Durable erasure requests (scope / session / project granularity) -------------
    op.execute(
        """
        CREATE TABLE erasure_requests (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            target_kind text NOT NULL
                CHECK (target_kind IN ('scope', 'session', 'project')),
            target_id text,
            idempotency_key text NOT NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'running', 'completed', 'partial', 'failed')),
            requested_by text,
            reason text,
            external_incomplete boolean NOT NULL DEFAULT false,
            attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            last_error text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz,
            CHECK (
                (target_kind = 'scope' AND target_id IS NULL)
                OR (target_kind <> 'scope' AND target_id IS NOT NULL)
            ),
            UNIQUE (scope_id, idempotency_key)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_erasure_requests_scope_status ON erasure_requests (scope_id, status)"
    )
    op.execute(
        "CREATE INDEX ix_erasure_requests_pending ON erasure_requests (updated_at) "
        "WHERE status IN ('pending', 'running')"
    )

    # --- Per-step ledger for idempotent, resumable, observable erasure ----------------
    op.execute(
        """
        CREATE TABLE erasure_steps (
            request_id text NOT NULL
                REFERENCES erasure_requests(id) ON DELETE CASCADE,
            scope_id text NOT NULL,
            step text NOT NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'done', 'skipped', 'unsupported', 'failed')),
            rows_affected bigint NOT NULL DEFAULT 0 CHECK (rows_affected >= 0),
            detail text,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (request_id, step)
        )
        """
    )
    op.execute("CREATE INDEX ix_erasure_steps_scope ON erasure_steps (scope_id)")

    # --- Session tombstones: erased content cannot be resurrected on rebuild -----------
    op.execute(
        """
        CREATE TABLE event_tombstones (
            scope_id text NOT NULL,
            session_id text NOT NULL,
            request_id text,
            reason text,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (scope_id, session_id)
        )
        """
    )

    # --- Durable per-scope retention overrides (layered over typed code defaults) -------
    op.execute(
        """
        CREATE TABLE retention_policies (
            scope_id text NOT NULL,
            resource_class text NOT NULL,
            retention_class text NOT NULL,
            ttl_seconds bigint CHECK (ttl_seconds IS NULL OR ttl_seconds >= 0),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (scope_id, resource_class)
        )
        """
    )

    for table in _SCOPED_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY scope_isolation ON {table} "
            "USING (scope_id = current_setting('app.scope_id', true)) "
            "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
        )


def downgrade() -> None:
    for table in reversed(_SCOPED_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
