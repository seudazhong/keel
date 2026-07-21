"""Agent Access edges + session ownership/visibility (R1B).

Revision ID: 0025_agent_access_sessions
Revises: 0024_agent_config_snapshot
Create Date: 2026-07-22

R1B foundation for two related-but-independent authorization axes named in
docs/ARCHITECTURE.md / docs/IDENTITY.md and INVARIANTS.md C2/C6:

1. **Agent Access** (``agent_access``) — a first-class ``(org, agent, principal) -> level``
   edge for **team** Agents. ``principal_type`` is ``user`` (a durable :class:`User`) or
   ``channel`` (an opaque, caller-defined identity — e.g. an IM chat/room key); ``level`` is
   ``discover`` < ``use`` < ``manage`` (a higher tier implies the lower ones, enforced in
   :mod:`keel_core.identity.authz`, never in SQL). Before this migration, bare org
   ``memberships`` role alone (``read``/``use`` capability) let *every* active member
   discover/use *every* team Agent in the org — this table + the accompanying authz change
   close that: a member/viewer now needs an active edge here (an org admin/owner keeps an
   explicit administrative path, unaffected by this schema). Personal Agents never carry
   edges (they stay owner-private). A composite FK to ``agents (id, org_id)`` makes a
   cross-org edge structurally impossible (the same confused-deputy defense as
   ``resource_grants`` in 0013). RLS mirrors 0013's tenant-owned tables: ``ENABLE`` +
   ``FORCE ROW LEVEL SECURITY`` keyed by the ``app.org_id`` GUC.

2. **Session ownership / visibility** (additive ``sessions`` columns + ``session_access``) —
   independent of Agent access: using a team Agent must never by itself grant reading another
   user's private session. Additive, nullable columns on ``sessions``:

   * ``org_id`` — best-effort denormalized org id (nullable; ``SET NULL`` on org erasure) used
     only to let the *identity* layer reason about a session's org without re-parsing
     ``scope_id``; the durable data-plane authority for isolation remains ``scope_id``/RLS
     (INVARIANTS.md C6 — scope is partition, not authority).
   * ``owner_user_id`` — the durable user a session belongs to (nullable; ``SET NULL`` on user
     erasure so a session is never silently deleted by identity erasure — lifecycle/purge owns
     session-content erasure separately).
   * ``channel_provider`` / ``channel_external_id`` — a paired (both-or-neither, enforced by
     CHECK) channel identity for an IM-admitted session (e.g. a Slack/Feishu chat), independent
     of any single human owner.
   * ``visibility`` — ``private`` (default for every NEW row) | ``agent_members`` | ``explicit``.
     Existing rows are explicitly backfilled to ``agent_members`` (see below) — never silently
     defaulted to ``private``, which would newly lock legitimate existing readers out of
     sessions that pre-date this column (a real regression, not a security fix): the
     application's ``can_view_session`` (:mod:`keel_core.session_visibility`) additionally
     special-cases an owner-less, channel-less row as "pre-R1B" and falls back to the exact
     pre-migration behavior (Agent-scope gating only), so this backfill value is consistent
     with what such a row already effectively allowed.

   ``session_access`` is the explicit per-user share edge for ``visibility = 'explicit'``. Its
   composite FK to ``sessions (scope_id, id)`` (safe: 0017 gave ``sessions`` that exact
   composite unique key) makes a share edge for a nonexistent/foreign-scope session
   structurally impossible. RLS is keyed by ``app.scope_id`` (matching ``sessions``/``events``
   from 0002) with ``ENABLE`` + ``FORCE ROW LEVEL SECURITY``.

Nothing here changes ``scope_id`` derivation, RLS keying, or FK direction on any existing
table; every new column is nullable or has a safe default so this migration is a no-op for
data that predates it beyond the one explicit, documented backfill UPDATE above.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0025_agent_access_sessions"
down_revision: str | None = "0024_agent_config_snapshot"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- Agent Access (tenant-owned; team Agents) -------------------------------------
    op.execute(
        """
        CREATE TABLE agent_access (
            id text PRIMARY KEY,
            org_id text NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            agent_id text NOT NULL,
            principal_type text NOT NULL CHECK (principal_type IN ('user', 'channel')),
            principal_id text NOT NULL,
            principal_user_id text REFERENCES users(id) ON DELETE CASCADE,
            level text NOT NULL CHECK (level IN ('discover', 'use', 'manage')),
            grantor_user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            revoked_at timestamptz,
            -- Composite FK pins the edge's agent to the SAME org (confused-deputy defense,
            -- identical pattern to resource_grants in 0013).
            FOREIGN KEY (agent_id, org_id) REFERENCES agents(id, org_id) ON DELETE CASCADE,
            CHECK (
                (principal_type = 'user' AND principal_user_id = principal_id)
                OR (principal_type = 'channel' AND principal_user_id IS NULL)
            ),
            UNIQUE (org_id, agent_id, principal_type, principal_id)
        )
        """
    )
    op.execute("CREATE INDEX ix_agent_access_agent ON agent_access (org_id, agent_id)")
    op.execute(
        "CREATE INDEX ix_agent_access_principal "
        "ON agent_access (org_id, principal_type, principal_id)"
    )

    op.execute("ALTER TABLE agent_access ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY org_isolation ON agent_access "
        "USING (org_id = current_setting('app.org_id', true)) "
        "WITH CHECK (org_id = current_setting('app.org_id', true))"
    )
    op.execute("ALTER TABLE agent_access FORCE ROW LEVEL SECURITY")

    # --- Session ownership / channel identity / visibility (additive) ----------------
    op.execute(
        "ALTER TABLE sessions ADD COLUMN org_id text "
        "REFERENCES organizations(id) ON DELETE SET NULL"
    )
    op.execute(
        "ALTER TABLE sessions ADD COLUMN owner_user_id text REFERENCES users(id) ON DELETE SET NULL"
    )
    op.execute("ALTER TABLE sessions ADD COLUMN channel_provider text")
    op.execute("ALTER TABLE sessions ADD COLUMN channel_external_id text")
    op.execute("ALTER TABLE sessions ADD COLUMN visibility text")
    # Explicit backfill (never the eventual 'private' default): see module docstring — this
    # preserves the pre-migration access shape for every session that already exists.
    op.execute("UPDATE sessions SET visibility = 'agent_members' WHERE visibility IS NULL")
    op.execute("ALTER TABLE sessions ALTER COLUMN visibility SET DEFAULT 'private'")
    op.execute("ALTER TABLE sessions ALTER COLUMN visibility SET NOT NULL")
    op.execute(
        "ALTER TABLE sessions ADD CONSTRAINT ck_sessions_visibility "
        "CHECK (visibility IN ('private', 'agent_members', 'explicit'))"
    )
    op.execute(
        "ALTER TABLE sessions ADD CONSTRAINT ck_sessions_channel_pair "
        "CHECK ((channel_provider IS NULL) = (channel_external_id IS NULL))"
    )
    op.execute(
        "CREATE INDEX ix_sessions_owner ON sessions (owner_user_id) WHERE owner_user_id IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX ix_sessions_channel ON sessions (channel_provider, channel_external_id) "
        "WHERE channel_provider IS NOT NULL"
    )

    # --- Explicit per-user session shares (scope-owned) -------------------------------
    op.execute(
        """
        CREATE TABLE session_access (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            session_id text NOT NULL,
            user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            granted_by_user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            revoked_at timestamptz,
            -- Composite FK pins the share to a session that actually exists in this exact
            -- scope (structurally safe — sessions carries the matching (scope_id, id) PK
            -- since 0017).
            FOREIGN KEY (scope_id, session_id) REFERENCES sessions (scope_id, id)
                ON DELETE CASCADE,
            UNIQUE (scope_id, session_id, user_id)
        )
        """
    )
    op.execute("CREATE INDEX ix_session_access_session ON session_access (scope_id, session_id)")
    op.execute("CREATE INDEX ix_session_access_user ON session_access (user_id)")

    op.execute("ALTER TABLE session_access ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON session_access "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )
    op.execute("ALTER TABLE session_access FORCE ROW LEVEL SECURITY")

    # --- non-owner runtime grants (guarded) -------------------------------------------
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        agent_access, session_access
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE
                        'keel_runtime agent_access/session_access grants skipped '
                        '(insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS session_access CASCADE")
    op.execute("DROP INDEX IF EXISTS ix_sessions_channel")
    op.execute("DROP INDEX IF EXISTS ix_sessions_owner")
    op.execute("ALTER TABLE sessions DROP CONSTRAINT IF EXISTS ck_sessions_channel_pair")
    op.execute("ALTER TABLE sessions DROP CONSTRAINT IF EXISTS ck_sessions_visibility")
    op.execute("ALTER TABLE sessions DROP COLUMN IF EXISTS visibility")
    op.execute("ALTER TABLE sessions DROP COLUMN IF EXISTS channel_external_id")
    op.execute("ALTER TABLE sessions DROP COLUMN IF EXISTS channel_provider")
    op.execute("ALTER TABLE sessions DROP COLUMN IF EXISTS owner_user_id")
    op.execute("ALTER TABLE sessions DROP COLUMN IF EXISTS org_id")
    op.execute("DROP TABLE IF EXISTS agent_access CASCADE")
