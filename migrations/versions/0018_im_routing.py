"""durable OneBot/Telegram (IM) channel routing, safe admission, and reply outbox.

Revision ID: 0018_im_routing
Revises: 0017_web_routing_isolation
Create Date: 2026-07-18

Adds the durable data plane for untrusted IM (OneBot QQ, Telegram) routing:

* ``im_channel_mappings`` — the **org-owned** binding from a provider + external bot/account +
  chat/user identity to exactly one org, persisted Agent, canonical data-plane scope and reply
  policy. Org-partitioned under ``FORCE ROW LEVEL SECURITY`` on ``app.org_id``, with a composite
  FK ``(agent_id, org_id) -> agents(id, org_id)`` so a mapping can only ever bind an Agent in its
  *own* org (cross-org Agent binding is impossible), plus ``ON DELETE CASCADE`` from the org so
  org erasure removes every mapping. The **run-as** org member the worker executes under is a
  separate, required ``run_as_user_id`` bound by a composite FK
  ``(org_id, run_as_user_id) -> memberships(org_id, user_id)`` so a mapping can only run as a
  member of its own org (the platform admin that ``created_by``-provisions it is only an audit
  identity, never the run actor). Personal vs group is a first-class column; status / version /
  audit columns carry revocation.

* ``im_route_index`` — a **minimal, global, opaque** route/capability index read by a pre-tenant
  webhook *before* any org/scope is bound: only an opaque ``route_key`` (a SHA-256 of provider +
  bot + chat — never the plaintext ids), the owning org + scope + agent, a coarse status and a
  ``reply_allowed`` capability flag. It holds **no** credentials and **no** message content, so
  it is deliberately **not** under RLS (the one index looked up across tenants). ``mapping_id``
  cascades from ``im_channel_mappings`` so revoking/erasing a mapping invalidates its route
  immediately.

* ``im_reply_intents`` — the durable, **scope-partitioned** reply outbox: one idempotent terminal
  reply intent bound to run + provider + account + chat + message id, with the reply text stored
  **encrypted** (envelope ciphertext + key id) at rest, plus a fenced lease and delivery result.
  ``FORCE ROW LEVEL SECURITY`` on ``app.scope_id``; ``UNIQUE (scope_id, idempotency_key)`` makes
  re-recording (a repaired crash window) a no-op.

* ``im_reply_dispatch_index`` — a **global** reply-dispatch pointer (``reply_id`` + ``scope_id``
  + a fenced reconciler lease), mirroring ``run_dispatch_outbox`` (0017): it carries **no**
  content, only the routing key a restart-safe sender needs to find which scopes have a reply to
  deliver. ``reply_id`` cascades from ``im_reply_intents`` so a scope erasure that deletes the
  reply rows removes the dispatch pointer too.

All four tables are reversible; the downgrade drops them (children before parents via CASCADE).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0018_im_routing"
down_revision: str | None = "0017_web_routing_isolation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The secret-key guard reused for the mapping policy JSON so no external credential can ever be
# smuggled into the (org-readable) mapping row.
_NO_SECRET_KEYS = (
    "secret",
    "token",
    "password",
    "client_secret",
    "private_key",
    "api_key",
    "access_token",
    "refresh_token",
    "app_secret",
    "encrypt_key",
    "verification_token",
)


def _no_secret_check(column: str) -> str:
    keys = ",".join(f"'{key}'" for key in _NO_SECRET_KEYS)
    return f"CHECK (NOT ({column} ?| ARRAY[{keys}]))"


def upgrade() -> None:
    # --- org-owned channel mappings (RLS on app.org_id) -------------------------------
    op.execute(
        f"""
        CREATE TABLE im_channel_mappings (
            id text PRIMARY KEY,
            org_id text NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
            provider text NOT NULL CHECK (provider IN ('onebot', 'telegram')),
            external_bot_id text NOT NULL,
            external_chat_id text NOT NULL,
            chat_kind text NOT NULL CHECK (chat_kind IN ('personal', 'group')),
            agent_id text NOT NULL,
            scope_id text NOT NULL,
            run_as_user_id text NOT NULL,
            policy jsonb NOT NULL DEFAULT '{{}}'::jsonb {_no_secret_check("policy")},
            status text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'disabled', 'revoked')),
            version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
            created_by text NOT NULL DEFAULT '',
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            revoked_by text NOT NULL DEFAULT '',
            revoked_at timestamptz,
            UNIQUE (id, org_id),
            UNIQUE (org_id, provider, external_bot_id, external_chat_id),
            FOREIGN KEY (agent_id, org_id)
                REFERENCES agents (id, org_id) ON DELETE CASCADE,
            FOREIGN KEY (org_id, run_as_user_id)
                REFERENCES memberships (org_id, user_id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_im_channel_mappings_org_provider "
        "ON im_channel_mappings (org_id, provider, status)"
    )
    op.execute("CREATE INDEX ix_im_channel_mappings_scope ON im_channel_mappings (scope_id)")
    op.execute(
        "CREATE INDEX ix_im_channel_mappings_run_as ON im_channel_mappings (org_id, run_as_user_id)"
    )

    op.execute("ALTER TABLE im_channel_mappings ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY org_isolation ON im_channel_mappings "
        "USING (org_id = current_setting('app.org_id', true)) "
        "WITH CHECK (org_id = current_setting('app.org_id', true))"
    )
    op.execute("ALTER TABLE im_channel_mappings FORCE ROW LEVEL SECURITY")

    # --- global opaque route/capability index (no RLS, no content/credentials) --------
    op.execute(
        """
        CREATE TABLE im_route_index (
            route_key text PRIMARY KEY,
            org_id text NOT NULL,
            scope_id text NOT NULL,
            mapping_id text NOT NULL REFERENCES im_channel_mappings (id) ON DELETE CASCADE,
            agent_id text NOT NULL,
            chat_kind text NOT NULL CHECK (chat_kind IN ('personal', 'group')),
            status text NOT NULL CHECK (status IN ('active', 'disabled', 'revoked')),
            reply_allowed boolean NOT NULL DEFAULT true,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX ix_im_route_index_scope ON im_route_index (scope_id)")
    op.execute("CREATE INDEX ix_im_route_index_mapping ON im_route_index (mapping_id)")

    # --- durable reply outbox (scope-partitioned, encrypted payload, RLS on app.scope_id) --
    op.execute(
        """
        CREATE TABLE im_reply_intents (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            run_id text NOT NULL,
            org_id text NOT NULL,
            provider text NOT NULL CHECK (provider IN ('onebot', 'telegram')),
            external_bot_id text NOT NULL,
            external_chat_id text NOT NULL,
            external_message_id text NOT NULL DEFAULT '',
            chat_kind text NOT NULL CHECK (chat_kind IN ('personal', 'group')),
            reply_kind text NOT NULL DEFAULT 'final'
                CHECK (reply_kind IN ('final', 'partial')),
            idempotency_key text NOT NULL,
            key_id text NOT NULL,
            ciphertext text NOT NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'leased', 'sent', 'failed')),
            attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            provider_message_id text NOT NULL DEFAULT '',
            error text NOT NULL DEFAULT '',
            lease_token text NOT NULL DEFAULT '',
            lease_expires_at timestamptz,
            next_attempt_at timestamptz NOT NULL DEFAULT now(),
            delivered_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (scope_id, idempotency_key),
            UNIQUE (scope_id, id)
        )
        """
    )
    op.execute("CREATE INDEX ix_im_reply_intents_run ON im_reply_intents (scope_id, run_id)")

    op.execute("ALTER TABLE im_reply_intents ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON im_reply_intents "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )
    op.execute("ALTER TABLE im_reply_intents FORCE ROW LEVEL SECURITY")

    # --- global reply-dispatch index (no RLS, no content) -----------------------------
    op.execute(
        """
        CREATE TABLE im_reply_dispatch_index (
            reply_id text PRIMARY KEY REFERENCES im_reply_intents (id) ON DELETE CASCADE,
            scope_id text NOT NULL,
            state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'leased')),
            attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            next_attempt_at timestamptz NOT NULL DEFAULT now(),
            lease_owner text,
            lease_expires_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_im_reply_dispatch_due "
        "ON im_reply_dispatch_index (next_attempt_at) WHERE state = 'pending'"
    )

    # --- non-owner runtime grants (guarded; global indices carry no RLS) --------------
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        im_channel_mappings, im_route_index,
                        im_reply_intents, im_reply_dispatch_index
                        TO keel_runtime;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime im-routing grants skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    # Children (route index, reply dispatch) cascade from their parents; drop explicitly in
    # reverse dependency order so a re-run of up/down/up is clean.
    op.execute("DROP TABLE IF EXISTS im_reply_dispatch_index CASCADE")
    op.execute("DROP TABLE IF EXISTS im_reply_intents CASCADE")
    op.execute("DROP TABLE IF EXISTS im_route_index CASCADE")
    op.execute("DROP TABLE IF EXISTS im_channel_mappings CASCADE")
