"""immutable Agent configuration snapshot on durable runs

Revision ID: 0024_agent_config_snapshot
Revises: 0023_oauth_state_metadata
Create Date: 2026-07-21

R1B: every durable run persists an immutable, schema-versioned snapshot of the exact Agent
configuration it was admitted against (docs/INVARIANTS.md C8) — the persisted Agent's
optimistic ``version``, its name/persona, the selected model, the bounded budget
(``max_iterations`` / ``token_budget``), a permission-profile identifier, the admitted
tool-name set, a (currently minimal) memory-policy object, and any admitted resource-grant
descriptors (non-secret: type/id/capability only). A worker reconstructs the run's
name/persona/model/bounded config from this frozen record instead of the Agent's later-mutated
fields; a later persona/model edit can never rewrite an already-admitted run.

Additive columns on ``runs``:

* ``snapshot_schema_version`` — the payload shape version (see
  ``keel_core.agent_config_snapshot.SNAPSHOT_SCHEMA_VERSION``).
* ``snapshot_json`` — the exact canonical JSON bytes (sorted keys, no whitespace) that were
  hashed; stored as ``text`` (not ``jsonb``) so the persisted bytes are never reformatted by
  Postgres and always re-hash to the same value on read (integrity/tamper detection depends on
  byte-for-byte round-tripping).
* ``snapshot_hash`` — a sha256 over ``snapshot_json``, folded into the admission fingerprint so
  a retried admission with a changed Agent version/persona/model/budget/snapshot fails closed
  as a conflict (never silently repaired).

Defaults (``1`` / ``'{}'`` / ``''``) make the migration safe on populated databases: an existing
run reads back with an empty ``snapshot_hash``, which ``RunRecord.snapshot`` treats as "no
snapshot captured" (a pre-migration row) rather than attempting to parse ``'{}'`` as a real
snapshot — it is never confused with a genuinely-captured empty-content snapshot (which always
hashes to a non-empty value).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0024_agent_config_snapshot"
down_revision: str | None = "0023_oauth_state_metadata"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE runs ADD COLUMN snapshot_schema_version integer NOT NULL DEFAULT 1 "
        "CHECK (snapshot_schema_version >= 1)"
    )
    op.execute("ALTER TABLE runs ADD COLUMN snapshot_json text NOT NULL DEFAULT '{}'")
    op.execute("ALTER TABLE runs ADD COLUMN snapshot_hash text NOT NULL DEFAULT ''")


def downgrade() -> None:
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS snapshot_hash")
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS snapshot_json")
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS snapshot_schema_version")
