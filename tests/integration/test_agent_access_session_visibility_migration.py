"""Live-Postgres verification of migration 0025 (Agent Access + session ownership/visibility).

Migration 0025 adds:

* ``agent_access`` — team-Agent discover/use/manage edges (composite FK to
  ``agents(id, org_id)``, unique per ``(org, agent, principal_type, principal_id)``).
* additive ``sessions`` columns (``org_id``, ``owner_user_id``, ``channel_provider``,
  ``channel_external_id``, ``visibility``) — existing rows are explicitly backfilled to
  ``visibility = 'agent_members'`` (never silently defaulted to ``'private'``, which would newly
  lock out legitimate existing readers), while the column's own ``DEFAULT`` for any *new* row is
  ``'private'``.
* ``session_access`` — explicit per-user session shares (composite FK to
  ``sessions(scope_id, id)``).

This exercises the real migration against a live database: pre-existing data backfills safely,
new rows get fail-closed defaults, structural FKs reject cross-org/orphan rows, and the migration
downgrades/re-upgrades cleanly.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PREV = "0024_agent_config_snapshot"


def _alembic_cfg(url: str):  # type: ignore[no-untyped-def]
    from alembic.config import Config

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _run_to(url: str, revision: str) -> None:
    from alembic import command

    cfg = _alembic_cfg(url)
    if revision == "head":
        command.upgrade(cfg, "head")
    else:
        command.downgrade(cfg, revision)


async def _seed_org_agent(engine: AsyncEngine) -> tuple[str, str, str]:
    """Insert a bare user/org/agent row set (RLS off) and return ``(user_id, org_id, agent_id)``."""
    user_id = f"usr-{uuid.uuid4().hex[:8]}"
    org_id = f"org-{uuid.uuid4().hex[:8]}"
    agent_id = f"agt-{uuid.uuid4().hex[:8]}"
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO users(id, display_name, status) VALUES (:id, 'U', 'active')"),
            {"id": user_id},
        )
        await conn.execute(
            text(
                "INSERT INTO organizations(id, slug, display_name, status) "
                "VALUES (:id, :id, :id, 'active')"
            ),
            {"id": org_id},
        )
        await conn.execute(
            text(
                "INSERT INTO agents(id, org_id, kind, owner_user_id, name) "
                "VALUES (:id, :org, 'team', :owner, :id)"
            ),
            {"id": agent_id, "org": org_id, "owner": user_id},
        )
    return user_id, org_id, agent_id


async def test_new_session_row_has_no_owner_or_channel_by_default(
    migrated_db: AsyncEngine,
) -> None:
    """A plain insert (no identity columns set) leaves owner/channel NULL — only the explicit
    ``ensure_session_identity`` admission path sets them (see keel_core.session_visibility)."""
    scope_id = f"agent:new-{uuid.uuid4().hex[:8]}/agt-1"
    session_id = f"s-{uuid.uuid4().hex[:8]}"
    async with migrated_db.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id, next_seq) VALUES (:id, :scope, 1)"),
            {"id": session_id, "scope": scope_id},
        )
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT visibility, owner_user_id, channel_provider FROM sessions "
                        "WHERE id = :id AND scope_id = :scope"
                    ),
                    {"id": session_id, "scope": scope_id},
                )
            )
            .mappings()
            .one()
        )
    assert row["owner_user_id"] is None
    assert row["channel_provider"] is None


async def test_new_session_defaults_to_private_visibility(migrated_db: AsyncEngine) -> None:
    scope_id = f"agent:new-{uuid.uuid4().hex[:8]}/agt-1"
    session_id = f"s-{uuid.uuid4().hex[:8]}"
    async with migrated_db.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id, next_seq) VALUES (:id, :scope, 1)"),
            {"id": session_id, "scope": scope_id},
        )
        visibility = await conn.scalar(
            text("SELECT visibility FROM sessions WHERE id = :id AND scope_id = :scope"),
            {"id": session_id, "scope": scope_id},
        )
    assert visibility == "private"


async def test_agent_access_composite_fk_rejects_cross_org_agent(
    migrated_db: AsyncEngine,
) -> None:
    user_id, org_a, agent_a = await _seed_org_agent(migrated_db)
    _user_b, org_b, _agent_b = await _seed_org_agent(migrated_db)
    with pytest.raises(IntegrityError):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SET row_security = off"))
            await conn.execute(
                text(
                    "INSERT INTO agent_access "
                    "(id, org_id, agent_id, principal_type, principal_id, principal_user_id, "
                    "level, grantor_user_id) "
                    "VALUES (:id, :org, :agent, 'user', :principal, :principal, 'use', :grantor)"
                ),
                {
                    "id": f"aac-{uuid.uuid4().hex[:8]}",
                    "org": org_b,  # wrong org for agent_a
                    "agent": agent_a,
                    "principal": user_id,
                    "grantor": user_id,
                },
            )


async def test_agent_access_unique_per_org_agent_principal(migrated_db: AsyncEngine) -> None:
    user_id, org_id, agent_id = await _seed_org_agent(migrated_db)
    async with migrated_db.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text(
                "INSERT INTO agent_access "
                "(id, org_id, agent_id, principal_type, principal_id, principal_user_id, "
                "level, grantor_user_id) "
                "VALUES (:id, :org, :agent, 'user', :principal, :principal, 'use', :grantor)"
            ),
            {
                "id": f"aac-{uuid.uuid4().hex[:8]}",
                "org": org_id,
                "agent": agent_id,
                "principal": user_id,
                "grantor": user_id,
            },
        )
    with pytest.raises(IntegrityError):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SET row_security = off"))
            await conn.execute(
                text(
                    "INSERT INTO agent_access "
                    "(id, org_id, agent_id, principal_type, principal_id, principal_user_id, "
                    "level, grantor_user_id) "
                    "VALUES (:id, :org, :agent, 'user', :principal, :principal, 'manage', :grantor)"
                ),
                {
                    "id": f"aac-{uuid.uuid4().hex[:8]}",
                    "org": org_id,
                    "agent": agent_id,
                    "principal": user_id,
                    "grantor": user_id,
                },
            )


async def test_user_principal_access_cascades_when_the_user_is_erased(
    migrated_db: AsyncEngine,
) -> None:
    grantor_id, org_id, agent_id = await _seed_org_agent(migrated_db)
    principal_id = f"usr-{uuid.uuid4().hex[:8]}"
    access_id = f"aac-{uuid.uuid4().hex[:8]}"
    async with migrated_db.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO users(id, display_name, status) VALUES (:id, 'P', 'active')"),
            {"id": principal_id},
        )
        await conn.execute(
            text(
                "INSERT INTO agent_access "
                "(id, org_id, agent_id, principal_type, principal_id, principal_user_id, "
                "level, grantor_user_id) "
                "VALUES (:id, :org, :agent, 'user', :principal, :principal, 'use', :grantor)"
            ),
            {
                "id": access_id,
                "org": org_id,
                "agent": agent_id,
                "principal": principal_id,
                "grantor": grantor_id,
            },
        )
        await conn.execute(text("DELETE FROM users WHERE id = :id"), {"id": principal_id})
        remaining = await conn.scalar(
            text("SELECT count(*) FROM agent_access WHERE id = :id"), {"id": access_id}
        )
    assert remaining == 0


async def test_session_access_composite_fk_rejects_orphan_session(
    migrated_db: AsyncEngine,
) -> None:
    _user_id, _org_id, _agent_id = await _seed_org_agent(migrated_db)
    user_id2 = f"usr-{uuid.uuid4().hex[:8]}"
    async with migrated_db.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO users(id, display_name, status) VALUES (:id, 'U2', 'active')"),
            {"id": user_id2},
        )
    with pytest.raises(IntegrityError):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SET row_security = off"))
            await conn.execute(
                text(
                    "INSERT INTO session_access "
                    "(id, scope_id, session_id, user_id, granted_by_user_id) "
                    "VALUES (:id, :scope, :sid, :user, :grantor)"
                ),
                {
                    "id": f"shr-{uuid.uuid4().hex[:8]}",
                    "scope": "agent:nonexistent/agt-x",
                    "sid": "no-such-session",
                    "user": user_id2,
                    "grantor": user_id2,
                },
            )


async def test_downgrade_drops_new_objects_and_reupgrade_restores_them(
    migrated_db: AsyncEngine,
) -> None:
    """0025 downgrades cleanly (new table/columns dropped) and re-upgrades reversibly, and the
    backfill runs again identically on re-upgrade for any row present at that time."""
    url = os.environ["KEEL_TEST_DATABASE_URL"]

    async def _table_exists(name: str) -> bool:
        async with migrated_db.connect() as conn:
            return bool(
                await conn.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                        "WHERE table_name = :name)"
                    ),
                    {"name": name},
                )
            )

    async def _session_columns() -> set[str]:
        async with migrated_db.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'sessions' AND column_name IN "
                        "('org_id', 'owner_user_id', 'channel_provider', "
                        "'channel_external_id', 'visibility')"
                    )
                )
            ).scalars()
            return set(rows)

    assert await _table_exists("agent_access")
    assert await _table_exists("session_access")
    assert await _session_columns() == {
        "org_id",
        "owner_user_id",
        "channel_provider",
        "channel_external_id",
        "visibility",
    }

    # A session present BEFORE the downgrade/re-upgrade cycle...
    scope_id = f"agent:cycle-{uuid.uuid4().hex[:8]}/agt-1"
    session_id = f"s-{uuid.uuid4().hex[:8]}"
    async with migrated_db.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id, next_seq) VALUES (:id, :scope, 1)"),
            {"id": session_id, "scope": scope_id},
        )

    await asyncio.to_thread(_run_to, url, _PREV)
    assert not await _table_exists("agent_access")
    assert not await _table_exists("session_access")
    assert await _session_columns() == set()

    await asyncio.to_thread(_run_to, url, "head")
    assert await _table_exists("agent_access")
    assert await _table_exists("session_access")
    assert await _session_columns() == {
        "org_id",
        "owner_user_id",
        "channel_provider",
        "channel_external_id",
        "visibility",
    }
    # ...re-upgrading re-ran the backfill: the row that predates THIS upgrade got the
    # 'agent_members' backfill, not the new-row 'private' default.
    async with migrated_db.connect() as conn:
        await conn.execute(text("SET row_security = off"))
        visibility = await conn.scalar(
            text("SELECT visibility FROM sessions WHERE id = :id AND scope_id = :scope"),
            {"id": session_id, "scope": scope_id},
        )
    assert visibility == "agent_members"
