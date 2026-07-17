"""Live-Postgres verification of migration 0016 up/down (web-routing rollback compatibility).

0016 makes session identity composite ``(scope_id, id)`` so two scopes may own the *same*
external session id. Its ``downgrade`` restores the pre-0016 *global* session namespace, which
cannot represent such a collision — so before restoring the global constraints it must
deterministically remap every non-canonical scoped session to a stable, collision-free id and
repoint all referencing rows in lock-step, with no data loss.

This exercises the real migration against a live database: seed identical session ids in two
scopes (plus attached events and runs and a non-colliding control session), downgrade one
revision, and assert the collision is collapsed losslessly and every event/run stays attached
to the correct session. Then re-upgrade to head to prove reversibility.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_HEAD = "0016_web_routing_isolation"
_PREV = "0015_projects_github"

_SCOPE_A = "agent:acme/support"
_SCOPE_B = "agent:globex/support"
_DUP = "external-session-42"
_SOLO = "external-session-solo"


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


def _expected_remap(scope_id: str, old_id: str) -> str:
    """Mirror the deterministic derivation in 0016.downgrade for assertion."""
    base = old_id[:180]
    digest = hashlib.md5(f"{scope_id}|{old_id}".encode()).hexdigest()  # noqa: S324
    return f"{base}.scope-{digest}"


async def _seed(engine: AsyncEngine) -> None:
    expires = datetime.now(UTC) + timedelta(hours=1)
    ts = datetime.now(UTC)
    async with engine.begin() as conn:
        # Two scopes owning the *same* external session id — only representable post-0016.
        for scope in (_SCOPE_A, _SCOPE_B):
            await conn.execute(
                text("INSERT INTO sessions (id, scope_id) VALUES (:id, :scope)"),
                {"id": _DUP, "scope": scope},
            )
            # An event whose payload names its scope, so we can prove it stays attached.
            await conn.execute(
                text(
                    "INSERT INTO events (session_id, scope_id, seq, type, ts) "
                    "VALUES (:sid, :scope, 1, :type, :ts)"
                ),
                {"sid": _DUP, "scope": scope, "type": f"marker::{scope}", "ts": ts},
            )
            await conn.execute(
                text(
                    "INSERT INTO runs (id, scope_id, org_id, actor, agent_id, session_id, "
                    "surface, idempotency_key, expires_at) VALUES (:id, :scope, :org, :actor, "
                    ":agent, :sid, 'web', :key, :expires)"
                ),
                {
                    "id": f"run-{scope}",
                    "scope": scope,
                    "org": scope.split(":")[1].split("/")[0],
                    "actor": "user-1",
                    "agent": "agent-1",
                    "sid": _DUP,
                    "key": "k1",
                    "expires": expires,
                },
            )
        # A non-colliding control session in scope A — must be left untouched by the remap.
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id) VALUES (:id, :scope)"),
            {"id": _SOLO, "scope": _SCOPE_A},
        )
        await conn.execute(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts) "
                "VALUES (:sid, :scope, 1, :type, :ts)"
            ),
            {"sid": _SOLO, "scope": _SCOPE_A, "type": "marker::solo", "ts": ts},
        )


async def test_downgrade_remaps_cross_scope_session_collisions(migrated_db: AsyncEngine) -> None:
    url = os.environ["KEEL_TEST_DATABASE_URL"]
    await _seed(migrated_db)

    await asyncio.to_thread(_run_to, url, _PREV)

    # Canonical holder = lexicographically smallest scope for the id (scope A keeps the id);
    # scope B is deterministically remapped.
    remapped = _expected_remap(_SCOPE_B, _DUP)
    async with migrated_db.connect() as conn:
        # 1) The global sessions PK (id) is restored and every id is now globally unique.
        pk_cols = (
            (
                await conn.execute(
                    text(
                        "SELECT a.attname FROM pg_index i "
                        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                        "WHERE i.indrelid = 'sessions'::regclass AND i.indisprimary"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert list(pk_cols) == ["id"]

        rows = (await conn.execute(text("SELECT scope_id, id FROM sessions ORDER BY id"))).all()
        ids = [r.id for r in rows]
        assert len(ids) == len(set(ids)), f"session ids not globally unique after downgrade: {ids}"

        by_scope = {(r.scope_id, r.id) for r in rows}
        assert (_SCOPE_A, _DUP) in by_scope  # canonical scope kept the original id
        assert (_SCOPE_B, remapped) in by_scope  # colliding scope was renamed deterministically
        assert (_SCOPE_A, _SOLO) in by_scope  # non-colliding control untouched
        assert (_SCOPE_B, _DUP) not in by_scope  # the collision is gone

        # 2) No event was detached: each scope's marker event points at that scope's session id.
        events = (await conn.execute(text("SELECT scope_id, session_id, type FROM events"))).all()
        attached = {(e.scope_id, e.session_id, e.type) for e in events}
        assert (_SCOPE_A, _DUP, f"marker::{_SCOPE_A}") in attached
        assert (_SCOPE_B, remapped, f"marker::{_SCOPE_B}") in attached
        assert (_SCOPE_A, _SOLO, "marker::solo") in attached
        # Every event still resolves to an existing session row in its own scope.
        for e in events:
            assert (e.scope_id, e.session_id) in by_scope

        # 3) The FORCE-RLS runs table was repointed across *both* scopes (proves the FORCE toggle).
        runs = (await conn.execute(text("SELECT scope_id, session_id FROM runs"))).all()
        run_map = {r.scope_id: r.session_id for r in runs}
        assert run_map[_SCOPE_A] == _DUP
        assert run_map[_SCOPE_B] == remapped

    # 4) Reversible: re-upgrade to head restores composite identity with the data intact.
    await asyncio.to_thread(_run_to, url, "head")
    async with migrated_db.connect() as conn:
        pk_cols = (
            (
                await conn.execute(
                    text(
                        "SELECT a.attname FROM pg_index i "
                        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                        "WHERE i.indrelid = 'sessions'::regclass AND i.indisprimary"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert set(pk_cols) == {"scope_id", "id"}
        count = await conn.scalar(text("SELECT count(*) FROM sessions"))
        assert count == 3


async def test_downgrade_is_idempotent_and_noop_without_collisions(
    migrated_db: AsyncEngine,
) -> None:
    """No cross-scope collision -> the remap is a pure no-op and downgrade succeeds cleanly."""
    url = os.environ["KEEL_TEST_DATABASE_URL"]
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id) VALUES (:id, :scope)"),
            {"id": _SOLO, "scope": _SCOPE_A},
        )

    await asyncio.to_thread(_run_to, url, _PREV)
    async with migrated_db.connect() as conn:
        rows = (await conn.execute(text("SELECT scope_id, id FROM sessions"))).all()
        assert [(r.scope_id, r.id) for r in rows] == [(_SCOPE_A, _SOLO)]

    # Restore head for subsequent tests.
    await asyncio.to_thread(_run_to, url, "head")
