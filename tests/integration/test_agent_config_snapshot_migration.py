"""Live-Postgres verification of migration 0024 (Agent config snapshot columns).

Migration 0024 adds three additive columns to ``runs`` (``snapshot_schema_version``,
``snapshot_json``, ``snapshot_hash``) that persist the immutable Agent configuration snapshot
captured at admission (R1B, INVARIANTS.md C8). This exercises the real migration against a live
database:

* a pre-existing run row (inserted before 0024, i.e. without the new columns) reads back safely
  with the documented backfill defaults after upgrading — ``RunRecord.snapshot`` treats the
  empty ``snapshot_hash`` default as "no snapshot captured" rather than a real one;
* a fresh admission through :class:`~keel_core.runs.PostgresRunStore` persists a real snapshot
  that round-trips byte-for-byte (the canonical JSON re-hashes to the stored hash);
* downgrading back to the prior head cleanly drops the columns, and re-upgrading is reversible.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.agent_config_snapshot import AgentConfigSnapshot, MemoryPolicySnapshot
from keel_core.runs import PostgresRunStore, RunBudgetSpec, RunSurface

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PREV = "0023_oauth_state_metadata"
_SCOPE = "web:local"


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


async def test_pre_existing_run_reads_back_safely_after_upgrade(migrated_db: AsyncEngine) -> None:
    """A run row inserted before 0024 upgrades with safe backfill defaults, never a snapshot
    that falsely claims to have been captured."""
    now = datetime.now(UTC)
    async with migrated_db.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO runs (id, scope_id, org_id, actor, agent_id, session_id, surface, "
                "idempotency_key, expires_at) VALUES (:id, :scope, :org, :actor, :agent, "
                ":session, :surface, :key, :expires)"
            ),
            {
                "id": "pre-existing-run",
                "scope": _SCOPE,
                "org": "org-1",
                "actor": "user-1",
                "agent": "agent-1",
                "session": "sess-1",
                "surface": RunSurface.web.value,
                "key": "legacy-key",
                "expires": now + timedelta(hours=1),
            },
        )
    store = PostgresRunStore(migrated_db, _SCOPE)
    record = await store.get("pre-existing-run")
    assert record is not None
    # Documented backfill defaults.
    assert record.snapshot_schema_version == 1
    assert record.snapshot_json == "{}"
    assert record.snapshot_hash == ""
    # A legacy row (empty hash) is "no snapshot captured", never a falsely-parsed empty one.
    assert record.snapshot is None


async def test_fresh_admission_persists_a_snapshot_that_round_trips(
    migrated_db: AsyncEngine,
) -> None:
    """A snapshot admitted through the real store re-hashes identically on read (integrity)."""
    store = PostgresRunStore(migrated_db, _SCOPE)
    snapshot = AgentConfigSnapshot(
        agent_id="agent-1",
        agent_version=3,
        agent_name="Scout",
        persona="Be terse.",
        model="gpt-5",
        max_iterations=12,
        token_budget=5000,
        permission_profile="default",
        tools=("read", "write"),
        memory_policy=MemoryPolicySnapshot(archival_enabled=True),
    )
    record, created = await store.create(
        run_id="run-with-snapshot",
        scope_id=_SCOPE,
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-2",
        surface=RunSurface.web.value,
        idempotency_key="k-snap",
        budget=RunBudgetSpec(max_iterations=12, token_budget=5000),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        snapshot=snapshot,
    )
    assert created is True
    assert record.snapshot_hash == snapshot.content_hash()
    reloaded = await store.get("run-with-snapshot")
    assert reloaded is not None
    restored = reloaded.snapshot
    assert restored is not None
    assert restored == snapshot
    assert restored.content_hash() == snapshot.content_hash()


async def test_downgrade_drops_columns_and_reupgrade_restores_them(
    migrated_db: AsyncEngine,
) -> None:
    """0024 downgrades cleanly (columns dropped) and re-upgrades reversibly."""
    url = os.environ["KEEL_TEST_DATABASE_URL"]

    async def _snapshot_columns() -> set[str]:
        async with migrated_db.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'runs' AND column_name LIKE 'snapshot_%'"
                    )
                )
            ).scalars()
            return set(rows)

    assert await _snapshot_columns() == {
        "snapshot_schema_version",
        "snapshot_json",
        "snapshot_hash",
    }
    await asyncio.to_thread(_run_to, url, _PREV)
    assert await _snapshot_columns() == set()
    await asyncio.to_thread(_run_to, url, "head")
    assert await _snapshot_columns() == {
        "snapshot_schema_version",
        "snapshot_json",
        "snapshot_hash",
    }
