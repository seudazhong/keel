"""cleanup_scope removes only the target scope's rows; guard rechecks the live DB name."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_worker.evals.database import (
    EvalDatabaseError,
    assert_current_database,
    assert_eval_database_name,
    case_scope,
    cleanup_scope,
)

pytestmark = pytest.mark.integration


async def test_cleanup_scope_is_scoped(migrated_db: AsyncEngine) -> None:
    scope = case_scope("v1", "db-smoke")
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :s, true)"), {"s": scope}
        )
        await conn.execute(
            text(
                "INSERT INTO memory_blocks (scope_id, key, value, version) "
                "VALUES (:s, 'human', 'x', 1)"
            ),
            {"s": scope},
        )
    await cleanup_scope(migrated_db, scope)
    async with migrated_db.connect() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :s, true)"), {"s": scope}
        )
        remaining = await conn.scalar(
            text("SELECT count(*) FROM memory_blocks WHERE scope_id = :s"), {"s": scope}
        )
    assert remaining == 0


async def test_assert_current_database_rejects_live(migrated_db: AsyncEngine) -> None:
    # migrated_db is keel_test; monkeypatch the recheck to simulate a live name.
    with pytest.raises(EvalDatabaseError):
        assert_eval_database_name("keel")
    await assert_current_database(migrated_db)  # keel_test passes
