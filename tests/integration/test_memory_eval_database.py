from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_worker.evals.database import (
    EvalDatabaseError,
    assert_current_database,
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


async def test_assert_current_database_rejects_live() -> None:
    fake_conn = AsyncMock()
    fake_conn.scalar = AsyncMock(return_value="keel")
    fake_engine = MagicMock()
    fake_engine.connect = MagicMock()
    fake_engine.connect.__aenter__ = AsyncMock(return_value=fake_conn)
    fake_engine.connect.__aexit__ = AsyncMock(return_value=None)

    with pytest.raises(EvalDatabaseError):
        await assert_current_database(fake_engine)


async def test_assert_current_database_accepts_eval_test(
    migrated_db: AsyncEngine,
) -> None:
    await assert_current_database(migrated_db)
