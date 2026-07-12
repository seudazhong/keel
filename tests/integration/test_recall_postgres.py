"""Integration: semantic session-recall projection and ranking."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


async def test_message_embeddings_schema_and_rls(migrated_db: AsyncEngine) -> None:
    async with migrated_db.connect() as conn:
        table_count = await conn.scalar(
            text(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'message_embeddings'"
            )
        )
        rls = await conn.scalar(
            text("SELECT relrowsecurity FROM pg_class WHERE relname = 'message_embeddings'")
        )
        columns = {
            str(row.column_name)
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'public' "
                        "AND table_name = 'message_embeddings'"
                    )
                )
            )
        }

    assert table_count == 1
    assert rls is True
    assert columns == {
        "event_id",
        "scope_id",
        "session_id",
        "seq",
        "role",
        "content",
        "model",
        "dim",
        "embedding",
        "created_at",
    }
