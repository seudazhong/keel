"""Schema assertions for migration 0008 (memory consolidation)."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


async def _columns(engine: AsyncEngine, table: str) -> dict[str, str]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = :t"
                ),
                {"t": table},
            )
        ).all()
    return {str(r.column_name): str(r.data_type) for r in rows}


async def test_consolidation_cursors_table(migrated_db: AsyncEngine) -> None:
    cols = await _columns(migrated_db, "consolidation_cursors")
    assert cols["scope_id"] == "text"
    assert cols["last_event_id"] == "bigint"
    assert "lease_token" in cols
    assert "lease_expires_at" in cols


async def test_memory_proposals_table(migrated_db: AsyncEngine) -> None:
    cols = await _columns(migrated_db, "memory_proposals")
    assert cols["block"] == "text"
    assert cols["expected_version"] == "integer"
    assert cols["source_event_ids"] == "ARRAY"
    assert cols["status"] == "text"


async def test_archival_provenance_columns(migrated_db: AsyncEngine) -> None:
    cols = await _columns(migrated_db, "archival")
    assert cols["origin"] == "text"
    assert "content_hash" in cols
    assert cols["source_event_ids"] == "ARRAY"


async def test_archival_content_hash_unique_index(migrated_db: AsyncEngine) -> None:
    async with migrated_db.connect() as conn:
        exists = (
            await conn.execute(
                text("SELECT 1 FROM pg_indexes WHERE indexname = 'ix_archival_scope_content_hash'")
            )
        ).one_or_none()
    assert exists is not None
