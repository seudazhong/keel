"""Integration: consolidation cursor lease (claim / complete / fail / steal)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.cursor import ConsolidationCursorStore

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 12, 9, 0, tzinfo=UTC)


async def test_claim_is_exclusive_until_released(migrated_db: AsyncEngine) -> None:
    store = ConsolidationCursorStore(migrated_db, "cur:excl")
    lease = await store.claim(_NOW, lease_seconds=600)
    assert lease is not None
    assert lease.last_event_id == 0
    assert await store.claim(_NOW, lease_seconds=600) is None  # busy


async def test_complete_advances_and_releases(migrated_db: AsyncEngine) -> None:
    store = ConsolidationCursorStore(migrated_db, "cur:done")
    lease = await store.claim(_NOW)
    assert lease is not None
    await store.complete(lease, 42, "completed")
    state = await store.get()
    assert state is not None
    assert state.last_event_id == 42
    assert state.last_status == "completed"
    assert state.lease_token is None
    next_lease = await store.claim(_NOW)
    assert next_lease is not None and next_lease.last_event_id == 42


async def test_fail_releases_without_advancing(migrated_db: AsyncEngine) -> None:
    store = ConsolidationCursorStore(migrated_db, "cur:fail")
    lease = await store.claim(_NOW)
    assert lease is not None
    await store.fail(lease, "error")
    state = await store.get()
    assert state is not None
    assert state.last_event_id == 0
    assert state.last_status == "error"
    reclaimed = await store.claim(_NOW)
    assert reclaimed is not None and reclaimed.last_event_id == 0


async def test_expired_lease_can_be_stolen(migrated_db: AsyncEngine) -> None:
    store = ConsolidationCursorStore(migrated_db, "cur:steal")
    first = await store.claim(_NOW, lease_seconds=600)
    assert first is not None
    assert await store.claim(_NOW, lease_seconds=600) is None
    stolen = await store.claim(_NOW + timedelta(seconds=601), lease_seconds=600)
    assert stolen is not None and stolen.token != first.token
