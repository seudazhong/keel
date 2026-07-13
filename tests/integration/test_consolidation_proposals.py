"""Integration: memory proposal store — idempotency + atomic CAS apply."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.proposals import MemoryProposalStore, ProposalOutcome
from keel_core.memory import PostgresMemoryStore

pytestmark = pytest.mark.integration


async def test_propose_is_idempotent(migrated_db: AsyncEngine) -> None:
    store = MemoryProposalStore(migrated_db, "prop:idem")
    pid1, created1 = await store.propose(
        block="human",
        proposed_value="likes tea",
        reason="stated",
        confidence=0.9,
        source_event_ids=[1, 2],
    )
    pid2, created2 = await store.propose(
        block="human",
        proposed_value="Tea is the user's preferred drink.",
        reason="stated",
        confidence=0.9,
        source_event_ids=[2, 1],
    )
    assert created1 is True
    assert created2 is False
    assert pid1 == pid2
    assert len(await store.list_proposals(status="pending")) == 1


async def test_approve_creates_block_when_absent(migrated_db: AsyncEngine) -> None:
    store = MemoryProposalStore(migrated_db, "prop:new")
    memory = PostgresMemoryStore(migrated_db, "prop:new")
    pid, _ = await store.propose(
        block="human",
        proposed_value="likes tea",
        reason="stated",
        confidence=0.9,
        source_event_ids=[1],
    )
    resolution = await store.approve(pid, "reviewer")
    assert resolution.outcome is ProposalOutcome.applied
    assert resolution.version == 1
    assert await memory.get("human") == "likes tea"
    assert (await memory.versions())["human"] == 1
    assert await memory.history("human") == [(1, "likes tea")]


async def test_approve_updates_existing_block(migrated_db: AsyncEngine) -> None:
    memory = PostgresMemoryStore(migrated_db, "prop:upd")
    await memory.set("persona", "v1")  # version 1
    store = MemoryProposalStore(migrated_db, "prop:upd")
    pid, _ = await store.propose(
        block="persona",
        proposed_value="v2",
        reason="clarified",
        confidence=0.9,
        source_event_ids=[1],
    )
    resolution = await store.approve(pid, "reviewer")
    assert resolution.outcome is ProposalOutcome.applied
    assert resolution.version == 2
    assert await memory.get("persona") == "v2"


async def test_approve_goes_stale_when_block_changed(migrated_db: AsyncEngine) -> None:
    memory = PostgresMemoryStore(migrated_db, "prop:stale")
    store = MemoryProposalStore(migrated_db, "prop:stale")
    pid, _ = await store.propose(
        block="human",
        proposed_value="likes tea",
        reason="stated",
        confidence=0.9,
        source_event_ids=[1],
    )  # expected_version 0 (block absent)
    await memory.set("human", "written directly")  # now version 1
    resolution = await store.approve(pid, "reviewer")
    assert resolution.outcome is ProposalOutcome.stale
    assert await memory.get("human") == "written directly"


async def test_reject_and_double_resolve(migrated_db: AsyncEngine) -> None:
    store = MemoryProposalStore(migrated_db, "prop:rej")
    pid, _ = await store.propose(
        block="human",
        proposed_value="likes tea",
        reason="stated",
        confidence=0.9,
        source_event_ids=[1],
    )
    assert (await store.reject(pid, "reviewer")).outcome is ProposalOutcome.rejected
    assert (await store.approve(pid, "reviewer")).outcome is ProposalOutcome.already_resolved
    assert (await store.approve("missing", "reviewer")).outcome is ProposalOutcome.not_found
