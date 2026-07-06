"""Durable event store tests (real Postgres): persistence, resume, scope isolation."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.agents import AgentSpec, Scope
from keel_core.errors import CrossScopeError
from keel_core.events import Event, EventType
from keel_core.loop import admit, run
from keel_core.protocols import ProviderChunk
from keel_core.state import PostgresEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, ScopeKind, StopReason

pytestmark = pytest.mark.integration


def _msg(session_id: str, scope_id: str, text: str) -> Event:
    return Event(
        type=EventType.message_token,
        seq=0,  # assigned by the store
        session_id=session_id,
        scope_id=scope_id,
        ts=datetime.now(UTC),
        payload={"role": "user", "text": text},
    )


async def test_append_read_and_resume(migrated_db: AsyncEngine) -> None:
    session_id = f"s-{uuid.uuid4().hex}"
    store = PostgresEventStore(migrated_db, "A")
    for i in range(1, 4):
        await store.append(_msg(session_id, "A", f"m{i}"))

    assert [e.seq async for e in store.read(session_id)] == [1, 2, 3]  # atomic per-session seq
    assert [e.seq async for e in store.read(session_id, after=1)] == [2, 3]

    # Resume: a fresh store over the same DB sees the durable log (0 lost).
    resumed = PostgresEventStore(migrated_db, "A")
    assert [e.payload["text"] async for e in resumed.read(session_id)] == ["m1", "m2", "m3"]


async def test_scope_bound_store_cannot_read_other_scope(migrated_db: AsyncEngine) -> None:
    session_id = f"s-{uuid.uuid4().hex}"
    await PostgresEventStore(migrated_db, "A").append(_msg(session_id, "A", "secret"))
    other = PostgresEventStore(migrated_db, "B")
    assert [e async for e in other.read(session_id)] == []  # scope A's session is invisible


async def test_append_rejects_foreign_scope(migrated_db: AsyncEngine) -> None:
    store = PostgresEventStore(migrated_db, "A")
    with pytest.raises(CrossScopeError):
        await store.append(_msg("s1", "B", "x"))  # event scope != store scope


async def test_loop_persists_and_resumes(migrated_db: AsyncEngine) -> None:
    session_id = f"s-{uuid.uuid4().hex}"
    store = PostgresEventStore(migrated_db, "u:1")
    agent = AgentSpec(
        id="a", name="A", model="test/model", scope=Scope(id="u:1", kind=ScopeKind.personal)
    )
    await admit(store, session_id, "u:1", "hello")
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="hi", finish_reason=FinishReason.end_turn)]]
    )
    result = await run(agent=agent, session_id=session_id, store=store, provider=provider)
    assert result.reason is StopReason.completed

    types = [e.type async for e in PostgresEventStore(migrated_db, "u:1").read(session_id)]
    assert types[0] == EventType.message_token  # the admitted user turn survived
    assert EventType.run_started in types
    assert types[-1] == EventType.run_ended
