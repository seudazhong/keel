"""Durable event store tests (real Postgres): persistence, resume, scope isolation."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.agents import AgentSpec, Scope
from keel_core.errors import CrossScopeError
from keel_core.events import Event, EventType
from keel_core.loop import admit, run
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ProviderChunk
from keel_core.state import PostgresEventStore, append_event_in_transaction
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, PermissionDecision, ScopeKind, StopReason

pytestmark = pytest.mark.integration
_TEST_ALLOW_ALL = RuleBasedPermissionEngine([Rule("*", PermissionDecision.allow)])


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
    result = await run(
        agent=agent,
        session_id=session_id,
        store=store,
        provider=provider,
        permissions=_TEST_ALLOW_ALL,
    )
    assert result.reason is StopReason.completed

    types = [e.type async for e in PostgresEventStore(migrated_db, "u:1").read(session_id)]
    assert types[0] == EventType.message_token  # the admitted user turn survived
    assert EventType.run_started in types
    assert types[-1] == EventType.run_ended


async def test_append_event_allocates_sequences_in_outer_transaction(
    migrated_db: AsyncEngine,
) -> None:
    session_id = f"s-{uuid.uuid4().hex}"
    first = _msg(session_id, "A", "first")
    second = _msg(session_id, "A", "second")

    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', 'A', true)"))
        assert await append_event_in_transaction(conn, first) == 1
        assert await append_event_in_transaction(conn, second) == 2
        assert first.seq == 0
        assert second.seq == 0
        sequences = (
            await conn.execute(
                text(
                    "SELECT seq FROM events WHERE session_id = :sid AND scope_id = 'A' ORDER BY seq"
                ),
                {"sid": session_id},
            )
        ).scalars()
        assert list(sequences) == [1, 2]

    assert [e.seq async for e in PostgresEventStore(migrated_db, "A").read(session_id)] == [
        1,
        2,
    ]


async def test_append_event_in_outer_transaction_rolls_back(
    migrated_db: AsyncEngine,
) -> None:
    session_id = f"s-{uuid.uuid4().hex}"
    event = _msg(session_id, "A", "rolled back")

    with pytest.raises(RuntimeError, match="force rollback"):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SELECT set_config('app.scope_id', 'A', true)"))
            assert await append_event_in_transaction(conn, event) == 1
            raise RuntimeError("force rollback")

    assert event.seq == 0
    store = PostgresEventStore(migrated_db, "A")
    assert [row async for row in store.read(session_id)] == []
    replacement = _msg(session_id, "A", "replacement")
    await store.append(replacement)
    assert replacement.seq == 1


async def test_append_event_can_require_an_existing_same_scope_session(
    migrated_db: AsyncEngine,
) -> None:
    missing = _msg(f"missing-{uuid.uuid4().hex}", "A", "no implicit session")
    with pytest.raises(LookupError, match="session"):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SELECT set_config('app.scope_id', 'A', true)"))
            await append_event_in_transaction(conn, missing, require_existing_session=True)

    session_id = f"s-{uuid.uuid4().hex}"
    await PostgresEventStore(migrated_db, "A").append(_msg(session_id, "A", "seed"))
    event = _msg(session_id, "A", "injected")
    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', 'A', true)"))
        assert await append_event_in_transaction(conn, event, require_existing_session=True) == 2
    assert [e.seq async for e in PostgresEventStore(migrated_db, "A").read(session_id)] == [
        1,
        2,
    ]


async def test_existing_session_guard_is_scope_safe(migrated_db: AsyncEngine) -> None:
    session_id = f"s-{uuid.uuid4().hex}"
    scope_a = PostgresEventStore(migrated_db, "A")
    await scope_a.append(_msg(session_id, "A", "seed"))

    # Composite (scope_id, id) identity: appending with require_existing under scope B — which
    # has no session of this id — is a plain "does not exist in this scope", not a cross-scope
    # conflict (the same external id in scope A is a different, isolated session).
    with pytest.raises(LookupError):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SELECT set_config('app.scope_id', 'B', true)"))
            await append_event_in_transaction(
                conn,
                _msg(session_id, "B", "cross-scope"),
                require_existing_session=True,
            )

    next_event = _msg(session_id, "A", "next")
    await scope_a.append(next_event)
    assert next_event.seq == 2
    assert [e async for e in PostgresEventStore(migrated_db, "B").read(session_id)] == []


async def test_transactional_append_rejects_guc_mismatch_and_scopes_reuse_ids(
    migrated_db: AsyncEngine,
) -> None:
    # The GUC guard still rejects an event whose scope does not match the active app.scope_id.
    with pytest.raises(CrossScopeError):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SELECT set_config('app.scope_id', 'A', true)"))
            await append_event_in_transaction(conn, _msg("mismatch", "B", "no"))

    # But an external session id already used by scope A can be freely reused by scope B: the
    # composite (scope_id, id) identity makes it a distinct, isolated session (M3.6 finding 2).
    session_id = f"s-{uuid.uuid4().hex}"
    scope_a = PostgresEventStore(migrated_db, "A")
    await scope_a.append(_msg(session_id, "A", "seed"))
    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', 'B', true)"))
        b_seq = await append_event_in_transaction(conn, _msg(session_id, "B", "b-own"))
    assert b_seq == 1  # scope B's own fresh session sequence, independent of scope A

    next_event = _msg(session_id, "A", "next")
    await scope_a.append(next_event)
    assert next_event.seq == 2  # scope A's sequence is untouched by scope B's reuse
    read_a = PostgresEventStore(migrated_db, "A").read(session_id)
    read_b = PostgresEventStore(migrated_db, "B").read(session_id)
    texts_a = [e.payload["text"] async for e in read_a]
    texts_b = [e.payload["text"] async for e in read_b]
    assert texts_a == ["seed", "next"] and texts_b == ["b-own"]  # fully isolated logs
