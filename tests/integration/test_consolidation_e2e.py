"""End-to-end acceptance: consolidation dispatch -> tools -> persistence (spec §17)."""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation import (
    ConsolidationCursorStore,
    MemoryProposalStore,
    consolidation_schedule_id,
)
from keel_core.embeddings import FakeEmbedder
from keel_core.memory import PostgresMemoryStore
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_scheduler.store import PostgresScheduleStore
from keel_worker.main import run_agent

pytestmark = pytest.mark.integration

_SET_SCOPE = text("select set_config('app.scope_id', :s, true)")


async def _emit(conn: Any, scope: str, session_id: str, seq: int, role: str, content: str) -> int:
    row = (
        await conn.execute(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts, payload) VALUES "
                "(:session, :scope, :seq, 'message.token', now(), CAST(:payload AS jsonb)) "
                "RETURNING id"
            ),
            {
                "session": session_id,
                "scope": scope,
                "seq": seq,
                "payload": json.dumps({"role": role, "text": content, "partial": False}),
            },
        )
    ).one()
    return int(row.id)


async def _seed_schedule(engine: AsyncEngine, scope: str) -> str:
    schedule_id = consolidation_schedule_id(scope)
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        await conn.execute(
            text(
                "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                "spec, next_run_at, interval_s, enabled) VALUES "
                "(:id, :s, 'memory-consolidator', :sess, 'interval', '86400', now(), 86400, true) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": schedule_id, "s": scope, "sess": f"consolidation:{scope}"},
        )
    return schedule_id


def _ctx(engine: AsyncEngine, scope: str, provider: ScriptedProviderGateway) -> dict[str, Any]:
    return {
        "engine": engine,
        "provider": provider,
        "embedder": FakeEmbedder(),
        "schedules": PostgresScheduleStore(engine, scope),
    }


def _scripted(*, propose_ids: list[int], archival_ids: list[int]) -> ScriptedProviderGateway:
    """A run that proposes a 'human' rewrite, inserts one archival fact, then ends."""
    return ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c1",
                        name="memory_propose_rewrite",
                        arguments={
                            "block": "human",
                            "proposed_value": "Prefers tea in the morning.",
                            "reason": "the user said so",
                            "confidence": 0.9,
                            "source_event_ids": propose_ids,
                        },
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c2",
                        name="archival_consolidate_insert",
                        arguments={
                            "content": "The user prefers tea in the morning.",
                            "confidence": 0.95,
                            "source_event_ids": archival_ids,
                        },
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )


async def _archival(engine: AsyncEngine, scope: str) -> list[tuple[str, bool, list[int]]]:
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        rows = (
            await conn.execute(
                text(
                    "SELECT origin, content_hash, source_event_ids FROM archival "
                    "WHERE scope_id = :s ORDER BY id"
                ),
                {"s": scope},
            )
        ).all()
    return [
        (str(r.origin), r.content_hash is not None, [int(i) for i in (r.source_event_ids or [])])
        for r in rows
    ]


async def test_e2e_consolidation_persists_then_dedupes(migrated_db: AsyncEngine) -> None:
    scope = f"e2e:{uuid.uuid4().hex}"
    session = f"chat:{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        ids = [
            await _emit(conn, scope, session, i, "user" if i % 2 else "assistant", f"message {i}")
            for i in range(1, 11)
        ]
    user_id, max_id = ids[0], ids[-1]
    schedule_id = await _seed_schedule(migrated_db, scope)

    result = await run_agent(
        _ctx(migrated_db, scope, _scripted(propose_ids=[user_id], archival_ids=[user_id])),
        schedule_id,
    )
    assert result == "completed"

    # Core memory is unchanged: the rewrite is a *proposal* awaiting human review.
    assert await PostgresMemoryStore(migrated_db, scope).get("human") is None
    proposals = await MemoryProposalStore(migrated_db, scope).list_proposals(status="pending")
    assert len(proposals) == 1
    assert proposals[0].block == "human"
    assert proposals[0].source_event_ids == [user_id]

    assert await _archival(migrated_db, scope) == [("consolidation", True, [user_id])]

    cursor = await ConsolidationCursorStore(migrated_db, scope).get()
    assert cursor is not None
    assert cursor.last_event_id == max_id
    assert cursor.last_status == "completed"

    # Re-run the identical batch (reset the cursor): idempotent proposal + archival merge.
    async with migrated_db.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        await conn.execute(
            text("DELETE FROM consolidation_cursors WHERE scope_id = :s"), {"s": scope}
        )

    rerun = await run_agent(
        _ctx(migrated_db, scope, _scripted(propose_ids=[user_id], archival_ids=[user_id])),
        schedule_id,
    )
    assert rerun == "completed"
    still = await MemoryProposalStore(migrated_db, scope).list_proposals(status="pending")
    assert len(still) == 1  # ON CONFLICT DO NOTHING -> the same proposal
    assert await _archival(migrated_db, scope) == [("consolidation", True, [user_id])]


async def test_e2e_validation_error_blocks_cursor(migrated_db: AsyncEngine) -> None:
    scope = f"e2e:{uuid.uuid4().hex}"
    session = f"chat:{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        for i in range(1, 11):
            await _emit(conn, scope, session, i, "user", f"message {i}")
    schedule_id = await _seed_schedule(migrated_db, scope)

    # The model cites an event id that is not in the batch -> both tools fail validation.
    result = await run_agent(
        _ctx(migrated_db, scope, _scripted(propose_ids=[999999], archival_ids=[999999])),
        schedule_id,
    )
    assert result == "error"

    cursor = await ConsolidationCursorStore(migrated_db, scope).get()
    assert cursor is not None
    assert cursor.last_event_id == 0  # NOT advanced: the batch is safely retried next tick
    assert cursor.last_status == "error"
    assert await MemoryProposalStore(migrated_db, scope).list_proposals() == []
    assert await _archival(migrated_db, scope) == []
