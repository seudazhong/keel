"""Integration: consolidation worker flow (busy, skipped) + seed idempotency."""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import get_settings
from keel_core.consolidation.cursor import ConsolidationCursorStore
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ProviderChunk
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_scheduler.store import PostgresScheduleStore, ScheduleRow
from keel_worker.main import consolidate_memory

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_NOW = datetime(2026, 7, 12, 9, 0, tzinfo=UTC)


def _row(scope: str) -> ScheduleRow:
    return ScheduleRow(
        id=f"memory-consolidation:{scope}",
        scope_id=scope,
        agent_id="memory-consolidator",
        session_id=f"consolidation:{scope}",
        trigger_kind="interval",
        spec="86400",
        next_run_at=_NOW,
        interval_s=86400,
    )


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
                "payload": json.dumps({"role": role, "text": content}),
            },
        )
    ).one()
    return int(row.id)


async def _seed_schedule(engine: AsyncEngine, scope: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        await conn.execute(
            text(
                "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                "spec, next_run_at, interval_s, enabled) VALUES "
                "(:id, :s, 'memory-consolidator', :sess, 'interval', '86400', now(), 86400, true) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": f"memory-consolidation:{scope}", "s": scope, "sess": f"consolidation:{scope}"},
        )


def _ctx(engine: AsyncEngine, scope: str) -> dict[str, Any]:
    return {
        "engine": engine,
        "provider": ScriptedProviderGateway(
            [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
        ),
        "embedder": FakeEmbedder(),
        "schedules": PostgresScheduleStore(engine, scope),
    }


async def test_consolidate_skips_below_min(migrated_db: AsyncEngine) -> None:
    scope = "wrk:skip"
    await _seed_schedule(migrated_db, scope)
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        await _emit(conn, scope, "chat:a", 1, "user", "just one message")

    result = await consolidate_memory(_ctx(migrated_db, scope), _row(scope), get_settings())

    assert result == "skipped"
    state = await ConsolidationCursorStore(migrated_db, scope).get()
    assert state is not None
    assert state.last_status == "skipped"
    assert state.last_event_id == 0


async def test_consolidate_reports_busy_when_leased(migrated_db: AsyncEngine) -> None:
    scope = "wrk:busy"
    # Pre-take a live lease so the worker's own claim loses.
    held = await ConsolidationCursorStore(migrated_db, scope).claim(datetime.now(UTC))
    assert held is not None

    result = await consolidate_memory(_ctx(migrated_db, scope), _row(scope), get_settings())

    assert result == "busy"


async def test_seed_consolidation_schedule_is_idempotent(migrated_db: AsyncEngine) -> None:
    spec = importlib.util.spec_from_file_location(
        "seed_consolidation_schedule",
        _REPO_ROOT / "scripts" / "seed_consolidation_schedule.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    scope = "wrk:seed"
    first = await module.seed(scope, migrated_db)
    second = await module.seed(scope, migrated_db)

    assert first == second == f"memory-consolidation:{scope}"
    rows = await PostgresScheduleStore(migrated_db, scope).list_all()
    assert [r.id for r in rows] == [first]
    assert rows[0].agent_id == "memory-consolidator"
