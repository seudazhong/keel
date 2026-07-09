"""Integration: migration 0005 tables + Postgres approval/schedule stores (scoped)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


async def test_schedules_and_approvals_tables_exist(migrated_db: AsyncEngine) -> None:
    async with migrated_db.connect() as conn:
        for table in ("schedules", "approvals"):
            n = await conn.scalar(
                text("select count(*) from information_schema.tables where table_name = :t"),
                {"t": table},
            )
            assert n == 1


async def test_postgres_approval_round_trip(migrated_db: AsyncEngine) -> None:
    from keel_core.approvals import PostgresApprovalStore

    store = PostgresApprovalStore(migrated_db, "u:1")
    aid = await store.create_pending(
        scope_id="u:1",
        run_id="r1",
        session_id="s1",
        tool="email.send",
        args={"to": "x"},
        call_id="c1",
        idempotency_key="k1",
        reason="tainted",
        expires_at=datetime.now(UTC) + timedelta(hours=24),
    )
    rec = await store.get(aid)
    assert rec is not None and rec.status == "pending" and rec.args == {"to": "x"}
    assert [r.id for r in await store.list_pending("u:1")] == [aid]
    assert await store.resolve(aid, "granted", "me") is True
    assert await store.resolve(aid, "denied", "me") is False  # single-shot
    other = PostgresApprovalStore(migrated_db, "u:2")
    assert await other.list_pending("u:2") == []  # scope-isolated


async def test_postgres_approval_expiry(migrated_db: AsyncEngine) -> None:
    from keel_core.approvals import PostgresApprovalStore

    store = PostgresApprovalStore(migrated_db, "u:1")
    aid = await store.create_pending(
        scope_id="u:1",
        run_id="r1",
        session_id="s1",
        tool="email.send",
        args={},
        call_id="c1",
        idempotency_key="k1",
        reason="tainted",
        expires_at=datetime.now(UTC) - timedelta(minutes=1),  # already past
    )
    assert await store.expire_due(datetime.now(UTC)) == [aid]
    rec = await store.get(aid)
    assert rec is not None and rec.status == "expired"


async def test_postgres_claim_is_compare_and_set(migrated_db: AsyncEngine) -> None:
    from keel_scheduler.store import PostgresClaimStore, PostgresScheduleStore

    t0 = datetime(2026, 7, 7, 9, 0, tzinfo=UTC)
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id','u:1',true)"))
        await conn.execute(
            text(
                "insert into schedules(id,scope_id,agent_id,session_id,trigger_kind,spec,"
                "next_run_at,interval_s,enabled) values "
                "('d','u:1','digest','digest:u:1','interval','86400',:t,86400,true)"
            ),
            {"t": t0},
        )
    claim = PostgresClaimStore(migrated_db, "u:1")
    assert await claim.claim("d", t0, t0 + timedelta(days=1)) is True
    assert await claim.claim("d", t0, t0 + timedelta(days=1)) is False  # stale expected
    assert await PostgresScheduleStore(migrated_db, "u:1").due(t0) == []  # advanced past t0


async def test_concurrent_claims_yield_exactly_one_winner(migrated_db: AsyncEngine) -> None:
    """N simultaneous ticks (worker replicas) claim a due schedule at most once (I9).

    The scale-out safety property: with the scheduler_tick cron firing on every worker
    replica, only one replica's compare-and-set on ``next_run_at`` may win, so a due
    schedule is enqueued exactly once regardless of the number of workers.
    """
    import asyncio

    from keel_scheduler.store import PostgresClaimStore

    t0 = datetime(2026, 7, 7, 9, 0, tzinfo=UTC)
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id','u:1',true)"))
        await conn.execute(
            text(
                "insert into schedules(id,scope_id,agent_id,session_id,trigger_kind,spec,"
                "next_run_at,interval_s,enabled) values "
                "('d','u:1','digest','digest:u:1','interval','86400',:t,86400,true)"
            ),
            {"t": t0},
        )
    claim = PostgresClaimStore(migrated_db, "u:1")
    new = t0 + timedelta(days=1)
    results = await asyncio.gather(*(claim.claim("d", t0, new) for _ in range(8)))
    assert results.count(True) == 1  # exactly one replica wins the race
    assert results.count(False) == 7
