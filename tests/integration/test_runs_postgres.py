"""Postgres integration tests for the durable run store + approval binding (M3.6).

Proves the load-bearing concurrency + isolation guarantees against a live Postgres:

* admission idempotency (a retried request creates no duplicate run),
* two workers racing a claim — exactly one wins,
* lease expiry -> reclaim -> fenced-out prior owner cannot write,
* idempotent terminalization,
* durable interrupt consumed exactly once (survives a "restart" = a new store instance),
* RLS: a scope-bound store cannot see or claim another scope's run,
* approval binding: a stale action-hash / wrong-attempt decision is rejected (fail closed).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import PostgresApprovalStore
from keel_core.runs import (
    PostgresRunStore,
    RunBudgetSpec,
    RunControlKind,
    RunCost,
    RunLeaseLostError,
    RunStatus,
    RunSurface,
    action_hash,
)

pytestmark = pytest.mark.integration


def _now() -> datetime:
    return datetime.now(UTC)


async def _admit(
    store: PostgresRunStore,
    *,
    scope: str = "web:local",
    run_id: str = "run-1",
    key: str = "idem-1",
    ttl_seconds: int = 3600,
) -> None:
    await store.admit(
        run_id=run_id,
        scope_id=scope,
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key=key,
        budget=RunBudgetSpec(max_iterations=5, token_budget=1000),
        expires_at=_now() + timedelta(seconds=ttl_seconds),
    )


async def test_admission_is_idempotent(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store, run_id="run-1", key="dup")
    await store.admit(
        run_id="run-2",
        scope_id="web:local",
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key="dup",
        budget=RunBudgetSpec(),
        expires_at=_now() + timedelta(hours=1),
    )
    assert await store.get("run-1") is not None
    assert await store.get("run-2") is None  # no duplicate row created


async def test_two_workers_race_a_claim_exactly_one_wins(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.mark_queued("run-1")
    now = _now()
    results = await asyncio.gather(
        store.claim("run-1", worker_id="w1", now=now, lease_seconds=30),
        store.claim("run-1", worker_id="w2", now=now, lease_seconds=30),
    )
    winners = [lease for lease in results if lease is not None]
    assert len(winners) == 1
    record = await store.get("run-1")
    assert record is not None and record.status is RunStatus.running and record.attempt == 1


async def test_lease_expiry_reclaim_and_fencing(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.mark_queued("run-1")
    t0 = _now()
    first = await store.claim("run-1", worker_id="w1", now=t0, lease_seconds=30)
    assert first is not None
    # Before expiry, nothing is reclaimable.
    assert await store.reclaimable(t0 + timedelta(seconds=10), 10) == []
    # After expiry, a second worker reclaims; the attempt advances.
    t1 = t0 + timedelta(seconds=40)
    assert await store.reclaimable(t1, 10) == ["run-1"]
    second = await store.claim("run-1", worker_id="w2", now=t1, lease_seconds=30)
    assert second is not None and second.token != first.token
    record = await store.get("run-1")
    assert record is not None and record.attempt == 2
    # The fenced-out first owner cannot heartbeat or terminalize.
    assert await store.heartbeat(first, t1 + timedelta(seconds=1)) is False
    with pytest.raises(RunLeaseLostError):
        await store.terminalize(first, status=RunStatus.completed, stop_reason="completed", now=t1)
    # The current owner terminalizes successfully.
    done = await store.terminalize(
        second,
        status=RunStatus.completed,
        stop_reason="completed",
        now=t1 + timedelta(seconds=2),
        cost=RunCost(prompt_tokens=7, completion_tokens=3, cost_usd=0.02),
    )
    assert done.status is RunStatus.completed and done.completion_tokens == 3


async def test_terminalize_is_idempotent(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.mark_queued("run-1")
    lease = await store.claim("run-1", worker_id="w1", now=_now(), lease_seconds=30)
    assert lease is not None
    await store.terminalize(lease, status=RunStatus.completed, stop_reason="completed")
    # A repeat with the now-stale lease returns the authoritative terminal row, not an error.
    again = await store.terminalize(lease, status=RunStatus.failed, stop_reason="error")
    assert again.status is RunStatus.completed


async def test_durable_interrupt_survives_restart_and_is_consumed_once(
    migrated_db: AsyncEngine,
) -> None:
    admitting = PostgresRunStore(migrated_db, "web:local")
    await _admit(admitting)
    await admitting.mark_queued("run-1")
    lease = await admitting.claim("run-1", worker_id="w1", now=_now(), lease_seconds=30)
    assert lease is not None
    assert await admitting.request_control(
        "run-1", kind=RunControlKind.interrupt, requested_by="user-1"
    )
    # "Restart": a brand-new store instance (new process) still sees the durable request.
    fresh = PostgresRunStore(migrated_db, "web:local")
    consumed = await fresh.consume_control("run-1")
    assert [c.kind for c in consumed] == [RunControlKind.interrupt]
    assert await fresh.consume_control("run-1") == []  # consumed exactly once


async def test_rls_isolates_runs_across_scopes(migrated_db: AsyncEngine) -> None:
    owner = PostgresRunStore(migrated_db, "web:local")
    other = PostgresRunStore(migrated_db, "im:other")
    await _admit(owner, scope="web:local", run_id="run-1", key="k")
    # A different-scope store cannot read or claim the run (RLS + FORCE RLS).
    assert await other.get("run-1") is None
    await owner.mark_queued("run-1")
    assert await other.claim("run-1", worker_id="wx", now=_now(), lease_seconds=30) is None


async def test_approval_binding_rejects_stale_hash_and_wrong_attempt(
    migrated_db: AsyncEngine,
) -> None:
    approvals = PostgresApprovalStore(migrated_db, "web:local")
    args = {"to": "x@example.com", "body": "hi"}
    bound = action_hash("email_send", args)
    approval_id = await approvals.create_pending(
        scope_id="web:local",
        run_id="run-1",
        session_id="sess-1",
        tool="email_send",
        args=args,
        call_id="call-1",
        idempotency_key="idem-1",
        reason="first_use",
        expires_at=_now() + timedelta(hours=1),
        org_id="org-1",
        actor="user-1",
        action_hash=bound,
        run_attempt=1,
    )
    # A decision bound to a *different* action hash is rejected (stale-approval defense).
    assert (
        await approvals.resolve(
            approval_id, "granted", "user-1", expected_action_hash=action_hash("email_send", {})
        )
        is False
    )
    # A decision bound to a different attempt is rejected.
    assert (
        await approvals.resolve(
            approval_id, "granted", "user-1", expected_action_hash=bound, expected_run_attempt=2
        )
        is False
    )
    # The correctly-bound decision is accepted exactly once.
    assert (
        await approvals.resolve(
            approval_id, "granted", "user-1", expected_action_hash=bound, expected_run_attempt=1
        )
        is True
    )
    assert (
        await approvals.resolve(
            approval_id, "denied", "user-1", expected_action_hash=bound, expected_run_attempt=1
        )
        is False
    )


async def test_expired_approval_denies_closed(migrated_db: AsyncEngine) -> None:
    approvals = PostgresApprovalStore(migrated_db, "web:local")
    approval_id = await approvals.create_pending(
        scope_id="web:local",
        run_id="run-1",
        session_id="sess-1",
        tool="email_send",
        args={},
        call_id="call-1",
        idempotency_key="idem-1",
        reason="first_use",
        expires_at=_now() - timedelta(seconds=1),  # already past
        action_hash=action_hash("email_send", {}),
        run_attempt=1,
    )
    expired = await approvals.expire_due(_now())
    assert approval_id in expired
    # A decision on an expired approval fails closed.
    assert await approvals.resolve(approval_id, "granted", "user-1") is False
