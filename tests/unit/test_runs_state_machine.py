"""Unit tests for the durable run state machine + in-memory store (M3.6).

These pin the state-machine legality table and the fenced-lease semantics of the
``InMemoryRunStore`` double so the same contract can be re-asserted against Postgres in
``tests/integration/test_runs_postgres.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from keel_core.runs import (
    TERMINAL_STATUSES,
    InMemoryRunStore,
    RunBudgetSpec,
    RunControlKind,
    RunCost,
    RunLeaseLostError,
    RunStateError,
    RunStatus,
    RunSurface,
    action_hash,
    can_transition,
)


def _t(offset: float = 0.0) -> datetime:
    return datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC) + timedelta(seconds=offset)


async def _admit(store: InMemoryRunStore, *, key: str = "k1", run_id: str = "r1") -> None:
    await store.admit(
        run_id=run_id,
        scope_id="web:local",
        org_id="org1",
        actor="user1",
        agent_id="agent1",
        session_id="sess1",
        surface=RunSurface.web.value,
        idempotency_key=key,
        budget=RunBudgetSpec(max_iterations=5, token_budget=1000),
        expires_at=_t(3600),
        now=_t(),
    )


# --- state machine ----------------------------------------------------------------------
def test_terminal_statuses_have_no_outgoing_edges() -> None:
    for status in TERMINAL_STATUSES:
        for target in RunStatus:
            assert not can_transition(status, target), (status, target)


def test_legal_and_illegal_transitions() -> None:
    assert can_transition(RunStatus.admitted, RunStatus.queued)
    assert can_transition(RunStatus.queued, RunStatus.running)
    assert can_transition(RunStatus.running, RunStatus.waiting_approval)
    assert can_transition(RunStatus.running, RunStatus.completed)
    assert can_transition(RunStatus.waiting_approval, RunStatus.queued)
    # Illegal: cannot jump straight from admitted to completed, or leave a terminal state.
    assert not can_transition(RunStatus.admitted, RunStatus.completed)
    assert not can_transition(RunStatus.completed, RunStatus.running)
    assert not can_transition(RunStatus.queued, RunStatus.waiting_approval)


def test_action_hash_is_stable_and_argument_sensitive() -> None:
    a = action_hash("email_send", {"to": "x@example.com", "body": "hi"})
    b = action_hash("email_send", {"body": "hi", "to": "x@example.com"})  # key order
    c = action_hash("email_send", {"to": "y@example.com", "body": "hi"})
    assert a == b  # canonical ordering
    assert a != c  # different args -> different hash


# --- admission idempotency --------------------------------------------------------------
async def test_admit_is_idempotent_by_scope_and_key() -> None:
    store = InMemoryRunStore()
    await _admit(store, key="dup", run_id="r1")
    # A retry with the same key returns the original run, not a second row.
    again = await store.admit(
        run_id="r2",
        scope_id="web:local",
        org_id="org1",
        actor="user1",
        agent_id="agent1",
        session_id="sess1",
        surface=RunSurface.web.value,
        idempotency_key="dup",
        budget=RunBudgetSpec(),
        expires_at=_t(3600),
        now=_t(1),
    )
    assert again.id == "r1"
    assert await store.get("r2") is None


# --- claim / lease / fencing ------------------------------------------------------------
async def test_claim_transitions_to_running_and_fences_a_second_claim() -> None:
    store = InMemoryRunStore()
    await _admit(store)
    await store.mark_queued("r1", now=_t(1))
    lease = await store.claim("r1", worker_id="w1", now=_t(2), lease_seconds=30)
    assert lease is not None
    record = await store.get("r1")
    assert record is not None and record.status is RunStatus.running
    assert record.attempt == 1
    # A second worker cannot claim a live lease.
    assert await store.claim("r1", worker_id="w2", now=_t(3), lease_seconds=30) is None


async def test_expired_lease_is_reclaimable_and_bumps_attempt() -> None:
    store = InMemoryRunStore()
    await _admit(store)
    await store.mark_queued("r1", now=_t(1))
    first = await store.claim("r1", worker_id="w1", now=_t(2), lease_seconds=30)
    assert first is not None
    # Before expiry: not reclaimable.
    assert await store.reclaimable(_t(10), 10) == []
    # After expiry: reclaimable, and a fresh claim supersedes the first lease.
    assert await store.reclaimable(_t(40), 10) == ["r1"]
    second = await store.claim("r1", worker_id="w2", now=_t(40), lease_seconds=30)
    assert second is not None and second.token != first.token
    record = await store.get("r1")
    assert record is not None and record.attempt == 2
    # The superseded (fenced-out) first lease can no longer write.
    assert await store.heartbeat(first, _t(41)) is False
    with pytest.raises(RunLeaseLostError):
        await store.terminalize(
            first, status=RunStatus.completed, stop_reason="completed", now=_t(42)
        )


async def test_terminalize_is_idempotent() -> None:
    store = InMemoryRunStore()
    await _admit(store)
    await store.mark_queued("r1", now=_t(1))
    lease = await store.claim("r1", worker_id="w1", now=_t(2), lease_seconds=30)
    assert lease is not None
    done = await store.terminalize(
        lease,
        status=RunStatus.completed,
        stop_reason="completed",
        now=_t(3),
        cost=RunCost(prompt_tokens=10, completion_tokens=5, cost_usd=0.01),
    )
    assert done.status is RunStatus.completed and done.completion_tokens == 5
    # A repeat terminalize with the (now stale) lease returns the authoritative row.
    again = await store.terminalize(lease, status=RunStatus.failed, stop_reason="error", now=_t(4))
    assert again.status is RunStatus.completed  # unchanged / idempotent


async def test_release_to_waiting_approval_and_resume_claim() -> None:
    store = InMemoryRunStore()
    await _admit(store)
    await store.mark_queued("r1", now=_t(1))
    lease = await store.claim("r1", worker_id="w1", now=_t(2), lease_seconds=30)
    assert lease is not None
    suspended = await store.release(lease, to_status=RunStatus.waiting_approval, now=_t(3))
    assert suspended.status is RunStatus.waiting_approval
    assert suspended.lease_token is None
    # The released lease can no longer heartbeat.
    assert await store.heartbeat(lease, _t(4)) is False
    # Approval resolved -> requeue -> a worker resumes.
    assert await store.mark_queued("r1", now=_t(5)) is False  # not from waiting_approval
    assert await store.requeue("r1", now=_t(5)) is True
    resumed = await store.claim("r1", worker_id="w2", now=_t(6), lease_seconds=30)
    assert resumed is not None and resumed.attempt == 2


async def test_release_rejects_non_suspend_target() -> None:
    store = InMemoryRunStore()
    await _admit(store)
    await store.mark_queued("r1", now=_t(1))
    lease = await store.claim("r1", worker_id="w1", now=_t(2), lease_seconds=30)
    assert lease is not None
    with pytest.raises(RunStateError):
        await store.release(lease, to_status=RunStatus.completed, now=_t(3))


# --- control signals --------------------------------------------------------------------
async def test_durable_control_is_consumed_once() -> None:
    store = InMemoryRunStore()
    await _admit(store)
    assert await store.request_control(
        "r1", kind=RunControlKind.interrupt, requested_by="user1", now=_t(1)
    )
    assert await store.request_control(
        "r1", kind=RunControlKind.steer, requested_by="user1", payload={"text": "go"}, now=_t(2)
    )
    consumed = await store.consume_control("r1", now=_t(3))
    assert [c.kind for c in consumed] == [RunControlKind.interrupt, RunControlKind.steer]
    # Consumed exactly once (restart/N-worker safe).
    assert await store.consume_control("r1", now=_t(4)) == []


async def test_control_rejected_for_terminal_run() -> None:
    store = InMemoryRunStore()
    await _admit(store)
    await store.mark_queued("r1", now=_t(1))
    lease = await store.claim("r1", worker_id="w1", now=_t(2), lease_seconds=30)
    assert lease is not None
    await store.terminalize(lease, status=RunStatus.completed, stop_reason="completed", now=_t(3))
    assert (
        await store.request_control(
            "r1", kind=RunControlKind.interrupt, requested_by="user1", now=_t(4)
        )
        is False
    )


# --- expiry -----------------------------------------------------------------------------
async def test_expire_due_fails_closed_past_deadline() -> None:
    store = InMemoryRunStore()
    await _admit(store)
    # Not yet due.
    assert await store.expire_due(_t(10), 10) == []
    # Past the admission deadline -> expired terminal.
    assert await store.expire_due(_t(4000), 10) == ["r1"]
    record = await store.get("r1")
    assert record is not None and record.status is RunStatus.expired
    assert record.is_terminal
