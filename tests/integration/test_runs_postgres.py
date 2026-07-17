"""Postgres integration tests for the durable run store + approval binding (M3.6).

Proves the load-bearing concurrency + isolation guarantees against a live Postgres:

* admission idempotency (a retried request creates no duplicate run),
* two workers racing a claim — exactly one wins,
* lease expiry -> reclaim -> fenced-out prior owner cannot write,
* idempotent terminalization,
* durable interrupt consumed exactly once (survives a "restart" = a new store instance),
* RLS: a scope-bound store cannot see or claim another scope's run,
* approval binding: a stale action-hash / wrong-attempt decision is rejected (fail closed),
* suspension checkpoint: a ``running`` row that crashed mid-suspension (durable approval,
  no ``waiting_approval`` transition) is reclaimed with ``resume=True`` and its source
  attempt preserved, so the (later approved) action is honoured exactly once — never
  silently discarded by an attempt-advancing reclaim.
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
    RunLease,
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


async def test_requeue_sets_resume_marker_and_claim_captures_it(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.mark_queued("run-1")
    lease = await store.claim("run-1", worker_id="w1", now=_now(), lease_seconds=30)
    assert lease is not None and lease.resume is False
    # Suspend on an approval (release to waiting_approval), then resolve -> requeue.
    await store.release(lease, to_status=RunStatus.waiting_approval)
    assert await store.requeue("run-1") is True
    record = await store.get("run-1")
    assert record is not None and record.status is RunStatus.queued and record.resume_requested
    # The claim captures resume atomically and clears the marker.
    resumed = await store.claim("run-1", worker_id="w2", now=_now(), lease_seconds=30)
    assert resumed is not None and resumed.resume is True
    cleared = await store.get("run-1")
    assert cleared is not None and cleared.resume_requested is False


async def test_reclaimed_waiting_run_resumes_even_without_marker(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.mark_queued("run-1")
    t0 = _now()
    lease = await store.claim("run-1", worker_id="w1", now=t0, lease_seconds=30)
    assert lease is not None
    # The run suspends on an approval; its owner then crashes (lease lapses, not requeued).
    await store.release(lease, to_status=RunStatus.waiting_approval, now=t0)
    # Force the lease to look expired so a reclaim is possible from waiting_approval.
    async with migrated_db.begin() as conn:
        from sqlalchemy import text as _text

        await conn.execute(_text("SELECT set_config('app.scope_id', 'web:local', true)"))
        await conn.execute(
            _text(
                "UPDATE runs SET lease_expires_at = :past, worker_id = 'w1', "
                "lease_token = 'stale' WHERE id = 'run-1'"
            ),
            {"past": t0 - timedelta(seconds=1)},
        )
    reclaimed = await store.claim(
        "run-1", worker_id="w2", now=t0 + timedelta(seconds=60), lease_seconds=30
    )
    assert reclaimed is not None and reclaimed.resume is True  # waiting_approval -> resume


async def test_redispatchable_finds_queued_unclaimed_runs(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    now = _now()
    await _admit(store, run_id="run-admitted", key="k1")
    await _admit(store, run_id="run-queued", key="k2")
    await store.mark_queued("run-queued", now=now - timedelta(seconds=120))
    # A claimed (owned) run is never redispatchable.
    await _admit(store, run_id="run-owned", key="k3")
    await store.mark_queued("run-owned")
    await store.claim("run-owned", worker_id="w1", now=now, lease_seconds=300)
    ids = set(await store.redispatchable(now + timedelta(seconds=60), 50, grace_seconds=30))
    assert "run-admitted" in ids and "run-queued" in ids and "run-owned" not in ids


async def test_peek_does_not_consume_ack_consumes_once(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.request_control("run-1", kind=RunControlKind.cancel, requested_by="user-1")
    # peek is repeatable (does not consume) — a crashed worker re-honors the control.
    first = await store.peek_control("run-1")
    second = await store.peek_control("run-1")
    assert [c.kind for c in first] == [RunControlKind.cancel]
    assert [c.kind for c in second] == [RunControlKind.cancel]
    # ack consumes exactly once.
    assert await store.ack_controls([first[0].id]) == 1
    assert await store.peek_control("run-1") == []
    assert await store.ack_controls([first[0].id]) == 0


async def test_mark_prompt_persisted_is_idempotent(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    assert (await store.get("run-1")).prompt_persisted is False  # type: ignore[union-attr]
    assert await store.mark_prompt_persisted("run-1") is True
    assert await store.mark_prompt_persisted("run-1") is False  # already set
    assert (await store.get("run-1")).prompt_persisted is True  # type: ignore[union-attr]


async def test_duplicate_admission_event_is_rejected_by_unique_index(
    migrated_db: AsyncEngine,
) -> None:
    from keel_core.errors import DuplicateEventError
    from keel_core.loop import admit_run, admit_steer
    from keel_core.state import PostgresEventStore

    events = PostgresEventStore(migrated_db, "web:local")
    await admit_run(events, "sess-1", "web:local", "hello", "run-1")
    with pytest.raises(DuplicateEventError):
        await admit_run(events, "sess-1", "web:local", "hello again", "run-1")
    # The steering marker is enforced by the same partial-unique index.
    await admit_steer(events, "sess-1", "web:local", "use staging", "run-1", "ctrl-1")
    with pytest.raises(DuplicateEventError):
        await admit_steer(events, "sess-1", "web:local", "use staging", "run-1", "ctrl-1")
    # Exactly one admission turn + one steer turn survive.
    rows = [e async for e in events.read("sess-1")]
    assert sum(1 for e in rows if e.payload.get("admission_run") == "run-1") == 1
    assert sum(1 for e in rows if e.payload.get("steer_control") == "ctrl-1") == 1


async def test_concurrent_admission_persists_one_prompt_and_enqueues_once(
    migrated_db: AsyncEngine,
) -> None:
    from keel_core.approvals import PostgresApprovalStore
    from keel_core.loop import admit as loop_admit
    from keel_core.run_service import DurableRunService
    from keel_core.state import PostgresEventStore

    events = PostgresEventStore(migrated_db, "web:local")
    runs = PostgresRunStore(migrated_db, "web:local")
    approvals = PostgresApprovalStore(migrated_db, "web:local")
    enqueued: list[str] = []

    async def enqueue(run_id: str) -> None:
        enqueued.append(run_id)

    service = DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=approvals,
        scope_id="web:local",
        enqueue=enqueue,
        admit_fn=loop_admit,
    )

    async def one() -> object:
        return await service.admit(
            org_id="org-1",
            actor="user-1",
            agent_id="agent-1",
            session_id="sess-1",
            surface=RunSurface.web.value,
            content="hello",
            idempotency_key="k1",
        )

    results = await asyncio.gather(*[one() for _ in range(6)], return_exceptions=True)
    ok = [r for r in results if not isinstance(r, BaseException)]
    assert len(ok) == 6  # every admitter completes (loser observes, never errors)
    run_ids = {r.run_id for r in ok}  # type: ignore[attr-defined]
    assert len(run_ids) == 1
    (run_id,) = run_ids
    rows = [e async for e in events.read("sess-1")]
    assert sum(1 for e in rows if e.payload.get("admission_run") == run_id) == 1  # one prompt
    assert enqueued == [run_id]  # dispatched exactly once


async def test_iterations_accrue_cumulatively_across_release_and_terminalize(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.mark_queued("run-1")
    lease = await store.claim("run-1", worker_id="w1", now=_now(), lease_seconds=30)
    assert lease is not None and lease.iterations_used == 0
    # Suspend on an approval, consuming one iteration (persisted cumulatively).
    await store.release(
        lease,
        to_status=RunStatus.waiting_approval,
        cost=RunCost(prompt_tokens=4, completion_tokens=2, cost_usd=0.01, iterations=1),
    )
    rec = await store.get("run-1")
    assert rec is not None and rec.iterations == 1 and rec.cost_usd == 0.01
    # Resume: the claim carries the cumulative iteration count so the budget is not reset.
    assert await store.requeue("run-1") is True
    resumed = await store.claim("run-1", worker_id="w2", now=_now(), lease_seconds=30)
    assert resumed is not None and resumed.iterations_used == 1 and resumed.resume is True
    final = await store.terminalize(
        resumed,
        status=RunStatus.completed,
        stop_reason="completed",
        cost=RunCost(iterations=2, cost_usd=0.02),
    )
    assert final.iterations == 3 and final.cost_usd == 0.03  # cumulative across both attempts


async def _suspend_pg(
    runs: PostgresRunStore,
    approvals: PostgresApprovalStore,
    *,
    run_id: str,
    key: str,
    org_id: str = "org-1",
    actor: str = "user-1",
    args: dict[str, object] | None = None,
    call_id: str = "c1",
    batch_id: str = "",
) -> str:
    """Admit + suspend a run and create one bound pending approval; return its id."""
    args = args if args is not None else {"to": "z@x"}
    await runs.admit(
        run_id=run_id,
        scope_id="web:local",
        org_id=org_id,
        actor=actor,
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key=key,
        budget=RunBudgetSpec(),
        expires_at=_now() + timedelta(hours=1),
    )
    await runs.mark_queued(run_id)
    lease = await runs.claim(run_id, worker_id="w1", now=_now(), lease_seconds=30)
    assert lease is not None
    await runs.release(lease, to_status=RunStatus.waiting_approval)
    return await approvals.create_pending(
        scope_id="web:local",
        run_id=run_id,
        session_id="sess-1",
        tool="email.send",
        args=args,
        call_id=call_id,
        idempotency_key=f"i-{run_id}-{call_id}",
        reason="first_use",
        expires_at=_now() + timedelta(hours=1),
        org_id=org_id,
        actor=actor,
        action_hash=action_hash("email.send", args),
        run_attempt=1,
        batch_id=batch_id,
    )


def _pg_service(
    runs: PostgresRunStore, approvals: PostgresApprovalStore, sink: list[str]
) -> object:
    from keel_core.loop import admit as loop_admit
    from keel_core.run_service import DurableRunService
    from keel_core.state import PostgresEventStore

    async def enqueue(run_id: str) -> None:
        sink.append(run_id)

    return DurableRunService(
        run_store=runs,
        event_store=PostgresEventStore(runs._engine, "web:local"),  # type: ignore[attr-defined]
        approvals=approvals,
        scope_id="web:local",
        enqueue=enqueue,
        admit_fn=loop_admit,
    )


async def test_resolve_and_requeue_commit_atomically(migrated_db: AsyncEngine) -> None:
    runs = PostgresRunStore(migrated_db, "web:local")
    approvals = PostgresApprovalStore(migrated_db, "web:local")
    approval_id = await _suspend_pg(runs, approvals, run_id="run-1", key="k1")
    enq: list[str] = []
    service = _pg_service(runs, approvals, enq)
    ok = await service.resolve_approval(  # type: ignore[attr-defined]
        approval_id, approved=True, resolved_by="user-1", actor="user-1", org_id="org-1"
    )
    assert ok is True and enq == ["run-1"]
    # Both writes are visible together: approval granted AND run requeued (one transaction).
    rec = await runs.get("run-1")
    assert rec is not None and rec.status is RunStatus.queued and rec.resume_requested
    approval = await approvals.get(approval_id)
    assert approval is not None and approval.status == "granted"


async def test_admission_fingerprint_conflict_and_tenant_namespace(
    migrated_db: AsyncEngine,
) -> None:
    from keel_core.loop import admit as loop_admit
    from keel_core.run_service import DurableRunService
    from keel_core.runs import RunAdmissionConflict
    from keel_core.state import PostgresEventStore

    runs = PostgresRunStore(migrated_db, "web:local")
    approvals = PostgresApprovalStore(migrated_db, "web:local")
    enq: list[str] = []

    async def enqueue(run_id: str) -> None:
        enq.append(run_id)

    service = DurableRunService(
        run_store=runs,
        event_store=PostgresEventStore(migrated_db, "web:local"),
        approvals=approvals,
        scope_id="web:local",
        enqueue=enqueue,
        admit_fn=loop_admit,
    )

    async def _admit_via(*, org_id: str, content: str, key: str = "shared") -> str:
        result = await service.admit(
            org_id=org_id,
            actor="user-1",
            agent_id="agent-1",
            session_id="sess-1",
            surface=RunSurface.web.value,
            content=content,
            idempotency_key=key,
        )
        return result.run_id

    a = await _admit_via(org_id="org-A", content="do X")
    b = await _admit_via(org_id="org-B", content="do X")
    assert a != b  # a shared key does not collide across tenants (org namespace)
    # Same identity + same content -> idempotent no-op (same run).
    assert await _admit_via(org_id="org-A", content="do X") == a
    # Same identity + different content -> conflict (never repaired with attacker values).
    with pytest.raises(RunAdmissionConflict):
        await _admit_via(org_id="org-A", content="do EVIL")


async def test_multi_approval_batch_requeues_only_when_all_terminal(
    migrated_db: AsyncEngine,
) -> None:
    runs = PostgresRunStore(migrated_db, "web:local")
    approvals = PostgresApprovalStore(migrated_db, "web:local")
    # A run suspended on a two-approval batch (shared batch id).
    a1 = await _suspend_pg(
        runs, approvals, run_id="run-1", key="k1", args={"to": "a@x"}, call_id="c1", batch_id="B"
    )
    a2 = await approvals.create_pending(
        scope_id="web:local",
        run_id="run-1",
        session_id="sess-1",
        tool="email.send",
        args={"to": "b@x"},
        call_id="c2",
        idempotency_key="i-run-1-c2",
        reason="first_use",
        expires_at=_now() + timedelta(hours=1),
        org_id="org-1",
        actor="user-1",
        action_hash=action_hash("email.send", {"to": "b@x"}),
        run_attempt=1,
        batch_id="B",
    )
    enq: list[str] = []
    service = _pg_service(runs, approvals, enq)

    # Resolving the first decision does NOT requeue (batch not terminal).
    assert await service.resolve_approval(  # type: ignore[attr-defined]
        a1, approved=True, resolved_by="user-1", actor="user-1"
    )
    assert enq == []
    rec = await runs.get("run-1")
    assert rec is not None and rec.status is RunStatus.waiting_approval

    # Resolving the last decision requeues exactly once.
    assert await service.resolve_approval(  # type: ignore[attr-defined]
        a2, approved=False, resolved_by="user-1", actor="user-1"
    )
    assert enq == ["run-1"]
    rec = await runs.get("run-1")
    assert rec is not None and rec.status is RunStatus.queued and rec.resume_requested
    # Each exact decision is preserved on the durable rows.
    assert (await approvals.get(a1)).status == "granted"  # type: ignore[union-attr]
    assert (await approvals.get(a2)).status == "denied"  # type: ignore[union-attr]


async def test_repair_stuck_resumes_backstops_missed_requeue_pg(migrated_db: AsyncEngine) -> None:
    runs = PostgresRunStore(migrated_db, "web:local")
    approvals = PostgresApprovalStore(migrated_db, "web:local")
    approval_id = await _suspend_pg(runs, approvals, run_id="run-1", key="k1")
    # Simulate a crash after the approval resolve committed but before the run requeued.
    assert await approvals.resolve(approval_id, "granted", "user-1") is True
    stuck = await runs.get("run-1")
    assert stuck is not None and stuck.status is RunStatus.waiting_approval
    enq: list[str] = []
    service = _pg_service(runs, approvals, enq)
    repaired = await service.repair_stuck_resumes()  # type: ignore[attr-defined]
    assert repaired == 1 and enq == ["run-1"]
    rec = await runs.get("run-1")
    assert rec is not None and rec.status is RunStatus.queued and rec.resume_requested


async def test_mark_checkpoint_is_fenced_and_idempotent(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.mark_queued("run-1")
    t0 = _now()
    lease = await store.claim("run-1", worker_id="w1", now=t0, lease_seconds=30)
    assert lease is not None and lease.attempt == 1
    # The active owner marks the checkpoint, fenced by its lease; re-marking is idempotent.
    assert await store.mark_checkpoint(lease, now=t0) is True
    assert await store.mark_checkpoint(lease, now=t0) is True
    rec = await store.get("run-1")
    assert rec is not None and rec.suspend_checkpoint is True and rec.checkpoint_attempt == 1
    # A superseded owner (wrong lease token) cannot write the marker (fail closed).
    stale = RunLease(
        run_id="run-1",
        scope_id="web:local",
        org_id="org-1",
        token="not-the-token",
        worker_id="w9",
        attempt=1,
        agent_id="agent-1",
        session_id="sess-1",
        lease_seconds=30,
    )
    with pytest.raises(RunLeaseLostError):
        await store.mark_checkpoint(stale, now=t0)


async def test_crash_mid_suspension_reclaims_as_resume_and_keeps_source_attempt(
    migrated_db: AsyncEngine,
) -> None:
    runs = PostgresRunStore(migrated_db, "web:local")
    approvals = PostgresApprovalStore(migrated_db, "web:local")
    await _admit(runs)
    await runs.mark_queued("run-1")
    t0 = _now()
    lease = await runs.claim("run-1", worker_id="w1", now=t0, lease_seconds=30)
    assert lease is not None and lease.attempt == 1

    # The worker begins to suspend: checkpoint marker persists (fenced), the approval is
    # durable and bound to attempt 1 — but the worker crashes BEFORE the run releases to
    # waiting_approval. The row is therefore still ``running`` with the marker set.
    assert await runs.mark_checkpoint(lease, now=t0) is True
    args = {"to": "z@x"}
    bound = action_hash("email.send", args)
    approval_id = await approvals.create_pending(
        scope_id="web:local",
        run_id="run-1",
        session_id="sess-1",
        tool="email.send",
        args=args,
        call_id="c1",
        idempotency_key="i1",
        reason="first_use",
        expires_at=t0 + timedelta(hours=1),
        org_id="org-1",
        actor="user-1",
        action_hash=bound,
        run_attempt=1,
        batch_id="b1",
    )
    crashed = await runs.get("run-1")
    assert crashed is not None and crashed.status is RunStatus.running
    assert crashed.suspend_checkpoint is True and crashed.checkpoint_attempt == 1

    # Lease expiry -> reclaim: the marker makes the reclaim RESUME (not restart-fresh). The
    # lease attempt advances to 2 for fencing, but the checkpoint (source) attempt stays 1.
    t1 = t0 + timedelta(seconds=60)
    assert await runs.reclaimable(t1, 10) == ["run-1"]
    reclaimed = await runs.claim("run-1", worker_id="w2", now=t1, lease_seconds=30)
    assert reclaimed is not None and reclaimed.attempt == 2 and reclaimed.resume is True
    record = await runs.get("run-1")
    assert record is not None and record.suspend_checkpoint is True
    assert record.checkpoint_attempt == 1  # source attempt preserved across the reclaim

    # The reclaimed worker resumes, finds the approval still pending, and safely restores
    # waiting_approval (release). The marker clears but the source attempt stays 1.
    await runs.release(reclaimed, to_status=RunStatus.waiting_approval, now=t1)
    restored = await runs.get("run-1")
    assert restored is not None and restored.status is RunStatus.waiting_approval
    assert restored.suspend_checkpoint is False and restored.checkpoint_attempt == 1

    # The operator's decision, bound to the SOURCE attempt (1), still binds and requeues even
    # though the lease attempt is now 2 — the approved action is NOT silently discarded.
    enq: list[str] = []
    service = _pg_service(runs, approvals, enq)
    ok = await service.resolve_approval(  # type: ignore[attr-defined]
        approval_id, approved=True, resolved_by="user-1", actor="user-1", org_id="org-1"
    )
    assert ok is True and enq == ["run-1"]
    requeued = await runs.get("run-1")
    assert requeued is not None and requeued.status is RunStatus.queued
    assert requeued.resume_requested
    assert (await approvals.get(approval_id)).status == "granted"  # type: ignore[union-attr]


async def test_release_to_waiting_clears_checkpoint_marker(migrated_db: AsyncEngine) -> None:
    store = PostgresRunStore(migrated_db, "web:local")
    await _admit(store)
    await store.mark_queued("run-1")
    t0 = _now()
    lease = await store.claim("run-1", worker_id="w1", now=t0, lease_seconds=30)
    assert lease is not None
    assert await store.mark_checkpoint(lease, now=t0) is True
    # Releasing to waiting_approval durably reflects the suspension; the crash-window marker
    # is cleared, while the source (checkpoint) attempt is preserved for approval binding.
    await store.release(lease, to_status=RunStatus.waiting_approval, now=t0)
    rec = await store.get("run-1")
    assert rec is not None and rec.status is RunStatus.waiting_approval
    assert rec.suspend_checkpoint is False and rec.checkpoint_attempt == 1
