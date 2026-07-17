"""Global dispatch outbox + cross-scope reconciliation (M3.6, review finding 4).

Unit coverage (no Postgres/Redis) for the outbox that lets a single worker reconcile durable
runs across every scope: admission records a dispatch intent; a lost enqueue leaves the intent
so the reconciler redispatches it; the fenced lease stops duplicate workers double-processing;
and a terminal run's intent is retired.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from keel_core.approvals import InMemoryApprovalStore
from keel_core.loop import admit
from keel_core.run_dispatch import InMemoryRunDispatchOutbox
from keel_core.run_service import DurableRunService
from keel_core.runs import InMemoryRunStore, RunStatus, RunSurface
from keel_core.state import InMemoryEventStore
from keel_worker.runs import reconcile_dispatch_tick

_SCOPE_A = "agent:orga/ag1"
_SCOPE_B = "agent:orgb/ag2"


async def _admit(
    runs: InMemoryRunStore,
    events: InMemoryEventStore,
    outbox: InMemoryRunDispatchOutbox,
    *,
    scope_id: str,
    session_id: str,
    enqueue_ok: bool = True,
) -> str:
    async def _enqueue(run_id: str) -> None:
        if not enqueue_ok:
            raise RuntimeError("queue down")

    service = DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=InMemoryApprovalStore(),
        scope_id=scope_id,
        enqueue=_enqueue,
        admit_fn=admit,
        dispatch_outbox=outbox,
    )
    result = await service.admit(
        org_id="orga",
        actor="machine:svc",
        agent_id="ag1",
        session_id=session_id,
        surface=RunSurface.web.value,
        content="hello",
        idempotency_key=f"idem-{session_id}",
    )
    return result.run_id


async def test_admit_records_dispatch_intent() -> None:
    runs, events, outbox = InMemoryRunStore(), InMemoryEventStore(), InMemoryRunDispatchOutbox()
    run_id = await _admit(runs, events, outbox, scope_id=_SCOPE_A, session_id="s1")
    assert await outbox.active_scopes() == {_SCOPE_A}
    intents = await outbox.claim_due(worker_id="w1")
    assert [i.run_id for i in intents] == [run_id]


async def test_lost_enqueue_keeps_intent_for_redispatch() -> None:
    # Crash/queue-down after the durable commit: the run is queued and the intent survives so
    # the reconciler can redispatch it (never a duplicate run, never a lost run).
    runs, events, outbox = InMemoryRunStore(), InMemoryEventStore(), InMemoryRunDispatchOutbox()
    run_id = await _admit(
        runs, events, outbox, scope_id=_SCOPE_A, session_id="s1", enqueue_ok=False
    )
    record = await runs.get(run_id)
    assert record is not None and record.status is RunStatus.queued
    assert await outbox.active_scopes() == {_SCOPE_A}  # intent recorded despite failed enqueue


async def test_outbox_lease_blocks_duplicate_worker() -> None:
    outbox = InMemoryRunDispatchOutbox()
    now = datetime(2026, 7, 18, tzinfo=UTC)
    await outbox.record("r1", _SCOPE_A, now=now)
    await outbox.record("r2", _SCOPE_B, now=now)

    first = await outbox.claim_due(worker_id="w1", now=now, lease_seconds=60)
    assert {i.run_id for i in first} == {"r1", "r2"}
    # A second worker sees nothing while the lease is live (no double processing).
    second = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=1))
    assert second == []
    # After the lease expires the intents are claimable again.
    third = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=120))
    assert {i.run_id for i in third} == {"r1", "r2"}


async def test_outbox_spans_multiple_scopes() -> None:
    outbox = InMemoryRunDispatchOutbox()
    await outbox.record("r1", _SCOPE_A)
    await outbox.record("r2", _SCOPE_B)
    assert await outbox.active_scopes() == {_SCOPE_A, _SCOPE_B}


def _ctx(runs: InMemoryRunStore, events: InMemoryEventStore, outbox: InMemoryRunDispatchOutbox):
    enqueued: list[tuple[str, tuple[Any, ...]]] = []

    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        enqueued.append((name, args))

    ctx: dict[str, Any] = {
        "runs": runs,
        "store": events,
        "approvals": InMemoryApprovalStore(),
        "dispatch_outbox": outbox,
        "enqueue": _enqueue,
        "durable_scope": _SCOPE_A,
    }
    return ctx, enqueued


async def test_reconcile_dispatch_redispatches_queued_run() -> None:
    runs, events, outbox = InMemoryRunStore(), InMemoryEventStore(), InMemoryRunDispatchOutbox()
    run_id = await _admit(
        runs, events, outbox, scope_id=_SCOPE_A, session_id="s1", enqueue_ok=False
    )
    ctx, enqueued = _ctx(runs, events, outbox)
    # Well past the redispatch grace so the queued-but-undispatched run is picked up.
    later = datetime.now(UTC) + timedelta(minutes=5)
    from unittest.mock import patch

    with patch("keel_worker.runs.datetime") as fake_dt:
        fake_dt.now.return_value = later
        await reconcile_dispatch_tick(ctx)
    assert ("run_interactive", (run_id, _SCOPE_A)) in enqueued
    # The run is still active, so its intent is deferred (rescheduled), not removed.
    assert await outbox.active_scopes() == {_SCOPE_A}


async def test_reconcile_dispatch_removes_terminal_intent() -> None:
    runs, events, outbox = InMemoryRunStore(), InMemoryEventStore(), InMemoryRunDispatchOutbox()
    run_id = await _admit(runs, events, outbox, scope_id=_SCOPE_A, session_id="s1")
    # Drive the run to a terminal state directly (claim a fenced lease, then terminalize).
    lease = await runs.claim(run_id, worker_id="w", now=datetime.now(UTC), lease_seconds=60)
    assert lease is not None
    await runs.terminalize(
        lease, status=RunStatus.completed, stop_reason="done", now=datetime.now(UTC)
    )
    ctx, _enqueued = _ctx(runs, events, outbox)
    await reconcile_dispatch_tick(ctx)
    assert await outbox.active_scopes() == set()  # terminal intent retired


class _FailingOutbox(InMemoryRunDispatchOutbox):
    """A dispatch outbox whose ``record`` always fails (intent-store fault injection)."""

    async def record(self, run_id: str, scope_id: str, *, now: Any = None) -> None:  # type: ignore[override]
        raise RuntimeError("outbox unavailable")


async def test_intent_write_failure_rolls_back_queued_transition() -> None:
    # Fault at the intent-store boundary: the queued transition + intent are atomic, so a
    # failed intent write must NOT leave a queued run without a discoverable dispatch pointer.
    # The transition rolls back (run stays admitted) and the error is NOT swallowed — the caller
    # can retry idempotently (or the reconciler recovers the admitted run).
    runs, events, outbox = InMemoryRunStore(), InMemoryEventStore(), _FailingOutbox()
    with pytest.raises(RuntimeError, match="outbox unavailable"):
        await _admit(runs, events, outbox, scope_id=_SCOPE_A, session_id="s1")
    # The run exists and is still admitted (never a queued-but-undiscoverable run), the prompt
    # is durably persisted, and no intent was recorded.
    (run_id,) = list(runs._rows)
    record = await runs.get(run_id)
    assert record is not None and record.status is RunStatus.admitted
    assert await outbox.active_scopes() == set()


async def test_mark_queued_with_intent_is_atomic_and_single_winner() -> None:
    # The atomic transition is single-winner: two admitters of the same idempotency key produce
    # exactly one queued run with exactly one intent.
    runs, events, outbox = InMemoryRunStore(), InMemoryEventStore(), InMemoryRunDispatchOutbox()
    run_a = await _admit(runs, events, outbox, scope_id=_SCOPE_A, session_id="s1")
    run_b = await _admit(runs, events, outbox, scope_id=_SCOPE_A, session_id="s1")
    assert run_a == run_b  # idempotent
    intents = await outbox.claim_due(worker_id="w1")
    assert [i.run_id for i in intents] == [run_a]  # exactly one intent
