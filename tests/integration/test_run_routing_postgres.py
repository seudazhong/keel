"""Postgres/Redis integration for the default durable Web→worker routing (M3.6, WS-M).

Proves the *routing* increment end-to-end against live Postgres: a durably admitted run
(the default Web path via :class:`~keel_core.run_service.DurableRunService`) is claimed and
driven to completion by the worker job body :func:`keel_worker.runs.run_interactive`, with
the admission user turn + assistant reply persisted in the durable event log. Also covers
admission idempotency and worker recovery after a "restart" (a fresh claim of a queued run).
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import PostgresApprovalStore
from keel_core.loop import admit
from keel_core.projections import project_messages
from keel_core.protocols import ProviderChunk
from keel_core.run_service import DurableRunService
from keel_core.runs import PostgresRunStore, RunStatus, RunSurface
from keel_core.state import PostgresEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.tools import UnavailableExecutionEnvironment
from keel_core.types import FinishReason
from keel_worker.runs import run_interactive

pytestmark = pytest.mark.integration

_SCOPE = "web:local"


def _stores(
    engine: AsyncEngine,
) -> tuple[PostgresRunStore, PostgresEventStore, PostgresApprovalStore]:
    return (
        PostgresRunStore(engine, _SCOPE),
        PostgresEventStore(engine, _SCOPE),
        PostgresApprovalStore(engine, _SCOPE),
    )


def _service(
    runs: PostgresRunStore,
    events: PostgresEventStore,
    approvals: PostgresApprovalStore,
    enqueued: list[str],
) -> DurableRunService:
    async def _enqueue(run_id: str) -> None:
        enqueued.append(run_id)

    return DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=approvals,
        scope_id=_SCOPE,
        enqueue=_enqueue,
        admit_fn=admit,
    )


def _ctx(
    runs: PostgresRunStore,
    events: PostgresEventStore,
    approvals: PostgresApprovalStore,
    engine: AsyncEngine,
    provider: ScriptedProviderGateway,
) -> dict[str, Any]:
    # Local-preview durable web run: no identity service (single-tenant), Postgres-backed.
    return {
        "durable_scope": _SCOPE,
        "runs": runs,
        "store": events,
        "approvals": approvals,
        "provider": provider,
        "execution_environment": UnavailableExecutionEnvironment(),
        "identity": None,
        "engine": engine,
        "embedder": None,
    }


async def test_web_admission_then_worker_completes(migrated_db: AsyncEngine) -> None:
    runs, events, approvals = _stores(migrated_db)
    enqueued: list[str] = []
    service = _service(runs, events, approvals, enqueued)
    admitted = await service.admit(
        org_id="local",
        actor="local:local",
        agent_id="web",
        session_id="sess-web",
        surface=RunSurface.web.value,
        content="hello there",
        idempotency_key="req-web-1",
    )
    assert enqueued == [admitted.run_id]  # admission dispatched run_interactive

    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="hi back", finish_reason=FinishReason.end_turn)]]
    )
    status = await run_interactive(
        _ctx(runs, events, approvals, migrated_db, provider), admitted.run_id, _SCOPE
    )
    assert status == RunStatus.completed.value

    record = await runs.get(admitted.run_id)
    assert record is not None and record.status is RunStatus.completed
    messages = project_messages([e async for e in events.read("sess-web")])
    roles = [m["role"] for m in messages]
    assert roles[0] == "user" and "assistant" in roles  # admission turn + worker reply persisted


async def test_duplicate_admission_is_idempotent(migrated_db: AsyncEngine) -> None:
    runs, events, approvals = _stores(migrated_db)
    enqueued: list[str] = []
    service = _service(runs, events, approvals, enqueued)
    kwargs = dict(
        org_id="local",
        actor="local:local",
        agent_id="web",
        session_id="sess-dup",
        surface=RunSurface.web.value,
        content="same message",
        idempotency_key="req-dup",
    )
    first = await service.admit(**kwargs)  # type: ignore[arg-type]
    second = await service.admit(**kwargs)  # type: ignore[arg-type]
    assert first.run_id == second.run_id and second.created is False
    assert enqueued == [first.run_id]  # enqueued exactly once
    messages = project_messages([e async for e in events.read("sess-dup")])
    assert [m["role"] for m in messages] == ["user"]  # exactly one admission turn


async def test_worker_recovers_queued_run_after_restart(migrated_db: AsyncEngine) -> None:
    # Admit (queued), then a *fresh* set of stores (a "restarted" worker) claims + completes it:
    # admission survives independently of any process-local task.
    runs, events, approvals = _stores(migrated_db)
    service = _service(runs, events, approvals, [])
    admitted = await service.admit(
        org_id="local",
        actor="local:local",
        agent_id="web",
        session_id="sess-restart",
        surface=RunSurface.web.value,
        content="do it",
        idempotency_key="req-restart",
    )
    fresh_runs, fresh_events, fresh_approvals = _stores(migrated_db)
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="recovered", finish_reason=FinishReason.end_turn)]]
    )
    status = await run_interactive(
        _ctx(fresh_runs, fresh_events, fresh_approvals, migrated_db, provider),
        admitted.run_id,
        _SCOPE,
    )
    assert status == RunStatus.completed.value
    record = await fresh_runs.get(admitted.run_id)
    assert record is not None and record.status is RunStatus.completed


async def test_two_orgs_identical_session_ids_are_isolated(migrated_db: AsyncEngine) -> None:
    """Two orgs/Agents are isolated across runs/events/session lists, and the *same* external
    session id can be reused by both orgs as two distinct sessions (M3.6 findings 2 + 3).

    Each org+Agent derives its own ``agent:<org>/<agent>`` data-plane scope. Distinct sessions
    are fully isolated per scope; and because session identity is the composite ``(scope_id,
    id)``, one org reusing another org's external session id creates its own isolated session
    rather than writing into (or being denied by) the other's.
    """
    from keel_core.scoping import derive_agent_scope
    from keel_core.state import list_sessions

    scope_a = derive_agent_scope("orga", "agta")
    scope_b = derive_agent_scope("orgb", "agtb")
    runs_a = PostgresRunStore(migrated_db, scope_a)
    runs_b = PostgresRunStore(migrated_db, scope_b)
    events_a = PostgresEventStore(migrated_db, scope_a)
    events_b = PostgresEventStore(migrated_db, scope_b)

    async def _noop(_run_id: str) -> None:
        return None

    svc_a = DurableRunService(
        run_store=runs_a,
        event_store=events_a,
        approvals=PostgresApprovalStore(migrated_db, scope_a),
        scope_id=scope_a,
        enqueue=_noop,
        admit_fn=admit,
    )
    svc_b = DurableRunService(
        run_store=runs_b,
        event_store=events_b,
        approvals=PostgresApprovalStore(migrated_db, scope_b),
        scope_id=scope_b,
        enqueue=_noop,
        admit_fn=admit,
    )

    admit_a = await svc_a.admit(
        org_id="orga",
        actor="user-a",
        agent_id="agta",
        session_id="sess-a",
        surface=RunSurface.web.value,
        content="secret for org A",
        idempotency_key="k",
    )
    admit_b = await svc_b.admit(
        org_id="orgb",
        actor="user-b",
        agent_id="agtb",
        session_id="sess-b",
        surface=RunSurface.web.value,
        content="secret for org B",
        idempotency_key="k",  # identical idempotency key, different scope + session
    )
    assert admit_a.run_id != admit_b.run_id  # distinct runs despite identical idempotency key

    for scope, run_id, reply in (
        (scope_a, admit_a.run_id, "reply A"),
        (scope_b, admit_b.run_id, "reply B"),
    ):
        provider = ScriptedProviderGateway(
            [[ProviderChunk(delta=reply, finish_reason=FinishReason.end_turn)]]
        )
        ctx = _ctx(
            PostgresRunStore(migrated_db, scope),
            PostgresEventStore(migrated_db, scope),
            PostgresApprovalStore(migrated_db, scope),
            migrated_db,
            provider,
        )
        assert await run_interactive(ctx, run_id, scope) == RunStatus.completed.value

    # Each scope's durable event log contains ONLY its own content.
    texts_a = " ".join([str(e.payload.get("text", "")) async for e in events_a.read("sess-a")])
    texts_b = " ".join([str(e.payload.get("text", "")) async for e in events_b.read("sess-b")])
    assert "org A" in texts_a and "org B" not in texts_a
    assert "org B" in texts_b and "org A" not in texts_b

    # A run admitted under org A's scope is invisible from org B's run store (cross-scope 404).
    assert await runs_b.get(admit_a.run_id) is None
    assert await runs_a.get(admit_b.run_id) is None

    # Session listings are per-scope: each org sees exactly its own session.
    sessions_a = await list_sessions(migrated_db, scope_a)
    sessions_b = await list_sessions(migrated_db, scope_b)
    assert [s.id for s in sessions_a] == ["sess-a"] and [s.id for s in sessions_b] == ["sess-b"]

    # Composite (scope_id, id) identity: org B can reuse org A's external session id as its own
    # isolated session — the same id in two orgs coexists (M3.6 finding 2), it never writes into
    # org A's data. Admission succeeds and produces a distinct run in org B's scope.
    reuse = await svc_b.admit(
        org_id="orgb",
        actor="user-b",
        agent_id="agtb",
        session_id="sess-a",  # same external id as org A's session, but a distinct scope
        surface=RunSurface.web.value,
        content="org B's own sess-a",
        idempotency_key="k2",
    )
    assert reuse.run_id not in {admit_a.run_id, admit_b.run_id}
    # Org A's "sess-a" log is untouched by org B's reuse; each scope reads only its own content.
    texts_a_after = " ".join(
        [str(e.payload.get("text", "")) async for e in events_a.read("sess-a")]
    )
    texts_b_reuse = " ".join(
        [str(e.payload.get("text", "")) async for e in events_b.read("sess-a")]
    )
    assert "org A" in texts_a_after and "org B's own" not in texts_a_after
    assert "org B's own" in texts_b_reuse and "org A" not in texts_b_reuse
    # Org A still never sees org B's run, and vice versa.
    assert await runs_a.get(reuse.run_id) is None


async def test_dispatch_outbox_leases_across_scopes_and_blocks_duplicate_worker(
    migrated_db: AsyncEngine,
) -> None:
    """The global dispatch outbox indexes runs across scopes and fences duplicate workers.

    A single reconciler can discover open work in every scope from this one non-RLS index, and
    the ``FOR UPDATE SKIP LOCKED`` lease guarantees two workers never both claim the same intent
    (M3.6 finding 4). Verified on live Postgres.
    """
    from datetime import UTC, datetime, timedelta

    from keel_core.run_dispatch import PostgresRunDispatchOutbox

    outbox = PostgresRunDispatchOutbox(migrated_db)
    now = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)
    await outbox.record("run-a", "agent:orga/agta", now=now)
    await outbox.record("run-b", "agent:orgb/agtb", now=now)
    assert await outbox.active_scopes() == {"agent:orga/agta", "agent:orgb/agtb"}

    first = await outbox.claim_due(worker_id="w1", now=now, lease_seconds=60)
    assert {i.run_id for i in first} == {"run-a", "run-b"}
    # A second worker sees nothing while the lease is live (SKIP LOCKED + lease window).
    second = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=1))
    assert second == []
    # After the lease expires the intents are claimable again.
    third = await outbox.claim_due(worker_id="w2", now=now + timedelta(seconds=120))
    assert {i.run_id for i in third} == {"run-a", "run-b"}

    # A terminal run's intent is removed; a still-active one is deferred.
    await outbox.remove("run-a")
    await outbox.reschedule("run-b", delay_seconds=300, now=now + timedelta(seconds=120))
    assert await outbox.active_scopes() == {"agent:orgb/agtb"}
    # run-b is deferred past its next attempt, so an immediate claim skips it.
    assert await outbox.claim_due(worker_id="w3", now=now + timedelta(seconds=121)) == []
