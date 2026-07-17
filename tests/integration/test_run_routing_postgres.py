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
