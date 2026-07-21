"""Worker task tests: run_agent (suspend), scheduler_tick (enqueue + expire), resume_run."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from arq.worker import Function

from keel_core.agent_config_snapshot import AgentConfigSnapshot
from keel_core.approvals import InMemoryApprovalStore
from keel_core.config import get_settings
from keel_core.digest import digest_session_id
from keel_core.embeddings import FakeEmbedder
from keel_core.jobs import CancelMode
from keel_core.knowledge import InMemoryKnowledgeStore
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, StopReason
from keel_scheduler.atmostonce import InMemoryClaimStore
from keel_scheduler.store import InMemoryScheduleStore, ScheduleRow
from keel_worker.jobs import JobRegistry, dispatch_jobs, run_job
from keel_worker.knowledge import knowledge_job_definitions, knowledge_job_registry
from keel_worker.main import (
    WorkerSettings,
    _enqueue_arq,
    resume_run,
    run_agent,
    scheduler_tick,
)

_NOW = datetime(2026, 7, 7, 9, 0, tzinfo=UTC)


def _row() -> ScheduleRow:
    return ScheduleRow(
        id="daily",
        scope_id="u:1",
        agent_id="digest",
        session_id=digest_session_id("u:1"),
        trigger_kind="interval",
        spec="86400",
        next_run_at=_NOW,
        interval_s=86400,
    )


def _read_then_send() -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="inbox_list", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c2",
                        name="email_send",
                        arguments={"to": "finance@external.example", "idempotency_key": "k"},
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )


async def test_run_agent_suspends_and_records_pending() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    ctx: dict[str, Any] = {
        "store": store,
        "approvals": approvals,
        "provider": _read_then_send(),
        "schedules": InMemoryScheduleStore([_row()]),
        "sent": [],
    }
    reason = await run_agent(ctx, "daily")
    assert reason == StopReason.suspended.value
    assert len(await approvals.list_pending("u:1")) == 1


async def test_run_agent_missing_schedule_is_a_noop() -> None:
    ctx: dict[str, Any] = {
        "store": InMemoryEventStore(),
        "approvals": InMemoryApprovalStore(),
        "provider": _read_then_send(),
        "schedules": InMemoryScheduleStore([]),
        "sent": [],
    }
    assert await run_agent(ctx, "nope") == "missing"


async def test_scheduler_tick_enqueues_due_and_does_not_own_approval_expiry() -> None:
    # Approval expiry moved to the durable reconciler (M3.6, item 6): the legacy scheduler
    # must never consume/route approvals, so a durable interactive approval can't be sent to
    # the wrong resume_run job. scheduler_tick now only advances the schedule due-loop.
    approvals = InMemoryApprovalStore()
    await approvals.create_pending(
        scope_id="u:1",
        run_id="r1",
        session_id="digest:u:1",
        tool="email.send",
        args={},
        call_id="c",
        idempotency_key="k",
        reason="tainted",
        expires_at=_NOW - timedelta(minutes=1),  # already stale
    )
    enqueued: list[tuple[Any, ...]] = []

    async def enqueue(name: str, *args: object) -> None:
        enqueued.append((name, *args))

    ctx: dict[str, Any] = {
        "schedules": InMemoryScheduleStore([_row()]),
        "claim": InMemoryClaimStore({"daily": _NOW}),
        "approvals": approvals,
        "enqueue": enqueue,
    }
    count = await scheduler_tick(ctx)
    assert count == 1
    assert ("run_agent", "daily") in enqueued
    # No approval routing here anymore — the reconciler owns expiry.
    assert not any(e[0] == "resume_run" for e in enqueued)
    # The approval stays pending (untouched) for the reconciler to expire + route.
    assert len(await approvals.list_pending("u:1")) == 1


async def test_resume_run_completes_after_grant() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    sent: list[dict[str, object]] = []
    ctx: dict[str, Any] = {
        "store": store,
        "approvals": approvals,
        "provider": _read_then_send(),
        "schedules": InMemoryScheduleStore([_row()]),
        "sent": sent,
    }
    await run_agent(ctx, "daily")  # suspends
    aid = (await approvals.list_pending("u:1"))[0].id
    await approvals.resolve(aid, "granted", "me")
    run_id = store.snapshot(digest_session_id("u:1"))[-1].run_id

    ctx2: dict[str, Any] = {
        "store": store,
        "approvals": approvals,
        "provider": ScriptedProviderGateway(
            [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
        ),
        "sent": sent,
    }
    reason = await resume_run(ctx2, digest_session_id("u:1"), run_id, "u:1")
    assert reason == StopReason.completed.value
    assert sent == [{"to": "finance@external.example", "idempotency_key": "k"}]


async def test_run_agent_dispatches_to_consolidation(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    async def fake_consolidate(ctx: dict[str, Any], row: ScheduleRow, settings: Any) -> str:
        seen.append(row.id)
        return "completed"

    monkeypatch.setattr("keel_worker.main.consolidate_memory", fake_consolidate)
    row = ScheduleRow(
        id="memory-consolidation:u:1",
        scope_id="u:1",
        agent_id="memory-consolidator",
        session_id="consolidation:u:1",
        trigger_kind="interval",
        spec="86400",
        next_run_at=_NOW,
        interval_s=86400,
    )
    ctx: dict[str, Any] = {"schedules": InMemoryScheduleStore([row])}
    assert await run_agent(ctx, "memory-consolidation:u:1") == "completed"
    assert seen == ["memory-consolidation:u:1"]


async def test_run_agent_rejects_unknown_agent_id() -> None:
    row = ScheduleRow(
        id="weird",
        scope_id="u:1",
        agent_id="mystery",
        session_id="s",
        trigger_kind="interval",
        spec="86400",
        next_run_at=_NOW,
        interval_s=86400,
    )
    ctx: dict[str, Any] = {"schedules": InMemoryScheduleStore([row])}
    assert await run_agent(ctx, "weird") == "unsupported"


async def test_reconcile_runs_tick_owns_approval_expiry_routing() -> None:
    # The reconciler is the single owner of approval expiry (item 6): it resumes a durable
    # interactive approval through run_interactive and a legacy approval through resume_run.
    from keel_core.runs import (
        InMemoryRunStore,
        RunBudgetSpec,
        RunStatus,
        RunSurface,
        action_hash,
    )
    from keel_worker.runs import reconcile_runs_tick

    scope = "web:local"
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    now = datetime.now(UTC)
    await runs.admit(
        run_id="run-1",
        scope_id=scope,
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="s1",
        surface=RunSurface.web.value,
        idempotency_key="k1",
        budget=RunBudgetSpec(),
        expires_at=now + timedelta(hours=1),
        snapshot=AgentConfigSnapshot(agent_id="agent-1"),
    )
    await runs.mark_queued("run-1")
    lease = await runs.claim("run-1", worker_id="w1", lease_seconds=30)
    assert lease is not None
    await runs.release(lease, to_status=RunStatus.waiting_approval)
    await approvals.create_pending(
        scope_id=scope,
        run_id="run-1",
        session_id="s1",
        tool="email.send",
        args={},
        call_id="c1",
        idempotency_key="i1",
        reason="tainted",
        expires_at=now - timedelta(seconds=1),
        org_id="org-1",
        actor="user-1",
        action_hash=action_hash("email.send", {}),
        run_attempt=1,
    )
    await approvals.create_pending(
        scope_id=scope,
        run_id="legacy-run",
        session_id="sched-sess",
        tool="email.send",
        args={},
        call_id="c2",
        idempotency_key="i2",
        reason="tainted",
        expires_at=now - timedelta(seconds=1),
    )
    enqueued: list[tuple[Any, ...]] = []

    async def enqueue(name: str, *args: object) -> None:
        enqueued.append((name, *args))

    ctx: dict[str, Any] = {
        "runs": runs,
        "store": events,
        "approvals": approvals,
        "enqueue": enqueue,
        "durable_scope": scope,
    }
    await reconcile_runs_tick(ctx)
    assert ("run_interactive", "run-1", scope) in enqueued  # durable interactive routing
    assert ("resume_run", "sched-sess", "legacy-run", scope) in enqueued  # legacy routing
    record = await runs.get("run-1")
    assert record is not None and record.status is RunStatus.queued


def test_worker_registers_job_functions_and_dispatch_cron() -> None:
    run_job_function = next(
        function
        for function in WorkerSettings.functions
        if isinstance(function, Function) and function.coroutine is run_job
    )
    assert run_job_function.name == "run_job"
    assert run_job_function.timeout_s == get_settings().job_execution_timeout_seconds == 3600
    assert run_job_function.max_tries == 1
    assert run_job not in WorkerSettings.functions
    assert dispatch_jobs in WorkerSettings.functions
    dispatch_cron = next(job for job in WorkerSettings.cron_jobs if job.coroutine is dispatch_jobs)
    assert dispatch_cron.second == {0, 30}


def test_production_job_registry_contains_only_knowledge_kinds() -> None:
    settings = get_settings()
    store = InMemoryKnowledgeStore(
        "web:local",
        document_max_bytes=settings.knowledge_document_max_bytes,
    )
    embedder = FakeEmbedder(dim=settings.embedding_dim, model=settings.embedding_model)
    registry = knowledge_job_registry(store, embedder, settings)
    assert isinstance(registry, JobRegistry)
    assert registry.kinds() == ("knowledge.delete", "knowledge.ingest")


def test_knowledge_job_definitions_use_durable_cancellation_and_leases() -> None:
    settings = get_settings()
    definitions = knowledge_job_definitions(
        InMemoryKnowledgeStore("web:local"),
        FakeEmbedder(dim=settings.embedding_dim, model=settings.embedding_model),
        settings,
    )
    by_kind = {definition.kind: definition for definition in definitions}

    ingest = by_kind["knowledge.ingest"]
    assert ingest.cancel_mode is CancelMode.cooperative
    assert ingest.max_attempts == 3
    assert ingest.lease_seconds == settings.job_lease_seconds
    assert ingest.on_cancelled is not None
    assert ingest.on_failed is not None

    delete = by_kind["knowledge.delete"]
    assert delete.cancel_mode is CancelMode.disabled
    assert delete.max_attempts == 2_147_483_647
    assert delete.lease_seconds == settings.job_lease_seconds
    assert delete.on_cancelled is None
    assert delete.on_failed is not None


async def test_worker_enqueue_adapter_forwards_arq_options() -> None:
    seen: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    class Pool:
        async def enqueue_job(self, name: str, *args: object, **options: object) -> None:
            seen.append((name, args, options))

    defer_until = _NOW + timedelta(seconds=5)
    await _enqueue_arq(
        Pool(),
        "run_job",
        "web:local",
        "job_1",
        _defer_until=defer_until,
    )
    assert seen == [
        (
            "run_job",
            ("web:local", "job_1"),
            {"_defer_until": defer_until},
        )
    ]
