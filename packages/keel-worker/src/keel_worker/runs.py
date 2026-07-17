"""Worker-owned durable interactive runs (M3.6, WS-M).

The arq job bodies that move interactive execution off ``keel-server``: ``run_interactive``
claims a fenced lease on a durable run and drives it through the shared agent loop via
:func:`keel_core.run_service.execute_run`; ``reconcile_runs_tick`` recovers stuck runs
(admitted-but-undispatched, expired leases, past-deadline). Both reuse the durable
``RunStore`` + event store + approvals + provider that ``startup`` wired into ``ctx``.

The worker owns the run task and the approval suspension — the server only admits + streams.
"""

from __future__ import annotations

import logging
import socket
import uuid
from datetime import UTC, datetime
from typing import Any

from keel_core.agents import AgentSpec, Scope
from keel_core.approvals import ApprovalStore
from keel_core.config import Settings, get_settings
from keel_core.interactive import build_interactive_tools, interactive_permissions
from keel_core.loop import ToolRegistry, admit
from keel_core.run_service import (
    DurableRunService,
    execute_run,
    prompt_persisted_in_log,
    reconcile_runs,
)
from keel_core.runs import RunRecord, RunStatus, RunStore
from keel_core.types import ScopeKind, TrustLevel

logger = logging.getLogger("keel.worker.runs")

_WORKER_ID = f"{socket.gethostname()}:{uuid.uuid4().hex[:8]}"


def _interactive_agent(scope_id: str, model: str) -> AgentSpec:
    scope = Scope(id=scope_id, kind=ScopeKind.personal, trust=TrustLevel.trusted)
    return AgentSpec(
        id="web",
        name="Keel Web",
        model=model,
        scope=scope,
        toolset=["read", "ls", "glob", "grep", "write", "edit", "shell"],
    )


async def run_interactive(ctx: dict[str, Any], run_id: str, scope_id: str) -> str:
    """Claim + execute (or resume) a durable interactive run under a fenced lease."""
    settings: Settings = get_settings()
    durable_scope = str(ctx["durable_scope"])
    if scope_id != durable_scope:
        logger.warning(
            "run_interactive scope mismatch configured=%s got=%s", durable_scope, scope_id
        )
        return "scope_mismatch"
    run_store: RunStore = ctx["runs"]
    event_store = ctx["store"]
    record = await run_store.get(run_id)
    if record is None:
        return "missing"
    if record.status in {
        RunStatus.completed,
        RunStatus.failed,
        RunStatus.cancelled,
        RunStatus.interrupted,
        RunStatus.expired,
    }:
        return record.status.value

    # Fail closed: never drive a run whose prompt was not durably admitted (invariant I2).
    if not await prompt_persisted_in_log(event_store, record.session_id, run_id):
        logger.warning("run_interactive prompt-less run=%s scope=%s", run_id, scope_id)
        return "prompt_missing"

    lease = await run_store.claim(
        run_id,
        worker_id=_WORKER_ID,
        now=datetime.now(UTC),
        lease_seconds=settings.job_lease_seconds,
    )
    if lease is None:
        # Another worker owns a live lease, or the run is not currently claimable.
        current = await run_store.get(run_id)
        return current.status.value if current is not None else "missing"

    environment = ctx["execution_environment"]
    registry = ToolRegistry(build_interactive_tools(environment))
    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=event_store,
        approvals=ctx["approvals"],
        agent=_interactive_agent(lease.scope_id, settings.default_model),
        provider=ctx["provider"],
        registry=registry,
        permissions=interactive_permissions(),
        admit_fn=admit,
        approval_ttl_hours=settings.approval_timeout_hours,
        # Resume vs fresh-start is decided atomically at claim time (explicit durable marker
        # or a reclaimed mid-approval run), never inferred from a mutable pre-claim status.
        resume=lease.resume,
    )
    logger.info(
        "run_interactive scope=%s run=%s attempt=%d resume=%s status=%s worker=%s",
        lease.scope_id,
        run_id,
        lease.attempt,
        lease.resume,
        final.status.value,
        _WORKER_ID,
    )
    return final.status.value


async def reconcile_runs_tick(ctx: dict[str, Any]) -> int:
    """Recover stuck runs + expire timed-out approvals (cron).

    Redispatches admitted/queued-but-undispatched runs (never prompt-less), reclaims expired
    leases, expires past-deadline runs, and resumes runs whose durable approval timed out."""
    run_store: RunStore = ctx["runs"]
    event_store = ctx["store"]
    approvals: ApprovalStore = ctx["approvals"]
    enqueue = ctx["enqueue"]
    scope_id = str(ctx["durable_scope"])

    async def _enqueue(run_id: str) -> None:
        await enqueue("run_interactive", run_id, scope_id)

    async def _prompt_persisted(record: RunRecord) -> bool:
        return await prompt_persisted_in_log(event_store, record.session_id, record.id)

    now = datetime.now(UTC)
    result = await reconcile_runs(
        run_store=run_store,
        enqueue=_enqueue,
        prompt_persisted=_prompt_persisted,
        now=now,
    )
    service = DurableRunService(
        run_store=run_store,
        event_store=event_store,
        approvals=approvals,
        scope_id=scope_id,
        enqueue=_enqueue,
        admit_fn=admit,
    )
    resumed = await service.expire_approvals(now=now)
    return result.redispatched + result.reclaimed + result.expired + resumed


__all__ = ["reconcile_runs_tick", "run_interactive"]
