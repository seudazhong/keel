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
from keel_core.config import Settings, get_settings
from keel_core.interactive import build_interactive_tools, interactive_permissions
from keel_core.loop import ToolRegistry, admit
from keel_core.run_service import execute_run, reconcile_runs
from keel_core.runs import RunStatus, RunStore
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

    # A run that had already suspended once and is now being resumed replays over the log.
    resume = record.attempt >= 1 and record.status is RunStatus.waiting_approval

    environment = ctx["execution_environment"]
    registry = ToolRegistry(build_interactive_tools(environment))
    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=ctx["store"],
        approvals=ctx["approvals"],
        agent=_interactive_agent(lease.scope_id, settings.default_model),
        provider=ctx["provider"],
        registry=registry,
        permissions=interactive_permissions(),
        admit_fn=admit,
        approval_ttl_hours=settings.approval_timeout_hours,
        resume=resume,
    )
    logger.info(
        "run_interactive scope=%s run=%s attempt=%d status=%s worker=%s",
        lease.scope_id,
        run_id,
        lease.attempt,
        final.status.value,
        _WORKER_ID,
    )
    return final.status.value


async def reconcile_runs_tick(ctx: dict[str, Any]) -> int:
    """Recover admitted-but-undispatched / expired-lease / past-deadline runs (cron)."""
    run_store: RunStore = ctx["runs"]
    enqueue = ctx["enqueue"]
    scope_id = str(ctx["durable_scope"])

    async def _enqueue(run_id: str) -> None:
        await enqueue("run_interactive", run_id, scope_id)

    result = await reconcile_runs(run_store=run_store, enqueue=_enqueue, now=datetime.now(UTC))
    return result.redispatched + result.reclaimed + result.expired


__all__ = ["reconcile_runs_tick", "run_interactive"]
