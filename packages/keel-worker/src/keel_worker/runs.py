"""Worker-owned durable interactive runs (M3.6, WS-M).

The arq job bodies that move interactive execution off ``keel-server``: ``run_interactive``
claims a fenced lease on a durable run and drives it through the shared agent loop via
:func:`keel_core.run_service.execute_run`; ``reconcile_runs_tick`` recovers stuck runs
(admitted-but-undispatched, expired leases, past-deadline). Both reuse the durable
``RunStore`` + event store + approvals + provider that ``startup`` wired into ``ctx``.

The worker owns the run task and the approval suspension — the server only admits + streams.
The interactive Agent is rebuilt from the **persisted** selected-Agent profile bound at
admission and the run's scope, with **capability parity** to the server web runtime (file/
shell + memory + Knowledge tools) via the shared builders. Agent visibility / org membership
/ archived status is re-checked at claim time (and fails the run closed if revoked).
"""

from __future__ import annotations

import logging
import socket
import uuid
from datetime import UTC, datetime
from typing import Any

from keel_core.approvals import ApprovalRecord, ApprovalStore
from keel_core.config import Settings, get_settings
from keel_core.errors import PermissionDenied
from keel_core.identity import IdentityService, NotFoundError
from keel_core.interactive import (
    LOCAL_PREVIEW_AGENT_NAME,
    LOCAL_PREVIEW_ORG_ID,
    InteractiveCapabilities,
    build_interactive_agent,
    build_interactive_registry,
    interactive_permissions,
)
from keel_core.loop import ToolRegistry, admit
from keel_core.memory import PostgresMemoryStore, format_core_memory
from keel_core.run_service import (
    DurableRunService,
    SystemContextFn,
    VisibilityCheck,
    execute_run,
    prompt_persisted_in_log,
    reconcile_runs,
)
from keel_core.runs import RunRecord, RunStatus, RunStore
from keel_core.types import ScopeId

logger = logging.getLogger("keel.worker.runs")

_WORKER_ID = f"{socket.gethostname()}:{uuid.uuid4().hex[:8]}"


def _capabilities(settings: Settings) -> InteractiveCapabilities:
    """The memory/Knowledge caps for the worker interactive registry (server parity)."""
    return InteractiveCapabilities(
        memory_block_max_chars=settings.memory_block_max_chars,
        session_embedding_batch_size=settings.session_embedding_batch_size,
        session_embedding_catchup_limit=settings.session_embedding_catchup_limit,
        knowledge_search_query_max_chars=settings.knowledge_search_query_max_chars,
        knowledge_search_k_max=settings.knowledge_search_k_max,
        knowledge_tool_output_max_chars=settings.knowledge_tool_output_max_chars,
    )


def _is_local_preview(record: RunRecord) -> bool:
    """A local-preview (non-cloud single-operator) run binds the explicit local org."""
    return record.org_id == LOCAL_PREVIEW_ORG_ID


async def _resolve_agent_profile(
    identity: IdentityService | None, record: RunRecord
) -> tuple[str, str, str]:
    """Resolve ``(agent_id, name, persona)`` from the persisted selected Agent.

    A local-preview run (or a worker with no identity service) uses the explicit local-
    preview profile. A cloud run loads the persisted Agent bound at admission; if it is not
    visible / permitted / archived at build time we fall back to a safe empty profile and let
    the claim-time visibility check terminalize the run closed before it executes.
    """
    if identity is None or _is_local_preview(record):
        return record.agent_id, LOCAL_PREVIEW_AGENT_NAME, ""
    try:
        agent = await identity.select_agent(record.org_id, record.actor, record.agent_id)
    except (NotFoundError, PermissionDenied):
        return record.agent_id, record.agent_id, ""
    return agent.id, agent.name, agent.persona


def _visibility_check(identity: IdentityService | None) -> VisibilityCheck | None:
    """Re-check membership / Agent visibility / archived status at claim (fail closed).

    Returns ``None`` (no check) for the local-preview single tenant or a worker without an
    identity service. For a cloud run it re-runs the exact use-authorization the admission
    performed (:meth:`IdentityService.select_agent`): a revoked membership, a hidden/deleted
    Agent, or an archived Agent between admit and claim fails the run closed.
    """
    if identity is None:
        return None

    async def _visible(record: RunRecord) -> bool:
        if _is_local_preview(record):
            return True
        try:
            await identity.select_agent(record.org_id, record.actor, record.agent_id)
        except (NotFoundError, PermissionDenied):
            return False
        return True

    return _visible


def _system_context(engine: Any, scope_id: ScopeId, persona: str) -> SystemContextFn | None:
    """The run's standing system prompt: the Agent persona + durable core-memory blocks.

    Matches the server web runtime's core-memory injection and additionally prepends the
    persisted Agent persona so a worker-owned run behaves as the selected Agent.
    """
    if engine is None and not persona:
        return None

    async def _ctx() -> str:
        parts: list[str] = []
        if persona:
            parts.append(persona)
        if engine is not None:
            blocks = await PostgresMemoryStore(engine, scope_id).blocks()
            memory = format_core_memory(blocks)
            if memory:
                parts.append(memory)
        return "\n\n".join(parts)

    return _ctx


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

    identity: IdentityService | None = ctx.get("identity")
    engine = ctx.get("engine")
    embedder = ctx.get("embedder")
    environment = ctx["execution_environment"]
    # Capability parity with the server web runtime: the same file/shell + memory + Knowledge
    # tools + permissions, built from the shared builders (one toolset contract, two surfaces).
    tools, extra_names = build_interactive_registry(
        environment,
        engine=engine,
        scope_id=lease.scope_id,
        embedder=embedder,
        caps=_capabilities(settings),
    )
    agent_id, agent_name, persona = await _resolve_agent_profile(identity, record)
    agent = build_interactive_agent(
        scope_id=lease.scope_id,
        model=settings.default_model,
        agent_id=agent_id,
        name=agent_name,
        persona=persona,
        extra_tool_names=extra_names,
    )
    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=event_store,
        approvals=ctx["approvals"],
        agent=agent,
        provider=ctx["provider"],
        registry=ToolRegistry(tools),
        permissions=interactive_permissions(extra_names),
        admit_fn=admit,
        approval_ttl_hours=settings.approval_timeout_hours,
        # Re-check Agent visibility / org membership / archived at claim (revoke fails closed).
        visibility_check=_visibility_check(identity),
        system_context=_system_context(engine, lease.scope_id, persona),
        # Resume vs fresh-start is decided atomically at claim time (explicit durable marker
        # or a reclaimed mid-approval run), never inferred from a mutable pre-claim status.
        resume=lease.resume,
    )
    logger.info(
        "run_interactive scope=%s run=%s attempt=%d resume=%s status=%s agent=%s worker=%s",
        lease.scope_id,
        run_id,
        lease.attempt,
        lease.resume,
        final.status.value,
        agent_id,
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

    async def _legacy_resume(record: ApprovalRecord) -> None:
        # A legacy scheduled/digest approval resumes through its own (non-durable) job.
        await enqueue("resume_run", record.session_id, record.run_id, record.scope_id)

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
    # Single owner of approval expiry: routes durable interactive approvals through the run
    # state machine and legacy approvals through resume_run (item 6).
    resumed = await service.expire_approvals(now=now, legacy_resume=_legacy_resume)
    # Crash-tolerant backstop (blocker 1): requeue any run left in waiting_approval whose
    # approval batch is fully terminal but whose atomic requeue/dispatch did not complete.
    repaired = await service.repair_stuck_resumes()
    return result.redispatched + result.reclaimed + result.expired + resumed + repaired


__all__ = ["reconcile_runs_tick", "run_interactive"]
