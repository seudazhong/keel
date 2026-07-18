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
from pathlib import Path
from typing import Any

from keel_core.approvals import ApprovalRecord, ApprovalStore, PostgresApprovalStore
from keel_core.config import Settings, get_settings
from keel_core.errors import PermissionDenied
from keel_core.identity import IdentityService, NotFoundError
from keel_core.im_routing import (
    ImChatKind,
    ImInboundContext,
    ImMappingStore,
    ImProvider,
    ImReplyDispatchIndex,
    ImReplyPolicy,
    ImReplyStore,
    PostgresImMappingStore,
    PostgresImReplyDispatchIndex,
    PostgresImReplyStore,
    ReplySender,
    build_im_safe_agent,
    deliver_reply,
    final_reply_text_in_log,
    im_context_in_log,
    im_safe_permissions,
    im_safe_tools,
    persist_terminal_reply,
)
from keel_core.interactive import (
    LOCAL_PREVIEW_AGENT_NAME,
    LOCAL_PREVIEW_ORG_ID,
    InteractiveCapabilities,
    build_im_readonly_extras,
    build_interactive_agent,
    build_interactive_registry,
    interactive_permissions,
)
from keel_core.loop import ToolRegistry, admission_model_in_log, admit
from keel_core.memory import PostgresMemoryStore, format_core_memory
from keel_core.run_dispatch import RunDispatchOutbox
from keel_core.run_service import (
    DurableRunService,
    SystemContextFn,
    VisibilityCheck,
    execute_run,
    prompt_persisted_in_log,
    reconcile_runs,
)
from keel_core.runs import PostgresRunStore, RunRecord, RunStatus, RunStore, RunSurface
from keel_core.scoping import ScopeValidationError, validate_scope_id
from keel_core.secrets import KeyRing, SecretsError, keyring_from_settings
from keel_core.state import PostgresEventStore
from keel_core.tools import (
    ExecutionEnvironment,
    UnavailableExecutionEnvironment,
    build_scoped_execution_environment,
)
from keel_core.types import ScopeId

logger = logging.getLogger("keel.worker.runs")

_WORKER_ID = f"{socket.gethostname()}:{uuid.uuid4().hex[:8]}"


def _scoped_stores(
    ctx: dict[str, Any], scope_id: ScopeId
) -> tuple[RunStore, PostgresEventStore | Any, ApprovalStore]:
    """Build the run/event/approval stores bound to ``scope_id`` (M3.6, item 3).

    The worker executes runs across many per-Agent scopes, so it constructs each run's stores
    for that run's *own* ``scope_id`` (revalidated) rather than a single process-wide scope. A
    Postgres engine yields per-scope stores; the in-memory test doubles in ``ctx`` are used
    unchanged when no engine is wired.
    """
    engine = ctx.get("engine")
    if engine is not None:
        return (
            PostgresRunStore(engine, scope_id),
            PostgresEventStore(engine, scope_id),
            PostgresApprovalStore(engine, scope_id),
        )
    return ctx["runs"], ctx["store"], ctx["approvals"]


def _scoped_environment(
    ctx: dict[str, Any], scope_id: ScopeId
) -> tuple[ExecutionEnvironment, bool]:
    """A per-scope execution environment + whether the caller owns (must close) it.

    Each scope gets its own isolated workspace (no shared writable workspace across scopes). If
    the worker has the settings + workspace root wired it builds a fresh scoped environment
    (owned here, closed after the run); otherwise it falls back to the shared ``ctx``
    environment (a fail-closed :class:`UnavailableExecutionEnvironment` when none was wired).
    """
    settings = ctx.get("job_settings") or ctx.get("settings")
    root = ctx.get("workspace_root")
    if settings is not None and root is not None:
        env = build_scoped_execution_environment(
            settings, Path(root), service="worker", scope_id=scope_id
        )
        return env, True
    shared = ctx.get("execution_environment")
    if shared is not None:
        return shared, False
    return UnavailableExecutionEnvironment(), False


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


def _im_mapping_store(ctx: dict[str, Any], engine: Any, org_id: str) -> ImMappingStore | None:
    """The org-scoped mapping store used to revalidate an IM run's binding (Postgres or double).

    Prefers an in-memory ``im_mappings`` test double when wired into ``ctx`` (unit tests), else a
    Postgres store bound to the run's org, else ``None`` (an in-memory worker with no mapping
    source available — the Agent-visibility check still applies, but the DB binding re-read is
    skipped)."""
    store: ImMappingStore | None = ctx.get("im_mappings")
    if store is not None:
        return store
    if engine is not None:
        return PostgresImMappingStore(engine, org_id)
    return None


def _im_visibility_check(
    identity: IdentityService | None,
    mapping_store: ImMappingStore | None,
    im_ctx: ImInboundContext | None,
) -> VisibilityCheck | None:
    """Claim-time revalidation for an IM run: Agent visibility **and** the exact channel binding.

    Layers the durable-run Agent/membership visibility check with a re-read of the persisted
    channel mapping: the current mapping must still be ``active`` **and** hash-identically match
    *every* field the run was admitted against — mapping id + version, run-as org member,
    provider/bot/chat identity + kind, bound Agent + scope, and the safe-policy fingerprint (see
    :meth:`ImInboundContext.matches_current_mapping`). A revoke, reprovision, run-as change, Agent
    change, chat-target/kind change or policy tightening between admit and claim therefore fails
    the stale queued run closed (deny) before any provider/model/tool effect — no cross-org Agent
    binding, no stale/hijacked/rebound mapping ever executes. The local-preview single tenant (or a
    worker with no mapping source) skips only the DB binding re-read."""
    base = _visibility_check(identity)

    async def _visible(rec: RunRecord) -> bool:
        if base is not None and not await base(rec):
            return False
        if _is_local_preview(rec) or im_ctx is None or not im_ctx.mapping_id:
            return True
        if mapping_store is None:
            return True
        mapping = await mapping_store.get(im_ctx.mapping_id)
        return im_ctx.matches_current_mapping(mapping)

    return _visible


def _im_reply_stores(
    ctx: dict[str, Any], engine: Any, scope_id: ScopeId
) -> tuple[ImReplyStore | None, ImReplyDispatchIndex | None]:
    """The scope-bound reply outbox + global reply-dispatch index (Postgres or test doubles)."""
    reply_store: ImReplyStore | None = ctx.get("im_replies")
    reply_dispatch: ImReplyDispatchIndex | None = ctx.get("im_reply_dispatch")
    if reply_store is None and engine is not None:
        reply_store = PostgresImReplyStore(engine, scope_id)
    if reply_dispatch is None and engine is not None:
        reply_dispatch = PostgresImReplyDispatchIndex(engine)
    return reply_store, reply_dispatch


def _reply_keyring(ctx: dict[str, Any], settings: Settings) -> KeyRing | None:
    """The envelope keyring used to encrypt a reply payload at rest (fail closed if unset)."""
    keyring: KeyRing | None = ctx.get("keyring")
    if keyring is not None:
        return keyring
    try:
        return keyring_from_settings(settings)
    except SecretsError:
        return None


async def _execute_im_run(
    ctx: dict[str, Any],
    record: RunRecord,
    lease: Any,
    run_store: RunStore,
    event_store: Any,
    approvals: ApprovalStore,
    environment: ExecutionEnvironment,
    settings: Settings,
    identity: IdentityService | None,
    engine: Any,
    embedder: Any,
) -> tuple[RunRecord, str, str]:
    """Execute a claimed **untrusted IM** run on the safe (read-only) Agent + persist its reply.

    The IM run reuses the single durable agent loop via :func:`execute_run`, but with the
    IM-safe toolset (read-only file + grant-gated read-only memory/Knowledge only — no
    write/edit/shell or connector action unless the mapping's policy explicitly approved it),
    an untrusted scope, and a claim-time mapping+Agent revalidation. On a clean completion the
    run's terminal reply is persisted idempotently to the durable, encrypted reply outbox and a
    global dispatch pointer is recorded for the restart-safe sender."""
    im_ctx = await im_context_in_log(event_store, record.session_id, record.id)
    policy = im_ctx.policy if im_ctx is not None else ImReplyPolicy(reply_enabled=False)
    chat_kind = im_ctx.chat_kind if im_ctx is not None else ImChatKind.personal
    extras = build_im_readonly_extras(engine, lease.scope_id, embedder, _capabilities(settings))
    extra_names = tuple(tool.name for tool in extras)
    tools = im_safe_tools(environment) + extras
    agent_id, agent_name, persona = await _resolve_agent_profile(identity, record)
    model = (
        await admission_model_in_log(event_store, record.session_id, record.id)
        or settings.default_model
    )
    agent = build_im_safe_agent(
        scope_id=lease.scope_id,
        model=model,
        agent_id=agent_id,
        name=agent_name,
        chat_kind=chat_kind,
        persona=persona,
        policy=policy,
        read_only_extra=extra_names,
    )
    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=event_store,
        approvals=approvals,
        agent=agent,
        provider=ctx["provider"],
        registry=ToolRegistry(tools),
        permissions=im_safe_permissions(policy, read_only_extra=extra_names),
        admit_fn=admit,
        approval_ttl_hours=settings.approval_timeout_hours,
        visibility_check=_im_visibility_check(
            identity, _im_mapping_store(ctx, engine, record.org_id), im_ctx
        ),
        system_context=_system_context(engine, lease.scope_id, persona),
        resume=lease.resume,
    )
    if final.status is RunStatus.completed and im_ctx is not None:
        await _persist_im_reply(ctx, record, lease.scope_id, im_ctx, event_store, engine, settings)
    return final, agent_id, model


async def _persist_im_reply(
    ctx: dict[str, Any],
    record: RunRecord,
    scope_id: ScopeId,
    im_ctx: ImInboundContext,
    event_store: Any,
    engine: Any,
    settings: Settings,
) -> None:
    """Persist the terminal reply intent (idempotently, encrypted) + its dispatch pointer."""
    reply_store, reply_dispatch = _im_reply_stores(ctx, engine, scope_id)
    keyring = _reply_keyring(ctx, settings)
    if reply_store is None or reply_dispatch is None or keyring is None:
        logger.warning(
            "im reply not persisted run=%s scope=%s (reply store/keyring unavailable)",
            record.id,
            scope_id,
        )
        return
    text_payload = await final_reply_text_in_log(event_store, record.session_id, record.id)
    reply_id = await persist_terminal_reply(
        reply_store,
        reply_dispatch,
        keyring,
        reply_id=uuid.uuid4().hex,
        scope_id=scope_id,
        run_id=record.id,
        org_id=record.org_id,
        context=im_ctx,
        text_payload=text_payload,
    )
    if reply_id is not None:
        logger.info("im reply persisted run=%s scope=%s reply=%s", record.id, scope_id, reply_id)


async def run_interactive(ctx: dict[str, Any], run_id: str, scope_id: str) -> str:
    """Claim + execute (or resume) a durable interactive run under a fenced lease.

    The run's stores, tools, execution environment, and model are all constructed from the
    run's **own** revalidated ``scope_id`` (M3.6, item 3) — the worker is multi-scope, not
    pinned to a single process scope. The model is the one captured at admission (item 5), not
    the worker's process default.
    """
    settings: Settings = get_settings()
    try:
        scope_id = validate_scope_id(scope_id)
    except ScopeValidationError:
        logger.warning("run_interactive rejected malformed scope=%r run=%s", scope_id, run_id)
        return "scope_invalid"
    run_store, event_store, approvals = _scoped_stores(ctx, scope_id)
    record = await run_store.get(run_id)
    if record is None:
        return "missing"
    # Fail closed: the run must live in the scope it was dispatched under (no cross-scope claim).
    if record.scope_id != scope_id:
        logger.warning(
            "run_interactive scope mismatch run=%s record=%s dispatched=%s",
            run_id,
            record.scope_id,
            scope_id,
        )
        return "scope_mismatch"
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
    # Per-scope, isolated execution environment (no shared writable workspace across scopes).
    environment, owns_environment = _scoped_environment(ctx, lease.scope_id)
    try:
        if record.surface == RunSurface.im.value:
            # Untrusted IM surface: same durable loop, but the read-only safe Agent + a
            # claim-time mapping/Agent/provider revalidation + a durable encrypted reply.
            final, agent_id, model = await _execute_im_run(
                ctx,
                record,
                lease,
                run_store,
                event_store,
                approvals,
                environment,
                settings,
                identity,
                engine,
                embedder,
            )
        else:
            # Capability parity with the server web runtime: the same file/shell + memory +
            # Knowledge tools + permissions, from the shared builders (one contract, two
            # surfaces).
            tools, extra_names = build_interactive_registry(
                environment,
                engine=engine,
                scope_id=lease.scope_id,
                embedder=embedder,
                caps=_capabilities(settings),
            )
            agent_id, agent_name, persona = await _resolve_agent_profile(identity, record)
            # The model captured at admission (reproducibility) — not the worker's default.
            model = (
                await admission_model_in_log(event_store, record.session_id, run_id)
                or settings.default_model
            )
            agent = build_interactive_agent(
                scope_id=lease.scope_id,
                model=model,
                agent_id=agent_id,
                name=agent_name,
                persona=persona,
                extra_tool_names=extra_names,
            )
            final = await execute_run(
                lease=lease,
                run_store=run_store,
                event_store=event_store,
                approvals=approvals,
                agent=agent,
                provider=ctx["provider"],
                registry=ToolRegistry(tools),
                permissions=interactive_permissions(extra_names),
                admit_fn=admit,
                approval_ttl_hours=settings.approval_timeout_hours,
                # Re-check Agent visibility / org membership / archived at claim (revoke fails
                # closed).
                visibility_check=_visibility_check(identity),
                system_context=_system_context(engine, lease.scope_id, persona),
                # Resume vs fresh-start is decided atomically at claim time (explicit durable
                # marker or a reclaimed mid-approval run), never inferred from a mutable
                # pre-claim status.
                resume=lease.resume,
            )
    finally:
        if owns_environment:
            await environment.aclose()
    logger.info(
        "run_interactive scope=%s run=%s attempt=%d resume=%s status=%s agent=%s model=%s "
        "worker=%s",
        lease.scope_id,
        run_id,
        lease.attempt,
        lease.resume,
        final.status.value,
        agent_id,
        model,
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


async def reconcile_dispatch_tick(ctx: dict[str, Any]) -> int:
    """Cross-scope durable reconciliation driven by the global dispatch outbox (finding 4).

    The scope-partitioned ``runs`` table is RLS-forced, so a worker bound to one scope cannot
    see another's runs. The global :class:`~keel_core.run_dispatch.RunDispatchOutbox` is the
    one cross-scope index: admission records ``(run_id, scope_id)`` there, and this tick leases
    a batch of due intents (fenced so duplicate workers never both process one), reconciles
    **each distinct scope** the batch touches (redispatch queued-but-undispatched, reclaim
    expired leases, expire past-deadline runs, expire approvals, repair stuck resumes), then
    removes the intents of runs that have reached a terminal state and defers the rest. This
    replaces the previous ``web:local``-pinned reconciler — every per-Agent scope with open work
    is now recovered by any worker.
    """
    outbox: RunDispatchOutbox | None = ctx.get("dispatch_outbox")
    if outbox is None:
        return 0
    enqueue = ctx["enqueue"]
    now = datetime.now(UTC)
    claimed = await outbox.claim_due(worker_id=_WORKER_ID, now=now)
    if not claimed:
        return 0

    scopes: set[ScopeId] = set()
    for intent in claimed:
        try:
            scopes.add(validate_scope_id(intent.scope_id))
        except ScopeValidationError:
            # A malformed scope can never be reconciled — drop its intent (fail closed).
            await outbox.remove(intent.run_id)
            logger.warning("dropped outbox intent with malformed scope=%r", intent.scope_id)

    reconciled = 0
    for scope_id in scopes:
        run_store, event_store, approvals = _scoped_stores(ctx, scope_id)

        async def _enqueue(run_id: str, _scope: ScopeId = scope_id) -> None:
            await enqueue("run_interactive", run_id, _scope)

        async def _prompt_persisted(record: RunRecord, _events: Any = event_store) -> bool:
            return await prompt_persisted_in_log(_events, record.session_id, record.id)

        async def _legacy_resume(record: ApprovalRecord) -> None:
            await enqueue("resume_run", record.session_id, record.run_id, record.scope_id)

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
            dispatch_outbox=outbox,
        )
        resumed = await service.expire_approvals(now=now, legacy_resume=_legacy_resume)
        repaired = await service.repair_stuck_resumes()
        reconciled += result.redispatched + result.reclaimed + result.expired + resumed + repaired

    # Retire intents whose run is terminal (nothing left to dispatch); defer the rest so they
    # are re-checked on a later tick without spinning.
    for intent in claimed:
        run_store, events_for_intent, _approvals = _scoped_stores(ctx, intent.scope_id)
        record = await run_store.get(intent.run_id)
        if record is None or record.status in {
            RunStatus.completed,
            RunStatus.failed,
            RunStatus.cancelled,
            RunStatus.interrupted,
            RunStatus.expired,
        }:
            # Backstop the crash-after-terminal / before-reply-intent window: a completed IM run
            # must have its durable reply persisted before its dispatch pointer is retired
            # (idempotent — a no-op if the worker already recorded it inline).
            if (
                record is not None
                and record.status is RunStatus.completed
                and (record.surface == RunSurface.im.value)
            ):
                await _ensure_im_reply(ctx, record, events_for_intent)
            await outbox.remove(intent.run_id)
        else:
            await outbox.reschedule(intent.run_id, now=now)
    return reconciled


async def _ensure_im_reply(ctx: dict[str, Any], record: RunRecord, event_store: Any) -> None:
    """Ensure a terminal IM run's durable reply intent exists (crash-window repair)."""
    im_ctx = await im_context_in_log(event_store, record.session_id, record.id)
    if im_ctx is None:
        return
    engine = ctx.get("engine")
    await _persist_im_reply(
        ctx, record, record.scope_id, im_ctx, event_store, engine, get_settings()
    )


async def _im_reply_still_bound(ctx: dict[str, Any], engine: Any, reply: Any) -> bool:
    """Whether the reply's admitted channel binding is still the exact, active mapping.

    Fail-closed reply-time revalidation mirroring claim time: re-reads the run's admitted IM
    binding from the durable log and the current mapping row, and confirms the mapping still
    exists, is ``active``, and hash-identically matches every admitted binding field. A
    revoked/reprovisioned/rebound mapping (or a run whose admitted binding can no longer be
    resolved) denies delivery. When no mapping source is wired (an in-memory worker with neither a
    Postgres engine nor an ``im_mappings`` double) the reply is allowed — there is nothing to
    revalidate against."""
    mapping_store = _im_mapping_store(ctx, engine, reply.org_id)
    if mapping_store is None:
        return True
    run_store, event_store, _approvals = _scoped_stores(ctx, reply.scope_id)
    record = await run_store.get(reply.run_id)
    if record is None:
        return False
    im_ctx = await im_context_in_log(event_store, record.session_id, reply.run_id)
    if im_ctx is None or not im_ctx.mapping_id:
        return False
    mapping = await mapping_store.get(im_ctx.mapping_id)
    return im_ctx.matches_current_mapping(mapping)


async def send_im_replies_tick(ctx: dict[str, Any]) -> int:
    """Restart-safe durable IM reply sender (cron), driven by the global reply-dispatch index.

    Leases a batch of due reply pointers (fenced so two senders never both deliver one), binds
    each pointer's scope, claims the encrypted reply intent under a fencing token, decrypts it
    in-memory, sends through the mapped provider's adapter with the durable idempotency key, and
    records the delivery result — retiring the pointer on success or rescheduling it on failure.
    A crash after send / before ack is repaired without a duplicate user-visible reply where the
    provider dedupes (otherwise at-least-once)."""
    reply_dispatch: ImReplyDispatchIndex | None = ctx.get("im_reply_dispatch")
    engine = ctx.get("engine")
    if reply_dispatch is None and engine is not None:
        reply_dispatch = PostgresImReplyDispatchIndex(engine)
    if reply_dispatch is None:
        return 0
    senders: dict[ImProvider, ReplySender] = ctx.get("im_senders", {})
    keyring = _reply_keyring(ctx, get_settings())
    if keyring is None or not senders:
        return 0
    now = datetime.now(UTC)
    claimed = await reply_dispatch.claim_due(worker_id=_WORKER_ID, now=now)
    sent = 0
    for intent in claimed:
        try:
            scope_id = validate_scope_id(intent.scope_id)
        except ScopeValidationError:
            await reply_dispatch.remove(intent.reply_id)
            continue
        reply_store, _rd = _im_reply_stores(ctx, engine, scope_id)
        if reply_store is None:
            continue
        current = await reply_store.get(intent.reply_id)
        if current is None:
            await reply_dispatch.remove(intent.reply_id)
            continue
        # Reply-time revalidation (fail closed): the run's admitted channel binding must still be
        # the exact active mapping. A mapping revoked/rebound/reprovisioned between run completion
        # and delivery denies the reply — a stale reply is never sent to a chat the org no longer
        # authorizes (the durable intent is terminally denied and its pointer retired).
        if not await _im_reply_still_bound(ctx, engine, current):
            await reply_store.deny(intent.reply_id, reason="mapping revoked/rebound", now=now)
            await reply_dispatch.remove(intent.reply_id)
            continue
        sender = senders.get(current.provider)
        if sender is None:
            await reply_dispatch.reschedule(intent.reply_id, now=now)
            continue
        delivered = await deliver_reply(
            reply_store,
            reply_dispatch,
            keyring,
            sender,
            reply_id=intent.reply_id,
            worker_id=_WORKER_ID,
            now=now,
        )
        sent += 1 if delivered else 0
    return sent


__all__ = ["reconcile_dispatch_tick", "reconcile_runs_tick", "run_interactive"]
