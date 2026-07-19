"""Production worker wiring for controlled patch proposals (WS-PP, P3b-1).

This module owns the *worker-side* construction for M4 controlled patches — it adds no server
router, SDK, or migration. It provides three things:

* :func:`build_patch_worker_components` — build the long-lived, shared patch dependencies once at
  worker startup (global proposal store + dispatch outbox, the project-service authorizer with
  coding storage + GitHub, the sandboxed-loop author over a *shared* transfer client, the
  generation/writeback services). Per-scope stores (run/approval/events/jobs) are built on demand,
  never shared across scopes.
* :func:`build_patch_coordinator` — a scope-pinned :class:`PatchCoordinator` factory used by BOTH
  the per-scope job handlers and the reconciler. Its run/approval factories fail closed when asked
  for any scope other than the one they are pinned to, so a proposal whose canonical per-Agent
  scope disagrees with the claimed scope can never create/claim/charge a cross-scope run.
* :class:`PatchOutboxReconciler` — a fenced, bounded reconciler over the global
  :class:`PatchProposalOutbox`. It leases a batch of due pointers (SKIP LOCKED under a random lease
  token), reconciles each proposal's authoritative status under org RLS, and drives the durable
  patch lifecycle forward — enqueueing generate/writeback jobs (with a cross-scope dispatch intent
  the existing job-dispatch reconciler dispatches), auto-healing ``ready`` into
  ``approval_pending``,
  repairing stranded proposals whose servicing job died, expiring on TTL, and retiring stale
  pointers — always presenting the claim lease token so a stale token stops processing with no
  unfenced mutation.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import ApprovalStore, PostgresApprovalStore
from keel_core.coding import LocalArtifactStore, LocalCodingStorage
from keel_core.coding.storage_root import (
    SharedStorageUnavailable,
    resolve_project_storage_root,
    verify_shared_storage,
)
from keel_core.config import Settings
from keel_core.errors import PermissionDenied
from keel_core.identity.store import IdentityStore
from keel_core.job_dispatch import JobDispatchOutbox
from keel_core.jobs import CancelMode, JobLimits, JobStatus, JobStore, PostgresJobStore
from keel_core.patch.author import SandboxedLoopPatchAuthor
from keel_core.patch.authorizer import ProjectServicePatchAuthorizer
from keel_core.patch.coordinator import PatchCoordinator
from keel_core.patch.errors import PatchError, PatchValidationError
from keel_core.patch.generation import PatchGenerationService
from keel_core.patch.jobs import (
    PATCH_GENERATE_KIND,
    PATCH_GENERATE_MAX_ATTEMPTS,
    PATCH_WRITEBACK_KIND,
    PATCH_WRITEBACK_MAX_ATTEMPTS,
    PatchJobHandlers,
    PatchWritebackJobPayload,
    patch_generate_idempotency_key,
    patch_writeback_idempotency_key,
)
from keel_core.patch.ledger import PostgresPatchWritebackLedger
from keel_core.patch.models import (
    TERMINAL_STATUSES,
    PatchProposal,
    PatchStatus,
    can_transition,
)
from keel_core.patch.outbox import (
    PatchOutboxEntry,
    PatchProposalOutbox,
    PostgresPatchProposalOutbox,
)
from keel_core.patch.store import PatchProposalStore, PostgresPatchProposalStore
from keel_core.patch.transfer_client import SandboxTransferClient
from keel_core.patch.writeback import PatchWritebackService
from keel_core.projects import PostgresProjectStore, ProjectService
from keel_core.projects.github_factory import build_github_integration
from keel_core.protocols import ProviderGateway
from keel_core.runs import PostgresRunStore, RunStore
from keel_core.scoping import ScopeValidationError, derive_agent_scope, validate_scope_id
from keel_core.tools.rpc import SandboxExecutionEnvironment

from .jobs import JobDefinition, JobRegistry

logger = logging.getLogger("keel.worker.patch")

# Bounded reconcile inputs (the outbox re-validates each against its own hard bounds).
PATCH_RECONCILE_LIMIT = 100
PATCH_RECONCILE_LEASE_SECONDS = 120
PATCH_RECONCILE_RESCHEDULE_SECONDS = 60

_TERMINAL_JOB_STATUSES = frozenset({JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled})
# The only proposal states from which a TTL lapse legally transitions to ``expired`` (a
# ``generating``/``writing`` proposal past TTL is repaired via its job, never force-expired).
_EXPIRABLE_STATUSES = frozenset(
    {PatchStatus.ready, PatchStatus.approval_pending, PatchStatus.approved}
)
# Typed failure set the per-pointer reconcile loop tolerates: a single pointer's expected failure
# (a fenced-out transition, a revoked authorization, a DB error, a malformed scope) must defer that
# one pointer without crashing the cron tick or leaking a raw traceback (which could carry source).
_RECONCILE_TOLERATED = (PatchError, PermissionDenied, SQLAlchemyError, ScopeValidationError)


# --- shared storage readiness --------------------------------------------------------------------


class PatchStorageNotReady(RuntimeError):
    """A patch-enabled worker cannot reach the shared storage patch worktrees live on.

    Raised at startup so the worker crash-loops (fail-fast) instead of silently running WITHOUT
    patch handlers/reconciler — a worker that claims/dispatches ``patch.generate`` /
    ``patch.writeback`` but cannot read/write the shared project-storage volume strands proposals.
    """


def resolve_patch_storage_root(
    settings: Settings, *, probe: Callable[[Path], None] = verify_shared_storage
) -> Path | None:
    """Resolve+verify the shared storage root for patches, or ``None`` when patches are disabled.

    * ``patch_enabled`` false: returns ``None`` — the worker builds neither the patch coordinator
      factory nor the reconciler, and a patch job fails closed as capability-unavailable (left
      queued for a capable worker).
    * ``patch_enabled`` true: resolves the shared root (shared with review) and probes it. A probe
      failure raises :class:`PatchStorageNotReady` (fail-fast) rather than degrading silently.

    ``probe`` is injectable for tests; by default it is
    :func:`keel_core.coding.storage_root.verify_shared_storage`.
    """
    if not settings.patch_enabled:
        logger.info("patch disabled (KEEL_PATCH_ENABLED=false); patch jobs not registered")
        return None
    root = resolve_project_storage_root(settings.project_storage_root, app_env=settings.app_env)
    try:
        probe(root)
    except SharedStorageUnavailable as exc:
        raise PatchStorageNotReady(
            "shared project storage is unavailable but patches are enabled; refusing to start a "
            "worker that would consume patch.generate/patch.writeback jobs without readable/"
            "writable shared storage (set KEEL_PATCH_ENABLED=false to disable patches on this "
            "worker)"
        ) from exc
    return root


def _github_hosts(settings: Settings) -> tuple[str, ...]:
    return tuple(h.strip().lower() for h in settings.github_allowed_hosts.split(",") if h.strip())


# --- shared components + scope-pinned coordinator factory ----------------------------------------


@dataclass(frozen=True)
class PatchWorkerComponents:
    """Shared, long-lived patch dependencies built once at worker startup.

    Every field is either global (the proposal store + dispatch outbox are org-RLS/scope-fenced
    internally) or scope-agnostic (the authorizer/generation/writeback resolve the exact scope per
    call). Per-scope run/approval/event/job stores are constructed on demand from ``engine`` inside
    the scope-pinned coordinator factory and the reconciler — never shared across scopes.
    """

    engine: AsyncEngine
    settings: Settings
    store: PatchProposalStore
    outbox: PatchProposalOutbox
    authorizer: ProjectServicePatchAuthorizer
    generation: PatchGenerationService
    writeback: PatchWritebackService | None
    artifacts: LocalArtifactStore


def build_patch_worker_components(
    settings: Settings,
    *,
    engine: AsyncEngine,
    provider: ProviderGateway,
    transfer_client: SandboxTransferClient,
    identity_store: IdentityStore,
    storage_root: Path,
) -> PatchWorkerComponents:
    """Build the shared patch dependencies for a worker over ``storage_root`` (the shared volume).

    The ``transfer_client`` is a single shared, long-lived HTTP client owned by the caller (closed
    at worker shutdown). Each generation run still gets its OWN per-namespace
    :class:`SandboxExecutionEnvironment` (closed by the author) so one run's file tools can never
    touch another's workspace. GitHub is optional: when the App is unconfigured writeback is left
    unwired (``None``) and an ``approved`` proposal's writeback job fails closed explicitly rather
    than pushing to a non-existent remote.
    """
    coding_storage = LocalCodingStorage(storage_root, allowed_https_hosts=_github_hosts(settings))
    artifacts = LocalArtifactStore(coding_storage)
    github = build_github_integration(settings)
    # The authorizer needs only the project store (repo/installation binding + active git handle)
    # and the GitHub integration (clone-URL re-validation); the coding driver is used *directly* by
    # generation/writeback below, never through the ``ProjectService.storage`` seam.
    project_service = ProjectService(
        PostgresProjectStore(engine),
        identity_store,
        github=github,
    )
    authorizer = ProjectServicePatchAuthorizer(project_service)

    def _environment(namespace: str) -> SandboxExecutionEnvironment:
        # A fresh, per-namespace sandbox client the author closes after the run (never shared) so a
        # per-run workspace is confined to its own ``ws_<hash>`` namespace.
        return SandboxExecutionEnvironment(
            settings.sandbox_url,
            shared_secret=settings.resolved_sandbox_rpc_secret(),
            allow_unauthenticated_local_test=settings.sandbox_rpc_local_test_mode,
            workspace=namespace,
        )

    author = SandboxedLoopPatchAuthor(
        provider=provider,
        transfer=transfer_client,
        environment_factory=_environment,
    )
    generation = PatchGenerationService(storage=coding_storage, artifacts=artifacts, author=author)
    writeback = (
        PatchWritebackService(
            github=github,
            storage=coding_storage,
            ledger=PostgresPatchWritebackLedger(engine),
        )
        if github is not None
        else None
    )
    return PatchWorkerComponents(
        engine=engine,
        settings=settings,
        store=PostgresPatchProposalStore(engine),
        outbox=PostgresPatchProposalOutbox(engine),
        authorizer=authorizer,
        generation=generation,
        writeback=writeback,
        artifacts=artifacts,
    )


def build_patch_coordinator(components: PatchWorkerComponents, scope_id: str) -> PatchCoordinator:
    """Build a :class:`PatchCoordinator` whose run/approval stores are pinned to ``scope_id``.

    The pinned factories fail closed (``PatchValidationError``) if asked for any other scope. Since
    ``execute_generation`` resolves the proposal's canonical per-Agent scope and calls
    ``run_store_factory(canonical)``, a pinned factory rejecting a mismatch IS the mechanism that
    enforces *claimed scope == proposal canonical scope*: a job claimed under the wrong scope can
    never construct a run/approval store and fails closed instead of touching another scope's data.
    """
    pinned = validate_scope_id(scope_id)
    engine = components.engine

    def _run_store(requested: str) -> RunStore:
        if requested != pinned:
            raise PatchValidationError(
                "patch run store requested for a scope other than the coordinator's pinned scope"
            )
        return PostgresRunStore(engine, pinned)

    def _approval_store(requested: str) -> ApprovalStore:
        if requested != pinned:
            raise PatchValidationError(
                "patch approval store requested for a scope other than the coordinator's pinned "
                "scope"
            )
        return PostgresApprovalStore(engine, pinned)

    return PatchCoordinator(
        store=components.store,
        outbox=components.outbox,
        run_store_factory=_run_store,
        authorizer=components.authorizer,
        approval_factory=_approval_store,
        generation=components.generation,
        writeback=components.writeback,
        artifacts=components.artifacts,
    )


# --- per-scope job registration ------------------------------------------------------------------


def patch_generate_job_definition(
    coordinator: PatchCoordinator, settings: Settings
) -> JobDefinition:
    # Cooperative cancel: generation runs a long model loop with an in-process interrupt seam (the
    # heartbeat sets a cancellation event the author polls each iteration), so a cancel must be
    # delivered cooperatively rather than by killing the task mid-write.
    handlers = PatchJobHandlers(coordinator)
    return JobDefinition(
        kind=PATCH_GENERATE_KIND,
        handler=handlers.generate,
        max_attempts=PATCH_GENERATE_MAX_ATTEMPTS,
        lease_seconds=settings.job_lease_seconds,
        cancel_mode=CancelMode.cooperative,
    )


def patch_writeback_job_definition(
    coordinator: PatchCoordinator, settings: Settings
) -> JobDefinition:
    handlers = PatchJobHandlers(coordinator)
    return JobDefinition(
        kind=PATCH_WRITEBACK_KIND,
        handler=handlers.writeback,
        max_attempts=PATCH_WRITEBACK_MAX_ATTEMPTS,
        lease_seconds=settings.job_lease_seconds,
        cancel_mode=CancelMode.immediate,
    )


def register_patch_jobs(
    registry: JobRegistry, coordinator: PatchCoordinator, settings: Settings
) -> None:
    """Register the per-scope patch handlers for the claimed scope.

    ``patch.generate`` is always registered. ``patch.writeback`` is registered ONLY when this worker
    has a wired writeback service (``coordinator.writeback is not None`` — i.e. the GitHub App is
    configured). A worker WITHOUT GitHub deliberately leaves ``patch.writeback`` unregistered so it
    stays *capability-unavailable* here: ``run_job`` skips a known-but-unregistered product kind
    (it is in ``_ALL_JOB_KINDS``) instead of claiming + terminally failing it, so the writeback job
    remains queued for a GitHub-capable peer in the fleet rather than being permanently failed on a
    worker that could never push a remote branch/PR. The reconciler still enqueues the writeback job
    + its cross-scope dispatch intent regardless of THIS worker's GitHub capability.
    """
    registry.register(patch_generate_job_definition(coordinator, settings))
    if coordinator.writeback is not None:
        registry.register(patch_writeback_job_definition(coordinator, settings))


# --- fenced patch reconciler ---------------------------------------------------------------------


def _lease_token(entry: PatchOutboxEntry) -> str:
    """The fencing token a claimed pointer must carry; a missing one fails the pointer closed."""
    if entry.lease_token is None:
        raise PatchValidationError("claimed patch outbox pointer is missing its lease token")
    return entry.lease_token


def _repair_status(job_status: JobStatus | None) -> PatchStatus:
    """The terminal proposal status to repair to when a servicing job died without finalizing."""
    return PatchStatus.cancelled if job_status is JobStatus.cancelled else PatchStatus.failed


@dataclass
class PatchOutboxReconciler:
    """Fenced, bounded reconciler over the global patch dispatch pointer index.

    All dependencies are injected (callables/stores) so a unit test can drive it with in-memory
    doubles and a live integration test with Postgres. One tick leases a bounded batch of due
    pointers under a random lease token and reconciles each independently; one pointer's expected
    failure defers only that pointer.
    """

    store: PatchProposalStore
    outbox: PatchProposalOutbox
    coordinator_factory: Callable[[str], PatchCoordinator]
    job_store_factory: Callable[[str], JobStore]
    job_dispatch_outbox: JobDispatchOutbox
    worker_id: str
    batch_limit: int = PATCH_RECONCILE_LIMIT
    claim_lease_seconds: int = PATCH_RECONCILE_LEASE_SECONDS
    reschedule_delay_seconds: int = PATCH_RECONCILE_RESCHEDULE_SECONDS
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    async def run(self, *, now: datetime | None = None) -> int:
        """Reconcile one bounded batch of due pointers; returns the number definitively handled."""
        moment = now or self.clock()
        claimed = await self.outbox.claim_due(
            worker_id=self.worker_id,
            now=moment,
            limit=self.batch_limit,
            lease_seconds=self.claim_lease_seconds,
        )
        handled = 0
        for entry in claimed:
            try:
                if await self._reconcile_entry(entry, now=moment):
                    handled += 1
            except _RECONCILE_TOLERATED as exc:
                # Log the pointer's coarse status + the error *type* only (never the message, which
                # could echo a path/branch/source). The lease is left to expire (or best-effort
                # released) so a later tick retries; never crash the loop, never adopt.
                logger.warning(
                    "patch reconcile deferred one pointer status_hint=%s error=%s",
                    entry.status_hint.value,
                    type(exc).__name__,
                )
                await self._release_on_error(entry, now=moment)
        if handled:
            logger.info("patch reconciler handled %d pointer(s)", handled)
        return handled

    async def _release_on_error(self, entry: PatchOutboxEntry, *, now: datetime) -> None:
        token = entry.lease_token
        if token is None:
            return
        try:
            await self.outbox.reschedule(
                entry.proposal_id,
                lease_token=token,
                delay_seconds=self.reschedule_delay_seconds,
                now=now,
            )
        except SQLAlchemyError:
            # Best-effort lease release after an already-logged failure; the claim lease expiry is
            # the durable backstop. A raise here would mask the original deferral.
            logger.debug("patch reconcile lease release failed after deferral")

    async def _reconcile_entry(self, entry: PatchOutboxEntry, *, now: datetime) -> bool:
        token = _lease_token(entry)
        scope_id = validate_scope_id(entry.scope_id)
        proposal = await self.store.get(entry.org_id, entry.proposal_id)
        if proposal is None:
            # Orphaned pointer (proposal erased/never existed): retire it under our fence.
            return await self.outbox.remove(entry.proposal_id, lease_token=token, now=now)
        canonical = derive_agent_scope(entry.org_id, proposal.agent_id)
        if canonical != scope_id:
            # The global pointer's scope disagrees with the proposal's canonical per-Agent scope. A
            # canonical scope is a pure function of the immutable (org_id, agent_id), so a pointer
            # written by any legitimate path can NEVER disagree — this is immutable pointer
            # corruption that can never self-heal. NEVER adopt the foreign scope (it would drive
            # RLS/job dispatch into the wrong tenant) and NEVER touch a foreign job/run. Fail the
            # proposal CLOSED under ITS OWN canonical scope with an atomic pointer delete, rather
            # than rescheduling forever behind a TTL backstop this early branch never reaches.
            logger.error(
                "patch pointer scope mismatch proposal=%s (failing closed, never adopted)",
                entry.proposal_id,
            )
            if not can_transition(proposal.status, PatchStatus.failed):
                # Already terminal (pointer stale), or a state with no legal ``failed`` edge
                # (``approval_pending`` awaiting a human under its real scope): retire the corrupt
                # pointer under our fence without forcing an illegal edge or disturbing it.
                return await self.outbox.remove(entry.proposal_id, lease_token=token, now=now)
            # A concurrent advance that changed the version raises StaleProposalVersion (a tolerated
            # PatchError) → this tick stops and a later tick re-evaluates the (fresh) proposal.
            await self.store.transition(
                entry.org_id,
                proposal.id,
                PatchStatus.failed,
                expected_version=proposal.version,
                updates={
                    "error_kind": "PatchScopeMismatch",
                    "error_message": "pointer scope did not match the proposal canonical scope",
                },
                outbox=self.outbox,
                scope_id=canonical,
                now=now,
            )
            return True
        if proposal.status in TERMINAL_STATUSES:
            # Terminal proposal (already done/expired): the pointer is stale — retire it.
            return await self.outbox.remove(entry.proposal_id, lease_token=token, now=now)
        if proposal.expires_at <= now and proposal.status in _EXPIRABLE_STATUSES:
            # TTL lapsed on a still-open proposal: expire it and delete the pointer atomically
            # (version-fenced so a concurrent advance wins instead).
            await self.store.transition(
                entry.org_id,
                proposal.id,
                PatchStatus.expired,
                expected_version=proposal.version,
                outbox=self.outbox,
                scope_id=scope_id,
                now=now,
            )
            return True
        if proposal.status is PatchStatus.generating:
            return await self._reconcile_generating(entry, proposal, scope_id, token, now=now)
        if proposal.status is PatchStatus.ready:
            return await self._heal_ready(entry, proposal, scope_id, token, now=now)
        if proposal.status is PatchStatus.approval_pending:
            # Awaiting a human carries NO pointer; this one is a stale delete that got lost. Retire.
            return await self.outbox.remove(entry.proposal_id, lease_token=token, now=now)
        if proposal.status in (PatchStatus.approved, PatchStatus.writing):
            return await self._reconcile_writeback(entry, proposal, scope_id, token, now=now)
        # No other non-terminal status carries a live pointer: defer + diagnostic (fail closed).
        logger.error(
            "patch pointer unexpected status=%s proposal=%s (deferred)",
            proposal.status.value,
            proposal.id,
        )
        await self.outbox.reschedule(
            entry.proposal_id,
            lease_token=token,
            delay_seconds=self.reschedule_delay_seconds,
            now=now,
        )
        return False

    async def _reconcile_generating(
        self,
        entry: PatchOutboxEntry,
        proposal: PatchProposal,
        scope_id: str,
        token: str,
        *,
        now: datetime,
    ) -> bool:
        job_store = self.job_store_factory(scope_id)
        if entry.job_id:
            job = await job_store.get(entry.job_id)
            if job is not None and job.status not in _TERMINAL_JOB_STATUSES:
                # Generation still in flight: leave the job to the dispatcher, defer the pointer.
                await self.outbox.reschedule(
                    entry.proposal_id,
                    lease_token=token,
                    delay_seconds=self.reschedule_delay_seconds,
                    now=now,
                )
                return False
            # The servicing job is terminal/absent but the proposal never left ``generating`` — a
            # crash between the job ending and the atomic generation finalize. Repair to a terminal
            # state (atomic pointer delete) so the run/proposal can never strand as reclaimable.
            await self.store.transition(
                entry.org_id,
                proposal.id,
                _repair_status(job.status if job is not None else None),
                expected_version=proposal.version,
                updates={
                    "error_kind": "patch_generation_stranded",
                    "error_message": "generation job terminated without finalizing the proposal",
                },
                outbox=self.outbox,
                scope_id=scope_id,
                now=now,
            )
            return True
        # No servicing job yet: (re-)create it from the durably-persisted request payload. Since
        # P4a the request is written in the SAME transaction as the proposal, so a committed
        # ``generating`` proposal ALWAYS has a reconstructable request. A missing/unreconstructable
        # one is terminal corruption (a legacy pre-0022 row, or a tampered payload): fail the
        # proposal closed with an atomic pointer delete -- NEVER fabricate one, and (unlike the
        # old event-log seam) never defer within TTL, because the request can never appear later.
        record = await self.store.get_generation_request(entry.org_id, proposal.id, scope_id)
        if record is None:
            await self.store.transition(
                entry.org_id,
                proposal.id,
                PatchStatus.failed,
                expected_version=proposal.version,
                updates={
                    "error_kind": "patch_generation_request_missing",
                    "error_message": "no durable generation request payload to resume generation",
                },
                outbox=self.outbox,
                scope_id=scope_id,
                now=now,
            )
            return True
        job, _created = await job_store.enqueue_once_with_dispatch_intent(
            kind=PATCH_GENERATE_KIND,
            payload=record.payload.model_dump(mode="json"),
            target_session_id=None,
            idempotency_key=patch_generate_idempotency_key(proposal.id),
            max_attempts=PATCH_GENERATE_MAX_ATTEMPTS,
            outbox=self.job_dispatch_outbox,
            cancel_mode=CancelMode.cooperative,
            now=now,
        )
        if not await self.outbox.set_job_id(proposal.id, job.id, lease_token=token, now=now):
            # Stale token: another worker now owns this pointer. The idempotency-keyed job already
            # exists, so a re-claim re-references the SAME job (no duplicate). Stop, unfenced.
            return False
        await self.outbox.reschedule(
            entry.proposal_id,
            lease_token=token,
            delay_seconds=self.reschedule_delay_seconds,
            now=now,
        )
        return True

    async def _heal_ready(
        self,
        entry: PatchOutboxEntry,
        proposal: PatchProposal,
        scope_id: str,
        token: str,
        *,
        now: datetime,
    ) -> bool:
        # Auto-heal: create-or-get the durable approval and move ready -> approval_pending, which
        # DELETES the pointer inside the store transaction (idempotent + reconciler-safe). The
        # fenced remove afterwards is a no-op on the normal path (row already gone) and cleans up
        # the fast path (already approval_pending, pointer left behind by a lost delete).
        coordinator = self.coordinator_factory(scope_id)
        await coordinator.request_approval(entry.org_id, proposal.id, actor=proposal.actor, now=now)
        await self.outbox.remove(entry.proposal_id, lease_token=token, now=now)
        return True

    async def _reconcile_writeback(
        self,
        entry: PatchOutboxEntry,
        proposal: PatchProposal,
        scope_id: str,
        token: str,
        *,
        now: datetime,
    ) -> bool:
        job_store = self.job_store_factory(scope_id)
        if entry.job_id:
            job = await job_store.get(entry.job_id)
            if job is not None and job.status not in _TERMINAL_JOB_STATUSES:
                # Writeback queued/running (or approved and not yet started): defer the pointer.
                await self.outbox.reschedule(
                    entry.proposal_id,
                    lease_token=token,
                    delay_seconds=self.reschedule_delay_seconds,
                    now=now,
                )
                return False
            # Terminal/absent writeback job but the proposal is still approved/writing — a crash
            # before the atomic writeback finalize (or a job that succeeded without advancing the
            # proposal). Repair to a terminal state + delete the pointer; never strand.
            await self.store.transition(
                entry.org_id,
                proposal.id,
                _repair_status(job.status if job is not None else None),
                expected_version=proposal.version,
                updates={
                    "error_kind": "patch_writeback_stranded",
                    "error_message": "writeback job terminated without finalizing the proposal",
                },
                outbox=self.outbox,
                scope_id=scope_id,
                now=now,
            )
            return True
        # No writeback job yet: create it. The writeback payload is trivially reconstructable from
        # the proposal (id + org), so no durable request-metadata lookup is needed.
        payload = PatchWritebackJobPayload(proposal_id=proposal.id, org_id=entry.org_id)
        job, _created = await job_store.enqueue_once_with_dispatch_intent(
            kind=PATCH_WRITEBACK_KIND,
            payload=payload.model_dump(mode="json"),
            target_session_id=None,
            idempotency_key=patch_writeback_idempotency_key(proposal.id),
            max_attempts=PATCH_WRITEBACK_MAX_ATTEMPTS,
            outbox=self.job_dispatch_outbox,
            cancel_mode=CancelMode.immediate,
            now=now,
        )
        if not await self.outbox.set_job_id(proposal.id, job.id, lease_token=token, now=now):
            return False
        await self.outbox.reschedule(
            entry.proposal_id,
            lease_token=token,
            delay_seconds=self.reschedule_delay_seconds,
            now=now,
        )
        return True


def build_patch_reconciler(
    components: PatchWorkerComponents,
    *,
    job_dispatch_outbox: JobDispatchOutbox,
    worker_id: str,
) -> PatchOutboxReconciler:
    """Assemble a Postgres-backed reconciler from the shared components."""
    engine = components.engine
    limits = JobLimits.from_settings(components.settings)
    return PatchOutboxReconciler(
        store=components.store,
        outbox=components.outbox,
        coordinator_factory=lambda s: build_patch_coordinator(components, s),
        job_store_factory=lambda s: PostgresJobStore(engine, s, limits=limits),
        job_dispatch_outbox=job_dispatch_outbox,
        worker_id=worker_id,
    )


async def reconcile_patch_outbox_tick(ctx: dict[str, Any]) -> int:
    """Cron entrypoint: run one bounded reconcile pass over the global patch pointer index.

    Returns ``0`` (no-op) on a worker where patches are disabled/unwired (no reconciler on ``ctx``).
    """
    reconciler: PatchOutboxReconciler | None = ctx.get("patch_reconciler")
    if reconciler is None:
        return 0
    return await reconciler.run()


__all__ = [
    "PATCH_RECONCILE_LIMIT",
    "PatchOutboxReconciler",
    "PatchStorageNotReady",
    "PatchWorkerComponents",
    "build_patch_coordinator",
    "build_patch_reconciler",
    "build_patch_worker_components",
    "patch_generate_job_definition",
    "patch_writeback_job_definition",
    "reconcile_patch_outbox_tick",
    "register_patch_jobs",
    "resolve_patch_storage_root",
]
