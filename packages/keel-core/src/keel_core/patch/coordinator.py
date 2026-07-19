"""PatchCoordinator — durable request/generate/approve/writeback lifecycle (WS-PP).

Ties the pure patch services (:mod:`.generation`, :mod:`.approval`, :mod:`.writeback`) and the
durable :class:`~keel_core.patch.store.PatchProposalStore` state machine to the existing durable
substrate:

* **request generation** — authorize the actor/Agent (project *write* + run == ``use``), create a
  durable run (``surface="patch"``), associate it to the project, create the ``generating``
  proposal row **and its global dispatch pointer atomically**, audit, and return a handle.
  Enqueueing the generation worker job is the caller's responsibility (it owns the job store).
* **execute generation** — the worker path: claim the run lease, re-authorize (revocation fails
  closed), run controlled generation into an isolated disposable worktree, persist the immutable
  bundle, transition the proposal ``generating -> ready`` (updating the pointer hint) BEFORE
  terminalizing the run, then *immediately* run the atomic ``ready -> approval_pending`` primitive
  so the normal terminal state is ``approval_pending``. ``ready`` is only ever a transient crash
  checkpoint a retry heals; a permanent failure terminalizes ``failed`` and retires the pointer,
  while a transient provider outage releases the lease (proposal stays ``generating``) charging the
  partial usage coherently onto the run.
* **request approval** — the reconciler-safe seam over the same atomic primitive: create-or-get a
  durable, org/actor-bound approval bound to the EXACT proposal and move it ``ready ->
  approval_pending`` in one transaction (idempotent). Approval never pushes.
* **decide** — resolve the fenced approval under the proposal's canonical scope; a *granted*
  decision moves ``approval_pending -> approved`` (re-creating the ``approved`` pointer) and signals
  the caller to enqueue the trusted writeback job; a *denied*/*expired*/*stale* decision
  terminalizes the proposal (retiring the pointer) with no remote effect. A stale/changed proposal
  can't reuse a decision.
* **execute writeback** — the trusted control-plane path: move ``approved -> writing`` (reserving
  the dedicated branch, keeping the ``approved`` pointer for retry), re-verify the base, push the
  exact approved commit, open an idempotent Draft PR, and move ``writing -> draft_pr_created``
  (retiring the pointer) — or ``stale`` on base drift / ``failed`` on a permanent writeback refusal;
  a transient remote outage keeps the proposal ``writing`` for a durable retry.

The GitHub App JIT token lives entirely in the writeback service on the control plane; it is never
handed to generation, the worktree, the sandbox, an artifact, or a log.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from keel_core.approvals import ApprovalStore
from keel_core.coding.protocols import ArtifactStore
from keel_core.protocols import Usage
from keel_core.runs import RunBudgetSpec, RunCost, RunStatus, RunStore

from .approval import (
    DEFAULT_APPROVAL_TTL_SECONDS,
    PATCH_APPROVAL_TOOL,
    PatchApprovalService,
    approval_binding_hash,
    patch_approval_idempotency_key,
)
from .bundle import PatchBundleReader
from .errors import (
    PatchApprovalError,
    PatchError,
    PatchLeaseLost,
    PatchProviderUnavailable,
    PatchRemoteUnavailable,
    PatchStaleError,
    PatchStateError,
    PatchValidationError,
    PatchWritebackError,
)
from .generation import PatchGenerationService
from .models import (
    PatchProposal,
    PatchProposalRequest,
    PatchStatus,
    new_proposal_id,
    run_branch_for,
    task_digest,
)
from .outbox import PatchProposalOutbox
from .store import ApprovalDraft, PatchProposalStore
from .writeback import PatchWritebackService, WritebackTarget

logger = logging.getLogger("keel.patch.coordinator")

PATCH_SURFACE = "patch"
DEFAULT_PATCH_AGENT_ID = "patch"
DEFAULT_PATCH_TTL_SECONDS = 14 * 24 * 3600
DEFAULT_PATCH_LEASE_SECONDS = 1800


@dataclass(frozen=True, slots=True)
class ProjectBinding:
    """The authorized project↔storage↔remote binding a proposal operates over."""

    project_handle: str
    scope_id: str
    coding_run_id: str
    target: WritebackTarget | None


class PatchAuthorizer(Protocol):
    """Authorizes patch operations and resolves the project storage/remote binding."""

    async def authorize_generation(
        self, org_id: str, actor: str, project_id: str, *, agent_id: str | None, run_id: str
    ) -> ProjectBinding: ...

    async def authorize_read(self, org_id: str, actor: str, project_id: str) -> None: ...

    async def authorize_writeback(
        self, org_id: str, actor: str, project_id: str, *, agent_id: str | None, run_id: str
    ) -> ProjectBinding: ...

    async def associate_run(
        self, org_id: str, actor: str, project_id: str, run_id: str, *, agent_id: str | None
    ) -> None: ...


class PatchAuditSink(Protocol):
    def record(
        self, *, action: str, org_id: str, actor: str, proposal_id: str, detail: dict[str, str]
    ) -> None: ...


class _LoggingAudit:
    def record(
        self, *, action: str, org_id: str, actor: str, proposal_id: str, detail: dict[str, str]
    ) -> None:
        logger.info("patch.%s org=%s proposal=%s %s", action, org_id, proposal_id, detail)


@dataclass(frozen=True, slots=True)
class PatchHandle:
    proposal_id: str
    run_id: str
    status: PatchStatus
    created: bool


@dataclass(frozen=True, slots=True)
class DecisionResult:
    applied: bool
    status: PatchStatus
    queue_writeback: bool


def _run_cost_from_usage(usage: object | None) -> RunCost:
    """Convert a provider ``Usage`` accounting record into a fenced ``RunCost`` delta.

    The run row is the authoritative cumulative charge ledger: a partial charge (provider
    unavailable) and the final success charge are both applied to it as deltas, so the coordinator
    can derive a coherent proposal cost that never loses a partial attempt nor double-counts one.
    Fails closed on a malformed usage payload rather than silently charging zero."""
    if usage is None:
        return RunCost()
    if isinstance(usage, Usage):
        return RunCost(
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            cost_usd=usage.cost_usd,
        )
    raise PatchValidationError("provider usage is not a Usage accounting record")


@dataclass
class PatchCoordinator:
    store: PatchProposalStore
    outbox: PatchProposalOutbox
    runs: RunStore
    authorizer: PatchAuthorizer
    generation: PatchGenerationService
    # A per-scope durable approval store: the shared in-memory double, or a Postgres store bound to
    # the proposal's canonical scope. There is no single bound approval — every approval operation
    # resolves the exact scope first, so a cross-scope decision can never leak.
    approval_factory: Callable[[str], ApprovalStore]
    writeback: PatchWritebackService
    artifacts: ArtifactStore
    audit: PatchAuditSink = _LoggingAudit()
    ttl_seconds: int = DEFAULT_PATCH_TTL_SECONDS
    lease_seconds: int = DEFAULT_PATCH_LEASE_SECONDS
    approval_ttl_seconds: int = DEFAULT_APPROVAL_TTL_SECONDS

    # --- request ----------------------------------------------------------------------
    async def request_generation(
        self, request: PatchProposalRequest, *, now: datetime | None = None
    ) -> PatchHandle:
        moment = now or datetime.now(UTC)
        proposal_id = new_proposal_id()
        run_id = proposal_id  # 1:1 durable run per proposal (idempotent by request key)
        binding = await self.authorizer.authorize_generation(
            request.org_id,
            request.actor,
            request.project_id,
            agent_id=request.agent_id,
            run_id=run_id,
        )
        await self.runs.create(
            run_id=run_id,
            scope_id=binding.scope_id,
            org_id=request.org_id,
            actor=request.actor,
            agent_id=request.agent_id or DEFAULT_PATCH_AGENT_ID,
            session_id=run_id,
            surface=PATCH_SURFACE,
            idempotency_key=request.idempotency_key,
            budget=RunBudgetSpec(
                max_iterations=request.max_iterations, token_budget=request.token_budget
            ),
            expires_at=moment + timedelta(seconds=self.ttl_seconds),
            fingerprint=request.idempotency_key,
            now=moment,
        )
        await self.authorizer.associate_run(
            request.org_id, request.actor, request.project_id, run_id, agent_id=request.agent_id
        )
        proposal, created = await self.store.create(
            proposal_id=proposal_id,
            org_id=request.org_id,
            project_id=request.project_id,
            run_id=run_id,
            run_attempt=1,
            agent_id=request.agent_id or DEFAULT_PATCH_AGENT_ID,
            actor=request.actor,
            base_ref=request.base_ref,
            source_ref=request.source_ref,
            task_digest=task_digest(request.task),
            idempotency_key=request.idempotency_key,
            fingerprint=request.idempotency_key,
            expires_at=moment + timedelta(seconds=self.ttl_seconds),
            now=moment,
            outbox=self.outbox,
            scope_id=binding.scope_id,
        )
        if created:
            self.audit.record(
                action="requested",
                org_id=request.org_id,
                actor=request.actor,
                proposal_id=proposal.id,
                detail={"project_id": request.project_id},
            )
        return PatchHandle(
            proposal_id=proposal.id, run_id=proposal.run_id, status=proposal.status, created=created
        )

    # --- generation -------------------------------------------------------------------
    async def execute_generation(
        self,
        org_id: str,
        run_id: str,
        request: PatchProposalRequest,
        *,
        worker_id: str,
        now: datetime | None = None,
    ) -> PatchProposal:
        moment = now or datetime.now(UTC)
        proposal = await self.store.get_by_run(org_id, run_id)
        if proposal is None:
            raise PatchValidationError("no proposal bound to this run")
        # ``approval_pending`` is generation's terminal-normal outcome (the dispatch pointer is
        # already retired and the durable approval is bound); there is nothing left to do.
        if proposal.status is PatchStatus.approval_pending:
            return proposal
        # ``ready`` is a *transient* checkpoint: a crash landed between persisting the immutable
        # bundle and the atomic approval transition. Heal it by driving the single-transaction
        # approval primitive — never simply return ``ready``.
        if proposal.status is PatchStatus.ready:
            return await self._advance_ready_to_approval_pending(org_id, proposal, now=moment)
        if proposal.status is not PatchStatus.generating:
            raise PatchStateError(f"cannot generate a proposal in status {proposal.status.value}")
        binding = await self.authorizer.authorize_generation(
            org_id,
            proposal.actor,
            proposal.project_id,
            agent_id=proposal.agent_id,
            run_id=run_id,
        )
        await self.runs.mark_queued(run_id, now=moment)
        lease = await self.runs.claim(
            run_id, worker_id=worker_id, now=moment, lease_seconds=self.lease_seconds
        )
        if lease is None:
            raise PatchStateError("patch generation run is leased by another worker")
        try:
            outcome = await self.generation.generate(
                request,
                proposal_id=proposal.id,
                run_id=run_id,
                coding_run_id=binding.coding_run_id,
                project_handle=binding.project_handle,
                now=moment,
            )
        except PatchLeaseLost:
            # The lease was reclaimed/expired mid-generation. Leave the proposal AND the run
            # untouched so the current lease owner (or a reclaim) can finish the attempt; a stale
            # lease must never terminalize the run or the proposal.
            raise
        except PatchProviderUnavailable as exc:
            # Transient upstream failure. The run row is the authoritative charge ledger: release
            # the lease back to the queue (the proposal stays ``generating`` for a retry) and apply
            # the partial usage as a cumulative delta, then mirror that cumulative onto the proposal
            # so no partial cost is lost. Rethrow for the P3 retry mapping.
            released = await self.runs.release(
                lease,
                to_status=RunStatus.queued,
                cost=_run_cost_from_usage(exc.usage),
                now=moment,
            )
            await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.generating,
                expected_version=proposal.version,
                updates={"cost_usd": released.cost_usd},
                now=moment,
            )
            raise
        except PatchError as exc:
            # A permanent generation failure: fail the proposal (deleting its dispatch pointer) and
            # the run, then rethrow. A permanent provider failure (cost-ceiling stop, malformed
            # completion, a permanent transfer rejection) may still have consumed tokens: charge
            # that partial usage onto the run as a fenced delta (cumulative = prior attempts + this
            # outcome) and mirror the cumulative onto the proposal, so a terminal failure neither
            # loses nor double-counts cost. A failure that carries no usage charges nothing.
            failure_cost = _run_cost_from_usage(getattr(exc, "usage", None))
            run_record = await self.runs.get(run_id)
            prior_cost = run_record.cost_usd if run_record is not None else 0.0
            await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.failed,
                expected_version=proposal.version,
                updates={
                    "error_kind": type(exc).__name__,
                    "error_message": str(exc)[:500],
                    "cost_usd": prior_cost + failure_cost.cost_usd,
                },
                now=moment,
                outbox=self.outbox,
                scope_id=binding.scope_id,
            )
            await self.runs.terminalize(
                lease,
                status=RunStatus.failed,
                stop_reason="generation_failed",
                error_kind=type(exc).__name__,
                error_message=str(exc)[:500],
                cost=failure_cost,
                now=moment,
            )
            raise
        # Success. Persist the proposal ``ready`` (updating its pointer hint) BEFORE terminalizing
        # the run, so a crash can never leave a *completed* run behind an unrecoverable
        # ``generating`` proposal (a resumed attempt would fail to re-claim a terminal run). The
        # proposal cost is derived from the run's cumulative charge (prior partial attempts + this
        # outcome) so a provider-unavailable retry neither loses nor double-counts cost.
        run_record = await self.runs.get(run_id)
        prior_cost = run_record.cost_usd if run_record is not None else 0.0
        outcome_cost = _run_cost_from_usage(outcome.usage)
        run_branch = run_branch_for(proposal.id)
        updated = await self.store.transition(
            org_id,
            proposal.id,
            PatchStatus.ready,
            expected_version=proposal.version,
            updates={
                "base_sha": outcome.base_sha,
                "head_sha": outcome.head_sha,
                "bundle_sha256": outcome.bundle_sha256,
                "diff_sha256": outcome.diff_sha256,
                "changed_path_digest": outcome.changed_path_digest,
                "changed_files": outcome.changed_files,
                "test_status": outcome.test_status,
                "remote_branch": run_branch,
                "cost_usd": prior_cost + outcome_cost.cost_usd,
            },
            now=moment,
            outbox=self.outbox,
            scope_id=binding.scope_id,
        )
        await self.runs.terminalize(
            lease,
            status=RunStatus.completed,
            stop_reason="generated",
            result_ref=outcome.bundle_sha256,
            cost=outcome_cost,
            now=moment,
        )
        self.audit.record(
            action="ready",
            org_id=org_id,
            actor=proposal.actor,
            proposal_id=proposal.id,
            detail={
                "bundle_sha256": outcome.bundle_sha256,
                "changed_files": str(outcome.changed_files),
            },
        )
        # ``ready`` is transient: immediately run the atomic approval transition so the normal
        # terminal state the caller observes is ``approval_pending`` (pointer retired, approval
        # bound). A reconciler can re-drive this idempotently after a crash.
        return await self._advance_ready_to_approval_pending(org_id, updated, now=moment)

    # --- approval ---------------------------------------------------------------------
    def _approval_service(self, scope_id: str) -> PatchApprovalService:
        """Bind an approval service to the proposal's canonical scope (never a global one)."""
        return PatchApprovalService(self.approval_factory(scope_id))

    def _approval_draft(self, proposal: PatchProposal, *, now: datetime) -> ApprovalDraft:
        """The full immutable approval binding a ``ready -> approval_pending`` transition raises.

        Mirrors :meth:`PatchApprovalService.request` exactly: the ``action_hash`` fences the
        decision to this exact bundle/base/changed-path/target/attempt, so a mutated or regenerated
        proposal produces a different binding and can never reuse an old approval."""
        if not proposal.bundle_sha256 or not proposal.base_sha:
            raise PatchApprovalError("proposal is not ready for approval (no bundle/base)")
        return ApprovalDraft(
            run_id=proposal.run_id,
            session_id=proposal.run_id,
            tool=PATCH_APPROVAL_TOOL,
            args={
                "proposal_id": proposal.id,
                "bundle_sha256": proposal.bundle_sha256,
                "base_sha": proposal.base_sha,
                "changed_path_digest": proposal.changed_path_digest,
                "project_id": proposal.project_id,
            },
            call_id=proposal.id,
            idempotency_key=patch_approval_idempotency_key(proposal.id, proposal.bundle_sha256),
            reason="Approve controlled patch proposal for GitHub Draft PR writeback",
            expires_at=now + timedelta(seconds=self.approval_ttl_seconds),
            actor=proposal.actor,
            action_hash=approval_binding_hash(proposal),
            run_attempt=proposal.run_attempt,
            batch_id=proposal.id,
        )

    async def _advance_ready_to_approval_pending(
        self, org_id: str, proposal: PatchProposal, *, now: datetime
    ) -> PatchProposal:
        """Atomically move a ``ready`` proposal to ``approval_pending`` behind a durable approval.

        Drives the P1 single-transaction primitive: create-or-get the durable approval, bump the
        proposal (+1 version, bound to the approval id) and delete the dispatch pointer — all or
        nothing. ``expected_version`` is intentionally omitted so a concurrent second caller (or a
        reconciler re-drive) observes ``approval_pending`` and takes the idempotent fast path: the
        same approval id, no second version bump, no attempt to re-delete the retired pointer."""
        scope_id = await self._scope_for(org_id, proposal)
        draft = self._approval_draft(proposal, now=now)
        updated, approval_id, created = await self.store.transition_to_approval_pending(
            org_id,
            proposal.id,
            approvals=self.approval_factory(scope_id),
            outbox=self.outbox,
            scope_id=scope_id,
            draft=draft,
            now=now,
        )
        if created:
            self.audit.record(
                action="approval_requested",
                org_id=org_id,
                actor=proposal.actor,
                proposal_id=proposal.id,
                detail={"approval_id": approval_id},
            )
        return updated

    async def request_approval(
        self, org_id: str, proposal_id: str, *, actor: str, now: datetime | None = None
    ) -> str:
        moment = now or datetime.now(UTC)
        proposal = await self._require(org_id, proposal_id)
        await self.authorizer.authorize_read(org_id, actor, proposal.project_id)
        if proposal.status is PatchStatus.approval_pending and proposal.approval_id:
            return proposal.approval_id
        if proposal.status is not PatchStatus.ready:
            raise PatchStateError("only a ready proposal can be sent for approval")
        updated = await self._advance_ready_to_approval_pending(org_id, proposal, now=moment)
        return updated.approval_id

    async def decide(
        self,
        org_id: str,
        proposal_id: str,
        *,
        approve: bool,
        actor: str,
        now: datetime | None = None,
    ) -> DecisionResult:
        moment = now or datetime.now(UTC)
        proposal = await self._require(org_id, proposal_id)
        await self.authorizer.authorize_read(org_id, actor, proposal.project_id)
        if proposal.status in {
            PatchStatus.approved,
            PatchStatus.denied,
            PatchStatus.expired,
        }:
            return DecisionResult(
                applied=False,
                status=proposal.status,
                queue_writeback=proposal.status is PatchStatus.approved,
            )
        if proposal.status is not PatchStatus.approval_pending:
            raise PatchStateError("only a proposal awaiting approval can be decided")
        if not proposal.approval_id:
            raise PatchApprovalError("proposal has no bound approval")
        # Resolve the exact canonical scope and bind the approval service to it — a decision can
        # only ever resolve an approval in the proposal's own scope, never a cross-scope guess.
        scope_id = await self._scope_for(org_id, proposal)
        approval = self._approval_service(scope_id)
        applied = await approval.decide(
            proposal, proposal.approval_id, approve=approve, resolved_by=actor, now=moment
        )
        resolved_status = (
            "granted"
            if applied and approve
            else "denied"
            if applied
            else await approval.resolved_status(proposal, proposal.approval_id)
        )
        if resolved_status is None:
            # The binding no longer matches (stale/changed proposal) — mark stale, delete the
            # pointer, fail closed.
            await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.stale,
                expected_version=proposal.version,
                updates={"error_message": "approval binding no longer matches the proposal"},
                now=moment,
                outbox=self.outbox,
                scope_id=scope_id,
            )
            return DecisionResult(applied=False, status=PatchStatus.stale, queue_writeback=False)
        target = {
            "granted": PatchStatus.approved,
            "denied": PatchStatus.denied,
            "expired": PatchStatus.expired,
        }[resolved_status]
        # ``approved`` re-creates the dispatch pointer with the ``approved`` hint (the writeback
        # worker drives it); ``denied``/``expired`` are terminal and retire the pointer. Both mirror
        # atomically with the proposal transition.
        updated = await self.store.transition(
            org_id,
            proposal.id,
            target,
            expected_version=proposal.version,
            now=moment,
            outbox=self.outbox,
            scope_id=scope_id,
        )
        self.audit.record(
            action="decided",
            org_id=org_id,
            actor=actor,
            proposal_id=proposal.id,
            detail={
                "decision": target.value,
                "reconciled": str(not applied).lower(),
            },
        )
        return DecisionResult(
            applied=applied,
            status=updated.status,
            queue_writeback=resolved_status == "granted",
        )

    # --- writeback --------------------------------------------------------------------
    async def execute_writeback(
        self, org_id: str, proposal_id: str, *, worker_id: str, now: datetime | None = None
    ) -> PatchProposal:
        moment = now or datetime.now(UTC)
        proposal = await self._require(org_id, proposal_id)
        if proposal.status is PatchStatus.draft_pr_created:
            return proposal  # idempotent
        if proposal.status is PatchStatus.writing:
            pass  # crash-recovery: resume the writeback
        elif proposal.status is not PatchStatus.approved:
            raise PatchStateError("only an approved proposal can be written back")
        binding = await self.authorizer.authorize_writeback(
            org_id,
            proposal.actor,
            proposal.project_id,
            agent_id=proposal.agent_id,
            run_id=proposal.run_id,
        )
        if binding.target is None:
            raise PatchValidationError("project is not bound to a GitHub repository for writeback")
        if proposal.status is PatchStatus.approved:
            proposal = await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.writing,
                expected_version=proposal.version,
                updates={"remote_branch": proposal.remote_branch or run_branch_for(proposal.id)},
                now=moment,
            )
        manifest = PatchBundleReader(self.artifacts).read_manifest(
            project_id=binding.project_handle,
            coding_run_id=binding.coding_run_id,
            bundle_sha256=proposal.bundle_sha256,
        )
        try:
            result = await self.writeback.write(
                proposal,
                project_handle=binding.project_handle,
                target=binding.target,
                manifest=manifest,
            )
        except PatchStaleError as exc:
            # The approved base drifted: terminalize ``stale`` and retire the pointer (never
            # silently rebase or reuse the approval).
            updated = await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.stale,
                expected_version=proposal.version,
                updates={"error_kind": "PatchStaleError", "error_message": str(exc)[:500]},
                now=moment,
                outbox=self.outbox,
                scope_id=binding.scope_id,
            )
            self.audit.record(
                action="stale",
                org_id=org_id,
                actor=proposal.actor,
                proposal_id=proposal.id,
                detail={"reason": "base_moved"},
            )
            return updated
        except PatchRemoteUnavailable:
            # A transient remote (GitHub/Git) failure. Keep the proposal in ``writing`` and its
            # ``approved`` dispatch pointer intact so the durable job can retry writeback; never
            # terminalize on a retryable failure.
            raise
        except PatchWritebackError as exc:
            # A permanent, structural writeback refusal: terminalize ``failed``, retire the pointer
            # with an explicit audit, then rethrow (consistent with a permanent generation failure).
            await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.failed,
                expected_version=proposal.version,
                updates={"error_kind": type(exc).__name__, "error_message": str(exc)[:500]},
                now=moment,
                outbox=self.outbox,
                scope_id=binding.scope_id,
            )
            self.audit.record(
                action="writeback_failed",
                org_id=org_id,
                actor=proposal.actor,
                proposal_id=proposal.id,
                detail={"error_kind": type(exc).__name__},
            )
            raise
        updated = await self.store.transition(
            org_id,
            proposal.id,
            PatchStatus.draft_pr_created,
            expected_version=proposal.version,
            updates={
                "remote_branch": result.remote_branch,
                "pr_number": result.pr_number,
                "pr_url": result.pr_url,
                "pr_node_id": result.pr_node_id,
            },
            now=moment,
            outbox=self.outbox,
            scope_id=binding.scope_id,
        )
        self.audit.record(
            action="draft_pr_created",
            org_id=org_id,
            actor=proposal.actor,
            proposal_id=proposal.id,
            detail={"pr_number": str(result.pr_number), "remote_branch": result.remote_branch},
        )
        return updated

    # --- reads ------------------------------------------------------------------------
    async def get(self, org_id: str, proposal_id: str, *, actor: str) -> PatchProposal:
        proposal = await self._require(org_id, proposal_id)
        await self.authorizer.authorize_read(org_id, actor, proposal.project_id)
        return proposal

    async def list_for_project(
        self, org_id: str, project_id: str, *, actor: str, limit: int = 100
    ) -> list[PatchProposal]:
        await self.authorizer.authorize_read(org_id, actor, project_id)
        return await self.store.list_for_project(org_id, project_id, limit=limit)

    async def cancel(
        self, org_id: str, proposal_id: str, *, actor: str, now: datetime | None = None
    ) -> PatchProposal:
        proposal = await self._require(org_id, proposal_id)
        await self.authorizer.authorize_read(org_id, actor, proposal.project_id)
        if proposal.status in {
            PatchStatus.draft_pr_created,
            PatchStatus.denied,
            PatchStatus.failed,
            PatchStatus.expired,
            PatchStatus.cancelled,
            PatchStatus.stale,
        }:
            return proposal
        # A terminal ``cancelled`` retires the dispatch pointer (no background work), in the same
        # transaction as the proposal transition and under the proposal's own canonical scope.
        scope_id = await self._scope_for(org_id, proposal)
        return await self.store.transition(
            org_id,
            proposal.id,
            PatchStatus.cancelled,
            expected_version=proposal.version,
            updates={"error_message": f"cancelled by {actor}"},
            now=now or datetime.now(UTC),
            outbox=self.outbox,
            scope_id=scope_id,
        )

    async def _require(self, org_id: str, proposal_id: str) -> PatchProposal:
        proposal = await self.store.get(org_id, proposal_id)
        if proposal is None:
            from .errors import PatchNotFound

            raise PatchNotFound(f"proposal not found: {proposal_id}")
        return proposal

    async def _scope_for(self, org_id: str, proposal: PatchProposal) -> str:
        run = await self.runs.get(proposal.run_id)
        if run is not None:
            return str(run.scope_id)
        return f"agent:{org_id}/{proposal.agent_id}"


__all__ = [
    "DecisionResult",
    "PATCH_SURFACE",
    "PatchAuditSink",
    "PatchAuthorizer",
    "PatchCoordinator",
    "PatchHandle",
    "ProjectBinding",
]
