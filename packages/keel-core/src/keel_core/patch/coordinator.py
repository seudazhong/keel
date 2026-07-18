"""PatchCoordinator — durable request/generate/approve/writeback lifecycle (WS-PP).

Ties the pure patch services (:mod:`.generation`, :mod:`.approval`, :mod:`.writeback`) and the
durable :class:`~keel_core.patch.store.PatchProposalStore` state machine to the existing durable
substrate:

* **request generation** — authorize the actor/Agent (project *write* + run == ``use``), create a
  durable run (``surface="patch"``), associate it to the project, create the ``generating``
  proposal row, audit, and return a handle. Enqueueing the generation worker job is the caller's
  responsibility (it owns the job store).
* **execute generation** — the worker path: claim the run lease, re-authorize (revocation fails
  closed), run controlled generation into an isolated disposable worktree, persist the immutable
  bundle, transition the proposal ``generating -> ready`` (or ``failed``), and terminalize the run.
* **request approval** — bind a durable, org/actor-bound approval to the EXACT proposal; move it
  ``ready -> approval_pending``. Approval never pushes.
* **decide** — resolve the fenced approval; a *granted* decision moves ``approval_pending ->
  approved`` and signals the caller to enqueue the trusted writeback job; a *denied* decision
  terminalizes the proposal with no remote effect. A stale/changed proposal can't reuse a decision.
* **execute writeback** — the trusted control-plane path: move ``approved -> writing`` (reserving
  the dedicated branch), re-verify the base, push the exact approved commit, open an idempotent
  Draft PR, and move ``writing -> draft_pr_created`` (or ``stale`` on base drift).

The GitHub App JIT token lives entirely in the writeback service on the control plane; it is never
handed to generation, the worktree, the sandbox, an artifact, or a log.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from keel_core.coding.protocols import ArtifactStore
from keel_core.runs import RunBudgetSpec, RunStatus, RunStore

from .approval import PatchApprovalService
from .bundle import PatchBundleReader
from .errors import (
    PatchApprovalError,
    PatchError,
    PatchStaleError,
    PatchStateError,
    PatchValidationError,
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
from .store import PatchProposalStore
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


@dataclass
class PatchCoordinator:
    store: PatchProposalStore
    runs: RunStore
    authorizer: PatchAuthorizer
    generation: PatchGenerationService
    approval: PatchApprovalService
    writeback: PatchWritebackService
    artifacts: ArtifactStore
    audit: PatchAuditSink = _LoggingAudit()
    ttl_seconds: int = DEFAULT_PATCH_TTL_SECONDS
    lease_seconds: int = DEFAULT_PATCH_LEASE_SECONDS

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
        if proposal.status in {PatchStatus.ready, PatchStatus.approval_pending}:
            return proposal  # idempotent: already generated
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
        try:
            outcome = await self.generation.generate(
                request,
                proposal_id=proposal.id,
                run_id=run_id,
                coding_run_id=binding.coding_run_id,
                project_handle=binding.project_handle,
                now=moment,
            )
        except PatchError as exc:
            await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.failed,
                expected_version=proposal.version,
                updates={"error_kind": type(exc).__name__, "error_message": str(exc)[:500]},
                now=moment,
            )
            if lease is not None:
                await self.runs.terminalize(
                    lease,
                    status=RunStatus.failed,
                    stop_reason="generation_failed",
                    error_kind=type(exc).__name__,
                    error_message=str(exc)[:500],
                    now=moment,
                )
            raise
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
                "cost_usd": float(getattr(outcome.usage, "cost_usd", 0.0) or 0.0),
            },
            now=moment,
        )
        if lease is not None:
            await self.runs.terminalize(
                lease,
                status=RunStatus.completed,
                stop_reason="generated",
                result_ref=outcome.bundle_sha256,
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
        return updated

    # --- approval ---------------------------------------------------------------------
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
        scope_id = await self._scope_for(org_id, proposal)
        approval_id = await self.approval.request(proposal, scope_id=scope_id, now=moment)
        await self.store.transition(
            org_id,
            proposal.id,
            PatchStatus.approval_pending,
            expected_version=proposal.version,
            updates={"approval_id": approval_id},
            now=moment,
        )
        self.audit.record(
            action="approval_requested",
            org_id=org_id,
            actor=actor,
            proposal_id=proposal.id,
            detail={"approval_id": approval_id},
        )
        return approval_id

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
        applied = await self.approval.decide(
            proposal, proposal.approval_id, approve=approve, resolved_by=actor, now=moment
        )
        resolved_status = (
            "granted"
            if applied and approve
            else "denied"
            if applied
            else await self.approval.resolved_status(proposal, proposal.approval_id)
        )
        if resolved_status is None:
            # The binding no longer matches (stale/changed proposal) — mark stale, fail closed.
            await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.stale,
                expected_version=proposal.version,
                updates={"error_message": "approval binding no longer matches the proposal"},
                now=moment,
            )
            return DecisionResult(applied=False, status=PatchStatus.stale, queue_writeback=False)
        target = {
            "granted": PatchStatus.approved,
            "denied": PatchStatus.denied,
            "expired": PatchStatus.expired,
        }[resolved_status]
        updated = await self.store.transition(
            org_id, proposal.id, target, expected_version=proposal.version, now=moment
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
            updated = await self.store.transition(
                org_id,
                proposal.id,
                PatchStatus.stale,
                expected_version=proposal.version,
                updates={"error_kind": "PatchStaleError", "error_message": str(exc)[:500]},
                now=moment,
            )
            self.audit.record(
                action="stale",
                org_id=org_id,
                actor=proposal.actor,
                proposal_id=proposal.id,
                detail={"reason": "base_moved"},
            )
            return updated
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
        return await self.store.transition(
            org_id,
            proposal.id,
            PatchStatus.cancelled,
            expected_version=proposal.version,
            updates={"error_message": f"cancelled by {actor}"},
            now=now or datetime.now(UTC),
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
