"""Human approval binding for controlled patch proposals (WS-PP).

A proposal is approved through the **existing org/actor-bound durable approval service**
(:class:`~keel_core.approvals.ApprovalStore`), never a bespoke side channel. The approval is bound
to the *exact* immutable proposal via a fenced ``action_hash`` computed from the proposal id, the
content-addressed bundle sha, the exact approved base sha, the changed-path digest, the target
project, and the run attempt. Because the decision carries that binding:

* a **stale or changed** proposal (a different bundle / base / changed-path digest, e.g. after a
  regeneration) produces a *different* binding, so an old decision can never authorize the new
  proposal — the resolve fails the ``expected_action_hash`` fence;
* one proposal cannot be approved twice or conflictingly — ``resolve`` only moves a ``pending`` row
  to a terminal decision once, and a retry reconciles from that immutable terminal decision.

Approval itself **never pushes**. The coordinator observes or crash-recovers a *granted* decision,
transitions the proposal to ``approved``, and signals the caller to enqueue trusted writeback; a
*denied* decision terminalizes the proposal without any remote effect.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from keel_core.approvals import ApprovalRecord, ApprovalStore

from .errors import PatchApprovalError
from .models import PatchProposal

# The durable-approval "tool" name a patch writeback approval is recorded under (audit only).
PATCH_APPROVAL_TOOL = "patch.writeback"
DEFAULT_APPROVAL_TTL_SECONDS = 7 * 24 * 3600


def approval_binding_hash(proposal: PatchProposal) -> str:
    """The fenced ``action_hash`` binding a decision to the EXACT immutable proposal.

    Any drift in the bundle, approved base, changed-path digest, or the target project changes the
    hash, so a decision recorded for one proposal can never authorize a mutated/regenerated one.
    """
    canonical = "\x1f".join(
        (
            "patch.writeback.v1",
            proposal.id,
            proposal.org_id,
            proposal.project_id,
            proposal.base_sha,
            proposal.head_sha,
            proposal.bundle_sha256,
            proposal.changed_path_digest,
            str(proposal.run_attempt),
        )
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def patch_approval_idempotency_key(proposal_id: str, bundle_sha256: str) -> str:
    return f"patch.writeback:{proposal_id}:{bundle_sha256}"


@dataclass
class PatchApprovalService:
    """Request + resolve a durable, org/actor-bound approval bound to an exact proposal."""

    approvals: ApprovalStore

    async def request(
        self,
        proposal: PatchProposal,
        *,
        scope_id: str,
        reason: str = "Approve controlled patch proposal for GitHub Draft PR writeback",
        ttl_seconds: int = DEFAULT_APPROVAL_TTL_SECONDS,
        now: datetime | None = None,
    ) -> str:
        if not proposal.bundle_sha256 or not proposal.base_sha:
            raise PatchApprovalError("proposal is not ready for approval (no bundle/base)")
        moment = now or datetime.now(UTC)
        binding = approval_binding_hash(proposal)
        approval_id = await self.approvals.create_pending(
            scope_id=scope_id,
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
            reason=reason,
            expires_at=moment + timedelta(seconds=ttl_seconds),
            org_id=proposal.org_id,
            actor=proposal.actor,
            action_hash=binding,
            run_attempt=proposal.run_attempt,
            batch_id=proposal.id,
        )
        return approval_id

    async def decide(
        self,
        proposal: PatchProposal,
        approval_id: str,
        *,
        approve: bool,
        resolved_by: str,
        now: datetime | None = None,
    ) -> bool:
        """Resolve the approval, fenced to the exact proposal binding + run attempt.

        Returns whether this call moved a pending decision to terminal. A stale/changed proposal
        (different binding) fails the fence and returns ``False`` — its decision cannot be reused.
        """
        binding = approval_binding_hash(proposal)
        status = "granted" if approve else "denied"
        return await self.approvals.resolve(
            approval_id,
            status,
            resolved_by,
            expected_action_hash=binding,
            expected_run_attempt=proposal.run_attempt,
        )

    async def get(self, approval_id: str) -> ApprovalRecord | None:
        return await self.approvals.get(approval_id)

    async def resolved_record(
        self,
        proposal: PatchProposal,
        approval_id: str,
    ) -> ApprovalRecord | None:
        """Return the terminal approval record bound to this exact proposal, or ``None``.

        Every persisted binding field is rechecked so an unrelated, regenerated, replayed, or
        tampered approval is never surfaced — only a terminal decision (``granted``/``denied``/
        ``expired``) whose full immutable binding matches this proposal. This is the single fenced
        read behind both the crash-recovery status check and the writeback-time re-verification of
        *who* granted the approval (its ``resolved_by``).
        """
        record = await self.get(approval_id)
        if record is None:
            return None
        expected_binding = approval_binding_hash(proposal)
        expected_idempotency = patch_approval_idempotency_key(proposal.id, proposal.bundle_sha256)
        if (
            record.tool != PATCH_APPROVAL_TOOL
            or record.call_id != proposal.id
            or record.idempotency_key != expected_idempotency
            or record.run_id != proposal.run_id
            or record.session_id != proposal.run_id
            or record.org_id != proposal.org_id
            or record.actor != proposal.actor
            or record.action_hash != expected_binding
            or record.run_attempt != proposal.run_attempt
            or record.batch_id != proposal.id
        ):
            return None
        if record.status not in {"granted", "denied", "expired"}:
            return None
        return record

    async def resolved_status(
        self,
        proposal: PatchProposal,
        approval_id: str,
    ) -> str | None:
        """Return a terminal decision status only when it is bound to this exact proposal.

        A thin status projection of :meth:`resolved_record` — the recovery path for a crash after
        the durable approval was resolved but before the proposal state transition committed. An
        unrelated, regenerated, or replayed approval can never advance the proposal.
        """
        record = await self.resolved_record(proposal, approval_id)
        return record.status if record is not None else None


__all__ = [
    "DEFAULT_APPROVAL_TTL_SECONDS",
    "PATCH_APPROVAL_TOOL",
    "PatchApprovalService",
    "approval_binding_hash",
    "patch_approval_idempotency_key",
]
