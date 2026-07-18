"""Tests for the durable approval binding of a patch proposal (WS-PP).

A decision is fenced to the EXACT immutable proposal: a regenerated proposal (different bundle /
base / changed-path digest) yields a different binding, so an old decision can never authorize it,
and one proposal cannot be approved twice/conflictingly.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from keel_core.approvals import InMemoryApprovalStore
from keel_core.patch.approval import PatchApprovalService, approval_binding_hash
from keel_core.patch.models import PatchProposal, PatchStatus, TestStatus


def _proposal(**overrides) -> PatchProposal:  # type: ignore[no-untyped-def]
    now = datetime.now(UTC)
    base = dict(
        id="pp_a",
        org_id="o",
        project_id="p",
        run_id="pp_a",
        run_attempt=1,
        agent_id="patch",
        actor="u",
        base_ref="main",
        base_sha="b" * 40,
        head_sha="c" * 40,
        bundle_sha256="e" * 64,
        diff_sha256="f" * 64,
        changed_path_digest="9" * 64,
        changed_files=1,
        test_status=TestStatus.passed,
        status=PatchStatus.ready,
        version=2,
        idempotency_key="k",
        fingerprint="k",
        expires_at=now + timedelta(hours=1),
        created_at=now,
        updated_at=now,
    )
    base.update(overrides)
    return PatchProposal(**base)  # type: ignore[arg-type]


def test_binding_changes_when_bundle_or_base_changes() -> None:
    p = _proposal()
    assert approval_binding_hash(p) == approval_binding_hash(_proposal())
    assert approval_binding_hash(p) != approval_binding_hash(_proposal(bundle_sha256="a" * 64))
    assert approval_binding_hash(p) != approval_binding_hash(_proposal(base_sha="a" * 40))
    assert approval_binding_hash(p) != approval_binding_hash(
        _proposal(changed_path_digest="0" * 64)
    )


@pytest.mark.asyncio
async def test_grant_then_regenerated_proposal_cannot_reuse_decision() -> None:
    store = InMemoryApprovalStore()
    svc = PatchApprovalService(store)
    proposal = _proposal()
    approval_id = await svc.request(proposal, scope_id="agent:o/patch")

    # A regenerated proposal (new bundle) must NOT be resolvable via the old approval binding.
    regenerated = replace(proposal, bundle_sha256="a" * 64)
    assert await svc.decide(regenerated, approval_id, approve=True, resolved_by="u") is False

    # The correct proposal binding resolves exactly once.
    assert await svc.decide(proposal, approval_id, approve=True, resolved_by="u") is True
    assert await svc.resolved_status(proposal, approval_id) == "granted"
    assert await svc.resolved_status(regenerated, approval_id) is None
    # A second (conflicting) decision on the now-terminal approval is a no-op.
    assert await svc.decide(proposal, approval_id, approve=False, resolved_by="u") is False


@pytest.mark.asyncio
async def test_request_requires_ready_bundle() -> None:
    svc = PatchApprovalService(InMemoryApprovalStore())
    from keel_core.patch.errors import PatchApprovalError

    with pytest.raises(PatchApprovalError):
        await svc.request(_proposal(bundle_sha256=""), scope_id="agent:o/patch")
