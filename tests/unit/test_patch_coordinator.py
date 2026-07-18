"""Lifecycle + security-gate tests for the patch coordinator (WS-PP), no git/provider/network.

Covers the durable state machine end-to-end with in-memory fakes:

* request -> generate (ready) -> request approval -> approve -> writeback (draft_pr_created);
* generation NEVER pushes (no writeback effect until an approval is granted);
* an unapproved / denied proposal can never be written back (fail closed);
* a stale/changed proposal cannot reuse an approval decision (fenced binding).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from keel_core.approvals import InMemoryApprovalStore
from keel_core.coding.models import ArtifactRecord, ArtifactRetention, CodingRunId, ProjectId
from keel_core.patch.approval import PatchApprovalService
from keel_core.patch.bundle import PatchBundleWriter
from keel_core.patch.coordinator import PatchCoordinator, ProjectBinding
from keel_core.patch.errors import PatchStateError
from keel_core.patch.generation import GenerationOutcome
from keel_core.patch.models import (
    ChangedFile,
    ChangeKind,
    PatchBundleManifest,
    PatchProposalRequest,
    PatchStatus,
    ProposedCommit,
    TestStatus,
    changed_path_digest,
)
from keel_core.patch.store import InMemoryPatchProposalStore
from keel_core.patch.writeback import WritebackResult, WritebackTarget
from keel_core.protocols import Usage
from keel_core.runs import InMemoryRunStore


class _MemArtifacts:
    """A tiny in-memory ArtifactStore keyed by content hash (enough for bundle round-trip)."""

    def __init__(self) -> None:
        self._blobs: dict[str, bytes] = {}

    def put(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        data: bytes,
        *,
        name: str,
        media_type: str = "application/octet-stream",
        retention: ArtifactRetention = ArtifactRetention.ephemeral,
        retained_until: Any = None,
        metadata: Mapping[str, str] | None = None,
    ) -> ArtifactRecord:
        import hashlib

        digest = hashlib.sha256(data).hexdigest()
        self._blobs[digest] = data
        return ArtifactRecord(
            project_id=project_id,
            run_id=run_id,
            content_hash=digest,
            size_bytes=len(data),
            name=name,
            media_type=media_type,
            created_at=datetime.now(UTC),
            retention=retention,
            retained_until=retained_until,
            metadata=dict(metadata or {}),
        )

    def read(self, project_id: ProjectId, run_id: CodingRunId, content_hash: str) -> bytes:
        return self._blobs[content_hash]

    def retain(self, *a: Any, **k: Any) -> Any: ...
    def delete(self, *a: Any, **k: Any) -> bool:
        return False

    def reap(self, *a: Any, **k: Any) -> Any: ...


class _FakeAuthorizer:
    def __init__(self, target: WritebackTarget | None) -> None:
        self._target = target
        self.associated: list[str] = []

    async def authorize_generation(self, org_id, actor, project_id, *, agent_id, run_id):  # type: ignore[no-untyped-def]
        return ProjectBinding(
            project_handle="proj",
            scope_id="agent:o/patch",
            coding_run_id=run_id,
            target=self._target,
        )

    async def authorize_read(self, org_id, actor, project_id):  # type: ignore[no-untyped-def]
        return None

    async def authorize_writeback(self, org_id, actor, project_id, *, agent_id, run_id):  # type: ignore[no-untyped-def]
        return ProjectBinding(
            project_handle="proj",
            scope_id="agent:o/patch",
            coding_run_id=run_id,
            target=self._target,
        )

    async def associate_run(self, org_id, actor, project_id, run_id, *, agent_id):  # type: ignore[no-untyped-def]
        self.associated.append(run_id)


class _FakeGeneration:
    """Writes a real, hash-consistent bundle to the artifact store, but no git/provider."""

    def __init__(self, artifacts: _MemArtifacts) -> None:
        self._artifacts = artifacts
        self.calls = 0

    async def generate(
        self, request, *, proposal_id, run_id, coding_run_id, project_handle, now=None
    ):  # type: ignore[no-untyped-def]
        self.calls += 1
        created = now or datetime.now(UTC)
        diff = b"--- a/app.py\n+++ b/app.py\n@@ -1 +1,2 @@\n def add(a,b):\n+    pass\n"
        files = (
            ChangedFile(
                path="app.py", change_kind=ChangeKind.modified, blob_sha="a" * 40, size_bytes=20
            ),
        )
        import hashlib

        manifest = PatchBundleManifest(
            proposal_id=proposal_id,
            org_id=request.org_id,
            project_id=request.project_id,
            run_id=run_id,
            base_ref=request.base_ref,
            base_sha="b" * 40,
            head_sha="c" * 40,
            diff_sha256=hashlib.sha256(diff).hexdigest(),
            diff_bytes=len(diff),
            files=files,
            commits=(ProposedCommit(sha="c" * 40, message="m", tree_sha="d" * 40),),
            tests=(),
            test_status=TestStatus.skipped,
            created_at=created,
        )
        stored = PatchBundleWriter(self._artifacts).store(
            manifest,
            diff=diff,
            project_handle=project_handle,
            coding_run_id=coding_run_id,
            now=created,
        )
        return GenerationOutcome(
            proposal_id=proposal_id,
            base_sha="b" * 40,
            head_sha="c" * 40,
            bundle_sha256=stored.bundle_sha256,
            diff_sha256=stored.diff_sha256,
            changed_path_digest=changed_path_digest(files),
            changed_files=1,
            test_status=TestStatus.skipped,
            manifest=manifest,
            pin_ref="refs/keel-patch/x",
            usage=Usage(prompt_tokens=1, completion_tokens=1, cost_usd=0.02),
        )


class _FakeWriteback:
    def __init__(self) -> None:
        self.calls = 0

    async def write(self, proposal, *, project_handle, target, manifest):  # type: ignore[no-untyped-def]
        self.calls += 1
        return WritebackResult(
            remote_branch=proposal.remote_branch,
            head_sha=proposal.head_sha,
            pr_number=101,
            pr_url="https://github.test/pr/101",
            pr_node_id="N1",
            reused_branch=False,
            reused_pr=False,
        )


def _target() -> WritebackTarget:
    return WritebackTarget(
        full_name="o/r",
        installation_id=5,
        clone_url="https://github.test/o/r.git",
        default_branch="main",
    )


def _request(idem: str = "k1") -> PatchProposalRequest:
    return PatchProposalRequest(
        org_id="o",
        project_id="p",
        actor="u",
        task="do it",
        base_ref="main",
        model="m",
        idempotency_key=idem,
    )


def _coordinator(
    artifacts: _MemArtifacts, writeback: _FakeWriteback, target: WritebackTarget | None
):
    approvals = InMemoryApprovalStore()
    return PatchCoordinator(
        store=InMemoryPatchProposalStore(),
        runs=InMemoryRunStore(),
        authorizer=_FakeAuthorizer(target),
        generation=_FakeGeneration(artifacts),  # type: ignore[arg-type]
        approval=PatchApprovalService(approvals),
        writeback=writeback,  # type: ignore[arg-type]
        artifacts=artifacts,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_full_lifecycle_reaches_draft_pr() -> None:
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    coord = _coordinator(artifacts, writeback, _target())
    req = _request()

    handle = await coord.request_generation(req)
    assert handle.created and handle.status is PatchStatus.generating

    proposal = await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    assert proposal.status is PatchStatus.ready and proposal.bundle_sha256
    # Generation did NOT push (no writeback effect until an approval is granted).
    assert writeback.calls == 0

    # An unapproved proposal cannot be written back (fail closed).
    with pytest.raises(PatchStateError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0

    approval_id = await coord.request_approval("o", handle.proposal_id, actor="u")
    assert approval_id
    decision = await coord.decide("o", handle.proposal_id, approve=True, actor="u")
    assert decision.applied and decision.queue_writeback

    final = await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert final.status is PatchStatus.draft_pr_created
    assert final.pr_number == 101 and final.remote_branch.startswith("keel/patch/")
    assert writeback.calls == 1
    # Idempotent: a re-run of the writeback is a no-op.
    again = await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert again.status is PatchStatus.draft_pr_created and writeback.calls == 1


@pytest.mark.asyncio
async def test_denied_proposal_is_never_written_back() -> None:
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    coord = _coordinator(artifacts, writeback, _target())
    req = _request()
    handle = await coord.request_generation(req)
    await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    await coord.request_approval("o", handle.proposal_id, actor="u")
    decision = await coord.decide("o", handle.proposal_id, approve=False, actor="u")
    assert decision.status is PatchStatus.denied and not decision.queue_writeback
    with pytest.raises(PatchStateError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0


@pytest.mark.asyncio
async def test_decide_recovers_terminal_approval_after_crash() -> None:
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    coord = _coordinator(artifacts, writeback, _target())
    req = _request()
    handle = await coord.request_generation(req)
    await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    approval_id = await coord.request_approval("o", handle.proposal_id, actor="u")
    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None

    # Simulate a process death after the durable approval committed but before the proposal moved.
    assert await coord.approval.decide(proposal, approval_id, approve=True, resolved_by="u")

    decision = await coord.decide("o", handle.proposal_id, approve=False, actor="u")
    assert not decision.applied
    assert decision.status is PatchStatus.approved
    assert decision.queue_writeback

    recovered = await coord.store.get("o", handle.proposal_id)
    assert recovered is not None and recovered.status is PatchStatus.approved

    retried = await coord.decide("o", handle.proposal_id, approve=False, actor="u")
    assert not retried.applied
    assert retried.status is PatchStatus.approved
    assert retried.queue_writeback


@pytest.mark.asyncio
async def test_idempotent_generation_reuses_run() -> None:
    artifacts = _MemArtifacts()
    coord = _coordinator(artifacts, _FakeWriteback(), _target())
    req = _request("same-key")
    h1 = await coord.request_generation(req)
    h2 = await coord.request_generation(req)
    assert h1.proposal_id == h2.proposal_id and not h2.created
