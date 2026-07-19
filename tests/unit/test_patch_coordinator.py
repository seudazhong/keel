"""Lifecycle + security-gate tests for the patch coordinator (WS-PP), no git/provider/network.

Covers the durable state machine end-to-end with in-memory fakes:

* request -> generate (auto-advances the transient ``ready`` to ``approval_pending``) ->
  approve -> writeback (draft_pr_created);
* generation NEVER pushes (no writeback effect until an approval is granted);
* the global dispatch pointer (outbox) tracks the proposal: written on fresh create, hinted
  ``ready``/``approved`` and retired on ``approval_pending`` / terminal states;
* an unapproved / denied proposal can never be written back (fail closed);
* a stale/changed proposal cannot reuse an approval decision (fenced binding);
* strict error semantics: lease-lost never terminalizes, provider-unavailable releases the lease
  and charges partial cost coherently, permanent errors fail closed and retire the pointer.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from keel_core.approvals import InMemoryApprovalStore
from keel_core.coding.models import ArtifactRecord, ArtifactRetention, CodingRunId, ProjectId
from keel_core.errors import PermissionDenied
from keel_core.patch.approval import PatchApprovalService
from keel_core.patch.bundle import PatchBundleWriter
from keel_core.patch.coordinator import (
    DEFAULT_PATCH_AGENT_ID,
    PatchCoordinator,
    ProjectBinding,
)
from keel_core.patch.errors import (
    PatchApprovalError,
    PatchLeaseLost,
    PatchProviderError,
    PatchProviderUnavailable,
    PatchRemoteUnavailable,
    PatchStateError,
    PatchValidationError,
    PatchWritebackError,
)
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
from keel_core.patch.outbox import InMemoryPatchProposalOutbox, PatchOutboxStatus
from keel_core.patch.store import InMemoryPatchProposalStore
from keel_core.patch.writeback import WritebackResult, WritebackTarget
from keel_core.protocols import Usage
from keel_core.runs import InMemoryRunStore, RunLease, RunStatus
from keel_core.scoping import derive_agent_scope
from keel_core.types import RunId

# The canonical per-Agent scope every default-request proposal (org "o", no explicit Agent) runs
# under — the in-memory analog of a Postgres run store bound to this scope.
_DEFAULT_SCOPE = derive_agent_scope("o", DEFAULT_PATCH_AGENT_ID)


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
        # Records every (requester_actor, approved_by) pair a writeback authorization was asked for.
        self.writebacks: list[tuple[str, str]] = []

    def _binding(self, org_id: str, agent_id: str | None, run_id: str) -> ProjectBinding:
        # Mirror the real authorizer: derive the canonical per-Agent scope so two orgs/Agents get
        # distinct, isolated scopes (and a default request lands on ``_DEFAULT_SCOPE``).
        return ProjectBinding(
            project_handle="proj",
            scope_id=derive_agent_scope(org_id, agent_id or DEFAULT_PATCH_AGENT_ID),
            coding_run_id=run_id,
            target=self._target,
        )

    async def authorize_generation(self, org_id, actor, project_id, *, agent_id, run_id):  # type: ignore[no-untyped-def]
        return self._binding(org_id, agent_id, run_id)

    async def authorize_read(self, org_id, actor, project_id):  # type: ignore[no-untyped-def]
        return None

    async def authorize_approval(self, org_id, actor, project_id):  # type: ignore[no-untyped-def]
        return None

    async def authorize_writeback(  # type: ignore[no-untyped-def]
        self, org_id, project_id, *, requester_actor, approved_by, agent_id, run_id
    ):
        self.writebacks.append((requester_actor, approved_by))
        return self._binding(org_id, agent_id, run_id)

    async def associate_run(self, org_id, actor, project_id, run_id, *, agent_id):  # type: ignore[no-untyped-def]
        self.associated.append(run_id)


class _FakeGeneration:
    """Writes a real, hash-consistent bundle to the artifact store, but no git/provider."""

    def __init__(self, artifacts: _MemArtifacts) -> None:
        self._artifacts = artifacts
        self.calls = 0

    async def generate(
        self,
        request,
        *,
        proposal_id,
        run_id,
        coding_run_id,
        project_handle,
        now=None,
        interrupt=None,
    ):  # type: ignore[no-untyped-def]
        self.calls += 1
        return _build_outcome(
            self._artifacts,
            request,
            proposal_id=proposal_id,
            run_id=run_id,
            coding_run_id=coding_run_id,
            project_handle=project_handle,
            now=now,
            usage=Usage(prompt_tokens=1, completion_tokens=1, cost_usd=0.02),
        )


class _ScriptedGeneration:
    """Generation double driven by a scripted list of behaviors, one popped per call.

    Each entry is either a ``BaseException`` to raise (e.g. a transient/permanent patch error) or a
    :class:`Usage` describing a successful attempt's cost — enough to prove the coordinator's error
    branches and cumulative cost handling without any git/provider."""

    def __init__(self, artifacts: _MemArtifacts, script: list[Any]) -> None:
        self._artifacts = artifacts
        self._script = list(script)
        self.calls = 0

    async def generate(
        self,
        request,
        *,
        proposal_id,
        run_id,
        coding_run_id,
        project_handle,
        now=None,
        interrupt=None,
    ):  # type: ignore[no-untyped-def]
        self.calls += 1
        behavior = self._script.pop(0)
        if isinstance(behavior, BaseException):
            raise behavior
        return _build_outcome(
            self._artifacts,
            request,
            proposal_id=proposal_id,
            run_id=run_id,
            coding_run_id=coding_run_id,
            project_handle=project_handle,
            now=now,
            usage=behavior,
        )


class _GetTrackingRunStore(InMemoryRunStore):
    """In-memory run store that counts ``get`` calls (and can be told to fail them).

    Proves the permanent generation-failure path never issues a pre-cleanup ``runs.get``: the P2
    invariant mirrors the run's cumulative cost onto the proposal, so the permanent catch reads
    ``proposal.cost_usd`` and cleans up (fail proposal, delete pointer, terminalize) without a fresh
    run fetch that could itself fail.
    """

    def __init__(self, *, fail: bool = False) -> None:
        super().__init__()
        self.get_calls = 0
        self.fail = fail

    async def get(self, run_id: RunId) -> Any:
        self.get_calls += 1
        if self.fail:
            raise RuntimeError("runs.get injected failure")
        return await super().get(run_id)


def _build_outcome(
    artifacts: _MemArtifacts,
    request,  # type: ignore[no-untyped-def]
    *,
    proposal_id: str,
    run_id: str,
    coding_run_id: str,
    project_handle: str,
    now: datetime | None,
    usage: Usage,
) -> GenerationOutcome:
    import hashlib

    created = now or datetime.now(UTC)
    diff = b"--- a/app.py\n+++ b/app.py\n@@ -1 +1,2 @@\n def add(a,b):\n+    pass\n"
    files = (
        ChangedFile(
            path="app.py", change_kind=ChangeKind.modified, blob_sha="a" * 40, size_bytes=20
        ),
    )
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
    stored = PatchBundleWriter(artifacts).store(  # type: ignore[arg-type]
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
        usage=usage,
    )


class _FakeWriteback:
    def __init__(self, *, raises: BaseException | None = None) -> None:
        self.calls = 0
        self._raises = raises

    async def write(self, proposal, *, project_handle, target, manifest):  # type: ignore[no-untyped-def]
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return WritebackResult(
            remote_branch=proposal.remote_branch,
            head_sha=proposal.head_sha,
            pr_number=101,
            pr_url="https://github.test/pr/101",
            pr_node_id="N1",
            reused_branch=False,
            reused_pr=False,
        )


class _ContendedRunStore(InMemoryRunStore):
    async def claim(
        self,
        run_id: RunId,
        *,
        worker_id: str,
        now: datetime | None = None,
        lease_seconds: int,
    ) -> RunLease | None:
        return None


class _RunFactory:
    """A per-scope in-memory run store factory (memoized).

    The in-memory analog of ``lambda scope: PostgresRunStore(engine, scope)``: each scope gets its
    own isolated store, so two Agents/orgs can never see each other's runs. Seedable so a test can
    inject a specific store (contended / get-tracking) for the default scope while other scopes get
    fresh isolated stores on demand."""

    def __init__(self, seed: dict[str, InMemoryRunStore] | None = None) -> None:
        self._stores: dict[str, InMemoryRunStore] = dict(seed or {})

    def __call__(self, scope_id: str) -> InMemoryRunStore:
        store = self._stores.get(scope_id)
        if store is None:
            store = InMemoryRunStore()
            self._stores[scope_id] = store
        return store


async def _run_get(coord: PatchCoordinator, run_id: str, *, scope: str = _DEFAULT_SCOPE) -> Any:
    """Read a run via the coordinator's public per-scope factory (memoized -> same store)."""
    return await coord.run_store_factory(scope).get(run_id)


class _ScopeSpyFactory:
    """A per-scope approval-store factory that records every scope it is asked to resolve.

    The in-memory approval store is intentionally *shared* across scopes (rows carry their own
    scope, and ``transition_to_approval_pending`` requires the same instance for create + decide),
    so this spy proves the coordinator resolves and passes the proposal's *canonical* scope on every
    approval operation rather than a single hard-wired global one."""

    def __init__(self) -> None:
        self.shared = InMemoryApprovalStore()
        self.scopes: list[str] = []

    def __call__(self, scope_id: str) -> InMemoryApprovalStore:
        self.scopes.append(scope_id)
        return self.shared


def _target() -> WritebackTarget:
    return WritebackTarget(
        full_name="o/r",
        installation_id=5,
        clone_url="https://github.test/o/r.git",
        default_branch="main",
    )


def _request(idem: str = "k1", *, org: str = "o", actor: str = "u") -> PatchProposalRequest:
    return PatchProposalRequest(
        org_id=org,
        project_id="p",
        actor=actor,
        task="do it",
        base_ref="main",
        model="m",
        idempotency_key=idem,
    )


def _coordinator(
    artifacts: _MemArtifacts,
    writeback: _FakeWriteback,
    target: WritebackTarget | None,
    *,
    runs: InMemoryRunStore | None = None,
    generation: Any | None = None,
    approval_factory: Any | None = None,
    authorizer: Any | None = None,
    outbox: InMemoryPatchProposalOutbox | None = None,
) -> PatchCoordinator:
    factory = approval_factory or _ScopeSpyFactory()
    return PatchCoordinator(
        store=InMemoryPatchProposalStore(),
        outbox=outbox or InMemoryPatchProposalOutbox(),
        run_store_factory=_RunFactory({_DEFAULT_SCOPE: runs or InMemoryRunStore()}),
        authorizer=authorizer or _FakeAuthorizer(target),
        generation=generation or _FakeGeneration(artifacts),  # type: ignore[arg-type]
        approval_factory=factory,
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

    # Generation auto-advances through the transient ``ready`` to ``approval_pending`` (the pointer
    # is retired and the durable approval bound), and NEVER pushes.
    proposal = await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    assert proposal.status is PatchStatus.approval_pending and proposal.bundle_sha256
    assert proposal.approval_id
    assert writeback.calls == 0
    assert await coord.outbox.get(handle.proposal_id) is None  # pointer retired at approval_pending

    # An unapproved proposal cannot be written back (fail closed).
    with pytest.raises(PatchStateError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0

    # ``request_approval`` is idempotent for a reconciler: it returns the already-bound approval.
    approval_id = await coord.request_approval("o", handle.proposal_id, actor="u")
    assert approval_id == proposal.approval_id
    decision = await coord.decide("o", handle.proposal_id, approve=True, actor="u")
    assert decision.applied and decision.queue_writeback

    final = await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert final.status is PatchStatus.draft_pr_created
    assert final.pr_number == 101 and final.remote_branch.startswith("keel/patch/")
    assert writeback.calls == 1
    assert await coord.outbox.get(handle.proposal_id) is None  # pointer retired at draft_pr_created
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
    assert await coord.outbox.get(handle.proposal_id) is None  # terminal deny retires the pointer
    with pytest.raises(PatchStateError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0


@pytest.mark.asyncio
async def test_generation_requires_run_lease() -> None:
    artifacts = _MemArtifacts()
    generation = _FakeGeneration(artifacts)
    coord = _coordinator(
        artifacts,
        _FakeWriteback(),
        _target(),
        runs=_ContendedRunStore(),
        generation=generation,
    )
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(PatchStateError, match="leased by another worker"):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w2")
    assert generation.calls == 0


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
    # The approval store is resolved through the same per-scope factory the coordinator uses.
    approval = PatchApprovalService(coord.approval_factory("agent:o/patch"))
    assert await approval.decide(proposal, approval_id, approve=True, resolved_by="u")

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


@pytest.mark.asyncio
async def test_request_generation_writes_pointer_but_replay_does_not_resurrect() -> None:
    artifacts = _MemArtifacts()
    coord = _coordinator(artifacts, _FakeWriteback(), _target())
    req = _request("same-key")
    handle = await coord.request_generation(req)
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating

    # Advance to approval_pending (pointer deleted), then an idempotent replay of the same request
    # must NOT resurrect the retired pointer (the P1 create invariant threaded through the seam).
    await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    assert await coord.outbox.get(handle.proposal_id) is None
    replay = await coord.request_generation(req)
    assert replay.proposal_id == handle.proposal_id and not replay.created
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_generation_auto_advances_and_charges_run_cost() -> None:
    artifacts = _MemArtifacts()
    coord = _coordinator(artifacts, _FakeWriteback(), _target())
    req = _request()
    handle = await coord.request_generation(req)

    proposal = await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    assert proposal.status is PatchStatus.approval_pending and proposal.approval_id
    assert proposal.cost_usd == pytest.approx(0.02)
    run = await _run_get(coord, handle.run_id)
    assert run is not None and run.status is RunStatus.completed
    assert run.cost_usd == pytest.approx(0.02)  # the run row is the authoritative charge ledger
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_ready_retry_heals_to_approval_pending() -> None:
    artifacts = _MemArtifacts()
    coord = _coordinator(artifacts, _FakeWriteback(), _target())
    req = _request()
    handle = await coord.request_generation(req)

    # Simulate a crash after the bundle was persisted ``ready`` (pointer hint ``ready``) but before
    # the atomic approval transition: drive the store to ``ready`` directly.
    await coord.store.transition(
        "o",
        handle.proposal_id,
        PatchStatus.ready,
        expected_version=1,
        updates={
            "base_sha": "b" * 40,
            "head_sha": "c" * 40,
            "bundle_sha256": "e" * 64,
            "changed_path_digest": "d" * 64,
        },
        outbox=coord.outbox,
        scope_id="agent:o/patch",
    )
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.ready

    # A retry observes the transient ``ready`` and heals to ``approval_pending`` without regen.
    healed = await coord.execute_generation("o", handle.run_id, req, worker_id="w2")
    assert healed.status is PatchStatus.approval_pending and healed.approval_id
    assert coord.generation.calls == 0  # type: ignore[attr-defined]
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_request_approval_is_idempotent_for_reconciler() -> None:
    artifacts = _MemArtifacts()
    coord = _coordinator(artifacts, _FakeWriteback(), _target())
    req = _request()
    handle = await coord.request_generation(req)
    proposal = await coord.execute_generation("o", handle.run_id, req, worker_id="w1")

    first = await coord.request_approval("o", handle.proposal_id, actor="u")
    second = await coord.request_approval("o", handle.proposal_id, actor="u")
    assert first == second == proposal.approval_id
    after = await coord.store.get("o", handle.proposal_id)
    assert after is not None and after.version == proposal.version  # no extra version bump


@pytest.mark.asyncio
async def test_scope_factory_isolation_and_approved_pointer() -> None:
    artifacts = _MemArtifacts()
    factory = _ScopeSpyFactory()
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), approval_factory=factory)
    req = _request()
    handle = await coord.request_generation(req)
    await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    decision = await coord.decide("o", handle.proposal_id, approve=True, actor="u")
    assert decision.applied and decision.status is PatchStatus.approved

    # ``approved`` re-creates the dispatch pointer with the ``approved`` hint under the exact scope.
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None
    assert entry.status_hint is PatchOutboxStatus.approved
    assert entry.scope_id == "agent:o/patch"
    # Every approval operation resolved the proposal's canonical scope — never a global/other one.
    assert set(factory.scopes) == {"agent:o/patch"}


@pytest.mark.asyncio
async def test_cancel_retires_pointer() -> None:
    artifacts = _MemArtifacts()
    coord = _coordinator(artifacts, _FakeWriteback(), _target())
    req = _request()
    handle = await coord.request_generation(req)
    assert await coord.outbox.get(handle.proposal_id) is not None

    cancelled = await coord.cancel("o", handle.proposal_id, actor="u")
    assert cancelled.status is PatchStatus.cancelled
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_lease_lost_leaves_proposal_and_run_untouched() -> None:
    artifacts = _MemArtifacts()
    gen = _ScriptedGeneration(artifacts, [PatchLeaseLost("lease reclaimed")])
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), generation=gen)
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(PatchLeaseLost):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")

    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.generating
    assert proposal.cost_usd == 0.0
    run = await _run_get(coord, handle.run_id)
    # lease still held, run never terminalized
    assert run is not None and run.status is RunStatus.running
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating
    assert gen.calls == 1


@pytest.mark.asyncio
async def test_provider_unavailable_releases_partial_cost_then_retry_accrues_cumulatively() -> None:
    artifacts = _MemArtifacts()
    gen = _ScriptedGeneration(
        artifacts,
        [
            PatchProviderUnavailable(
                "upstream 503",
                usage=Usage(prompt_tokens=5, completion_tokens=0, cost_usd=0.01),
            ),
            Usage(prompt_tokens=7, completion_tokens=3, cost_usd=0.02),
        ],
    )
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), generation=gen)
    req = _request()
    handle = await coord.request_generation(req)

    # 1) Transient outage: the lease is released to the queue, the proposal stays ``generating``
    #    and the partial usage is durably charged (never lost).
    with pytest.raises(PatchProviderUnavailable):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    p1 = await coord.store.get("o", handle.proposal_id)
    assert p1 is not None and p1.status is PatchStatus.generating
    assert p1.cost_usd == pytest.approx(0.01)
    r1 = await _run_get(coord, handle.run_id)
    assert r1 is not None and r1.status is RunStatus.queued and r1.cost_usd == pytest.approx(0.01)
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating

    # 2) Retry succeeds: cost accrues cumulatively (prior partial + this outcome) — no double count.
    p2 = await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    assert p2.status is PatchStatus.approval_pending
    assert p2.cost_usd == pytest.approx(0.03)
    r2 = await _run_get(coord, handle.run_id)
    assert r2 is not None and r2.status is RunStatus.completed
    assert r2.cost_usd == pytest.approx(0.03)
    assert gen.calls == 2
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_permanent_generation_error_fails_and_retires_pointer() -> None:
    artifacts = _MemArtifacts()
    gen = _ScriptedGeneration(artifacts, [PatchValidationError("empty proposal")])
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), generation=gen)
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(PatchValidationError):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")

    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.failed
    # A failure that carries no usage charges nothing (cost stays zero).
    assert proposal.cost_usd == pytest.approx(0.0)
    run = await _run_get(coord, handle.run_id)
    assert run is not None and run.status is RunStatus.failed
    assert run.cost_usd == pytest.approx(0.0)
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_permanent_provider_error_charges_partial_usage_on_run_and_proposal() -> None:
    # A permanent PatchProviderError (cost ceiling, malformed output, or a permanent transfer
    # rejection all reach the coordinator as this error) still consumed tokens: the terminal failure
    # must charge that usage onto the run *and* mirror it onto the proposal — never lose it.
    artifacts = _MemArtifacts()
    gen = _ScriptedGeneration(
        artifacts,
        [PatchProviderError("ceiling exceeded", usage=Usage(prompt_tokens=8, cost_usd=0.05))],
    )
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), generation=gen)
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(PatchProviderError):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")

    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.failed
    assert proposal.cost_usd == pytest.approx(0.05)
    run = await _run_get(coord, handle.run_id)
    assert run is not None and run.status is RunStatus.failed
    assert run.cost_usd == pytest.approx(0.05)  # the run row is the authoritative charge ledger
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_transient_then_permanent_charges_cumulatively_without_double_count() -> None:
    # A transient outage charges a partial cost and requeues; a later *permanent* failure charges
    # its own usage on top (cumulative = prior partial + this outcome), proving the terminal-failure
    # path neither loses nor double-counts what earlier attempts already charged.
    artifacts = _MemArtifacts()
    gen = _ScriptedGeneration(
        artifacts,
        [
            PatchProviderUnavailable("upstream 503", usage=Usage(prompt_tokens=5, cost_usd=0.01)),
            PatchProviderError("permanent failure", usage=Usage(prompt_tokens=3, cost_usd=0.02)),
        ],
    )
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), generation=gen)
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(PatchProviderUnavailable):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    r1 = await _run_get(coord, handle.run_id)
    assert r1 is not None and r1.status is RunStatus.queued and r1.cost_usd == pytest.approx(0.01)

    with pytest.raises(PatchProviderError):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.failed
    assert proposal.cost_usd == pytest.approx(0.03)
    r2 = await _run_get(coord, handle.run_id)
    assert r2 is not None and r2.status is RunStatus.failed
    assert r2.cost_usd == pytest.approx(0.03)  # 0.01 (prior partial) + 0.02 (this failure)
    assert gen.calls == 2
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_permanent_failure_cleanup_does_not_read_run_and_survives_get_failure() -> None:
    # The permanent catch must not depend on a pre-cleanup ``runs.get``: it derives ``prior_cost``
    # from the proposal (the P2 mirror). Injecting a failing ``runs.get`` before the terminal
    # attempt must not disturb the cleanup — the run is still charged + terminalized and the pointer
    # is retired.
    artifacts = _MemArtifacts()
    gen = _ScriptedGeneration(
        artifacts,
        [PatchProviderError("permanent failure", usage=Usage(prompt_tokens=4, cost_usd=0.04))],
    )
    runs = _GetTrackingRunStore()
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), generation=gen, runs=runs)
    req = _request()
    handle = await coord.request_generation(req)

    runs.fail = True  # any runs.get from here on raises
    calls_before = runs.get_calls
    with pytest.raises(PatchProviderError):  # the generation error, never a runs.get RuntimeError
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    assert runs.get_calls == calls_before  # the permanent catch issued no runs.get

    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.failed
    assert proposal.cost_usd == pytest.approx(0.04)
    assert await coord.outbox.get(handle.proposal_id) is None
    runs.fail = False  # re-enable reads to assert the run was still charged + terminalized
    run = await _run_get(coord, handle.run_id)
    assert run is not None and run.status is RunStatus.failed
    assert run.cost_usd == pytest.approx(0.04)


@pytest.mark.asyncio
async def test_writeback_remote_unavailable_keeps_writing_and_pointer() -> None:
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback(raises=PatchRemoteUnavailable("github 503"))
    coord = _coordinator(artifacts, writeback, _target())
    req = _request()
    handle = await coord.request_generation(req)
    await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    await coord.decide("o", handle.proposal_id, approve=True, actor="u")

    with pytest.raises(PatchRemoteUnavailable):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")

    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.writing
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.approved  # kept for retry
    assert writeback.calls == 1


@pytest.mark.asyncio
async def test_writeback_permanent_error_fails_and_retires_pointer() -> None:
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback(raises=PatchWritebackError("verify refused"))
    coord = _coordinator(artifacts, writeback, _target())
    req = _request()
    handle = await coord.request_generation(req)
    await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    await coord.decide("o", handle.proposal_id, approve=True, actor="u")

    with pytest.raises(PatchWritebackError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")

    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.failed
    assert await coord.outbox.get(handle.proposal_id) is None


# --- P3a-3: authorizer hardening, optional deps, per-scope run factory --------------------


class _RoleAuthorizer(_FakeAuthorizer):
    """Authorizer double that models the real patch capability matrix.

    read/deny require only ``read`` (any actor); a human *approval* requires ``write`` (a listed
    writer). The trusted *writeback* re-verifies both principals independently: the generation
    *requester* must still hold ``use`` (modeled by ``users`` — ``None`` means everyone) and the
    *approver* who granted the decision must still hold ``write`` (a listed writer). The role sets
    are mutable so a test can revoke a capability between approval and writeback."""

    def __init__(
        self,
        target: WritebackTarget | None,
        *,
        writers: set[str],
        users: set[str] | None = None,
    ) -> None:
        super().__init__(target)
        self.writers = set(writers)
        self.users = set(users) if users is not None else None

    async def authorize_approval(self, org_id, actor, project_id):  # type: ignore[no-untyped-def]
        if actor not in self.writers:
            raise PermissionDenied("approval requires the 'write' capability")

    async def authorize_writeback(  # type: ignore[no-untyped-def]
        self, org_id, project_id, *, requester_actor, approved_by, agent_id, run_id
    ):
        if self.users is not None and requester_actor not in self.users:
            raise PermissionDenied("writeback requester requires the 'use' capability")
        if approved_by not in self.writers:
            raise PermissionDenied("writeback approver requires the 'write' capability")
        self.writebacks.append((requester_actor, approved_by))
        return self._binding(org_id, agent_id, run_id)


class _WrongScopeAuthorizer(_FakeAuthorizer):
    """Returns a binding whose scope is NOT the proposal's canonical per-Agent scope."""

    async def authorize_generation(self, org_id, actor, project_id, *, agent_id, run_id):  # type: ignore[no-untyped-def]
        return ProjectBinding(
            project_handle="proj",
            scope_id="agent:someone-else/patch",
            coding_run_id=run_id,
            target=self._target,
        )


async def _drive_to_approval(coord: PatchCoordinator, req: PatchProposalRequest) -> Any:
    handle = await coord.request_generation(req)
    await coord.execute_generation(req.org_id, handle.run_id, req, worker_id="w1")
    return handle


@pytest.mark.asyncio
async def test_read_only_actor_can_deny_but_not_approve() -> None:
    # The decision gate: an approval unlocks the remote side effect so it needs ``write``; a
    # denial is read-only. A read-only actor can decline a proposal but can never approve it,
    # while a writer can. Reauthorization happens BEFORE the durable approval resolves.
    artifacts = _MemArtifacts()
    coord = _coordinator(
        artifacts,
        _FakeWriteback(),
        _target(),
        authorizer=_RoleAuthorizer(_target(), writers={"boss"}),
    )

    handle_a = await _drive_to_approval(coord, _request("kA"))
    with pytest.raises(PermissionDenied):
        await coord.decide("o", handle_a.proposal_id, approve=True, actor="viewer")
    # The failed authorization left the proposal untouched (still awaiting approval).
    still = await coord.store.get("o", handle_a.proposal_id)
    assert still is not None and still.status is PatchStatus.approval_pending
    granted = await coord.decide("o", handle_a.proposal_id, approve=True, actor="boss")
    assert granted.applied and granted.status is PatchStatus.approved

    handle_b = await _drive_to_approval(coord, _request("kB"))
    denied = await coord.decide("o", handle_b.proposal_id, approve=False, actor="viewer")
    assert denied.applied and denied.status is PatchStatus.denied


@pytest.mark.asyncio
async def test_server_only_coordinator_runs_control_plane_but_refuses_execution() -> None:
    # A server coordinator wires request/read/decide/cancel WITHOUT generation/writeback/artifacts.
    # The control plane works; calling an execution entrypoint raises an explicit PatchStateError
    # instead of crashing on a ``None`` attribute.
    coord = PatchCoordinator(
        store=InMemoryPatchProposalStore(),
        outbox=InMemoryPatchProposalOutbox(),
        run_store_factory=_RunFactory({_DEFAULT_SCOPE: InMemoryRunStore()}),
        authorizer=_FakeAuthorizer(_target()),
        approval_factory=_ScopeSpyFactory(),
    )
    req = _request()
    handle = await coord.request_generation(req)
    assert handle.created and handle.status is PatchStatus.generating

    with pytest.raises(PatchStateError):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")
    with pytest.raises(PatchStateError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")

    got = await coord.get("o", handle.proposal_id, actor="u")
    assert got.status is PatchStatus.generating
    cancelled = await coord.cancel("o", handle.proposal_id, actor="u")
    assert cancelled.status is PatchStatus.cancelled
    assert await coord.outbox.get(handle.proposal_id) is None


@pytest.mark.asyncio
async def test_run_store_factory_isolates_scopes_and_orgs() -> None:
    # Each canonical per-Agent scope gets its own run store: a run created under one scope is
    # invisible under another, and proposals are org-isolated in the store.
    artifacts = _MemArtifacts()
    coord = _coordinator(artifacts, _FakeWriteback(), _target())
    h1 = await coord.request_generation(_request("k1", org="o"))
    h2 = await coord.request_generation(_request("k2", org="o2"))

    scope1 = derive_agent_scope("o", DEFAULT_PATCH_AGENT_ID)
    scope2 = derive_agent_scope("o2", DEFAULT_PATCH_AGENT_ID)
    assert scope1 != scope2

    assert await coord.run_store_factory(scope1).get(h1.run_id) is not None
    assert await coord.run_store_factory(scope2).get(h2.run_id) is not None
    # cross-scope runs are invisible
    assert await coord.run_store_factory(scope1).get(h2.run_id) is None
    assert await coord.run_store_factory(scope2).get(h1.run_id) is None
    # cross-org proposals are invisible
    assert await coord.store.get("o2", h1.proposal_id) is None
    assert await coord.store.get("o", h2.proposal_id) is None


@pytest.mark.asyncio
async def test_request_rejects_non_canonical_scope() -> None:
    # If the authorizer resolves a scope that is not the proposal's canonical per-Agent scope, the
    # request fails closed BEFORE any durable run/proposal is created.
    artifacts = _MemArtifacts()
    coord = _coordinator(
        artifacts, _FakeWriteback(), _target(), authorizer=_WrongScopeAuthorizer(_target())
    )
    with pytest.raises(PatchValidationError):
        await coord.request_generation(_request())
    assert await coord.store.list_for_project("o", "p") == []
    assert await coord.run_store_factory(_DEFAULT_SCOPE).get("k1") is None


# --- P3a-3 (follow-up): writeback re-verifies requester ``use`` + approver ``write`` -------


async def _drive_to_approved(
    coord: PatchCoordinator, req: PatchProposalRequest, *, approver: str
) -> Any:
    """Drive a proposal to ``approved`` (generation -> approval_pending -> granted decision)."""
    handle = await _drive_to_approval(coord, req)
    decision = await coord.decide(req.org_id, handle.proposal_id, approve=True, actor=approver)
    assert decision.applied and decision.status is PatchStatus.approved
    return handle


@pytest.mark.asyncio
async def test_writeback_reauthorizes_use_requester_and_write_approver() -> None:
    # The writeback gate re-verifies TWO independent principals: the generation *requester* keeps
    # only ``use`` (a use-only author who can never push directly) and the *approver* who granted
    # the decision keeps ``write``. A use-only requester + a writer approver is written back, and
    # the authorizer is asked with exactly (requester=author, approved_by=granter).
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    authz = _RoleAuthorizer(_target(), writers={"boss"}, users={"author", "boss"})
    coord = _coordinator(artifacts, writeback, _target(), authorizer=authz)

    handle = await _drive_to_approved(coord, _request(actor="author"), approver="boss")
    final = await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")

    assert final.status is PatchStatus.draft_pr_created and writeback.calls == 1
    assert authz.writebacks == [("author", "boss")]


@pytest.mark.asyncio
async def test_writeback_fails_when_requester_use_revoked() -> None:
    # A requester whose ``use`` is revoked after approval fails the writeback closed — even though a
    # valid writer approved it. The proposal is untouched (still ``approved``) and never pushes.
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    authz = _RoleAuthorizer(_target(), writers={"boss"}, users={"author", "boss"})
    coord = _coordinator(artifacts, writeback, _target(), authorizer=authz)

    handle = await _drive_to_approved(coord, _request(actor="author"), approver="boss")
    authz.users.discard("author")  # requester loses ``use`` between approval and writeback

    with pytest.raises(PermissionDenied):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0
    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.approved


@pytest.mark.asyncio
async def test_writeback_fails_when_approver_write_revoked() -> None:
    # The approver who granted the decision losing ``write`` after approval fails the writeback
    # closed — a revoked approver cannot retroactively authorize a push.
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    authz = _RoleAuthorizer(_target(), writers={"boss"}, users={"author", "boss"})
    coord = _coordinator(artifacts, writeback, _target(), authorizer=authz)

    handle = await _drive_to_approved(coord, _request(actor="author"), approver="boss")
    authz.writers.discard("boss")  # approver loses ``write`` between approval and writeback

    with pytest.raises(PermissionDenied):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0
    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.approved


@pytest.mark.asyncio
async def test_writeback_fails_on_missing_approval_record() -> None:
    # The durable approval is re-read at job time: if the granted record is gone (erased/purged),
    # the writeback fails closed with a PatchApprovalError and never authorizes or pushes.
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    factory = _ScopeSpyFactory()
    authz = _RoleAuthorizer(_target(), writers={"boss"}, users={"author", "boss"})
    coord = _coordinator(
        artifacts, writeback, _target(), authorizer=authz, approval_factory=factory
    )

    handle = await _drive_to_approved(coord, _request(actor="author"), approver="boss")
    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.approval_id
    del factory.shared._rows[proposal.approval_id]  # the granted approval disappears

    with pytest.raises(PatchApprovalError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0
    assert authz.writebacks == []  # authorization was never reached


@pytest.mark.asyncio
async def test_writeback_fails_on_tampered_approval_binding() -> None:
    # A durable approval whose immutable binding no longer matches the proposal (e.g. a rebound
    # action_hash) is rejected by the fenced re-read: the writeback fails closed, no push.
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    factory = _ScopeSpyFactory()
    authz = _RoleAuthorizer(_target(), writers={"boss"}, users={"author", "boss"})
    coord = _coordinator(
        artifacts, writeback, _target(), authorizer=authz, approval_factory=factory
    )

    handle = await _drive_to_approved(coord, _request(actor="author"), approver="boss")
    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.approval_id
    factory.shared._rows[proposal.approval_id].action_hash = "tampered"  # binding no longer matches

    with pytest.raises(PatchApprovalError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0
    assert authz.writebacks == []


@pytest.mark.asyncio
async def test_writeback_fails_when_approval_not_granted() -> None:
    # Defense in depth: an ``approved`` proposal whose durable approval record is not ``granted``
    # (e.g. tampered to ``denied``) fails the writeback closed rather than pushing on a non-grant.
    artifacts = _MemArtifacts()
    writeback = _FakeWriteback()
    factory = _ScopeSpyFactory()
    authz = _RoleAuthorizer(_target(), writers={"boss"}, users={"author", "boss"})
    coord = _coordinator(
        artifacts, writeback, _target(), authorizer=authz, approval_factory=factory
    )

    handle = await _drive_to_approved(coord, _request(actor="author"), approver="boss")
    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.approval_id
    factory.shared._rows[proposal.approval_id].status = "denied"  # not a grant

    with pytest.raises(PatchApprovalError):
        await coord.execute_writeback("o", handle.proposal_id, worker_id="w1")
    assert writeback.calls == 0
    assert authz.writebacks == []


# --- P3b-0: atomic generation finalize is all-or-nothing (run + proposal + pointer) --------


class _PointerFailureOutbox(InMemoryPatchProposalOutbox):
    """In-memory outbox whose pointer *mirror* op fails, to prove finalize rolls everything back.

    The ``generating`` pointer is still recorded normally at create (``record_in_connection``);
    only the finalize-time mirror op fails, landing AFTER the run has been terminalized and the
    proposal transitioned inside the same in-memory transaction — so a raised failure must restore
    the run, the proposal, AND the pointer (never a completed/failed run behind a
    still-``generating`` proposal, and never a half-applied pointer)."""

    def __init__(self, *, fail_hint: bool = False, fail_delete: bool = False) -> None:
        super().__init__()
        self.fail_hint = fail_hint
        self.fail_delete = fail_delete

    async def set_status_hint_in_connection(
        self, conn: Any, proposal_id: str, status: Any, *, now: Any = None
    ) -> None:  # type: ignore[override]
        if self.fail_hint:
            raise RuntimeError("pointer hint update failed")
        await super().set_status_hint_in_connection(conn, proposal_id, status, now=now)

    async def delete_in_connection(self, conn: Any, proposal_id: str) -> None:  # type: ignore[override]
        if self.fail_delete:
            raise RuntimeError("pointer delete failed")
        await super().delete_in_connection(conn, proposal_id)


@pytest.mark.asyncio
async def test_finalize_ready_rolls_back_run_and_proposal_when_pointer_fails() -> None:
    # Success finalize composes: run terminalize(completed) + proposal generating->ready + pointer
    # hint ready, atomically. If the pointer mirror fails, the WHOLE unit rolls back — the run is
    # NOT left completed behind a still-generating proposal, and no cost is mirrored.
    artifacts = _MemArtifacts()
    outbox = _PointerFailureOutbox(fail_hint=True)
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), outbox=outbox)
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(RuntimeError, match="pointer hint update failed"):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")

    # Proposal fully rolled back: still generating, version unchanged, no cost mirrored.
    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.generating
    assert proposal.version == 1 and proposal.cost_usd == pytest.approx(0.0)
    # Run rolled back: still running (NOT terminalized completed), no charge.
    run = await _run_get(coord, handle.run_id)
    assert run is not None and run.status is RunStatus.running
    assert run.cost_usd == pytest.approx(0.0)
    # Pointer rolled back: still the generating dispatch intent.
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating


@pytest.mark.asyncio
async def test_finalize_failed_rolls_back_run_and_proposal_when_pointer_fails() -> None:
    # The permanent-failure finalize composes: run terminalize(failed) + proposal generating->failed
    # + pointer delete, atomically. If the pointer delete fails, the whole unit rolls back — the run
    # is NOT left failed behind a still-generating proposal, and the pointer survives.
    artifacts = _MemArtifacts()
    outbox = _PointerFailureOutbox(fail_delete=True)
    gen = _ScriptedGeneration(
        artifacts, [PatchProviderError("permanent failure", usage=Usage(cost_usd=0.05))]
    )
    coord = _coordinator(artifacts, _FakeWriteback(), _target(), generation=gen, outbox=outbox)
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(RuntimeError, match="pointer delete failed"):
        await coord.execute_generation("o", handle.run_id, req, worker_id="w1")

    proposal = await coord.store.get("o", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.generating
    assert proposal.version == 1 and proposal.cost_usd == pytest.approx(0.0)
    run = await _run_get(coord, handle.run_id)
    assert run is not None and run.status is RunStatus.running
    assert run.cost_usd == pytest.approx(0.0)
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating
