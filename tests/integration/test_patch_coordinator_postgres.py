"""Live-Postgres verification of the PatchCoordinator wired to the durable P1 primitives (P2).

Drives the coordinator against the real ``PostgresPatchProposalStore`` + Postgres outbox + per-scope
``PostgresApprovalStore`` (via the ``approval_factory``) with an in-memory run store and no
git/provider/network, proving the pointer lifecycle end-to-end on a live database:

* ``request_generation`` writes the ``generating`` pointer atomically with the proposal;
* ``execute_generation`` auto-advances the transient ``ready`` to ``approval_pending`` in a single
  transaction — the durable approval is pending and the dispatch pointer is deleted;
* a granted ``decide`` re-creates the ``approved`` pointer under the proposal's own canonical scope;
* a terminal ``cancel``/``deny`` retires the pointer; and
* a transient provider outage releases the run lease and charges the partial usage coherently onto
  both the run and the proposal (never lost), and a later success accrues the cost cumulatively with
  no double-count — all against the real Postgres proposal row.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import PostgresApprovalStore
from keel_core.coding.models import ArtifactRecord, ArtifactRetention, CodingRunId, ProjectId
from keel_core.patch.bundle import PatchBundleWriter
from keel_core.patch.coordinator import DEFAULT_PATCH_AGENT_ID, PatchCoordinator, ProjectBinding
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
from keel_core.patch.outbox import PatchOutboxStatus, PostgresPatchProposalOutbox
from keel_core.patch.store import PostgresPatchProposalStore
from keel_core.patch.writeback import WritebackResult, WritebackTarget
from keel_core.protocols import Usage
from keel_core.runs import InMemoryRunStore, PostgresRunStore, RunStatus
from keel_core.scoping import derive_agent_scope

pytestmark = pytest.mark.integration

_SCOPE = "agent:org-a/patch"
_BASE_SHA = "b" * 40
_HEAD_SHA = "c" * 40
# A constant diff whose content hash is the bundle's ``diff_sha256``; the manifest uses a fixed
# ``created_at`` so a provider-unavailable retry reproduces byte-identical bundle bytes (a stable
# ``bundle_sha256``), keeping the human approval binding stable across attempts.
_DIFF = b"--- a/app.py\n+++ b/app.py\n@@ -1 +1,2 @@\n def add(a,b):\n+    pass\n"
_DIFF_SHA = hashlib.sha256(_DIFF).hexdigest()
_CREATED_AT = datetime(2024, 1, 1, tzinfo=UTC)
_CHANGED_DIGEST = changed_path_digest(
    (ChangedFile(path="app.py", change_kind=ChangeKind.modified, blob_sha="a" * 40, size_bytes=20),)
)


async def _seed_org_project(engine: AsyncEngine, org: str, project: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text(
                "INSERT INTO organizations(id,slug,display_name,status) "
                "VALUES(:id,:id,:id,'active') ON CONFLICT DO NOTHING"
            ),
            {"id": org},
        )
        await conn.execute(
            text(
                "INSERT INTO projects(id,org_id,slug,display_name) "
                "VALUES(:pid,:org,:pid,:pid) ON CONFLICT DO NOTHING"
            ),
            {"pid": project, "org": org},
        )


def _outcome(
    artifacts: _MemArtifacts,
    proposal_id: str,
    run_id: str,
    coding_run_id: str,
    project_handle: str,
    request: PatchProposalRequest,
    usage: Usage,
) -> GenerationOutcome:
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
        base_sha=_BASE_SHA,
        head_sha=_HEAD_SHA,
        diff_sha256=_DIFF_SHA,
        diff_bytes=len(_DIFF),
        files=files,
        commits=(ProposedCommit(sha=_HEAD_SHA, message="m", tree_sha="d" * 40),),
        tests=(),
        test_status=TestStatus.skipped,
        created_at=_CREATED_AT,
    )
    # Persist the real content-addressed bundle so writeback's ``PatchBundleReader`` can re-read and
    # hash-verify the manifest; re-storing identical bytes on a retry is idempotent (same hash).
    stored = PatchBundleWriter(artifacts).store(  # type: ignore[arg-type]
        manifest,
        diff=_DIFF,
        project_handle=project_handle,
        coding_run_id=coding_run_id,
        now=_CREATED_AT,
    )
    return GenerationOutcome(
        proposal_id=proposal_id,
        base_sha=_BASE_SHA,
        head_sha=_HEAD_SHA,
        bundle_sha256=stored.bundle_sha256,
        diff_sha256=stored.diff_sha256,
        changed_path_digest=_CHANGED_DIGEST,
        changed_files=1,
        test_status=TestStatus.skipped,
        manifest=manifest,
        pin_ref="refs/keel-patch/x",
        usage=usage,
    )


class _Generation:
    """Deterministic generation double: no git/provider, driven by a scripted list of behaviors.

    Each entry is either a ``BaseException`` to raise (a transient/permanent patch error) or a
    :class:`Usage` describing a successful attempt's cost. A successful attempt persists a real,
    content-addressed bundle into the shared artifact store; the emitted bundle hashes are constant
    (fixed manifest ``created_at``) so the approval binding is stable across a provider-unavailable
    retry."""

    def __init__(self, script: list[Any], artifacts: _MemArtifacts) -> None:
        self._script = list(script)
        self._artifacts = artifacts
        self.calls = 0

    async def generate(
        self, request, *, proposal_id, run_id, coding_run_id, project_handle, now=None
    ):  # type: ignore[no-untyped-def]
        self.calls += 1
        behavior = self._script.pop(0)
        if isinstance(behavior, BaseException):
            raise behavior
        return _outcome(
            self._artifacts,
            proposal_id,
            run_id,
            coding_run_id,
            project_handle,
            request,
            behavior,
        )


class _Authorizer:
    def __init__(self) -> None:
        self.scopes: list[str] = []
        self.writebacks: list[tuple[str, str]] = []

    def _binding(self, org_id: str, agent_id: str | None, run_id: str) -> ProjectBinding:
        return ProjectBinding(
            project_handle="proj",
            scope_id=derive_agent_scope(org_id, agent_id or DEFAULT_PATCH_AGENT_ID),
            coding_run_id=run_id,
            target=WritebackTarget(
                full_name="org-a/repo",
                installation_id=7,
                clone_url="https://github.test/org-a/repo.git",
                default_branch="main",
            ),
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
        return None


class _Writeback:
    async def write(self, proposal, *, project_handle, target, manifest):  # type: ignore[no-untyped-def]
        return WritebackResult(
            remote_branch=proposal.remote_branch,
            head_sha=proposal.head_sha,
            pr_number=1,
            pr_url="https://github.test/pr/1",
            pr_node_id="N",
            reused_branch=False,
            reused_pr=False,
        )


class _MemArtifacts:
    """A tiny in-memory ArtifactStore keyed by content hash (enough for the bundle round-trip)."""

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


class _ScopeSpyFactory:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self.scopes: list[str] = []

    def __call__(self, scope_id: str) -> PostgresApprovalStore:
        self.scopes.append(scope_id)
        return PostgresApprovalStore(self._engine, scope_id)


class _RunFactory:
    """Per-scope in-memory run store factory (memoized) for the coordinator's run substrate."""

    def __init__(self) -> None:
        self._stores: dict[str, InMemoryRunStore] = {}

    def __call__(self, scope_id: str) -> InMemoryRunStore:
        store = self._stores.get(scope_id)
        if store is None:
            store = InMemoryRunStore()
            self._stores[scope_id] = store
        return store


def _coordinator(
    engine: AsyncEngine,
    script: list[Any],
    *,
    run_store_factory: Any | None = None,
) -> tuple[PatchCoordinator, _ScopeSpyFactory]:
    factory = _ScopeSpyFactory(engine)
    artifacts = _MemArtifacts()
    coord = PatchCoordinator(
        store=PostgresPatchProposalStore(engine),
        outbox=PostgresPatchProposalOutbox(engine),
        run_store_factory=run_store_factory or _RunFactory(),
        authorizer=_Authorizer(),
        generation=_Generation(script, artifacts),  # type: ignore[arg-type]
        approval_factory=factory,
        writeback=_Writeback(),  # type: ignore[arg-type]
        artifacts=artifacts,  # type: ignore[arg-type]
    )
    return coord, factory


def _request(org: str = "org-a", project: str = "proj-a") -> PatchProposalRequest:
    return PatchProposalRequest(
        org_id=org,
        project_id=project,
        actor="alice",
        task="fix the bug",
        base_ref="main",
        model="m",
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )


async def test_coordinator_generate_reaches_approval_pending_and_deletes_pointer(
    migrated_db: AsyncEngine,
) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, factory = _coordinator(
        migrated_db, [Usage(prompt_tokens=1, completion_tokens=1, cost_usd=0.02)]
    )
    req = _request()

    handle = await coord.request_generation(req)
    assert handle.created and handle.status is PatchStatus.generating
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None
    assert entry.status_hint is PatchOutboxStatus.generating
    assert entry.scope_id == _SCOPE

    proposal = await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    assert proposal.status is PatchStatus.approval_pending and proposal.approval_id
    assert proposal.cost_usd == pytest.approx(0.02)

    # The durable approval is pending and the dispatch pointer was deleted in the single txn.
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    record = await approvals.get(proposal.approval_id)
    assert record is not None and record.status == "pending"
    assert await coord.outbox.get(handle.proposal_id) is None
    run = await coord.run_store_factory(_SCOPE).get(handle.run_id)
    assert run is not None and run.status is RunStatus.completed
    assert run.cost_usd == pytest.approx(0.02)
    # Every approval operation resolved the proposal's exact canonical scope.
    assert set(factory.scopes) == {_SCOPE}


async def test_coordinator_decide_approve_recreates_approved_pointer(
    migrated_db: AsyncEngine,
) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, factory = _coordinator(migrated_db, [Usage(cost_usd=0.01)])
    req = _request()
    handle = await coord.request_generation(req)
    await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")

    decision = await coord.decide("org-a", handle.proposal_id, approve=True, actor="alice")
    assert decision.applied and decision.status is PatchStatus.approved
    assert decision.queue_writeback

    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None
    assert entry.status_hint is PatchOutboxStatus.approved
    assert entry.scope_id == _SCOPE
    assert set(factory.scopes) == {_SCOPE}


async def test_coordinator_writeback_reverifies_postgres_approval_resolver(
    migrated_db: AsyncEngine,
) -> None:
    # End-to-end over live Postgres: writeback re-reads the durable *granted* approval from the
    # proposal's canonical scope, extracts its resolver, and reauthorizes (the requester keeps
    # ``use``, the approver keeps ``write``) before pushing — proving the fenced re-read works
    # against a real PostgresApprovalStore, with requester != approver.
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, _ = _coordinator(migrated_db, [Usage(cost_usd=0.01)])
    req = _request()  # requester (proposal actor) is "alice"
    handle = await coord.request_generation(req)
    await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    decision = await coord.decide("org-a", handle.proposal_id, approve=True, actor="carol")
    assert decision.applied and decision.status is PatchStatus.approved

    final = await coord.execute_writeback("org-a", handle.proposal_id, worker_id="w1")
    assert final.status is PatchStatus.draft_pr_created
    # The requester (proposal actor) and the approver (durable approval resolver) are reauthorized
    # exactly — the resolver came from the Postgres approval row, not the caller.
    authorizer = coord.authorizer
    assert isinstance(authorizer, _Authorizer)
    assert authorizer.writebacks == [("alice", "carol")]
    assert await coord.outbox.get(handle.proposal_id) is None  # pointer retired at draft_pr_created


async def test_coordinator_deny_and_cancel_retire_pointer(migrated_db: AsyncEngine) -> None:
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, _ = _coordinator(migrated_db, [Usage(cost_usd=0.0), Usage(cost_usd=0.0)])

    # A denied decision is terminal and retires the pointer.
    denied_req = _request()
    denied = await coord.request_generation(denied_req)
    await coord.execute_generation("org-a", denied.run_id, denied_req, worker_id="w1")
    decision = await coord.decide("org-a", denied.proposal_id, approve=False, actor="alice")
    assert decision.status is PatchStatus.denied
    assert await coord.outbox.get(denied.proposal_id) is None

    # A cancel on a fresh generating proposal retires its pointer too.
    cancel_req = _request()
    cancelled_handle = await coord.request_generation(cancel_req)
    assert await coord.outbox.get(cancelled_handle.proposal_id) is not None
    cancelled = await coord.cancel("org-a", cancelled_handle.proposal_id, actor="alice")
    assert cancelled.status is PatchStatus.cancelled
    assert await coord.outbox.get(cancelled_handle.proposal_id) is None


async def test_coordinator_provider_unavailable_partial_cost_then_retry_cumulative(
    migrated_db: AsyncEngine,
) -> None:
    from keel_core.patch.errors import PatchProviderUnavailable

    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, _ = _coordinator(
        migrated_db,
        [
            PatchProviderUnavailable("upstream 503", usage=Usage(cost_usd=0.01)),
            Usage(prompt_tokens=7, completion_tokens=3, cost_usd=0.02),
        ],
    )
    req = _request()
    handle = await coord.request_generation(req)

    # 1) Transient outage: proposal stays generating (pointer intact) and the partial cost is
    #    durably charged to both the run and the Postgres proposal row.
    with pytest.raises(PatchProviderUnavailable):
        await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    p1 = await coord.store.get("org-a", handle.proposal_id)
    assert p1 is not None and p1.status is PatchStatus.generating
    assert p1.cost_usd == pytest.approx(0.01)
    r1 = await coord.run_store_factory(_SCOPE).get(handle.run_id)
    assert r1 is not None and r1.status is RunStatus.queued and r1.cost_usd == pytest.approx(0.01)
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating

    # 2) Retry succeeds: cost accrues cumulatively (0.01 + 0.02) with no double-count.
    p2 = await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    assert p2.status is PatchStatus.approval_pending
    assert p2.cost_usd == pytest.approx(0.03)
    r2 = await coord.run_store_factory(_SCOPE).get(handle.run_id)
    assert (
        r2 is not None and r2.status is RunStatus.completed and r2.cost_usd == pytest.approx(0.03)
    )
    assert await coord.outbox.get(handle.proposal_id) is None


async def test_run_store_factory_isolates_scope_on_postgres(migrated_db: AsyncEngine) -> None:
    # The per-scope run factory over live Postgres: a run created for one Agent scope is invisible
    # under another, and proposals stay org-isolated — no global run/proposal scan.
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    await _seed_org_project(migrated_db, "org-b", "proj-b")

    def pg_factory(scope_id: str) -> PostgresRunStore:
        return PostgresRunStore(migrated_db, scope_id)

    coord, _ = _coordinator(migrated_db, [], run_store_factory=pg_factory)
    handle_a = await coord.request_generation(_request("org-a", "proj-a"))
    handle_b = await coord.request_generation(_request("org-b", "proj-b"))

    scope_a = derive_agent_scope("org-a", DEFAULT_PATCH_AGENT_ID)
    scope_b = derive_agent_scope("org-b", DEFAULT_PATCH_AGENT_ID)
    assert scope_a != scope_b

    assert await PostgresRunStore(migrated_db, scope_a).get(handle_a.run_id) is not None
    assert await PostgresRunStore(migrated_db, scope_b).get(handle_b.run_id) is not None
    # cross-scope runs are invisible (the get is scope-filtered in SQL)
    assert await PostgresRunStore(migrated_db, scope_a).get(handle_b.run_id) is None
    assert await PostgresRunStore(migrated_db, scope_b).get(handle_a.run_id) is None
    # cross-org proposals are invisible in the Postgres proposal store
    assert await coord.store.get("org-b", handle_a.proposal_id) is None
    assert await coord.store.get("org-a", handle_b.proposal_id) is None


async def test_server_only_coordinator_refuses_execution_on_postgres(
    migrated_db: AsyncEngine,
) -> None:
    # A server coordinator over live Postgres wires request/read/decide/cancel WITHOUT generation
    # or writeback deps. The control plane works; execution entrypoints fail closed with
    # PatchStateError.
    from keel_core.patch.errors import PatchStateError

    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord = PatchCoordinator(
        store=PostgresPatchProposalStore(migrated_db),
        outbox=PostgresPatchProposalOutbox(migrated_db),
        run_store_factory=_RunFactory(),
        authorizer=_Authorizer(),
        approval_factory=_ScopeSpyFactory(migrated_db),
    )
    req = _request()
    handle = await coord.request_generation(req)
    assert handle.created and handle.status is PatchStatus.generating

    with pytest.raises(PatchStateError):
        await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    with pytest.raises(PatchStateError):
        await coord.execute_writeback("org-a", handle.proposal_id, worker_id="w1")

    # Read + terminal cancel still work and retire the pointer without any execution dependency.
    got = await coord.get("org-a", handle.proposal_id, actor="alice")
    assert got.status is PatchStatus.generating
    cancelled = await coord.cancel("org-a", handle.proposal_id, actor="alice")
    assert cancelled.status is PatchStatus.cancelled
    assert await coord.outbox.get(handle.proposal_id) is None
