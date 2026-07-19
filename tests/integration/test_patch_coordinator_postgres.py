"""Live-Postgres verification of the PatchCoordinator wired to the durable P1 primitives (P2).

Drives the coordinator against the real ``PostgresPatchProposalStore`` + Postgres outbox + per-scope
``PostgresRunStore`` (via the ``run_store_factory``) + per-scope ``PostgresApprovalStore`` (via the
``approval_factory``) with no git/provider/network, proving the pointer lifecycle end-to-end on a
live database:

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

import asyncio
import hashlib
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import PostgresApprovalStore
from keel_core.coding.models import ArtifactRecord, ArtifactRetention, CodingRunId, ProjectId
from keel_core.patch.bundle import PatchBundleWriter
from keel_core.patch.coordinator import DEFAULT_PATCH_AGENT_ID, PatchCoordinator, ProjectBinding
from keel_core.patch.errors import PatchLeaseLost
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
from keel_core.runs import PostgresRunStore, RunStatus
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
    """Per-scope **Postgres** run store factory (memoized) for the coordinator's run substrate.

    P3b-0's atomic generation finalize composes the run terminalize/release with the proposal
    transition + pointer mirror inside ONE Postgres transaction, so the run substrate MUST be the
    same backend as the proposal store (``PostgresPatchProposalStore`` rejects a mixed in-memory run
    store). Each canonical per-Agent scope gets its own ``PostgresRunStore(engine, scope)``."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._stores: dict[str, PostgresRunStore] = {}

    def __call__(self, scope_id: str) -> PostgresRunStore:
        store = self._stores.get(scope_id)
        if store is None:
            store = PostgresRunStore(self._engine, scope_id)
            self._stores[scope_id] = store
        return store


def _coordinator(
    engine: AsyncEngine,
    script: list[Any],
    *,
    run_store_factory: Any | None = None,
    outbox: Any | None = None,
    make_generation: Callable[[_MemArtifacts], Any] | None = None,
    lease_seconds: int | None = None,
    run_renew_interval_seconds: float | None = None,
) -> tuple[PatchCoordinator, _ScopeSpyFactory]:
    factory = _ScopeSpyFactory(engine)
    artifacts = _MemArtifacts()
    generation = (
        make_generation(artifacts)
        if make_generation is not None
        else _Generation(script, artifacts)
    )
    extra: dict[str, Any] = {}
    if lease_seconds is not None:
        extra["lease_seconds"] = lease_seconds
    if run_renew_interval_seconds is not None:
        extra["run_renew_interval_seconds"] = run_renew_interval_seconds
    coord = PatchCoordinator(
        store=PostgresPatchProposalStore(engine),
        outbox=outbox or PostgresPatchProposalOutbox(engine),
        run_store_factory=run_store_factory or _RunFactory(engine),
        authorizer=_Authorizer(),
        generation=generation,  # type: ignore[arg-type]
        approval_factory=factory,
        writeback=_Writeback(),  # type: ignore[arg-type]
        artifacts=artifacts,  # type: ignore[arg-type]
        **extra,
    )
    return coord, factory


async def _steal_run_lease(
    engine: AsyncEngine, scope_id: str, run_id: str, *, new_token: str = "stolen-token"
) -> None:
    """Simulate another worker reclaiming the run: bump its ``lease_token`` under the scope GUC.

    A fenced UPDATE (scoped by RLS, keeping the run ``running``) rotates the lease token so the
    original lease held by the coordinator no longer matches — the keeper's next renew and any
    finalize terminalize/release fence closed (0 rows -> ``RunLeaseLostError``)."""
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"), {"scope": scope_id}
        )
        result = await conn.execute(
            text(
                "UPDATE runs SET lease_token = :tok, worker_id = 'thief', version = version + 1 "
                "WHERE scope_id = :scope AND id = :id AND status = 'running'"
            ),
            {"tok": new_token, "scope": scope_id, "id": run_id},
        )
        if result.rowcount != 1:
            raise AssertionError(
                f"expected to steal exactly one running run, got {result.rowcount}"
            )


async def _wait_for_run_status(
    run_store: PostgresRunStore, run_id: str, status: RunStatus, *, timeout_seconds: float = 10.0
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        record = await run_store.get(run_id)
        if record is not None and record.status is status:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"run {run_id} did not reach {status} within {timeout_seconds}s")


class _BlockingGeneration:
    """Generation double that *blocks* (polling ``interrupt``) so the keeper has time to renew.

    Mimics a slow author: it sleeps in small steps for ``block_seconds`` while honouring the
    combined interrupt (external cancel folded with a lost run lease). If interrupted it raises
    :class:`PatchLeaseLost` exactly like a real author aborting mid-flight; otherwise it persists a
    real bundle and returns a successful outcome."""

    def __init__(self, artifacts: _MemArtifacts, usage: Usage, *, block_seconds: float) -> None:
        self._artifacts = artifacts
        self._usage = usage
        self._block_seconds = block_seconds
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
        deadline = time.monotonic() + self._block_seconds
        while time.monotonic() < deadline:
            if interrupt is not None and interrupt():
                raise PatchLeaseLost("patch author interrupted mid-generation")
            await asyncio.sleep(0.05)
        return _outcome(
            self._artifacts,
            proposal_id,
            run_id,
            coding_run_id,
            project_handle,
            request,
            self._usage,
        )


class _LeaseStealingGeneration:
    """Generation double that steals its own run lease, then returns a *successful* outcome.

    Proves the atomic finalize fences on the run lease: the completed generation's terminalize runs
    under a token that a concurrent worker has already rotated, so ``finalize_generation_ready``
    fails closed and the whole transaction rolls back (nothing terminalized/transitioned)."""

    def __init__(
        self, engine: AsyncEngine, scope_id: str, artifacts: _MemArtifacts, usage: Usage
    ) -> None:
        self._engine = engine
        self._scope_id = scope_id
        self._artifacts = artifacts
        self._usage = usage
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
        await _steal_run_lease(self._engine, self._scope_id, run_id)
        return _outcome(
            self._artifacts,
            proposal_id,
            run_id,
            coding_run_id,
            project_handle,
            request,
            self._usage,
        )


class _BoundaryInjection(RuntimeError):
    """Injected at a former cross-await boundary to force the atomic finalize to roll back."""


class _InjectingOutbox(PostgresPatchProposalOutbox):
    """Postgres outbox whose in-connection pointer mirror raises once, at the finalize boundary.

    The atomic finalize terminalizes/releases the run and transitions the proposal *before*
    mirroring the pointer on the same connection. Raising inside the mirror proves the whole
    transaction rolls back — the run is never left terminal behind a still-``generating`` proposal,
    and the pointer is untouched. ``fail_on`` picks which mirror edge (``ready`` hint or terminal
    ``delete``) trips."""

    def __init__(self, engine: AsyncEngine, *, fail_on: str) -> None:
        super().__init__(engine)
        self._fail_on = fail_on
        self.fired = False

    async def set_status_hint_in_connection(  # type: ignore[override]
        self, conn, proposal_id, status_hint, *, now=None
    ):
        if self._fail_on == "hint" and not self.fired:
            self.fired = True
            raise _BoundaryInjection("injected outbox hint failure at finalize boundary")
        await super().set_status_hint_in_connection(conn, proposal_id, status_hint, now=now)

    async def delete_in_connection(self, conn, proposal_id):  # type: ignore[override]
        if self._fail_on == "delete" and not self.fired:
            self.fired = True
            raise _BoundaryInjection("injected outbox delete failure at finalize boundary")
        await super().delete_in_connection(conn, proposal_id)


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
        run_store_factory=_RunFactory(migrated_db),
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


# --- P3b-0: atomic generation finalize + run-lease keeper over live Postgres --------------------


async def test_finalize_ready_rolls_back_on_injected_pointer_failure(
    migrated_db: AsyncEngine,
) -> None:
    # The success finalize composes run terminalize + proposal ready + pointer hint in ONE txn.
    # Injecting a failure at the (former) run->pointer cross-await boundary must roll back the whole
    # transaction: the run is never left terminal behind a still-``generating`` proposal.
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    outbox = _InjectingOutbox(migrated_db, fail_on="hint")
    coord, _ = _coordinator(
        migrated_db, [Usage(prompt_tokens=1, completion_tokens=1, cost_usd=0.02)], outbox=outbox
    )
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(_BoundaryInjection):
        await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")

    assert outbox.fired
    proposal = await coord.store.get("org-a", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.generating
    assert not proposal.approval_id
    assert proposal.cost_usd == pytest.approx(0.0)
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating
    run = await coord.run_store_factory(_SCOPE).get(handle.run_id)
    assert run is not None and run.status is RunStatus.running
    assert run.cost_usd == pytest.approx(0.0)


async def test_finalize_failed_rolls_back_on_injected_pointer_failure(
    migrated_db: AsyncEngine,
) -> None:
    # The permanent-failure finalize composes run failed + proposal failed + pointer delete in ONE
    # txn. Injecting a failure at the pointer delete boundary must roll everything back: the run is
    # not marked failed and the proposal stays ``generating`` with its pointer intact for a retry.
    from keel_core.patch.errors import PatchProviderError

    await _seed_org_project(migrated_db, "org-a", "proj-a")
    outbox = _InjectingOutbox(migrated_db, fail_on="delete")
    coord, _ = _coordinator(
        migrated_db,
        [PatchProviderError("permanent boom")],
        outbox=outbox,
    )
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(_BoundaryInjection):
        await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")

    assert outbox.fired
    proposal = await coord.store.get("org-a", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.generating
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating
    run = await coord.run_store_factory(_SCOPE).get(handle.run_id)
    assert run is not None and run.status is RunStatus.running


async def test_finalize_ready_fences_on_stolen_lease(migrated_db: AsyncEngine) -> None:
    # A completed generation whose run lease was reclaimed mid-flight must NOT terminalize: the
    # atomic finalize fences on the run ``lease_token`` and fails closed, rolling back entirely.
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, _ = _coordinator(
        migrated_db,
        [],
        make_generation=lambda arts: _LeaseStealingGeneration(
            migrated_db, _SCOPE, arts, Usage(cost_usd=0.02)
        ),
    )
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(PatchLeaseLost):
        await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")

    # Nothing was written under the stale fence: proposal still generating, pointer intact, no
    # approval, and the run row is owned by the thief token (never terminalized).
    proposal = await coord.store.get("org-a", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.generating
    assert not proposal.approval_id
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating
    run = await coord.run_store_factory(_SCOPE).get(handle.run_id)
    assert run is not None and run.status is RunStatus.running and run.lease_token == "stolen-token"


async def test_execute_generation_idempotent_after_success(migrated_db: AsyncEngine) -> None:
    # A crash *after* the success transaction (proposal ``approval_pending``, run completed) must
    # re-drive as a pure no-op: the fast-path returns the terminal proposal without re-invoking the
    # generation double, re-charging cost, or churning the run/pointer.
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, _ = _coordinator(migrated_db, [Usage(cost_usd=0.02)])
    req = _request()
    handle = await coord.request_generation(req)

    p1 = await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    assert p1.status is PatchStatus.approval_pending and p1.approval_id
    gen = coord.generation
    assert isinstance(gen, _Generation)
    calls_before = gen.calls

    p2 = await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    assert p2.status is PatchStatus.approval_pending
    assert p2.approval_id == p1.approval_id
    assert p2.cost_usd == pytest.approx(p1.cost_usd)
    assert gen.calls == calls_before  # generation not re-invoked
    run = await coord.run_store_factory(_SCOPE).get(handle.run_id)
    assert run is not None and run.status is RunStatus.completed
    assert run.cost_usd == pytest.approx(0.02)
    assert await coord.outbox.get(handle.proposal_id) is None


async def test_run_keeper_renews_lease_over_postgres(migrated_db: AsyncEngine) -> None:
    # With a short lease and a slow (blocking) author, the in-flight keeper renews the run lease in
    # real Postgres: its ``lease_expires_at`` advances while generation is still running, and the
    # generation ultimately succeeds to ``approval_pending``.
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, _ = _coordinator(
        migrated_db,
        [],
        make_generation=lambda arts: _BlockingGeneration(
            arts, Usage(cost_usd=0.02), block_seconds=2.4
        ),
        lease_seconds=2,
    )
    req = _request()
    handle = await coord.request_generation(req)
    run_store = coord.run_store_factory(_SCOPE)

    gen_task = asyncio.create_task(
        coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    )
    try:
        await _wait_for_run_status(run_store, handle.run_id, RunStatus.running)
        first = await run_store.get(handle.run_id)
        assert first is not None and first.lease_expires_at is not None
        await asyncio.sleep(1.4)  # let the keeper renew (interval derived < 2s lease)
        mid = await run_store.get(handle.run_id)
        assert mid is not None and mid.lease_expires_at is not None
        assert mid.lease_expires_at > first.lease_expires_at
        proposal = await asyncio.wait_for(gen_task, timeout=10)
    finally:
        if not gen_task.done():
            gen_task.cancel()
    assert proposal.status is PatchStatus.approval_pending and proposal.approval_id
    assert proposal.cost_usd == pytest.approx(0.02)
    run = await run_store.get(handle.run_id)
    assert run is not None and run.status is RunStatus.completed


async def test_run_keeper_aborts_when_lease_reclaimed_midflight(migrated_db: AsyncEngine) -> None:
    # Reclaiming the run lease mid-generation must abort *before* any terminal write: the keeper's
    # next renew fails closed, folds ``lost`` into the author interrupt, and the abort surfaces as
    # PatchLeaseLost with the proposal/pointer/approval untouched (the run belongs to the thief).
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, _ = _coordinator(
        migrated_db,
        [],
        make_generation=lambda arts: _BlockingGeneration(
            arts, Usage(cost_usd=0.02), block_seconds=8.0
        ),
        lease_seconds=2,
    )
    req = _request()
    handle = await coord.request_generation(req)
    run_store = coord.run_store_factory(_SCOPE)

    gen_task = asyncio.create_task(
        coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    )
    try:
        await _wait_for_run_status(run_store, handle.run_id, RunStatus.running)
        await _steal_run_lease(migrated_db, _SCOPE, handle.run_id)
        with pytest.raises(PatchLeaseLost):
            await asyncio.wait_for(gen_task, timeout=15)
    finally:
        if not gen_task.done():
            gen_task.cancel()

    proposal = await coord.store.get("org-a", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.generating
    assert not proposal.approval_id
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating
    run = await run_store.get(handle.run_id)
    assert run is not None and run.lease_token == "stolen-token"


async def test_run_keeper_external_cancel_aborts_generation(migrated_db: AsyncEngine) -> None:
    # An external cancel (a job cancellation event) folds into the author interrupt: the author
    # aborts with PatchLeaseLost before any terminal write, leaving the proposal generating.
    await _seed_org_project(migrated_db, "org-a", "proj-a")
    coord, _ = _coordinator(
        migrated_db,
        [],
        make_generation=lambda arts: _BlockingGeneration(
            arts, Usage(cost_usd=0.02), block_seconds=8.0
        ),
        lease_seconds=30,
    )
    req = _request()
    handle = await coord.request_generation(req)

    with pytest.raises(PatchLeaseLost):
        await coord.execute_generation(
            "org-a", handle.run_id, req, worker_id="w1", interrupt=lambda: True
        )

    proposal = await coord.store.get("org-a", handle.proposal_id)
    assert proposal is not None and proposal.status is PatchStatus.generating
    assert not proposal.approval_id
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating
    run = await coord.run_store_factory(_SCOPE).get(handle.run_id)
    assert run is not None and run.status is RunStatus.running
