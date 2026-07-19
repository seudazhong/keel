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

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import PostgresApprovalStore
from keel_core.patch.coordinator import PatchCoordinator, ProjectBinding
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
from keel_core.runs import InMemoryRunStore, RunStatus

pytestmark = pytest.mark.integration

_SCOPE = "agent:org-a/patch"
_BASE_SHA = "b" * 40
_HEAD_SHA = "c" * 40
_BUNDLE_SHA = "e" * 64
_DIFF_SHA = "f" * 64
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
    proposal_id: str, run_id: str, request: PatchProposalRequest, usage: Usage
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
        diff_bytes=64,
        files=files,
        commits=(ProposedCommit(sha=_HEAD_SHA, message="m", tree_sha="d" * 40),),
        tests=(),
        test_status=TestStatus.skipped,
        created_at=datetime.now(UTC),
    )
    return GenerationOutcome(
        proposal_id=proposal_id,
        base_sha=_BASE_SHA,
        head_sha=_HEAD_SHA,
        bundle_sha256=_BUNDLE_SHA,
        diff_sha256=_DIFF_SHA,
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
    :class:`Usage` describing a successful attempt's cost. The emitted bundle hashes are constant so
    the approval binding is stable across a provider-unavailable retry."""

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)
        self.calls = 0

    async def generate(
        self, request, *, proposal_id, run_id, coding_run_id, project_handle, now=None
    ):  # type: ignore[no-untyped-def]
        self.calls += 1
        behavior = self._script.pop(0)
        if isinstance(behavior, BaseException):
            raise behavior
        return _outcome(proposal_id, run_id, request, behavior)


class _Authorizer:
    def __init__(self) -> None:
        self.scopes: list[str] = []

    async def authorize_generation(self, org_id, actor, project_id, *, agent_id, run_id):  # type: ignore[no-untyped-def]
        return ProjectBinding(
            project_handle="proj",
            scope_id=_SCOPE,
            coding_run_id=run_id,
            target=WritebackTarget(
                full_name="org-a/repo",
                installation_id=7,
                clone_url="https://github.test/org-a/repo.git",
                default_branch="main",
            ),
        )

    async def authorize_read(self, org_id, actor, project_id):  # type: ignore[no-untyped-def]
        return None

    async def authorize_writeback(self, org_id, actor, project_id, *, agent_id, run_id):  # type: ignore[no-untyped-def]
        return await self.authorize_generation(
            org_id, actor, project_id, agent_id=agent_id, run_id=run_id
        )

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


class _Artifacts:
    """Unused stub — the coordinator only touches the artifact store during writeback."""

    def put(self, *a: Any, **k: Any) -> Any: ...  # pragma: no cover - never called
    def read(self, *a: Any, **k: Any) -> Any: ...  # pragma: no cover - never called
    def retain(self, *a: Any, **k: Any) -> Any: ...  # pragma: no cover - never called
    def delete(self, *a: Any, **k: Any) -> Any: ...  # pragma: no cover - never called
    def reap(self, *a: Any, **k: Any) -> Any: ...  # pragma: no cover - never called


class _ScopeSpyFactory:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self.scopes: list[str] = []

    def __call__(self, scope_id: str) -> PostgresApprovalStore:
        self.scopes.append(scope_id)
        return PostgresApprovalStore(self._engine, scope_id)


def _coordinator(
    engine: AsyncEngine, script: list[Any]
) -> tuple[PatchCoordinator, _ScopeSpyFactory]:
    factory = _ScopeSpyFactory(engine)
    coord = PatchCoordinator(
        store=PostgresPatchProposalStore(engine),
        outbox=PostgresPatchProposalOutbox(engine),
        runs=InMemoryRunStore(),
        authorizer=_Authorizer(),
        generation=_Generation(script),  # type: ignore[arg-type]
        approval_factory=factory,
        writeback=_Writeback(),  # type: ignore[arg-type]
        artifacts=_Artifacts(),  # type: ignore[arg-type]
    )
    return coord, factory


def _request() -> PatchProposalRequest:
    return PatchProposalRequest(
        org_id="org-a",
        project_id="proj-a",
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
    run = await coord.runs.get(handle.run_id)
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
    r1 = await coord.runs.get(handle.run_id)
    assert r1 is not None and r1.status is RunStatus.queued and r1.cost_usd == pytest.approx(0.01)
    entry = await coord.outbox.get(handle.proposal_id)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating

    # 2) Retry succeeds: cost accrues cumulatively (0.01 + 0.02) with no double-count.
    p2 = await coord.execute_generation("org-a", handle.run_id, req, worker_id="w1")
    assert p2.status is PatchStatus.approval_pending
    assert p2.cost_usd == pytest.approx(0.03)
    r2 = await coord.runs.get(handle.run_id)
    assert (
        r2 is not None and r2.status is RunStatus.completed and r2.cost_usd == pytest.approx(0.03)
    )
    assert await coord.outbox.get(handle.proposal_id) is None
