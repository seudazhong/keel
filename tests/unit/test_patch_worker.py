"""Unit tests for the worker-side controlled-patch wiring (M4 P3b-1).

Covers the three deliverables of :mod:`keel_worker.patch` with in-memory doubles:

* registry / kind-set membership + per-scope :func:`register_patch_jobs`,
* the scope-pinned :func:`build_patch_coordinator` factory (fails closed off-scope),
* :func:`resolve_patch_storage_root` (disabled / ready / unavailable),
* the fenced :class:`PatchOutboxReconciler` across *every* proposal status + the stale-token,
  scope-mismatch, TTL, job absent/active/terminal, ready-heal and duplicate-suppression paths.

The reconciler's collaborators (proposal store, outbox, job store, event store, dispatch outbox)
are the real in-memory implementations so the fencing/idempotency semantics are exercised for real;
only the *coordinator* is stubbed in the ready-heal unit test (the coordinator's own approval
create-or-get correctness is proven end-to-end in ``test_patch_coordinator``/the PG integration).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from keel_core.approvals import PostgresApprovalStore
from keel_core.coding.storage_root import SharedStorageUnavailable
from keel_core.config import Settings
from keel_core.job_dispatch import InMemoryJobDispatchOutbox
from keel_core.jobs import (
    CancelMode,
    InMemoryJobStore,
    JobStatus,
)
from keel_core.patch.coordinator import DEFAULT_PATCH_AGENT_ID
from keel_core.patch.errors import PatchValidationError
from keel_core.patch.jobs import (
    PATCH_GENERATE_KIND,
    PATCH_GENERATE_MAX_ATTEMPTS,
    PATCH_WRITEBACK_KIND,
    PATCH_WRITEBACK_MAX_ATTEMPTS,
    PatchGenerateJobPayload,
    patch_generate_idempotency_key,
    patch_writeback_idempotency_key,
    persist_generate_metadata,
)
from keel_core.patch.models import PatchStatus
from keel_core.patch.outbox import (
    InMemoryPatchProposalOutbox,
    PatchOutboxEntry,
    PatchOutboxStatus,
)
from keel_core.patch.store import InMemoryPatchProposalStore
from keel_core.runs import PostgresRunStore
from keel_core.scoping import derive_agent_scope
from keel_core.state import InMemoryEventStore
from keel_worker.jobs import _ALL_JOB_KINDS, _CROSS_SCOPE_JOB_KINDS, JobRegistry
from keel_worker.patch import (
    PatchOutboxReconciler,
    PatchStorageNotReady,
    PatchWorkerComponents,
    _lease_token,
    _repair_status,
    build_patch_coordinator,
    reconcile_patch_outbox_tick,
    register_patch_jobs,
    resolve_patch_storage_root,
)

_ORG = "o"
_AGENT = DEFAULT_PATCH_AGENT_ID
_SCOPE = derive_agent_scope(_ORG, _AGENT)
_OTHER_SCOPE = derive_agent_scope(_ORG, "agent-other")

_STATUS_CHAIN = (
    PatchStatus.generating,
    PatchStatus.ready,
    PatchStatus.approval_pending,
    PatchStatus.approved,
    PatchStatus.writing,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _settings(**over: Any) -> Settings:
    return Settings(**over)


# --- registry / kind sets ------------------------------------------------------------------------


def test_patch_kinds_are_cross_scope_and_known() -> None:
    for kind in (PATCH_GENERATE_KIND, PATCH_WRITEBACK_KIND):
        assert kind in _ALL_JOB_KINDS
        assert kind in _CROSS_SCOPE_JOB_KINDS


def _components(**over: Any) -> PatchWorkerComponents:
    base: dict[str, Any] = {
        "engine": over.pop("engine", object()),
        "settings": _settings(),
        "store": InMemoryPatchProposalStore(),
        "outbox": InMemoryPatchProposalOutbox(),
        "authorizer": object(),
        "generation": object(),
        "writeback": None,
        "artifacts": object(),
    }
    base.update(over)
    return PatchWorkerComponents(**base)


def test_register_patch_jobs_registers_both_when_writeback_wired() -> None:
    # A GitHub-capable worker (writeback service wired) registers BOTH kinds.
    coordinator = build_patch_coordinator(_components(writeback=object()), _SCOPE)
    registry = JobRegistry()
    register_patch_jobs(registry, coordinator, _settings())

    gen = registry.get(PATCH_GENERATE_KIND)
    wb = registry.get(PATCH_WRITEBACK_KIND)
    assert gen.kind == PATCH_GENERATE_KIND
    assert gen.max_attempts == PATCH_GENERATE_MAX_ATTEMPTS
    assert gen.cancel_mode is CancelMode.cooperative
    assert wb.kind == PATCH_WRITEBACK_KIND
    assert wb.max_attempts == PATCH_WRITEBACK_MAX_ATTEMPTS
    assert wb.cancel_mode is CancelMode.immediate


def test_register_patch_jobs_omits_writeback_when_github_unconfigured() -> None:
    # A heterogeneous fleet: a GitHub-UNCONFIGURED worker (coordinator.writeback is None) registers
    # ONLY patch.generate. patch.writeback is left unregistered so it stays capability-unavailable
    # here — because it is a KNOWN kind (in _ALL_JOB_KINDS), run_job skips it (leaving it queued for
    # a GitHub-capable peer) instead of claiming + permanently failing it on a worker that can never
    # push a remote branch/PR.
    coordinator = build_patch_coordinator(_components(writeback=None), _SCOPE)
    assert coordinator.writeback is None
    registry = JobRegistry()
    register_patch_jobs(registry, coordinator, _settings())

    assert registry.get(PATCH_GENERATE_KIND) is not None
    assert registry.get(PATCH_WRITEBACK_KIND) is None
    # The invariant that makes run_job skip (not fail) the unregistered writeback kind:
    assert PATCH_WRITEBACK_KIND in _ALL_JOB_KINDS


# --- scope-pinned coordinator factory ------------------------------------------------------------


def test_build_patch_coordinator_pins_run_and_approval_stores_to_scope() -> None:
    components = _components(engine=object())
    coordinator = build_patch_coordinator(components, _SCOPE)

    # The pinned scope constructs a real per-scope store...
    assert isinstance(coordinator.run_store_factory(_SCOPE), PostgresRunStore)
    assert isinstance(coordinator.approval_factory(_SCOPE), PostgresApprovalStore)

    # ...any other scope fails closed (a job claimed under the wrong scope can never touch another
    # scope's runs/approvals).
    with pytest.raises(PatchValidationError):
        coordinator.run_store_factory(_OTHER_SCOPE)
    with pytest.raises(PatchValidationError):
        coordinator.approval_factory(_OTHER_SCOPE)


# --- storage readiness ---------------------------------------------------------------------------


def test_resolve_patch_storage_root_none_when_disabled() -> None:
    assert resolve_patch_storage_root(_settings(patch_enabled=False)) is None


def test_resolve_patch_storage_root_returns_probed_root_when_enabled() -> None:
    probed: list[Any] = []
    root = resolve_patch_storage_root(_settings(patch_enabled=True), probe=probed.append)
    assert root is not None
    assert probed == [root]


def test_resolve_patch_storage_root_raises_when_storage_unavailable() -> None:
    def _fail(_root: Any) -> None:
        raise SharedStorageUnavailable("volume missing")

    with pytest.raises(PatchStorageNotReady):
        resolve_patch_storage_root(_settings(patch_enabled=True), probe=_fail)


# --- reconciler tick entrypoint ------------------------------------------------------------------


async def test_reconcile_tick_noop_without_reconciler() -> None:
    assert await reconcile_patch_outbox_tick({}) == 0


async def test_reconcile_tick_delegates_to_reconciler() -> None:
    class _Rec:
        async def run(self) -> int:
            return 4

    assert await reconcile_patch_outbox_tick({"patch_reconciler": _Rec()}) == 4


# --- lease-token + repair-status helpers ---------------------------------------------------------


def _entry(**over: Any) -> PatchOutboxEntry:
    base: dict[str, Any] = {
        "proposal_id": "pp1",
        "org_id": _ORG,
        "scope_id": _SCOPE,
        "status_hint": PatchOutboxStatus.generating,
        "job_id": "",
        "attempts": 0,
        "lease_token": None,
        "lease_owner": None,
        "lease_expires_at": None,
        "next_attempt_at": _now(),
        "expires_at": _now(),
    }
    base.update(over)
    return PatchOutboxEntry(**base)


def test_lease_token_fails_closed_when_missing() -> None:
    with pytest.raises(PatchValidationError):
        _lease_token(_entry(lease_token=None))
    assert _lease_token(_entry(lease_token="tok")) == "tok"


def test_repair_status_maps_cancelled_else_failed() -> None:
    assert _repair_status(JobStatus.cancelled) is PatchStatus.cancelled
    assert _repair_status(JobStatus.failed) is PatchStatus.failed
    assert _repair_status(JobStatus.succeeded) is PatchStatus.failed
    assert _repair_status(None) is PatchStatus.failed


# --- reconciler harness ---------------------------------------------------------------------------


class _StubCoordinator:
    """Records ``request_approval`` calls; the reconciler must call it, then retire the pointer."""

    def __init__(self) -> None:
        self.request_approval_calls: list[tuple[str, str, str]] = []

    async def request_approval(
        self, org_id: str, proposal_id: str, *, actor: str, now: datetime | None = None
    ) -> str:
        self.request_approval_calls.append((org_id, proposal_id, actor))
        return f"appr-{proposal_id}"


class _Harness:
    def __init__(self, *, coordinator: Any | None = None) -> None:
        self.store = InMemoryPatchProposalStore()
        self.outbox = InMemoryPatchProposalOutbox()
        self.job_store = InMemoryJobStore(_SCOPE)
        self.events = InMemoryEventStore()
        self.dispatch = InMemoryJobDispatchOutbox()
        self.coordinator = coordinator or _StubCoordinator()
        self.reconciler = PatchOutboxReconciler(
            store=self.store,
            outbox=self.outbox,
            coordinator_factory=lambda _s: self.coordinator,
            job_store_factory=lambda _s: self.job_store,
            run_events_factory=lambda _s: self.events,
            job_dispatch_outbox=self.dispatch,
            worker_id="rec-1",
        )

    async def seed_proposal(
        self,
        *,
        pid: str,
        status: PatchStatus = PatchStatus.generating,
        expires_at: datetime | None = None,
        agent_id: str = _AGENT,
        org: str = _ORG,
    ) -> Any:
        expires = expires_at or (_now() + timedelta(hours=1))
        proposal, created = await self.store.create(
            proposal_id=pid,
            org_id=org,
            project_id="p",
            run_id=pid,
            run_attempt=0,
            agent_id=agent_id,
            actor="u",
            base_ref="main",
            source_ref="",
            task_digest="d" * 64,
            idempotency_key=pid,
            fingerprint="f" * 64,
            expires_at=expires,
        )
        assert created
        if status in _STATUS_CHAIN:
            p = proposal
            for st in _STATUS_CHAIN[1 : _STATUS_CHAIN.index(status) + 1]:
                p = await self.store.transition(org, pid, st, expected_version=p.version)
        else:  # a terminal seed (e.g. failed) reached directly from generating
            await self.store.transition(org, pid, status, expected_version=proposal.version)
        return await self.store.get(org, pid)

    async def seed_pointer(
        self,
        *,
        pid: str,
        scope: str = _SCOPE,
        org: str = _ORG,
        expires_at: datetime | None = None,
        status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
        job_id: str = "",
    ) -> None:
        await self.outbox.record(
            proposal_id=pid,
            org_id=org,
            scope_id=scope,
            expires_at=expires_at or (_now() + timedelta(hours=1)),
            status_hint=status_hint,
            job_id=job_id,
        )

    async def persist_metadata(self, *, pid: str) -> None:
        payload = PatchGenerateJobPayload(
            proposal_id=pid,
            run_id=pid,
            org_id=_ORG,
            project_id="p",
            actor="u",
            task="do the thing",
            base_ref="main",
            model="m",
            idempotency_key=pid,
        )
        await persist_generate_metadata(self.events, scope_id=_SCOPE, payload=payload)

    async def enqueue_active_job(self, *, kind: str, pid: str) -> str:
        idem = (
            patch_generate_idempotency_key(pid)
            if kind == PATCH_GENERATE_KIND
            else patch_writeback_idempotency_key(pid)
        )
        record, _ = await self.job_store.enqueue_once_with_dispatch_intent(
            kind=kind,
            payload={"proposal_id": pid, "org_id": _ORG},
            target_session_id=None,
            idempotency_key=idem,
            max_attempts=3,
            outbox=self.dispatch,
        )
        return record.id


# --- reconciler: pointer housekeeping ------------------------------------------------------------


async def test_reconcile_removes_pointer_for_absent_proposal() -> None:
    h = _Harness()
    await h.seed_pointer(pid="ghost")
    assert await h.reconciler.run() == 1
    assert await h.outbox.get("ghost") is None


async def test_reconcile_fails_proposal_closed_on_generating_scope_mismatch() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1")  # canonical scope == _SCOPE, status generating
    await h.seed_pointer(pid="pp1", scope=_OTHER_SCOPE)  # pointer claims a foreign scope
    # Immutable corruption (a canonical scope is a pure function of org+agent): fail CLOSED, not an
    # infinite reschedule. Handled (==1), the corrupt pointer is gone, the proposal is failed.
    assert await h.reconciler.run() == 1
    assert await h.outbox.get("pp1") is None
    proposal = await h.store.get(_ORG, "pp1")
    assert proposal.status is PatchStatus.failed
    assert proposal.error_kind == "PatchScopeMismatch"
    # The foreign scope was NEVER adopted: no job/dispatch intent enqueued in any scope.
    assert len(await h.job_store.list()) == 0
    assert await h.dispatch.active_scopes() == set()


async def test_reconcile_fails_proposal_closed_on_approved_scope_mismatch() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1", status=PatchStatus.approved)  # canonical scope == _SCOPE
    await h.seed_pointer(pid="pp1", scope=_OTHER_SCOPE, status_hint=PatchOutboxStatus.approved)
    assert await h.reconciler.run() == 1
    assert await h.outbox.get("pp1") is None
    proposal = await h.store.get(_ORG, "pp1")
    assert proposal.status is PatchStatus.failed
    assert proposal.error_kind == "PatchScopeMismatch"
    # No writeback job/foreign job touched.
    assert len(await h.job_store.list()) == 0
    assert await h.dispatch.active_scopes() == set()


async def test_reconcile_retires_pointer_on_approval_pending_scope_mismatch() -> None:
    # ``approval_pending`` has no legal ``failed`` edge and is awaiting a human under its real
    # scope; the corrupt pointer is retired under our fence WITHOUT forcing an illegal edge or
    # disturbing the legitimate proposal.
    h = _Harness()
    await h.seed_proposal(pid="pp1", status=PatchStatus.approval_pending)
    await h.seed_pointer(pid="pp1", scope=_OTHER_SCOPE)
    assert await h.reconciler.run() == 1
    assert await h.outbox.get("pp1") is None
    assert (await h.store.get(_ORG, "pp1")).status is PatchStatus.approval_pending


async def test_reconcile_removes_pointer_for_terminal_proposal() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1", status=PatchStatus.failed)
    await h.seed_pointer(pid="pp1")
    assert await h.reconciler.run() == 1
    assert await h.outbox.get("pp1") is None


async def test_reconcile_expires_on_ttl_and_deletes_pointer() -> None:
    h = _Harness()
    past = _now() - timedelta(hours=1)
    await h.seed_proposal(pid="pp1", status=PatchStatus.ready, expires_at=past)
    await h.seed_pointer(pid="pp1", status_hint=PatchOutboxStatus.ready)
    assert await h.reconciler.run() == 1
    assert (await h.store.get(_ORG, "pp1")).status is PatchStatus.expired
    assert await h.outbox.get("pp1") is None


# --- reconciler: generating ----------------------------------------------------------------------


async def test_reconcile_generating_enqueues_generate_job_and_fences_job_id() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1")
    await h.persist_metadata(pid="pp1")
    await h.seed_pointer(pid="pp1")

    assert await h.reconciler.run() == 1
    # The pointer now references the enqueued generate job.
    entry = await h.outbox.get("pp1")
    assert entry is not None and entry.job_id
    stored = await h.job_store.get(entry.job_id)
    assert stored is not None and stored.kind == PATCH_GENERATE_KIND
    assert _SCOPE in await h.dispatch.active_scopes()

    # A second tick (past the reschedule) sees the still-queued job and DEFERS — no duplicate job or
    # dispatch intent.
    later = _now() + timedelta(minutes=5)
    assert await h.reconciler.run(now=later) == 0
    assert len(await h.job_store.list()) == 1
    intents = await h.dispatch.claim_due(worker_id="probe")
    assert len(intents) == 1


async def test_reconcile_generating_without_metadata_defers_within_ttl() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1")  # future TTL, no durable metadata persisted
    await h.seed_pointer(pid="pp1")
    assert await h.reconciler.run() == 0
    assert len(await h.job_store.list()) == 0
    assert (await h.store.get(_ORG, "pp1")).status is PatchStatus.generating
    assert await h.outbox.get("pp1") is not None


async def test_reconcile_generating_without_metadata_fails_past_ttl() -> None:
    h = _Harness()
    past = _now() - timedelta(hours=1)
    await h.seed_proposal(pid="pp1", expires_at=past)
    await h.seed_pointer(pid="pp1")
    assert await h.reconciler.run() == 1
    proposal = await h.store.get(_ORG, "pp1")
    assert proposal.status is PatchStatus.failed
    assert proposal.error_kind == "patch_generation_unrecoverable"
    assert await h.outbox.get("pp1") is None


async def test_reconcile_generating_defers_when_job_active() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1")
    job_id = await h.enqueue_active_job(kind=PATCH_GENERATE_KIND, pid="pp1")
    await h.seed_pointer(pid="pp1", job_id=job_id)
    assert await h.reconciler.run() == 0
    assert (await h.store.get(_ORG, "pp1")).status is PatchStatus.generating


async def test_reconcile_generating_repairs_when_job_absent() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1")
    await h.seed_pointer(pid="pp1", job_id="ghost-job")
    assert await h.reconciler.run() == 1
    proposal = await h.store.get(_ORG, "pp1")
    assert proposal.status is PatchStatus.failed
    assert proposal.error_kind == "patch_generation_stranded"
    assert await h.outbox.get("pp1") is None


# --- reconciler: ready / approval_pending --------------------------------------------------------


async def test_reconcile_ready_heals_via_coordinator_and_retires_pointer() -> None:
    stub = _StubCoordinator()
    h = _Harness(coordinator=stub)
    await h.seed_proposal(pid="pp1", status=PatchStatus.ready)
    await h.seed_pointer(pid="pp1", status_hint=PatchOutboxStatus.ready)
    assert await h.reconciler.run() == 1
    assert stub.request_approval_calls == [(_ORG, "pp1", "u")]
    assert await h.outbox.get("pp1") is None


async def test_reconcile_approval_pending_retires_stale_pointer() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1", status=PatchStatus.approval_pending)
    await h.seed_pointer(pid="pp1")
    assert await h.reconciler.run() == 1
    assert await h.outbox.get("pp1") is None
    assert (await h.store.get(_ORG, "pp1")).status is PatchStatus.approval_pending


# --- reconciler: approved / writeback ------------------------------------------------------------


async def test_reconcile_approved_enqueues_writeback_job() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1", status=PatchStatus.approved)
    await h.seed_pointer(pid="pp1", status_hint=PatchOutboxStatus.approved)
    assert await h.reconciler.run() == 1
    entry = await h.outbox.get("pp1")
    assert entry is not None and entry.job_id
    stored = await h.job_store.get(entry.job_id)
    assert stored is not None and stored.kind == PATCH_WRITEBACK_KIND
    assert _SCOPE in await h.dispatch.active_scopes()


async def test_reconcile_writeback_repairs_when_job_absent() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1", status=PatchStatus.approved)
    await h.seed_pointer(pid="pp1", status_hint=PatchOutboxStatus.approved, job_id="ghost-job")
    assert await h.reconciler.run() == 1
    proposal = await h.store.get(_ORG, "pp1")
    assert proposal.status is PatchStatus.failed
    assert proposal.error_kind == "patch_writeback_stranded"
    assert await h.outbox.get("pp1") is None


async def test_reconcile_writeback_defers_when_job_active() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1", status=PatchStatus.writing)
    job_id = await h.enqueue_active_job(kind=PATCH_WRITEBACK_KIND, pid="pp1")
    await h.seed_pointer(pid="pp1", status_hint=PatchOutboxStatus.approved, job_id=job_id)
    assert await h.reconciler.run() == 0
    assert (await h.store.get(_ORG, "pp1")).status is PatchStatus.writing


# --- reconciler: fencing + fault isolation -------------------------------------------------------


async def test_reconcile_stops_on_stale_token_without_rescheduling() -> None:
    h = _Harness()
    await h.seed_proposal(pid="pp1")
    await h.persist_metadata(pid="pp1")
    await h.seed_pointer(pid="pp1")

    async def _stale(*_a: Any, **_k: Any) -> bool:
        return False

    h.outbox.set_job_id = _stale  # type: ignore[method-assign]
    assert await h.reconciler.run() == 0
    # The idempotency-keyed job still exists (a re-claim references the SAME job, no duplicate)...
    assert len(await h.job_store.list()) == 1
    # ...but this worker made no fenced pointer mutation after losing the token.
    entry = await h.outbox.get("pp1")
    assert entry is not None and not entry.job_id


async def test_reconcile_isolates_one_pointers_typed_failure() -> None:
    h = _Harness()
    await h.seed_proposal(pid="good")
    await h.persist_metadata(pid="good")
    await h.seed_pointer(pid="good")
    await h.seed_pointer(pid="poison")  # no proposal + a poisoned get raises below

    real_get = h.store.get

    async def _get(org_id: str, proposal_id: str) -> Any:
        if proposal_id == "poison":
            raise PatchValidationError("boom")
        return await real_get(org_id, proposal_id)

    h.store.get = _get  # type: ignore[method-assign]
    # The healthy pointer is handled; the poisoned one is deferred (not raised).
    assert await h.reconciler.run() == 1
    good = await h.outbox.get("good")
    assert good is not None and good.job_id
