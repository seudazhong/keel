"""Atomic proposal-store <-> dispatch-outbox seam + create-or-get approval primitive (M4, P1).

In-memory coverage for the P1 store hooks the durable path mirrors:

* ``create``/``transition`` optionally couple a global dispatch pointer in the *same* logical unit
  (record the ``generating`` intent atomically; drive the coarse status hint; delete the pointer
  when awaiting a human or once terminal; re-create it ``approved`` for writeback), rolling the
  whole unit back on any failure;
* :meth:`ApprovalStore.create_pending_or_get` is truly idempotent on the interactive binding and
  fails closed on any immutable-binding divergence; and
* :meth:`PatchProposalStore.transition_to_approval_pending` atomically raises the durable approval,
  advances ``ready -> approval_pending`` (one version bump), and deletes the pointer — with a
  concurrent second caller taking the idempotent fast path (same approval, no second bump) and any
  failure leaving **no** residual approval / half-transition / orphaned pointer.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from keel_core.approvals import InMemoryApprovalStore
from keel_core.patch.errors import PatchApprovalError, PatchStateError, PatchValidationError
from keel_core.patch.models import PatchStatus
from keel_core.patch.outbox import (
    InMemoryPatchProposalOutbox,
    PatchOutboxStatus,
    PatchProposalOutbox,
    PostgresPatchProposalOutbox,
)
from keel_core.patch.store import ApprovalDraft, InMemoryPatchProposalStore

_SCOPE = "agent:org-a/patch"
_T0 = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)


def _exp() -> datetime:
    return _T0 + timedelta(hours=1)


async def _create(
    store: InMemoryPatchProposalStore,
    *,
    outbox: PatchProposalOutbox | None = None,
    scope_id: str | None = None,
    pid: str = "pp_1",
    idem: str = "idem-1",
    org: str = "org-a",
) -> str:
    proposal, _ = await store.create(
        proposal_id=pid,
        org_id=org,
        project_id="proj-a",
        run_id="run-1",
        run_attempt=1,
        agent_id="patch",
        actor="u",
        base_ref="main",
        source_ref="",
        task_digest="d",
        idempotency_key=idem,
        fingerprint="fp",
        expires_at=_exp(),
        now=_T0,
        outbox=outbox,
        scope_id=scope_id,
    )
    return proposal.id


async def _ready(
    store: InMemoryPatchProposalStore,
    pid: str,
    *,
    outbox: PatchProposalOutbox | None = None,
    scope_id: str | None = None,
) -> int:
    p = await store.transition(
        "org-a",
        pid,
        PatchStatus.ready,
        expected_version=1,
        updates={"bundle_sha256": "e" * 64},
        now=_T0,
        outbox=outbox,
        scope_id=scope_id,
    )
    return p.version


def _draft(**over: object) -> ApprovalDraft:
    kw: dict[str, object] = dict(
        run_id="run-1",
        session_id="sess-1",
        tool="patch.apply",
        args={"pid": "pp_1"},
        call_id="call-1",
        idempotency_key="aidem-1",
        reason="human approval required",
        expires_at=_exp(),
    )
    kw.update(over)
    return ApprovalDraft(**kw)  # type: ignore[arg-type]


# --- create seam ---------------------------------------------------------------------


async def test_create_without_outbox_is_backward_compatible() -> None:
    store = InMemoryPatchProposalStore()
    pid = await _create(store)
    assert (await store.get("org-a", pid)) is not None  # no outbox needed


async def test_create_records_generating_pointer_atomically() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    entry = await outbox.get(pid)
    assert entry is not None
    assert entry.status_hint is PatchOutboxStatus.generating
    assert entry.org_id == "org-a" and entry.scope_id == _SCOPE


async def test_create_with_outbox_requires_scope() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    with pytest.raises(PatchValidationError):
        await _create(store, outbox=outbox, scope_id=None)
    # Fail closed: neither the proposal nor a pointer was written.
    assert await store.get("org-a", "pp_1") is None
    assert await outbox.get("pp_1") is None


async def test_create_rejects_mismatched_outbox_type() -> None:
    store = InMemoryPatchProposalStore()
    pg_outbox = PostgresPatchProposalOutbox(cast("object", None))  # type: ignore[arg-type]
    with pytest.raises(PatchValidationError):
        await _create(store, outbox=cast(PatchProposalOutbox, pg_outbox), scope_id=_SCOPE)


class _FailingRecordOutbox(InMemoryPatchProposalOutbox):
    async def record_in_connection(self, *a: object, **k: object) -> None:  # type: ignore[override]
        raise RuntimeError("pointer store down")


async def test_create_rolls_back_when_pointer_write_fails() -> None:
    store = InMemoryPatchProposalStore()
    outbox = _FailingRecordOutbox()
    with pytest.raises(RuntimeError, match="pointer store down"):
        await _create(store, outbox=outbox, scope_id=_SCOPE)
    # The proposal write is rolled back with the failed pointer — no undiscoverable proposal.
    assert await store.get("org-a", "pp_1") is None


# --- transition seam: status-hint lifecycle ------------------------------------------


async def test_transition_ready_updates_hint() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)
    entry = await outbox.get(pid)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.ready


async def test_transition_terminal_deletes_pointer() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    await store.transition(
        "org-a", pid, PatchStatus.failed, now=_T0, outbox=outbox, scope_id=_SCOPE
    )
    assert await outbox.get(pid) is None  # no background work once terminal


async def test_transition_approved_recreates_pointer_hint() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    v = await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)
    v = (
        await store.transition(
            "org-a",
            pid,
            PatchStatus.approval_pending,
            expected_version=v,
            now=_T0,
            outbox=outbox,
            scope_id=_SCOPE,
        )
    ).version
    assert await outbox.get(pid) is None  # deleted while awaiting a human
    await store.transition(
        "org-a",
        pid,
        PatchStatus.approved,
        expected_version=v,
        now=_T0,
        outbox=outbox,
        scope_id=_SCOPE,
    )
    entry = await outbox.get(pid)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.approved


async def test_transition_approved_requires_scope() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    v = await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)
    v = (
        await store.transition(
            "org-a",
            pid,
            PatchStatus.approval_pending,
            expected_version=v,
            now=_T0,
            outbox=outbox,
            scope_id=_SCOPE,
        )
    ).version
    with pytest.raises(PatchValidationError):
        await store.transition(
            "org-a",
            pid,
            PatchStatus.approved,
            expected_version=v,
            now=_T0,
            outbox=outbox,
            scope_id=None,
        )


class _FailingHintOutbox(InMemoryPatchProposalOutbox):
    async def set_status_hint_in_connection(self, *a: object, **k: object) -> None:  # type: ignore[override]
        raise RuntimeError("hint store down")


async def test_transition_rolls_back_proposal_and_pointer_on_failure() -> None:
    store = InMemoryPatchProposalStore()
    outbox = _FailingHintOutbox()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    with pytest.raises(RuntimeError, match="hint store down"):
        await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)
    # The proposal stays ``generating`` (no half transition) and the pointer keeps its hint.
    proposal = await store.get("org-a", pid)
    assert proposal is not None and proposal.status is PatchStatus.generating
    entry = await outbox.get(pid)
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating


# --- create_pending_or_get idempotency + binding ------------------------------------


async def test_create_pending_or_get_is_idempotent() -> None:
    approvals = InMemoryApprovalStore()
    kw = dict(
        scope_id=_SCOPE,
        run_id="run-1",
        session_id="sess-1",
        tool="patch.apply",
        args={"pid": "pp_1"},
        call_id="call-1",
        idempotency_key="aidem-1",
        reason="tainted",
        expires_at=_exp(),
        batch_id="pp_1",
    )
    first_id, created_1 = await approvals.create_pending_or_get(**kw)  # type: ignore[arg-type]
    second_id, created_2 = await approvals.create_pending_or_get(**kw)  # type: ignore[arg-type]
    assert created_1 is True and created_2 is False
    assert first_id == second_id
    assert len(approvals._rows) == 1


async def test_create_pending_or_get_requires_batch_id() -> None:
    approvals = InMemoryApprovalStore()
    with pytest.raises(PatchApprovalError):
        await approvals.create_pending_or_get(
            scope_id=_SCOPE,
            run_id="run-1",
            session_id="sess-1",
            tool="patch.apply",
            args={},
            call_id="call-1",
            idempotency_key="aidem-1",
            reason="tainted",
            expires_at=_exp(),
            batch_id="",  # empty -> the partial-unique index would never dedupe
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tool", "patch.other"),
        ("session_id", "sess-2"),
        ("idempotency_key", "aidem-2"),
        ("org_id", "org-b"),
        ("actor", "mallory"),
        ("action_hash", "deadbeef"),
        ("args", {"pid": "pp_other"}),
    ],
)
async def test_create_pending_or_get_binding_mismatch_fails_closed(
    field: str, value: object
) -> None:
    approvals = InMemoryApprovalStore()
    base = dict(
        scope_id=_SCOPE,
        run_id="run-1",
        session_id="sess-1",
        tool="patch.apply",
        args={"pid": "pp_1"},
        call_id="call-1",
        idempotency_key="aidem-1",
        reason="tainted",
        expires_at=_exp(),
        org_id="org-a",
        actor="alice",
        action_hash="abc123",
        run_attempt=0,
        batch_id="pp_1",
    )
    await approvals.create_pending_or_get(**base)  # type: ignore[arg-type]
    # A replay whose immutable binding diverges must fail closed, naming the mismatched field, and
    # never adopt the existing approval.
    conflicting = dict(base)
    conflicting[field] = value
    with pytest.raises(PatchApprovalError, match=field):
        await approvals.create_pending_or_get(**conflicting)  # type: ignore[arg-type]
    assert len(approvals._rows) == 1  # no second row


# --- transition_to_approval_pending: atomic ready -> approval_pending -----------------


async def test_transition_to_approval_pending_happy_path() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    approvals = InMemoryApprovalStore()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    ready_v = await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)

    proposal, approval_id, created = await store.transition_to_approval_pending(
        "org-a",
        pid,
        approvals=approvals,
        outbox=outbox,
        scope_id=_SCOPE,
        draft=_draft(),
        expected_version=ready_v,
        now=_T0,
    )
    assert created is True
    assert proposal.status is PatchStatus.approval_pending
    assert proposal.version == ready_v + 1  # exactly one bump
    assert proposal.approval_id == approval_id
    assert await outbox.get(pid) is None  # pointer deleted while awaiting a human
    record = await approvals.get(approval_id)
    assert record is not None and record.status == "pending"


async def test_transition_to_approval_pending_idempotent_fast_path() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    approvals = InMemoryApprovalStore()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    ready_v = await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)

    p1, aid1, created1 = await store.transition_to_approval_pending(
        "org-a",
        pid,
        approvals=approvals,
        outbox=outbox,
        scope_id=_SCOPE,
        draft=_draft(),
        now=_T0,
    )
    p2, aid2, created2 = await store.transition_to_approval_pending(
        "org-a",
        pid,
        approvals=approvals,
        outbox=outbox,
        scope_id=_SCOPE,
        draft=_draft(),
        now=_T0,
    )
    assert aid1 == aid2
    assert created1 is True and created2 is False
    assert p2.version == p1.version == ready_v + 1  # no second bump
    assert len(approvals._rows) == 1


class _FailingDeleteOutbox(InMemoryPatchProposalOutbox):
    async def delete_in_connection(self, conn: object, proposal_id: str) -> None:  # type: ignore[override]
        raise RuntimeError("pointer delete down")


async def test_transition_to_approval_pending_rolls_back_all_on_failure() -> None:
    store = InMemoryPatchProposalStore()
    outbox = _FailingDeleteOutbox()
    approvals = InMemoryApprovalStore()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    ready_v = await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)

    with pytest.raises(RuntimeError, match="pointer delete down"):
        await store.transition_to_approval_pending(
            "org-a",
            pid,
            approvals=approvals,
            outbox=outbox,
            scope_id=_SCOPE,
            draft=_draft(),
            now=_T0,
        )
    # Whole unit rolled back: proposal still ready (no bump / no approval binding), NO residual
    # approval, pointer intact.
    proposal = await store.get("org-a", pid)
    assert proposal is not None
    assert proposal.status is PatchStatus.ready
    assert proposal.version == ready_v
    assert proposal.approval_id == ""
    assert approvals._rows == {}  # never a residual approval
    assert await outbox.get(pid) is not None


async def test_transition_to_approval_pending_concurrent_same_approval() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    approvals = InMemoryApprovalStore()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    ready_v = await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)

    results = await asyncio.gather(
        store.transition_to_approval_pending(
            "org-a",
            pid,
            approvals=approvals,
            outbox=outbox,
            scope_id=_SCOPE,
            draft=_draft(),
            now=_T0,
        ),
        store.transition_to_approval_pending(
            "org-a",
            pid,
            approvals=approvals,
            outbox=outbox,
            scope_id=_SCOPE,
            draft=_draft(),
            now=_T0,
        ),
    )
    approval_ids = {r[1] for r in results}
    assert len(approval_ids) == 1  # both callers resolve the same approval
    assert sum(1 for r in results if r[2]) == 1  # exactly one created it
    final = await store.get("org-a", pid)
    assert final is not None
    assert final.status is PatchStatus.approval_pending
    assert final.version == ready_v + 1  # version bumped exactly once
    assert len(approvals._rows) == 1


async def test_transition_to_approval_pending_requires_ready_source() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    approvals = InMemoryApprovalStore()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)  # still ``generating``
    with pytest.raises(PatchStateError):
        await store.transition_to_approval_pending(
            "org-a",
            pid,
            approvals=approvals,
            outbox=outbox,
            scope_id=_SCOPE,
            draft=_draft(),
            now=_T0,
        )
    assert approvals._rows == {}  # fail closed: no approval raised for an illegal edge


async def test_transition_to_approval_pending_rejects_bad_scope() -> None:
    store = InMemoryPatchProposalStore()
    outbox = InMemoryPatchProposalOutbox()
    approvals = InMemoryApprovalStore()
    pid = await _create(store, outbox=outbox, scope_id=_SCOPE)
    await _ready(store, pid, outbox=outbox, scope_id=_SCOPE)
    from keel_core.scoping import ScopeValidationError

    with pytest.raises(ScopeValidationError):
        await store.transition_to_approval_pending(
            "org-a",
            pid,
            approvals=approvals,
            outbox=outbox,
            scope_id="not-a-scope",
            draft=_draft(),
            now=_T0,
        )
    assert approvals._rows == {}
