"""Global patch-proposal dispatch outbox — in-memory unit coverage (M4, WS-PP, P1).

The outbox is the cross-org pointer/index a single worker scans to reconcile controlled-patch
work across every org. These tests pin the behaviour the durable (Postgres) store must mirror:
idempotent admission, coarse status-hint lifecycle, a scope re-validated on every write, bounded
claim/backoff inputs, and — the crux — a random per-claim ``lease_token`` fence so a worker holding
a stale lease can never ack/defer/retire/annotate a pointer a newer worker has since re-leased.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from keel_core.patch.errors import PatchValidationError
from keel_core.patch.outbox import (
    InMemoryPatchProposalOutbox,
    PatchOutboxStatus,
)
from keel_core.scoping import ScopeValidationError

_SCOPE_A = "agent:org-a/patch"
_SCOPE_B = "agent:org-b/patch"
_T0 = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)


def _exp() -> datetime:
    return _T0 + timedelta(hours=1)


async def _record(
    outbox: InMemoryPatchProposalOutbox,
    proposal_id: str,
    *,
    org_id: str = "org-a",
    scope_id: str = _SCOPE_A,
    status_hint: PatchOutboxStatus = PatchOutboxStatus.generating,
    now: datetime = _T0,
) -> None:
    await outbox.record(
        proposal_id=proposal_id,
        org_id=org_id,
        scope_id=scope_id,
        expires_at=_exp(),
        status_hint=status_hint,
        now=now,
    )


# --- admission / idempotency / scope validation --------------------------------------


async def test_record_is_idempotent_on_proposal_id() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    # A second record for the same proposal is a no-op (ON CONFLICT DO NOTHING), never a dup or
    # a silent overwrite of the hint.
    await _record(outbox, "p1", status_hint=PatchOutboxStatus.ready)
    entry = await outbox.get("p1")
    assert entry is not None
    assert entry.status_hint is PatchOutboxStatus.generating
    assert entry.attempts == 0
    assert await outbox.active_scopes() == {_SCOPE_A}


async def test_record_spans_multiple_scopes() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1", org_id="org-a", scope_id=_SCOPE_A)
    await _record(outbox, "p2", org_id="org-b", scope_id=_SCOPE_B)
    assert await outbox.active_scopes() == {_SCOPE_A, _SCOPE_B}


@pytest.mark.parametrize("scope", ["", "not-a-scope", "ambient", "agent:onlyorg"])
async def test_record_rejects_malformed_scope(scope: str) -> None:
    outbox = InMemoryPatchProposalOutbox()
    with pytest.raises(ScopeValidationError):
        await _record(outbox, "p1", scope_id=scope)
    assert await outbox.get("p1") is None  # fail closed: nothing written


async def test_record_requires_proposal_and_org() -> None:
    outbox = InMemoryPatchProposalOutbox()
    with pytest.raises(PatchValidationError):
        await outbox.record(proposal_id="", org_id="org-a", scope_id=_SCOPE_A, expires_at=_exp())
    with pytest.raises(PatchValidationError):
        await outbox.record(proposal_id="p1", org_id="", scope_id=_SCOPE_A, expires_at=_exp())


async def test_record_rejects_unknown_status_hint() -> None:
    outbox = InMemoryPatchProposalOutbox()
    with pytest.raises(PatchValidationError):
        await outbox.record(
            proposal_id="p1",
            org_id="org-a",
            scope_id=_SCOPE_A,
            expires_at=_exp(),
            status_hint="approval_pending",  # type: ignore[arg-type]
        )


# --- status-hint lifecycle (store-owned, unfenced) -----------------------------------


async def test_status_hint_transitions() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    await outbox.set_status_hint_in_connection(None, "p1", PatchOutboxStatus.ready)
    entry = await outbox.get("p1")
    assert entry is not None and entry.status_hint is PatchOutboxStatus.ready
    # set_status_hint on a missing pointer is a silent no-op (nothing to update).
    await outbox.set_status_hint_in_connection(None, "missing", PatchOutboxStatus.ready)
    assert await outbox.get("missing") is None


async def test_upsert_status_hint_recreates_deleted_pointer() -> None:
    outbox = InMemoryPatchProposalOutbox()
    # approval_pending deletes the pointer; the later -> approved re-creates it with the approved
    # hint (the worker then drives writeback). upsert must both create and update.
    await outbox.upsert_status_hint_in_connection(
        None,
        proposal_id="p1",
        org_id="org-a",
        scope_id=_SCOPE_A,
        status_hint=PatchOutboxStatus.approved,
        expires_at=_exp(),
    )
    entry = await outbox.get("p1")
    assert entry is not None and entry.status_hint is PatchOutboxStatus.approved
    # A subsequent upsert updates the hint/expiry in place (still one row).
    await outbox.upsert_status_hint_in_connection(
        None,
        proposal_id="p1",
        org_id="org-a",
        scope_id=_SCOPE_A,
        status_hint=PatchOutboxStatus.ready,
        expires_at=_exp() + timedelta(hours=1),
    )
    entry = await outbox.get("p1")
    assert entry is not None and entry.status_hint is PatchOutboxStatus.ready


async def test_delete_in_connection_removes_pointer() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    await outbox.delete_in_connection(None, "p1")
    assert await outbox.get("p1") is None
    # Deleting a missing pointer is a silent no-op.
    await outbox.delete_in_connection(None, "p1")


# --- claim / lease / fencing ---------------------------------------------------------


async def test_claim_due_leases_with_distinct_tokens() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    await _record(outbox, "p2")
    claimed = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=300)
    assert {c.proposal_id for c in claimed} == {"p1", "p2"}
    for c in claimed:
        assert c.lease_owner == "w1"
        assert c.lease_token is not None
        assert c.lease_expires_at == _T0 + timedelta(seconds=300)
        assert c.attempts == 1
    # A random token is stamped per row (fencing) — the two claims never share a token.
    assert claimed[0].lease_token != claimed[1].lease_token


async def test_claim_due_lease_blocks_duplicate_then_reclaims_on_expiry() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    first = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=60)
    assert [c.proposal_id for c in first] == ["p1"]
    # A second worker sees nothing while the lease is live.
    second = await outbox.claim_due(worker_id="w2", now=_T0 + timedelta(seconds=1))
    assert second == []
    # After the lease expires it is reclaimable (attempts keeps climbing).
    third = await outbox.claim_due(worker_id="w2", now=_T0 + timedelta(seconds=120))
    assert [c.proposal_id for c in third] == ["p1"]
    assert third[0].lease_owner == "w2"
    assert third[0].attempts == 2


async def test_claim_due_skips_future_next_attempt() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    claimed = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=60)
    token = claimed[0].lease_token
    assert token is not None
    # Reschedule pushes next_attempt_at into the future: not due until then.
    assert await outbox.reschedule("p1", lease_token=token, delay_seconds=120, now=_T0) is True
    assert await outbox.claim_due(worker_id="w1", now=_T0 + timedelta(seconds=60)) == []
    due = await outbox.claim_due(worker_id="w1", now=_T0 + timedelta(seconds=120))
    assert [c.proposal_id for c in due] == ["p1"]


async def test_fencing_rejects_stale_token_after_release() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    first = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=60)
    stale_token = first[0].lease_token
    assert stale_token is not None
    # The lease expires and a newer worker re-leases the pointer (fresh token).
    second = await outbox.claim_due(worker_id="w2", now=_T0 + timedelta(seconds=120))
    fresh_token = second[0].lease_token
    assert fresh_token is not None and fresh_token != stale_token
    # The stale worker can no longer ack/defer/retire/annotate the pointer.
    when = _T0 + timedelta(seconds=121)
    assert await outbox.complete("p1", lease_token=stale_token, now=when) is False
    assert await outbox.reschedule("p1", lease_token=stale_token, now=when) is False
    assert await outbox.remove("p1", lease_token=stale_token, now=when) is False
    assert await outbox.set_job_id("p1", "job-x", lease_token=stale_token, now=when) is False
    # The pointer still exists under the newer lease.
    assert await outbox.get("p1") is not None
    # The fresh token still works.
    assert await outbox.complete("p1", lease_token=fresh_token, now=when) is True


async def test_complete_releases_lease_and_resets_attempts() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    claimed = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=60)
    token = claimed[0].lease_token
    assert token is not None
    assert await outbox.complete("p1", lease_token=token, now=_T0 + timedelta(seconds=5)) is True
    entry = await outbox.get("p1")
    assert entry is not None
    assert entry.lease_owner is None
    assert entry.lease_token is None
    assert entry.lease_expires_at is None
    assert entry.attempts == 0  # retry budget reset
    assert entry.next_attempt_at == _T0 + timedelta(seconds=5)  # due now


async def test_reschedule_defers_and_keeps_attempts() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    claimed = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=60)
    token = claimed[0].lease_token
    assert token is not None
    assert await outbox.reschedule("p1", lease_token=token, delay_seconds=90, now=_T0) is True
    entry = await outbox.get("p1")
    assert entry is not None
    assert entry.lease_owner is None and entry.lease_token is None
    assert entry.attempts == 1  # kept for backoff (not reset)
    assert entry.next_attempt_at == _T0 + timedelta(seconds=90)


async def test_set_job_id_fenced() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    claimed = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=60)
    token = claimed[0].lease_token
    assert token is not None
    assert await outbox.set_job_id("p1", "job-1", lease_token=token, now=_T0) is True
    entry = await outbox.get("p1")
    assert entry is not None and entry.job_id == "job-1"
    # An unclaimed pointer / wrong token cannot be annotated.
    assert await outbox.set_job_id("p1", "job-2", lease_token="wrong", now=_T0) is False


async def test_remove_fenced() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    claimed = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=60)
    token = claimed[0].lease_token
    assert token is not None
    assert await outbox.remove("p1", lease_token="wrong", now=_T0) is False
    assert await outbox.get("p1") is not None
    assert await outbox.remove("p1", lease_token=token, now=_T0) is True
    assert await outbox.get("p1") is None


# --- bounded inputs (fail closed) ----------------------------------------------------


@pytest.mark.parametrize("limit", [0, -1, 501])
async def test_claim_due_rejects_out_of_bounds_limit(limit: int) -> None:
    outbox = InMemoryPatchProposalOutbox()
    with pytest.raises(PatchValidationError):
        await outbox.claim_due(worker_id="w1", now=_T0, limit=limit)


@pytest.mark.parametrize("lease_seconds", [0, -5, 3601])
async def test_claim_due_rejects_out_of_bounds_lease(lease_seconds: int) -> None:
    outbox = InMemoryPatchProposalOutbox()
    with pytest.raises(PatchValidationError):
        await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=lease_seconds)


@pytest.mark.parametrize("delay_seconds", [-1, 86_401])
async def test_reschedule_rejects_out_of_bounds_delay(delay_seconds: int) -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    claimed = await outbox.claim_due(worker_id="w1", now=_T0, lease_seconds=60)
    token = claimed[0].lease_token
    assert token is not None
    with pytest.raises(PatchValidationError):
        await outbox.reschedule("p1", lease_token=token, delay_seconds=delay_seconds, now=_T0)


async def test_snapshot_restore_rolls_back_mutations() -> None:
    outbox = InMemoryPatchProposalOutbox()
    await _record(outbox, "p1")
    snap = outbox._txn_snapshot()
    await _record(outbox, "p2")
    await outbox.set_status_hint_in_connection(None, "p1", PatchOutboxStatus.ready)
    outbox._txn_restore(snap)
    # p2 is gone and p1's hint reverted — the snapshot is a faithful value copy.
    assert await outbox.get("p2") is None
    entry = await outbox.get("p1")
    assert entry is not None and entry.status_hint is PatchOutboxStatus.generating
