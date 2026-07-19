"""Durable + in-memory ``patch_proposals`` repositories (WS-PP).

A thin :class:`PatchProposalStore` seam over the org-owned proposal record, with:

* :class:`InMemoryPatchProposalStore` — a faithful, dependency-free implementation for unit tests
  and the ``lite`` profile that enforces the same idempotency, optimistic-fence, legal-transition
  and branch-collision rules the schema does; and
* :class:`PostgresPatchProposalStore` — the durable implementation. Tenant-owned reads/writes set
  the ``app.org_id`` GUC so Postgres RLS + FORCE is engaged as defense-in-depth. Every mutating
  transition is a single fenced ``UPDATE`` (optimistic ``version`` guard + legal-edge check) so a
  concurrent or stale writer never wins a conflicting transition.

Validation, authorization, bundle materialization and audit live in the coordinator/service; this
repository is intentionally thin and truthful.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.approvals import (
    ApprovalStore,
    InMemoryApprovalStore,
    PostgresApprovalStore,
    insert_pending_or_get_in_transaction,
)
from keel_core.runs import (
    InMemoryRunStore,
    PostgresRunStore,
    RunCost,
    RunLease,
    RunRecord,
    RunStatus,
    RunStore,
    release_in_transaction,
    terminalize_in_transaction,
)
from keel_core.scoping import validate_scope_id

from .errors import PatchStateError, PatchValidationError
from .models import (
    TERMINAL_STATUSES,
    PatchProposal,
    PatchStatus,
    TestStatus,
    ensure_transition,
)
from .outbox import (
    InMemoryPatchProposalOutbox,
    PatchOutboxStatus,
    PatchProposalOutbox,
    PostgresPatchProposalOutbox,
)
from .payload import PatchGenerationRequestRecord, canonical_payload_json

logger = logging.getLogger("keel.patch.store")

_SET_ORG = text("SELECT set_config('app.org_id', :org, true)")
_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")

# The immutable per-proposal generation-request row (scope-RLS; migration 0022). Written in the same
# transaction as the proposal + dispatch pointer so a committed ``generating`` proposal always
# implies a committed, reconstructable request.
_INSERT_GENERATION_REQUEST = text(
    "INSERT INTO patch_generation_requests "
    "(proposal_id, org_id, scope_id, payload, fingerprint, created_at) "
    "VALUES (:pid, :org, :scope, CAST(:payload AS jsonb), :fp, :now)"
)
_SELECT_GENERATION_REQUEST = text(
    "SELECT proposal_id, org_id, scope_id, payload, fingerprint, created_at "
    "FROM patch_generation_requests WHERE proposal_id = :pid AND org_id = :org"
)


def _validate_generation_request(
    record: PatchGenerationRequestRecord,
    *,
    proposal_id: str,
    org_id: str,
    scope_id: str | None,
) -> None:
    """Fail closed if a supplied generation request does not match the proposal it is admitted with.

    The record is built from the same authorized request, so its binding must line up exactly with
    the proposal + outbox scope (defense-in-depth against a caller that wires a mismatched payload).
    """
    if not scope_id:
        raise PatchValidationError(
            "patch proposal create with a generation request requires scope_id"
        )
    if record.proposal_id != proposal_id or record.org_id != org_id or record.scope_id != scope_id:
        raise PatchValidationError(
            "generation request binding does not match the proposal being created"
        )


async def insert_generation_request_in_connection(
    conn: AsyncConnection, record: PatchGenerationRequestRecord
) -> None:
    """Insert the immutable request row inside the caller's transaction (module-level helper).

    Shared by the atomic proposal-store create path so the proposal, its scoped request payload, and
    the global dispatch pointer commit (or roll back) together. The caller MUST have set the
    ``app.scope_id`` GUC to ``record.scope_id`` so the row satisfies the scope-RLS ``WITH CHECK``.
    """
    await conn.execute(
        _INSERT_GENERATION_REQUEST,
        {
            "pid": record.proposal_id,
            "org": record.org_id,
            "scope": record.scope_id,
            "payload": canonical_payload_json(record.payload),
            "fp": record.fingerprint,
            "now": record.created_at,
        },
    )


_COLS = (
    "id, org_id, project_id, run_id, run_attempt, agent_id, actor, source_ref, task_digest, "
    "base_ref, base_sha, head_sha, bundle_sha256, diff_sha256, changed_path_digest, changed_files, "
    "test_status, status, version, approval_id, remote_branch, pr_number, pr_url, pr_node_id, "
    "idempotency_key, fingerprint, cost_usd, error_kind, error_message, created_at, updated_at, "
    "ready_at, decided_at, written_at, expires_at"
)

# Columns a fenced transition may set (never id/org/version/created_at — those are structural).
_UPDATABLE = frozenset(
    {
        "base_sha",
        "head_sha",
        "bundle_sha256",
        "diff_sha256",
        "changed_path_digest",
        "changed_files",
        "test_status",
        "approval_id",
        "remote_branch",
        "pr_number",
        "pr_url",
        "pr_node_id",
        "cost_usd",
        "error_kind",
        "error_message",
        "ready_at",
        "decided_at",
        "written_at",
        "expires_at",
    }
)


def _now() -> datetime:
    return datetime.now(UTC)


class ProposalConflictError(PatchValidationError):
    """A durable uniqueness constraint (idempotency key / branch collision) was violated."""


class ProposalNotFoundInStore(PatchStateError):
    """The proposal to transition does not exist in this org (fail closed)."""


class StaleProposalVersion(PatchStateError):
    """An optimistic-fenced transition cited a stale ``version`` (a concurrent writer won)."""


@runtime_checkable
class PatchProposalStore(Protocol):
    """Durable seam for controlled patch-proposal persistence."""

    async def create(
        self,
        *,
        proposal_id: str,
        org_id: str,
        project_id: str,
        run_id: str,
        run_attempt: int,
        agent_id: str,
        actor: str,
        base_ref: str,
        source_ref: str,
        task_digest: str,
        idempotency_key: str,
        fingerprint: str,
        expires_at: datetime,
        now: datetime | None = None,
        outbox: PatchProposalOutbox | None = None,
        scope_id: str | None = None,
        generation_request: PatchGenerationRequestRecord | None = None,
    ) -> tuple[PatchProposal, bool]: ...

    async def get(self, org_id: str, proposal_id: str) -> PatchProposal | None: ...

    async def get_generation_request(
        self, org_id: str, proposal_id: str, scope_id: str
    ) -> PatchGenerationRequestRecord | None:
        """Load the immutable generation-request row for a proposal behind its scope (or ``None``).

        Scoped: a caller bound to one scope can never read another scope's request payload. Returns
        ``None`` when no request row exists (a legacy pre-0022 proposal, or a payload that fails
        fail-closed reconstruction) so a caller can treat an unreconstructable ``generating``
        proposal as terminal corruption.
        """
        ...

    async def get_by_run(self, org_id: str, run_id: str) -> PatchProposal | None: ...

    async def list_for_project(
        self, org_id: str, project_id: str, *, limit: int = 100
    ) -> list[PatchProposal]: ...

    async def transition(
        self,
        org_id: str,
        proposal_id: str,
        target: PatchStatus,
        *,
        expected_version: int | None = None,
        updates: Mapping[str, Any] | None = None,
        now: datetime | None = None,
        outbox: PatchProposalOutbox | None = None,
        scope_id: str | None = None,
    ) -> PatchProposal: ...

    async def transition_to_approval_pending(
        self,
        org_id: str,
        proposal_id: str,
        *,
        approvals: ApprovalStore,
        outbox: PatchProposalOutbox,
        scope_id: str,
        draft: ApprovalDraft,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, str, bool]:
        """Atomically move a ``ready`` proposal to ``approval_pending`` behind a durable approval.

        The single fail-closed primitive that (in one transaction) locks the ``ready`` proposal,
        create-or-gets its durable approval (idempotent on the interactive binding), bumps the
        proposal to ``approval_pending`` (+1 version, bound to the approval id) and deletes the
        dispatch pointer. Returns ``(proposal, approval_id, created)``. A concurrent second caller
        observes ``approval_pending`` and takes the idempotent fast path: the *same* approval id,
        ``created=False``, and **no** second version bump. Any failure rolls the whole unit back —
        there is never a residual approval, half-transition, or orphaned pointer."""
        ...

    async def finalize_generation_ready(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        updates: Mapping[str, Any],
        result_ref: str,
        stop_reason: str,
        outbox: PatchProposalOutbox,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        """Atomically complete a successful generation: run ``completed`` + proposal ``ready``.

        In ONE transaction: terminalize the freshly-claimed run (fenced by its lease token, applying
        ``cost`` as a cumulative delta), move the proposal ``generating -> ready`` (mirroring the
        run's authoritative cumulative cost onto ``cost_usd`` and the supplied outcome ``updates``),
        and set its dispatch pointer hint to ``ready``. Closes the P2 cross-await window: a crash
        can never leave a *completed* run behind a still-``generating`` proposal (nor the reverse).
        A lost lease raises :class:`~keel_core.runs.RunLeaseLostError` and rolls the unit back.
        Returns ``(proposal, run_record)``."""
        ...

    async def finalize_generation_failed(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        error_kind: str,
        error_message: str,
        stop_reason: str,
        outbox: PatchProposalOutbox,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        """Atomically fail a generation: run ``failed`` + proposal ``failed`` + pointer deleted.

        Terminalizes the run ``failed`` (charging any partial ``cost`` cumulatively) and moves the
        proposal ``generating -> failed`` (mirroring the run's cumulative cost, recording the
        error), and deletes its dispatch pointer — all or nothing."""
        ...

    async def release_generation_transient(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        """Atomically release a run to the queue on transient outage; proposal stays ``generating``.

        Releases the run lease back to ``queued`` (charging the partial ``cost`` cumulatively) and
        mirrors the run's cumulative cost onto the still-``generating`` proposal — no status change,
        no version bump, the dispatch pointer stays ``generating`` — so a retry re-claims and
        resumes without losing the partial charge. Returns ``(proposal, run_record)``."""
        ...

    async def expire_due(self, now: datetime, limit: int) -> list[tuple[str, str]]: ...


def _validate_updates(updates: Mapping[str, Any] | None) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in (updates or {}).items():
        if key not in _UPDATABLE:
            raise PatchValidationError(f"column '{key}' is not a legal fenced update")
        if key == "test_status" and isinstance(value, TestStatus):
            value = value.value
        clean[key] = value
    return clean


_STATUS_TIMESTAMP = {
    PatchStatus.ready: "ready_at",
    PatchStatus.approved: "decided_at",
    PatchStatus.denied: "decided_at",
    PatchStatus.draft_pr_created: "written_at",
}


@dataclass(frozen=True, slots=True)
class ApprovalDraft:
    """The immutable binding of the approval a ``ready -> approval_pending`` transition raises.

    Carries every field of the durable approval *except* its id (the store generates one only when a
    row is actually created), so the create-or-get primitive can bind/verify the approval to the
    exact run/call/actor/action without the caller pre-committing an id. A concurrent retry with the
    same ``(scope_id, run_id, call_id, run_attempt)`` returns the same approval. ``args`` is never
    hashed. ``batch_id`` defaults to the proposal id when empty (the partial-unique index the
    create-or-get relies on requires a non-empty batch id)."""

    run_id: str
    session_id: str
    tool: str
    args: dict[str, Any]
    call_id: str
    idempotency_key: str
    reason: str
    expires_at: datetime
    actor: str = ""
    action_hash: str = ""
    run_attempt: int = 0
    batch_id: str = ""


async def _apply_outbox_transition(
    conn: AsyncConnection | None,
    outbox: PatchProposalOutbox,
    proposal: PatchProposal,
    target: PatchStatus,
    scope_id: str | None,
    now: datetime,
) -> None:
    """Mirror a proposal transition onto its global dispatch pointer, in the caller's transaction.

    ``ready`` updates the coarse hint; ``approval_pending`` and every terminal state *delete* the
    pointer (there is no background work while awaiting a human, or once the proposal is done);
    ``approved`` re-creates the pointer with the ``approved`` hint (the worker drives writeback);
    ``writing`` (and any non-mirrored edge) leaves the ``approved`` pointer untouched. A pointer
    write (``approved``) fails closed without a validated ``scope_id``."""
    if target == PatchStatus.ready:
        await outbox.set_status_hint_in_connection(
            conn, proposal.id, PatchOutboxStatus.ready, now=now
        )
    elif target == PatchStatus.approved:
        if not scope_id:
            raise PatchValidationError("transition to approved requires scope_id for the outbox")
        await outbox.upsert_status_hint_in_connection(
            conn,
            proposal_id=proposal.id,
            org_id=proposal.org_id,
            scope_id=scope_id,
            status_hint=PatchOutboxStatus.approved,
            expires_at=proposal.expires_at,
            now=now,
        )
    elif target == PatchStatus.approval_pending or target in TERMINAL_STATUSES:
        await outbox.delete_in_connection(conn, proposal.id)


async def apply_transition_in_connection(
    conn: AsyncConnection,
    org_id: str,
    proposal_id: str,
    target: PatchStatus,
    *,
    expected_version: int | None = None,
    updates: Mapping[str, Any] | None = None,
    outbox: PatchProposalOutbox | None = None,
    scope_id: str | None = None,
    now: datetime,
) -> PatchProposal:
    """Apply a fenced proposal status transition (and mirror its pointer) on the caller's conn.

    The caller MUST already own the transaction and have set the org RLS GUC (and, for an
    ``approved`` pointer write, a validated ``scope_id`` + scope GUC). Selects the row ``FOR
    UPDATE``, enforces the optional ``expected_version`` fence, applies the legal transition
    (bumping the version + stamping the status timestamp only when the status actually changes),
    and mirrors the change onto the outbox pointer — all within the caller's unit of work. Lets
    ``IntegrityError`` propagate so the composing caller can roll back and translate it. This is the
    shared primitive behind :meth:`PostgresPatchProposalStore.transition` and atomic finalize."""
    clean = _validate_updates(updates)
    if outbox is not None and not isinstance(outbox, PostgresPatchProposalOutbox):
        raise PatchValidationError("PostgresPatchProposalStore requires a Postgres outbox")
    current = (
        (
            await conn.execute(
                text(
                    f"SELECT {_COLS} FROM patch_proposals "
                    "WHERE id = :id AND org_id = :org FOR UPDATE"
                ),
                {"id": proposal_id, "org": org_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if current is None:
        raise ProposalNotFoundInStore(f"proposal not found: {proposal_id}")
    proposal = _to_proposal(current)
    if expected_version is not None and proposal.version != expected_version:
        raise StaleProposalVersion(
            f"stale proposal version {expected_version} (current {proposal.version})"
        )
    assignments: dict[str, Any] = dict(clean)
    bump_version = proposal.status != target
    if bump_version:
        ensure_transition(proposal.status, target)
        ts_col = _STATUS_TIMESTAMP.get(target)
        if ts_col is not None and ts_col not in assignments:
            assignments[ts_col] = now
    set_parts = ["status = :__status", "updated_at = :__now"]
    bind: dict[str, Any] = {
        "__status": target.value,
        "__now": now,
        "id": proposal_id,
        "org": org_id,
    }
    if bump_version:
        set_parts.append("version = version + 1")
    for i, (key, value) in enumerate(assignments.items()):
        param = f"__u{i}"
        set_parts.append(f"{key} = :{param}")
        bind[param] = value
    row = (
        (
            await conn.execute(
                text(
                    "UPDATE patch_proposals SET "
                    + ", ".join(set_parts)
                    + f" WHERE id = :id AND org_id = :org RETURNING {_COLS}"
                ),
                bind,
            )
        )
        .mappings()
        .one()
    )
    updated = _to_proposal(row)
    # Mirror the transition onto the dispatch pointer in the SAME transaction, so the durable
    # status and its global pointer can never diverge (both commit / roll back together).
    if outbox is not None:
        await _apply_outbox_transition(conn, outbox, updated, target, scope_id, now)
    return updated


# --- In-memory -----------------------------------------------------------------------


class InMemoryPatchProposalStore:
    """Dependency-free store enforcing the same invariants as the schema (tests / lite)."""

    def __init__(self) -> None:
        self._rows: dict[str, PatchProposal] = {}
        self._by_idem: dict[tuple[str, str], str] = {}
        self._requests: dict[str, PatchGenerationRequestRecord] = {}

    def _txn_snapshot(
        self,
    ) -> tuple[
        dict[str, PatchProposal],
        dict[tuple[str, str], str],
        dict[str, PatchGenerationRequestRecord],
    ]:
        """Capture a rollback snapshot so an atomic outbox-coupled operation can undo a partial
        write on failure (mirrors the durable store's single-transaction all-or-nothing)."""
        return dict(self._rows), dict(self._by_idem), dict(self._requests)

    def _txn_restore(
        self,
        snapshot: tuple[
            dict[str, PatchProposal],
            dict[tuple[str, str], str],
            dict[str, PatchGenerationRequestRecord],
        ],
    ) -> None:
        self._rows = dict(snapshot[0])
        self._by_idem = dict(snapshot[1])
        self._requests = dict(snapshot[2])

    def _verify_generation_request_replay(
        self, existing_proposal_id: str, record: PatchGenerationRequestRecord
    ) -> None:
        """Fail closed on an idempotent replay whose durable request is absent or divergent.

        A committed proposal created *with* a request always has its immutable row; a replay that
        carries a request must match it by the request-identity fingerprint (which excludes the
        per-attempt ids, so a retry with a fresh proposal/run id still matches). A missing
        row is an unrecoverable conflict (never insert after the fact); a fingerprint mismatch is
        a request-binding conflict. The row is looked up by the *existing* proposal id (the retry's
        fresh id was discarded when the proposal deduped on its idempotency key).
        """
        existing = self._requests.get(existing_proposal_id)
        if existing is None:
            raise ProposalConflictError(
                "existing proposal has no durable generation request payload"
            )
        if existing.fingerprint != record.fingerprint:
            raise ProposalConflictError(
                "generation request fingerprint mismatch on idempotent replay"
            )

    async def create(
        self,
        *,
        proposal_id: str,
        org_id: str,
        project_id: str,
        run_id: str,
        run_attempt: int,
        agent_id: str,
        actor: str,
        base_ref: str,
        source_ref: str,
        task_digest: str,
        idempotency_key: str,
        fingerprint: str,
        expires_at: datetime,
        now: datetime | None = None,
        outbox: PatchProposalOutbox | None = None,
        scope_id: str | None = None,
        generation_request: PatchGenerationRequestRecord | None = None,
    ) -> tuple[PatchProposal, bool]:
        created_at = now or _now()
        key = (org_id, idempotency_key)
        existing_id = self._by_idem.get(key)
        if existing_id is not None:
            proposal = self._rows[existing_id]
            created = False
        else:
            proposal = PatchProposal(
                id=proposal_id,
                org_id=org_id,
                project_id=project_id,
                run_id=run_id,
                run_attempt=run_attempt,
                agent_id=agent_id,
                actor=actor,
                source_ref=source_ref,
                task_digest=task_digest,
                base_ref=base_ref,
                base_sha="",
                head_sha="",
                bundle_sha256="",
                diff_sha256="",
                changed_path_digest="",
                changed_files=0,
                test_status=TestStatus.unknown,
                status=PatchStatus.generating,
                version=1,
                idempotency_key=idempotency_key,
                fingerprint=fingerprint,
                expires_at=expires_at,
                created_at=created_at,
                updated_at=created_at,
            )
            created = True
        if generation_request is not None:
            _validate_generation_request(
                generation_request, proposal_id=proposal_id, org_id=org_id, scope_id=scope_id
            )
            if outbox is None:
                raise PatchValidationError(
                    "patch proposal create with a generation request requires an outbox"
                )
        if outbox is None:
            if created:
                self._rows[proposal_id] = proposal
                self._by_idem[key] = proposal_id
            return proposal, created
        # Outbox seam. The ``generating`` dispatch intent (and, when supplied, the immutable request
        # payload) is recorded atomically with the proposal so a proposal never becomes durable
        # without a discoverable pointer AND a reconstructable request (all roll back together) --
        # but ONLY on a genuine create. On the idempotent replay of an existing proposal the pointer
        # already reflects that proposal's current lifecycle stage (it may have been intentionally
        # deleted at ``approval_pending`` or a terminal state), so re-recording here would resurrect
        # a retired pointer; the existing path never touches the outbox (it only verifies the
        # persisted request, never re-inserts it). The seam contract (concrete outbox type + a
        # validated scope) is still enforced on both paths.
        if not isinstance(outbox, InMemoryPatchProposalOutbox):
            raise PatchValidationError("InMemoryPatchProposalStore requires an in-memory outbox")
        if not scope_id:
            raise PatchValidationError("patch proposal create with an outbox requires scope_id")
        if not created:
            if generation_request is not None:
                self._verify_generation_request_replay(proposal.id, generation_request)
            return proposal, created
        store_snap = self._txn_snapshot()
        outbox_snap = outbox._txn_snapshot()
        try:
            self._rows[proposal_id] = proposal
            self._by_idem[key] = proposal_id
            if generation_request is not None:
                self._requests[proposal_id] = generation_request
            await outbox.record_in_connection(
                None,
                proposal_id=proposal.id,
                org_id=org_id,
                scope_id=scope_id,
                expires_at=expires_at,
                status_hint=PatchOutboxStatus.generating,
                now=created_at,
            )
        except BaseException:
            self._txn_restore(store_snap)
            outbox._txn_restore(outbox_snap)
            raise
        return proposal, created

    async def get_generation_request(
        self, org_id: str, proposal_id: str, scope_id: str
    ) -> PatchGenerationRequestRecord | None:
        record = self._requests.get(proposal_id)
        if record is None:
            return None
        # Cross-org / cross-scope isolation (mirrors the Postgres RLS filter): a caller bound to a
        # different scope/org can never observe this request payload.
        if record.org_id != org_id or record.scope_id != scope_id:
            return None
        return record

    async def get(self, org_id: str, proposal_id: str) -> PatchProposal | None:
        row = self._rows.get(proposal_id)
        return row if row is not None and row.org_id == org_id else None

    async def get_by_run(self, org_id: str, run_id: str) -> PatchProposal | None:
        for row in self._rows.values():
            if row.org_id == org_id and row.run_id == run_id:
                return row
        return None

    async def list_for_project(
        self, org_id: str, project_id: str, *, limit: int = 100
    ) -> list[PatchProposal]:
        rows = [r for r in self._rows.values() if r.org_id == org_id and r.project_id == project_id]
        rows.sort(key=lambda r: r.created_at, reverse=True)
        return rows[:limit]

    async def transition(
        self,
        org_id: str,
        proposal_id: str,
        target: PatchStatus,
        *,
        expected_version: int | None = None,
        updates: Mapping[str, Any] | None = None,
        now: datetime | None = None,
        outbox: PatchProposalOutbox | None = None,
        scope_id: str | None = None,
    ) -> PatchProposal:
        clean = _validate_updates(updates)
        if outbox is not None and not isinstance(outbox, InMemoryPatchProposalOutbox):
            raise PatchValidationError("InMemoryPatchProposalStore requires an in-memory outbox")
        row = self._rows.get(proposal_id)
        if row is None or row.org_id != org_id:
            raise ProposalNotFoundInStore(f"proposal not found: {proposal_id}")
        if expected_version is not None and row.version != expected_version:
            raise StaleProposalVersion(
                f"stale proposal version {expected_version} (current {row.version})"
            )
        store_snap = self._txn_snapshot()
        outbox_snap = outbox._txn_snapshot() if outbox is not None else None
        try:
            updated = self._apply_transition(org_id, proposal_id, row, target, clean, now)
            if outbox is not None:
                await _apply_outbox_transition(
                    None, outbox, updated, target, scope_id, updated.updated_at
                )
        except BaseException:
            if outbox is not None:
                self._txn_restore(store_snap)
                if outbox_snap is not None:
                    outbox._txn_restore(outbox_snap)
            raise
        return updated

    def _apply_transition(
        self,
        org_id: str,
        proposal_id: str,
        row: PatchProposal,
        target: PatchStatus,
        clean: dict[str, Any],
        now: datetime | None,
    ) -> PatchProposal:
        moment = now or _now()
        # Idempotent re-apply of the same terminal/decision state: no version bump.
        if row.status == target:
            updated = replace(row, updated_at=moment, **_coerce(clean))
            self._rows[proposal_id] = updated
            return updated
        ensure_transition(row.status, target)
        # Branch-collision guard mirrors the partial unique index (live branch reservation).
        new_branch = clean.get("remote_branch", row.remote_branch)
        live = {PatchStatus.approved, PatchStatus.writing, PatchStatus.draft_pr_created}
        if new_branch and target in live:
            for other in self._rows.values():
                if (
                    other.id != proposal_id
                    and other.org_id == org_id
                    and other.project_id == row.project_id
                    and other.remote_branch == new_branch
                    and other.status in live
                ):
                    raise ProposalConflictError("remote branch already reserved for this project")
        fields: dict[str, Any] = dict(_coerce(clean))
        ts_col = _STATUS_TIMESTAMP.get(target)
        if ts_col is not None and ts_col not in fields:
            fields[ts_col] = moment
        updated = replace(row, status=target, version=row.version + 1, updated_at=moment, **fields)
        self._rows[proposal_id] = updated
        return updated

    async def transition_to_approval_pending(
        self,
        org_id: str,
        proposal_id: str,
        *,
        approvals: ApprovalStore,
        outbox: PatchProposalOutbox,
        scope_id: str,
        draft: ApprovalDraft,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, str, bool]:
        if not isinstance(outbox, InMemoryPatchProposalOutbox):
            raise PatchValidationError("InMemoryPatchProposalStore requires an in-memory outbox")
        if not isinstance(approvals, InMemoryApprovalStore):
            raise PatchValidationError(
                "InMemoryPatchProposalStore requires an in-memory approval store"
            )
        validate_scope_id(scope_id)
        moment = now or _now()
        batch_id = draft.batch_id or proposal_id
        row = self._rows.get(proposal_id)
        if row is None or row.org_id != org_id:
            raise ProposalNotFoundInStore(f"proposal not found: {proposal_id}")
        if expected_version is not None and row.version != expected_version:
            raise StaleProposalVersion(
                f"stale proposal version {expected_version} (current {row.version})"
            )
        store_snap = self._txn_snapshot()
        approvals_snap = approvals._txn_snapshot()
        outbox_snap = outbox._txn_snapshot()
        try:
            approval_id, created = await approvals.create_pending_or_get(
                scope_id=scope_id,
                run_id=draft.run_id,
                session_id=draft.session_id,
                tool=draft.tool,
                args=draft.args,
                call_id=draft.call_id,
                idempotency_key=draft.idempotency_key,
                reason=draft.reason,
                expires_at=draft.expires_at,
                org_id=org_id,
                actor=draft.actor,
                action_hash=draft.action_hash,
                run_attempt=draft.run_attempt,
                batch_id=batch_id,
            )
            # Idempotent fast path: a concurrent caller already advanced the proposal — reuse the
            # same approval, do not bump the version or re-delete the (already gone) pointer.
            if row.status == PatchStatus.approval_pending:
                return row, approval_id, created
            ensure_transition(row.status, PatchStatus.approval_pending)
            updated = replace(
                row,
                status=PatchStatus.approval_pending,
                version=row.version + 1,
                approval_id=approval_id,
                updated_at=moment,
            )
            self._rows[proposal_id] = updated
            await outbox.delete_in_connection(None, proposal_id)
            return updated, approval_id, created
        except BaseException:
            self._txn_restore(store_snap)
            approvals._txn_restore(approvals_snap)
            outbox._txn_restore(outbox_snap)
            raise

    async def _finalize_generation(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        run_op: Callable[[InMemoryRunStore, datetime], Awaitable[RunRecord]],
        target: PatchStatus,
        updates: Mapping[str, Any] | None,
        outbox: PatchProposalOutbox | None,
        scope_id: str,
        expected_version: int | None,
        now: datetime | None,
    ) -> tuple[PatchProposal, RunRecord]:
        """Atomic run-finalize + proposal-transition + pointer mirror, with all-or-nothing rollback.

        Snapshots the proposal store, the run store, and the outbox, runs the fenced run operation
        (terminalize/release) FIRST — so the proposal's mirrored ``cost_usd`` is derived from the
        run's authoritative cumulative charge — then applies the proposal transition + pointer, and
        restores all three on any failure (a lost run lease, a stale proposal version, a conflict).
        The durable analog composes the identical run + proposal + pointer writes in one PG txn."""
        if not isinstance(run_store, InMemoryRunStore):
            raise PatchValidationError("InMemoryPatchProposalStore requires an in-memory run store")
        if outbox is not None and not isinstance(outbox, InMemoryPatchProposalOutbox):
            raise PatchValidationError("InMemoryPatchProposalStore requires an in-memory outbox")
        validate_scope_id(scope_id)
        moment = now or _now()
        row = self._rows.get(proposal_id)
        if row is None or row.org_id != org_id:
            raise ProposalNotFoundInStore(f"proposal not found: {proposal_id}")
        if expected_version is not None and row.version != expected_version:
            raise StaleProposalVersion(
                f"stale proposal version {expected_version} (current {row.version})"
            )
        store_snap = self._txn_snapshot()
        run_snap = run_store._txn_snapshot()
        outbox_snap = outbox._txn_snapshot() if outbox is not None else None
        try:
            run_record = await run_op(run_store, moment)
            merged = dict(updates or {})
            merged["cost_usd"] = run_record.cost_usd
            clean = _validate_updates(merged)
            updated = self._apply_transition(org_id, proposal_id, row, target, clean, moment)
            if outbox is not None:
                await _apply_outbox_transition(
                    None, outbox, updated, target, scope_id, updated.updated_at
                )
        except BaseException:
            self._txn_restore(store_snap)
            run_store._txn_restore(run_snap)
            if outbox is not None and outbox_snap is not None:
                outbox._txn_restore(outbox_snap)
            raise
        return updated, run_record

    async def finalize_generation_ready(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        updates: Mapping[str, Any],
        result_ref: str,
        stop_reason: str,
        outbox: PatchProposalOutbox,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        async def _op(rs: InMemoryRunStore, moment: datetime) -> RunRecord:
            return await rs.terminalize(
                lease,
                status=RunStatus.completed,
                stop_reason=stop_reason,
                result_ref=result_ref,
                cost=cost,
                now=moment,
            )

        return await self._finalize_generation(
            org_id,
            proposal_id,
            run_store=run_store,
            run_op=_op,
            target=PatchStatus.ready,
            updates=updates,
            outbox=outbox,
            scope_id=scope_id,
            expected_version=expected_version,
            now=now,
        )

    async def finalize_generation_failed(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        error_kind: str,
        error_message: str,
        stop_reason: str,
        outbox: PatchProposalOutbox,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        async def _op(rs: InMemoryRunStore, moment: datetime) -> RunRecord:
            return await rs.terminalize(
                lease,
                status=RunStatus.failed,
                stop_reason=stop_reason,
                error_kind=error_kind,
                error_message=error_message,
                cost=cost,
                now=moment,
            )

        return await self._finalize_generation(
            org_id,
            proposal_id,
            run_store=run_store,
            run_op=_op,
            target=PatchStatus.failed,
            updates={"error_kind": error_kind, "error_message": error_message},
            outbox=outbox,
            scope_id=scope_id,
            expected_version=expected_version,
            now=now,
        )

    async def release_generation_transient(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        async def _op(rs: InMemoryRunStore, moment: datetime) -> RunRecord:
            return await rs.release(lease, to_status=RunStatus.queued, cost=cost, now=moment)

        return await self._finalize_generation(
            org_id,
            proposal_id,
            run_store=run_store,
            run_op=_op,
            target=PatchStatus.generating,
            updates=None,
            outbox=None,
            scope_id=scope_id,
            expected_version=expected_version,
            now=now,
        )

    async def expire_due(self, now: datetime, limit: int) -> list[tuple[str, str]]:
        """Cross-org TTL scan (maintenance-only; the runtime reconciler uses the outbox instead)."""
        due = [
            (r.org_id, r.id)
            for r in self._rows.values()
            if r.expires_at <= now
            and r.status
            in {
                PatchStatus.generating,
                PatchStatus.ready,
                PatchStatus.approval_pending,
                PatchStatus.approved,
                PatchStatus.writing,
            }
        ]
        return due[:limit]


def _coerce(clean: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in clean.items():
        if key == "test_status" and isinstance(value, str):
            out[key] = TestStatus(value)
        else:
            out[key] = value
    return out


# --- Postgres ------------------------------------------------------------------------


def _to_proposal(row: Mapping[Any, Any]) -> PatchProposal:
    return PatchProposal(
        id=row["id"],
        org_id=row["org_id"],
        project_id=row["project_id"],
        run_id=row["run_id"],
        run_attempt=row["run_attempt"],
        agent_id=row["agent_id"],
        actor=row["actor"],
        source_ref=row["source_ref"],
        task_digest=row["task_digest"],
        base_ref=row["base_ref"],
        base_sha=row["base_sha"],
        head_sha=row["head_sha"],
        bundle_sha256=row["bundle_sha256"],
        diff_sha256=row["diff_sha256"],
        changed_path_digest=row["changed_path_digest"],
        changed_files=row["changed_files"],
        test_status=TestStatus(row["test_status"]),
        status=PatchStatus(row["status"]),
        version=row["version"],
        approval_id=row["approval_id"],
        remote_branch=row["remote_branch"],
        pr_number=row["pr_number"],
        pr_url=row["pr_url"],
        pr_node_id=row["pr_node_id"],
        idempotency_key=row["idempotency_key"],
        fingerprint=row["fingerprint"],
        cost_usd=float(row["cost_usd"]),
        error_kind=row["error_kind"],
        error_message=row["error_message"],
        expires_at=row["expires_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        ready_at=row["ready_at"],
        decided_at=row["decided_at"],
        written_at=row["written_at"],
    )


class PostgresPatchProposalStore:
    """Durable proposal store; tenant-owned access sets ``app.org_id`` (RLS + FORCE)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def create(
        self,
        *,
        proposal_id: str,
        org_id: str,
        project_id: str,
        run_id: str,
        run_attempt: int,
        agent_id: str,
        actor: str,
        base_ref: str,
        source_ref: str,
        task_digest: str,
        idempotency_key: str,
        fingerprint: str,
        expires_at: datetime,
        now: datetime | None = None,
        outbox: PatchProposalOutbox | None = None,
        scope_id: str | None = None,
        generation_request: PatchGenerationRequestRecord | None = None,
    ) -> tuple[PatchProposal, bool]:
        created_at = now or _now()
        outbox_scope = ""
        if outbox is not None:
            if not isinstance(outbox, PostgresPatchProposalOutbox):
                raise PatchValidationError("PostgresPatchProposalStore requires a Postgres outbox")
            if not scope_id:
                raise PatchValidationError("patch proposal create with an outbox requires scope_id")
            outbox_scope = scope_id
        if generation_request is not None:
            _validate_generation_request(
                generation_request, proposal_id=proposal_id, org_id=org_id, scope_id=scope_id
            )
            if outbox is None:
                raise PatchValidationError(
                    "patch proposal create with a generation request requires an outbox"
                )
        params = {
            "id": proposal_id,
            "org": org_id,
            "pid": project_id,
            "rid": run_id,
            "attempt": run_attempt,
            "agent": agent_id,
            "actor": actor,
            "base_ref": base_ref,
            "source_ref": source_ref,
            "task_digest": task_digest,
            "idem": idempotency_key,
            "fp": fingerprint,
            "expires": expires_at,
            "now": created_at,
        }
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                if generation_request is not None:
                    # The scoped row is written (WITH CHECK) / verified (USING) under its own
                    # scope RLS, so set the scope GUC alongside the org GUC in this transaction.
                    await conn.execute(_SET_SCOPE, {"scope": generation_request.scope_id})
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO patch_proposals "
                                "(id, org_id, project_id, run_id, run_attempt, agent_id, actor, "
                                " source_ref, task_digest, base_ref, idempotency_key, fingerprint, "
                                " created_at, updated_at, expires_at) "
                                "VALUES (:id, :org, :pid, :rid, :attempt, :agent, :actor, "
                                " :source_ref, :task_digest, :base_ref, :idem, :fp, "
                                " :now, :now, :expires) "
                                "ON CONFLICT (org_id, idempotency_key) DO NOTHING "
                                f"RETURNING {_COLS}"
                            ),
                            params,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is not None:
                    proposal, created = _to_proposal(row), True
                else:
                    existing = (
                        (
                            await conn.execute(
                                text(
                                    f"SELECT {_COLS} FROM patch_proposals "
                                    "WHERE org_id = :org AND idempotency_key = :idem"
                                ),
                                {"org": org_id, "idem": idempotency_key},
                            )
                        )
                        .mappings()
                        .one()
                    )
                    proposal, created = _to_proposal(existing), False
                # On a genuine create, record the immutable request payload (when supplied) AND the
                # ``generating`` dispatch pointer in this SAME txn (all commit or roll back),
                # so a proposal never becomes durable without a reconstructable request and a
                # discoverable pointer. The idempotent replay of an existing one never re-records
                # either: the pointer already reflects that proposal's current lifecycle stage (it
                # may have been deleted at ``approval_pending`` or a terminal state) and
                # re-inserting would resurrect a retired pointer; the request row is only *verified*
                # (fail closed) against the persisted one, never inserted after the fact.
                if created:
                    if generation_request is not None:
                        await insert_generation_request_in_connection(conn, generation_request)
                    if outbox is not None:
                        await outbox.record_in_connection(
                            conn,
                            proposal_id=proposal.id,
                            org_id=org_id,
                            scope_id=outbox_scope,
                            expires_at=expires_at,
                            status_hint=PatchOutboxStatus.generating,
                            now=created_at,
                        )
                elif generation_request is not None:
                    await self._verify_stored_generation_request(
                        conn,
                        proposal_id=proposal.id,
                        org_id=org_id,
                        fingerprint=generation_request.fingerprint,
                    )
                return proposal, created
        except IntegrityError as exc:  # composite FK to projects(id, org_id) violated
            raise ProposalConflictError("proposal binding is invalid for this org/project") from exc

    @staticmethod
    async def _verify_stored_generation_request(
        conn: AsyncConnection, *, proposal_id: str, org_id: str, fingerprint: str
    ) -> None:
        """Fail closed if an idempotent replay's request diverges from the persisted one.

        Looked up by the *existing* proposal id under the scope GUC set by ``create`` (RLS scopes
        the row). A missing row is an unrecoverable conflict (never inserted after the fact); a
        fingerprint mismatch is a request-binding conflict. The identity fingerprint excludes the
        per-attempt ids, so a legitimate retry (fresh proposal/run id) still matches.
        """
        existing_fp = await conn.scalar(
            text(
                "SELECT fingerprint FROM patch_generation_requests "
                "WHERE proposal_id = :pid AND org_id = :org"
            ),
            {"pid": proposal_id, "org": org_id},
        )
        if existing_fp is None:
            raise ProposalConflictError(
                "existing proposal has no durable generation request payload"
            )
        if existing_fp != fingerprint:
            raise ProposalConflictError(
                "generation request fingerprint mismatch on idempotent replay"
            )

    async def get_generation_request(
        self, org_id: str, proposal_id: str, scope_id: str
    ) -> PatchGenerationRequestRecord | None:
        async with self._engine.begin() as conn:
            # Scope RLS isolates the row: a caller bound to a different scope reads nothing (the
            # ``WHERE org_id`` predicate is belt-and-braces behind the composite FK).
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            row = (
                (
                    await conn.execute(
                        _SELECT_GENERATION_REQUEST, {"pid": proposal_id, "org": org_id}
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        try:
            return PatchGenerationRequestRecord.from_stored(
                proposal_id=row["proposal_id"],
                org_id=row["org_id"],
                scope_id=row["scope_id"],
                payload=row["payload"],
                fingerprint=row["fingerprint"],
                created_at=row["created_at"],
            )
        except PatchValidationError:
            # A persisted row that no longer reconstructs (schema drift / tampered digest) fails
            # closed to ``None`` -- the caller (reconciler) treats an unreconstructable generating
            # proposal as terminal corruption rather than re-dispatching an unverified request.
            logger.warning(
                "patch generation request failed fail-closed reconstruction proposal=%s",
                proposal_id,
            )
            return None

    async def get(self, org_id: str, proposal_id: str) -> PatchProposal | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLS} FROM patch_proposals WHERE id = :id AND org_id = :org"
                        ),
                        {"id": proposal_id, "org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_proposal(row)

    async def get_by_run(self, org_id: str, run_id: str) -> PatchProposal | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLS} FROM patch_proposals "
                            "WHERE run_id = :rid AND org_id = :org "
                            "ORDER BY created_at DESC LIMIT 1"
                        ),
                        {"rid": run_id, "org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_proposal(row)

    async def list_for_project(
        self, org_id: str, project_id: str, *, limit: int = 100
    ) -> list[PatchProposal]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLS} FROM patch_proposals "
                            "WHERE project_id = :pid AND org_id = :org "
                            "ORDER BY created_at DESC LIMIT :lim"
                        ),
                        {"pid": project_id, "org": org_id, "lim": limit},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_proposal(r) for r in rows]

    async def transition(
        self,
        org_id: str,
        proposal_id: str,
        target: PatchStatus,
        *,
        expected_version: int | None = None,
        updates: Mapping[str, Any] | None = None,
        now: datetime | None = None,
        outbox: PatchProposalOutbox | None = None,
        scope_id: str | None = None,
    ) -> PatchProposal:
        moment = now or _now()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                return await apply_transition_in_connection(
                    conn,
                    org_id,
                    proposal_id,
                    target,
                    expected_version=expected_version,
                    updates=updates,
                    outbox=outbox,
                    scope_id=scope_id,
                    now=moment,
                )
        except IntegrityError as exc:  # branch-collision partial unique index
            raise ProposalConflictError("remote branch already reserved for this project") from exc

    async def transition_to_approval_pending(
        self,
        org_id: str,
        proposal_id: str,
        *,
        approvals: ApprovalStore,
        outbox: PatchProposalOutbox,
        scope_id: str,
        draft: ApprovalDraft,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, str, bool]:
        if not isinstance(outbox, PostgresPatchProposalOutbox):
            raise PatchValidationError("PostgresPatchProposalStore requires a Postgres outbox")
        if not isinstance(approvals, PostgresApprovalStore):
            raise PatchValidationError(
                "PostgresPatchProposalStore requires a Postgres approval store"
            )
        validate_scope_id(scope_id)
        moment = now or _now()
        batch_id = draft.batch_id or proposal_id
        approval_id = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            # 1) Lock the proposal under its org RLS context.
            await conn.execute(_SET_ORG, {"org": org_id})
            current = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_COLS} FROM patch_proposals "
                            "WHERE id = :id AND org_id = :org FOR UPDATE"
                        ),
                        {"id": proposal_id, "org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if current is None:
                raise ProposalNotFoundInStore(f"proposal not found: {proposal_id}")
            proposal = _to_proposal(current)
            if expected_version is not None and proposal.version != expected_version:
                raise StaleProposalVersion(
                    f"stale proposal version {expected_version} (current {proposal.version})"
                )
            # 2) create-or-get the durable approval under the scope RLS context (same transaction;
            #    the org GUC stays set — the two GUCs coexist for their respective tables).
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            resolved_id, created = await insert_pending_or_get_in_transaction(
                conn,
                id=approval_id,
                scope_id=scope_id,
                run_id=draft.run_id,
                session_id=draft.session_id,
                tool=draft.tool,
                args=draft.args,
                call_id=draft.call_id,
                idempotency_key=draft.idempotency_key,
                reason=draft.reason,
                expires_at=draft.expires_at,
                org_id=org_id,
                actor=draft.actor,
                action_hash=draft.action_hash,
                run_attempt=draft.run_attempt,
                batch_id=batch_id,
            )
            # 3) Idempotent fast path: a concurrent caller already advanced the proposal. Reuse the
            #    same approval and do NOT bump the version or re-delete the (already gone) pointer.
            if proposal.status == PatchStatus.approval_pending:
                return proposal, resolved_id, created
            ensure_transition(proposal.status, PatchStatus.approval_pending)
            # 4) Advance the proposal (+1 version, bound to the approval) ...
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE patch_proposals SET status = :status, "
                            "version = version + 1, approval_id = :aid, updated_at = :now "
                            f"WHERE id = :id AND org_id = :org RETURNING {_COLS}"
                        ),
                        {
                            "status": PatchStatus.approval_pending.value,
                            "aid": resolved_id,
                            "now": moment,
                            "id": proposal_id,
                            "org": org_id,
                        },
                    )
                )
                .mappings()
                .one()
            )
            updated = _to_proposal(row)
            # 5) ... and delete the dispatch pointer (no background work while awaiting a human).
            await outbox.delete_in_connection(conn, proposal_id)
            return updated, resolved_id, created

    async def _finalize_generation(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        run_op: Callable[[AsyncConnection, datetime], Awaitable[RunRecord]],
        target: PatchStatus,
        updates: Mapping[str, Any] | None,
        outbox: PatchProposalOutbox | None,
        scope_id: str,
        expected_version: int | None,
        now: datetime | None,
    ) -> tuple[PatchProposal, RunRecord]:
        """Atomic run-finalize + proposal-transition + pointer mirror in ONE engine transaction.

        Sets both the org (``patch_proposals`` RLS) and scope (``runs`` RLS + any pointer write)
        GUCs — proven to coexist by :meth:`transition_to_approval_pending` — then terminalizes or
        releases the run FIRST (a fenced in-connection UPDATE returning its authoritative cumulative
        cost), mirrors that cost onto the proposal's ``cost_usd``, and applies the proposal
        transition + its dispatch pointer through :func:`apply_transition_in_connection`. A lost run
        lease raises :class:`~keel_core.runs.RunLeaseLostError`; the whole transaction rolls back so
        a crash never strands a terminal run behind a ``generating`` proposal (or the reverse)."""
        if not isinstance(run_store, PostgresRunStore):
            raise PatchValidationError("PostgresPatchProposalStore requires a Postgres run store")
        if run_store.scope_id != scope_id:
            raise PatchValidationError("run store scope does not match the generation scope")
        if outbox is not None and not isinstance(outbox, PostgresPatchProposalOutbox):
            raise PatchValidationError("PostgresPatchProposalStore requires a Postgres outbox")
        validate_scope_id(scope_id)
        moment = now or _now()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                await conn.execute(_SET_SCOPE, {"scope": scope_id})
                run_record = await run_op(conn, moment)
                merged = dict(updates or {})
                merged["cost_usd"] = run_record.cost_usd
                updated = await apply_transition_in_connection(
                    conn,
                    org_id,
                    proposal_id,
                    target,
                    expected_version=expected_version,
                    updates=merged,
                    outbox=outbox,
                    scope_id=scope_id,
                    now=moment,
                )
                return updated, run_record
        except IntegrityError as exc:  # branch-collision partial unique index (ready path)
            raise ProposalConflictError("remote branch already reserved for this project") from exc

    async def finalize_generation_ready(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        updates: Mapping[str, Any],
        result_ref: str,
        stop_reason: str,
        outbox: PatchProposalOutbox,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        async def _op(conn: AsyncConnection, moment: datetime) -> RunRecord:
            return await terminalize_in_transaction(
                conn,
                scope_id,
                lease,
                status=RunStatus.completed,
                stop_reason=stop_reason,
                result_ref=result_ref,
                cost=cost,
                now=moment,
            )

        return await self._finalize_generation(
            org_id,
            proposal_id,
            run_store=run_store,
            run_op=_op,
            target=PatchStatus.ready,
            updates=updates,
            outbox=outbox,
            scope_id=scope_id,
            expected_version=expected_version,
            now=now,
        )

    async def finalize_generation_failed(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        error_kind: str,
        error_message: str,
        stop_reason: str,
        outbox: PatchProposalOutbox,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        async def _op(conn: AsyncConnection, moment: datetime) -> RunRecord:
            return await terminalize_in_transaction(
                conn,
                scope_id,
                lease,
                status=RunStatus.failed,
                stop_reason=stop_reason,
                error_kind=error_kind,
                error_message=error_message,
                cost=cost,
                now=moment,
            )

        return await self._finalize_generation(
            org_id,
            proposal_id,
            run_store=run_store,
            run_op=_op,
            target=PatchStatus.failed,
            updates={"error_kind": error_kind, "error_message": error_message},
            outbox=outbox,
            scope_id=scope_id,
            expected_version=expected_version,
            now=now,
        )

    async def release_generation_transient(
        self,
        org_id: str,
        proposal_id: str,
        *,
        run_store: RunStore,
        lease: RunLease,
        cost: RunCost,
        scope_id: str,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> tuple[PatchProposal, RunRecord]:
        async def _op(conn: AsyncConnection, moment: datetime) -> RunRecord:
            return await release_in_transaction(
                conn, scope_id, lease, to_status=RunStatus.queued, cost=cost, now=moment
            )

        return await self._finalize_generation(
            org_id,
            proposal_id,
            run_store=run_store,
            run_op=_op,
            target=PatchStatus.generating,
            updates=None,
            outbox=None,
            scope_id=scope_id,
            expected_version=expected_version,
            now=now,
        )

    async def expire_due(self, now: datetime, limit: int) -> list[tuple[str, str]]:
        """Cross-org scan for proposals past their TTL (maintenance reconciler path only).

        Maintenance-only: the runtime reconciler discovers work through the global dispatch outbox
        (:class:`~keel_core.patch.outbox.PatchProposalOutbox`), never this privileged scan. RLS is
        disabled for the scan (the maintenance role owns the table); only ``(org_id, id)`` pointers
        are returned so the caller re-enters each org's RLS context to expire them.
        """
        async with self._engine.begin() as conn:
            await conn.execute(text("SET LOCAL row_security = off"))
            rows = (
                await conn.execute(
                    text(
                        "SELECT org_id, id FROM patch_proposals "
                        "WHERE expires_at <= :now AND status IN "
                        "('generating','ready','approval_pending','approved','writing') "
                        "ORDER BY expires_at LIMIT :lim"
                    ),
                    {"now": now, "lim": limit},
                )
            ).all()
        return [(r.org_id, r.id) for r in rows]


__all__ = [
    "ApprovalDraft",
    "InMemoryPatchProposalStore",
    "PatchProposalStore",
    "PostgresPatchProposalStore",
    "ProposalConflictError",
    "ProposalNotFoundInStore",
    "StaleProposalVersion",
]
