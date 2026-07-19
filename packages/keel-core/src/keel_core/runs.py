"""Durable, worker-owned interactive runs — the run state machine + repository (M3.6).

An interactive run is no longer a server-local ``asyncio.Task``: it is a **durable row**
that a worker claims under a fenced lease and drives to a named terminal state. The
server *admits* a run (persisting a row + the user turn) and streams the event log; a
worker *owns* execution. This mirrors the durable-jobs substrate (``keel_core.jobs``) but
is specialized for interactive/agent runs so admission, interrupt, steering, and approval
suspension survive server **and** worker restarts and are safe under N racing workers.

Load-bearing invariants proven here:

* **Atomic claim + fenced writes** — a single conditional ``UPDATE`` assigns a unique
  ``lease_token``; every later mutation is gated on that token (and bumps ``version``), so
  a superseded owner (lease expired, another worker reclaimed) can never write.
* **Reclaim after expiry** — a run whose lease lapsed is reclaimable by any worker; the
  attempt counter advances so replay is bounded.
* **Idempotent admission** — ``(scope_id, org_id, actor, idempotency_key)`` is unique, so
  retrying a user request returns the same run instead of creating a duplicate message/run;
  and admission identity is namespaced by tenant + actor so one caller cannot collide with
  (or hijack) another's run via a shared idempotency key. An immutable ``fingerprint`` binds
  the admission to its exact org/actor/agent/session/surface/content — a retry that reuses
  the identity but mismatches the binding is a **conflict**, never a silent repair.
* **Idempotent terminalization** — terminalizing an already-terminal run is a no-op that
  returns the authoritative row (fail-safe under retries/races).
* **Fail-closed transitions** — an illegal or stale-version transition raises rather than
  silently corrupting state.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.run_dispatch import (
    PostgresRunDispatchOutbox,
    RunDispatchOutbox,
    record_intent_in_connection,
)
from keel_core.types import RunId, ScopeId, SessionId

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


def _now() -> datetime:
    return datetime.now(UTC)


class RunSurface(StrEnum):
    """The surface a run was admitted from (audit/trace tag; never trusted for authz)."""

    web = "web"
    im = "im"
    api = "api"
    schedule = "schedule"


class RunStatus(StrEnum):
    """The durable lifecycle state of an interactive run."""

    admitted = "admitted"  # row + user turn persisted; not yet dispatched to the queue
    queued = "queued"  # dispatchable; awaiting a worker claim
    running = "running"  # a worker owns the lease and is executing the agent loop
    waiting_approval = "waiting_approval"  # suspended on a durable tool approval
    completed = "completed"  # named success termination
    failed = "failed"  # named failure termination (error carried on the row)
    cancelled = "cancelled"  # a durable cancel request was honored
    interrupted = "interrupted"  # a durable interrupt request was honored
    expired = "expired"  # admission/lease deadline passed before completion


TERMINAL_STATUSES: frozenset[RunStatus] = frozenset(
    {
        RunStatus.completed,
        RunStatus.failed,
        RunStatus.cancelled,
        RunStatus.interrupted,
        RunStatus.expired,
    }
)

# Legal state transitions. Anything not listed here fails closed (RunStateError).
_TRANSITIONS: Mapping[RunStatus, frozenset[RunStatus]] = {
    RunStatus.admitted: frozenset(
        {RunStatus.queued, RunStatus.running, RunStatus.cancelled, RunStatus.expired}
    ),
    RunStatus.queued: frozenset({RunStatus.running, RunStatus.cancelled, RunStatus.expired}),
    RunStatus.running: frozenset(
        {
            RunStatus.waiting_approval,
            RunStatus.queued,  # cooperative release / reclaim back to the queue
            RunStatus.completed,
            RunStatus.failed,
            RunStatus.cancelled,
            RunStatus.interrupted,
            RunStatus.expired,
        }
    ),
    RunStatus.waiting_approval: frozenset(
        {
            RunStatus.queued,  # approval resolved -> requeue for resume
            RunStatus.running,  # direct in-worker resume
            RunStatus.cancelled,
            RunStatus.failed,
            RunStatus.interrupted,
            RunStatus.expired,
        }
    ),
    # Terminal states have no outgoing edges.
    RunStatus.completed: frozenset(),
    RunStatus.failed: frozenset(),
    RunStatus.cancelled: frozenset(),
    RunStatus.interrupted: frozenset(),
    RunStatus.expired: frozenset(),
}


def can_transition(current: RunStatus, target: RunStatus) -> bool:
    """Whether ``current -> target`` is a legal edge in the run state machine."""
    return target in _TRANSITIONS.get(current, frozenset())


class RunStateError(RuntimeError):
    """Raised on an illegal or stale-version run transition (fail closed)."""


class RunLeaseLostError(RuntimeError):
    """Raised when a fenced write is attempted with a lost/superseded lease."""

    def __init__(self, run_id: RunId) -> None:
        super().__init__(f"run lease lost: {run_id}")
        self.run_id = run_id


class RunAdmissionConflict(RuntimeError):
    """Raised when a retried admission reuses an identity but mismatches its binding.

    The unique admission identity is ``(scope_id, org_id, actor, idempotency_key)``. A retry
    that presents the same identity but a *different* immutable fingerprint (a different
    agent / session / surface / content) is an attacker or client bug — fail closed with a
    conflict rather than repair the half-admitted row using the caller-supplied values.
    """

    def __init__(self, run_id: RunId) -> None:
        super().__init__(f"run admission conflict: {run_id}")
        self.run_id = run_id


class RunControlKind(StrEnum):
    """A durable, worker-consumed control signal against a run."""

    interrupt = "interrupt"
    cancel = "cancel"
    steer = "steer"


def action_hash(tool: str, args: Mapping[str, Any]) -> str:
    """Stable hash binding an approval decision to an exact tool + argument set.

    Canonical JSON (sorted keys) so the same action always hashes identically and a
    decision cannot be replayed against different arguments (stale-approval defense).
    """
    canonical = json.dumps(
        {"tool": tool, "args": args}, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def admission_fingerprint(
    *,
    org_id: str,
    actor: str,
    agent_id: str,
    session_id: str,
    surface: str,
    content: str,
    model: str | None = None,
) -> str:
    """Immutable fingerprint of an admission request (M3.6 blocker 3).

    Covers the full tenant/actor binding, the selected agent, the target session, the
    surface, the selected model, and a hash of the normalized admission content. Two
    admissions that share an idempotency identity must present an identical fingerprint; a
    mismatch is a conflict. Canonical JSON with sorted keys makes the hash stable across
    equivalent inputs."""
    return _fingerprint(
        org_id=org_id,
        actor=actor,
        agent_id=agent_id,
        session_id=session_id,
        surface=surface,
        content=content,
        model=model,
        include_model=True,
    )


def legacy_admission_fingerprint(
    *,
    org_id: str,
    actor: str,
    agent_id: str,
    session_id: str,
    surface: str,
    content: str,
) -> str:
    """The *pre-model* admission fingerprint (deployment-rollout compatibility).

    Runs admitted before the model was folded into the fingerprint (M3.6) stored a hash whose
    canonical JSON omitted the ``model`` field **entirely** — not ``"model":""``. On retry we
    accept a match against this precise legacy form so an in-flight run admitted by the old
    binary can be idempotently completed after a deploy, instead of being falsely rejected as a
    :class:`RunAdmissionConflict`. A model-aware row's stored hash always includes the ``model``
    field, so it can never equal this value — a model change therefore cannot hijack a
    new-model row via the legacy path (only genuinely legacy rows match)."""
    return _fingerprint(
        org_id=org_id,
        actor=actor,
        agent_id=agent_id,
        session_id=session_id,
        surface=surface,
        content=content,
        model=None,
        include_model=False,
    )


def _fingerprint(
    *,
    org_id: str,
    actor: str,
    agent_id: str,
    session_id: str,
    surface: str,
    content: str,
    model: str | None,
    include_model: bool,
) -> str:
    payload: dict[str, str] = {
        "org_id": org_id,
        "actor": actor,
        "agent_id": agent_id,
        "session_id": session_id,
        "surface": surface,
        "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
    }
    # The model key is present for model-aware fingerprints and absent for the legacy form, so
    # the two hashes are structurally distinct and never collide across the model boundary.
    if include_model:
        payload["model"] = model or ""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _fingerprint_matches(stored: str, current: str, legacy: str = "") -> bool:
    """Whether a retry's fingerprint is compatible with an existing run's stored one.

    An empty ``stored`` or empty ``current`` fingerprint is treated as "unverifiable" and
    accepted (back-compat with rows/callers that never carried a fingerprint). Otherwise the
    stored hash must equal the current model-aware fingerprint **or** the precise legacy
    pre-model fingerprint (deployment rollout). Because a model-aware stored hash always
    encodes the ``model`` field it can never equal ``legacy``, so a changed model can never be
    laundered through the legacy path onto a new-model row."""
    if not current or not stored:
        return True
    if stored == current:
        return True
    return bool(legacy) and stored == legacy


@dataclass(frozen=True)
class RunBudgetSpec:
    """The caps + running cost/usage summary carried on a durable run."""

    max_iterations: int = 20
    token_budget: int | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float = 0.0


@dataclass
class RunRecord:
    """A durable interactive run row."""

    id: RunId
    scope_id: ScopeId
    org_id: str
    actor: str
    agent_id: str
    session_id: SessionId
    surface: str
    idempotency_key: str
    status: RunStatus
    attempt: int
    version: int
    max_iterations: int
    token_budget: int | None
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    created_at: datetime
    updated_at: datetime
    expires_at: datetime
    stop_reason: str | None = None
    worker_id: str | None = None
    lease_token: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result_ref: str | None = None
    error_kind: str | None = None
    error_message: str | None = None
    # Explicit durable resume/checkpoint marker: set when an approval resolves and the run is
    # requeued (waiting_approval -> queued) so the claiming worker calls loop.resume() instead
    # of inferring resume from a pre-claim status that requeue has already overwritten.
    resume_requested: bool = False
    # Durable admission progress: the user turn is persisted before the run is dispatched.
    # Reconciliation must never dispatch a run whose prompt was not durably admitted.
    prompt_persisted: bool = False
    # Cumulative agent-loop iterations consumed across every claim/suspend/resume attempt.
    # Persisted so ``max_iterations`` bounds the *whole* run: a resumed run resumes the
    # counter rather than minting a fresh iteration budget (M3.6 cumulative-budget invariant).
    iterations: int = 0
    # Immutable admission fingerprint (org/actor/agent/session/surface/content hash). A
    # retried admission that reuses the identity but mismatches this is a conflict.
    fingerprint: str = ""
    # Durable suspension/checkpoint intent (M3.6 crash boundary). Set — fenced by the active
    # lease — the instant a worker begins persisting an approval batch, BEFORE the
    # ``running -> waiting_approval`` release. If the worker crashes in that window the row is
    # still ``running`` with a durable approval bound to its attempt; without this marker the
    # lease-expiry reclaim advances the attempt with ``resume=False`` and silently discards
    # the (later approved) action. The marker makes the reclaiming worker *resume* instead.
    suspend_checkpoint: bool = False
    # The source attempt whose approval batch the current checkpoint owns. Preserved across a
    # crash reclaim (the lease attempt advances for fencing, this does not) so an approval
    # decision bound to the suspended source attempt stays applicable exactly once on resume,
    # while stale approvals from a different/older checkpoint attempt remain rejected.
    checkpoint_attempt: int = 0
    # The exact batch id the current checkpoint owns. Written — fenced by the lease — in the
    # SAME transaction as the batch's approval rows + events, so resume's reconstruction repair
    # adopts a durable approval row only when its ``batch_id`` (and source attempt) match the
    # checkpoint exactly; a foreign/older/newer batch fails closed. Empty for pre-checkpoint or
    # never-suspended rows (older-build repair then falls back to attempt + action-hash match).
    checkpoint_batch_id: str = ""

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def budget(self) -> RunBudgetSpec:
        return RunBudgetSpec(
            max_iterations=self.max_iterations,
            token_budget=self.token_budget,
            prompt_tokens=self.prompt_tokens,
            completion_tokens=self.completion_tokens,
            cost_usd=self.cost_usd,
        )


@dataclass(frozen=True)
class RunLease:
    """A fenced claim on a run. The ``token`` fences every subsequent write."""

    run_id: RunId
    scope_id: ScopeId
    org_id: str
    token: str
    worker_id: str
    attempt: int
    agent_id: str
    session_id: SessionId
    lease_seconds: int
    # Whether the claimed run should be resumed (loop.resume) rather than started fresh:
    # true when it was reclaimed mid-approval or explicitly requeued after an approval
    # resolution. Captured atomically at claim time so it cannot be inferred incorrectly.
    resume: bool = False
    max_iterations: int = 20
    token_budget: int | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    # Cumulative iterations already consumed by prior attempts — the resume loop seeds its
    # counter here so ``max_iterations`` bounds the run across suspend/resume (never reset).
    iterations_used: int = 0


@dataclass(frozen=True)
class RunControl:
    """A durable control signal (interrupt/cancel/steer) awaiting worker consumption."""

    id: str
    scope_id: ScopeId
    run_id: RunId
    kind: RunControlKind
    payload: Mapping[str, Any]
    requested_by: str
    requested_at: datetime


@dataclass(frozen=True)
class RunCost:
    """A cost/usage delta recorded on a fenced transition."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float = 0.0
    # Iteration delta consumed this attempt; accrued onto the run's cumulative counter so a
    # suspend/resume cannot reset the iteration budget (M3.6).
    iterations: int = 0


@runtime_checkable
class RunStore(Protocol):
    """Durable, scope-bound store of interactive runs + control signals."""

    async def create(
        self,
        *,
        run_id: RunId,
        scope_id: ScopeId,
        org_id: str,
        actor: str,
        agent_id: str,
        session_id: SessionId,
        surface: str,
        idempotency_key: str,
        budget: RunBudgetSpec,
        expires_at: datetime,
        fingerprint: str = "",
        legacy_fingerprint: str = "",
        now: datetime | None = None,
    ) -> tuple[RunRecord, bool]: ...

    async def admit(
        self,
        *,
        run_id: RunId,
        scope_id: ScopeId,
        org_id: str,
        actor: str,
        agent_id: str,
        session_id: SessionId,
        surface: str,
        idempotency_key: str,
        budget: RunBudgetSpec,
        expires_at: datetime,
        fingerprint: str = "",
        legacy_fingerprint: str = "",
        now: datetime | None = None,
    ) -> RunRecord: ...

    async def get(self, run_id: RunId) -> RunRecord | None: ...

    async def mark_queued(self, run_id: RunId, *, now: datetime | None = None) -> bool: ...

    async def mark_queued_with_dispatch_intent(
        self,
        run_id: RunId,
        scope_id: ScopeId,
        outbox: RunDispatchOutbox,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Transition ``admitted -> queued`` and record the dispatch intent **atomically**.

        The queued transition and the global dispatch intent commit in one unit (one Postgres
        transaction on the same engine), so a committed ``queued`` run is always accompanied by
        a discoverable cross-scope dispatch pointer. If recording the intent fails the whole
        unit rolls back — the run stays ``admitted`` (recovered by admission retry / reconcile)
        rather than becoming an undiscoverable queued run. Returns whether *this* caller won the
        transition (``mark_queued`` semantics).
        """
        ...

    async def mark_prompt_persisted(
        self, run_id: RunId, *, now: datetime | None = None
    ) -> bool: ...

    async def requeue(self, run_id: RunId, *, now: datetime | None = None) -> bool: ...

    async def mark_checkpoint(
        self, lease: RunLease, *, batch_id: str = "", now: datetime | None = None
    ) -> bool: ...

    async def claim(
        self,
        run_id: RunId,
        *,
        worker_id: str,
        now: datetime | None = None,
        lease_seconds: int,
    ) -> RunLease | None: ...

    async def heartbeat(self, lease: RunLease, now: datetime | None = None) -> bool: ...

    async def renew(
        self, lease: RunLease, *, lease_seconds: int, now: datetime | None = None
    ) -> bool: ...

    async def release(
        self,
        lease: RunLease,
        *,
        to_status: RunStatus,
        now: datetime | None = None,
        cost: RunCost | None = None,
    ) -> RunRecord: ...

    async def terminalize(
        self,
        lease: RunLease,
        *,
        status: RunStatus,
        stop_reason: str,
        now: datetime | None = None,
        cost: RunCost | None = None,
        result_ref: str | None = None,
        error_kind: str | None = None,
        error_message: str | None = None,
    ) -> RunRecord: ...

    async def request_control(
        self,
        run_id: RunId,
        *,
        kind: RunControlKind,
        requested_by: str,
        payload: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool: ...

    async def consume_control(
        self, run_id: RunId, *, now: datetime | None = None
    ) -> list[RunControl]: ...

    async def peek_control(self, run_id: RunId) -> list[RunControl]: ...

    async def ack_controls(self, control_ids: list[str], *, now: datetime | None = None) -> int: ...

    async def undispatched(self, now: datetime, limit: int) -> list[str]: ...

    async def redispatchable(
        self, now: datetime, limit: int, *, grace_seconds: int = 0
    ) -> list[str]: ...

    async def surface_pending_dispatch(
        self,
        surface: str,
        now: datetime,
        limit: int,
        *,
        grace_seconds: int = 0,
    ) -> list[str]: ...

    async def reclaimable(self, now: datetime, limit: int) -> list[str]: ...

    async def waiting_approval_ids(self, limit: int) -> list[str]: ...

    async def expire_due(self, now: datetime, limit: int) -> list[str]: ...


def _ensure_transition(record: RunRecord, target: RunStatus) -> None:
    if record.is_terminal:
        raise RunStateError(f"run {record.id} is terminal ({record.status}); cannot -> {target}")
    if not can_transition(record.status, target):
        raise RunStateError(f"illegal run transition {record.status} -> {target}")


# ----------------------------------------------------------------------------------------
# In-memory store — deterministic double for unit tests and single-process dev.
# ----------------------------------------------------------------------------------------
@dataclass
class InMemoryRunStore:
    """Deterministic in-memory RunStore (tests + single-process dev)."""

    _rows: dict[RunId, RunRecord] = field(default_factory=dict)
    _by_key: dict[tuple[str, str, str, str], RunId] = field(default_factory=dict)
    _control: dict[str, RunControl] = field(default_factory=dict)
    _consumed: set[str] = field(default_factory=set)

    async def create(
        self,
        *,
        run_id: RunId,
        scope_id: ScopeId,
        org_id: str,
        actor: str,
        agent_id: str,
        session_id: SessionId,
        surface: str,
        idempotency_key: str,
        budget: RunBudgetSpec,
        expires_at: datetime,
        fingerprint: str = "",
        legacy_fingerprint: str = "",
        now: datetime | None = None,
    ) -> tuple[RunRecord, bool]:
        now = now or _now()
        # Admission identity is namespaced by tenant (org) + admitting actor, not a shared
        # (scope, key) — so two tenants/actors cannot collide on one idempotency key.
        key = (scope_id, org_id, actor, idempotency_key)
        existing_id = self._by_key.get(key)
        if existing_id is not None:
            existing = self._rows[existing_id]
            # A retry that reuses the identity but presents a different immutable fingerprint
            # is a conflict (never repaired with the caller-supplied binding/content). A row
            # admitted by a pre-model binary matches the precise legacy fingerprint, so an
            # in-flight legacy run still completes idempotently across a deploy.
            if not _fingerprint_matches(existing.fingerprint, fingerprint, legacy_fingerprint):
                raise RunAdmissionConflict(existing_id)
            return existing, False
        record = RunRecord(
            id=run_id,
            scope_id=scope_id,
            org_id=org_id,
            actor=actor,
            agent_id=agent_id,
            session_id=session_id,
            surface=surface,
            idempotency_key=idempotency_key,
            status=RunStatus.admitted,
            attempt=0,
            version=1,
            max_iterations=budget.max_iterations,
            token_budget=budget.token_budget,
            prompt_tokens=budget.prompt_tokens,
            completion_tokens=budget.completion_tokens,
            cost_usd=budget.cost_usd,
            created_at=now,
            updated_at=now,
            expires_at=expires_at,
            fingerprint=fingerprint,
        )
        self._rows[run_id] = record
        self._by_key[key] = run_id
        return record, True

    async def admit(
        self,
        *,
        run_id: RunId,
        scope_id: ScopeId,
        org_id: str,
        actor: str,
        agent_id: str,
        session_id: SessionId,
        surface: str,
        idempotency_key: str,
        budget: RunBudgetSpec,
        expires_at: datetime,
        fingerprint: str = "",
        legacy_fingerprint: str = "",
        now: datetime | None = None,
    ) -> RunRecord:
        record, _ = await self.create(
            run_id=run_id,
            scope_id=scope_id,
            org_id=org_id,
            actor=actor,
            agent_id=agent_id,
            session_id=session_id,
            surface=surface,
            idempotency_key=idempotency_key,
            budget=budget,
            expires_at=expires_at,
            fingerprint=fingerprint,
            legacy_fingerprint=legacy_fingerprint,
            now=now,
        )
        return record

    async def get(self, run_id: RunId) -> RunRecord | None:
        return self._rows.get(run_id)

    async def mark_queued(self, run_id: RunId, *, now: datetime | None = None) -> bool:
        record = self._rows.get(run_id)
        if record is None or record.status is not RunStatus.admitted:
            return False
        record.status = RunStatus.queued
        record.version += 1
        record.updated_at = now or _now()
        return True

    async def mark_queued_with_dispatch_intent(
        self,
        run_id: RunId,
        scope_id: ScopeId,
        outbox: RunDispatchOutbox,
        *,
        now: datetime | None = None,
    ) -> bool:
        now = now or _now()
        record = self._rows.get(run_id)
        if record is None or record.status is not RunStatus.admitted:
            return False
        # Atomic parity with Postgres: if recording the intent fails, roll the transition back
        # so the run is never left ``queued`` without a discoverable dispatch intent.
        prior_status, prior_version, prior_updated = (
            record.status,
            record.version,
            record.updated_at,
        )
        record.status = RunStatus.queued
        record.version += 1
        record.updated_at = now
        try:
            await outbox.record(run_id, scope_id, now=now)
        except Exception:
            record.status = prior_status
            record.version = prior_version
            record.updated_at = prior_updated
            raise
        return True

    async def mark_prompt_persisted(self, run_id: RunId, *, now: datetime | None = None) -> bool:
        record = self._rows.get(run_id)
        if record is None or record.prompt_persisted:
            return False
        record.prompt_persisted = True
        record.updated_at = now or _now()
        return True

    async def requeue(self, run_id: RunId, *, now: datetime | None = None) -> bool:
        """Move a suspended run back onto the queue after its approval resolved."""
        record = self._rows.get(run_id)
        if record is None or record.status is not RunStatus.waiting_approval:
            return False
        record.status = RunStatus.queued
        record.resume_requested = True
        record.version += 1
        record.updated_at = now or _now()
        return True

    async def mark_checkpoint(
        self, lease: RunLease, *, batch_id: str = "", now: datetime | None = None
    ) -> bool:
        """Durably record a suspension checkpoint intent, fenced by the active lease.

        Called (with the batch's approval rows/events, atomically) so that a crash before the
        ``running -> waiting_approval`` release still leaves a marker on the ``running`` row.
        Records the current attempt as the checkpoint's source attempt and ``batch_id`` as the
        exact suspended batch for approval binding. Idempotent within an attempt for the same
        batch; raises if the lease is lost (never writes unfenced)."""
        record = self._rows.get(lease.run_id)
        if (
            record is None
            or record.lease_token != lease.token
            or record.status is not RunStatus.running
        ):
            raise RunLeaseLostError(lease.run_id)
        if (
            record.suspend_checkpoint
            and record.checkpoint_attempt == record.attempt
            and record.checkpoint_batch_id == batch_id
        ):
            return True  # idempotent no-op for the current attempt + batch
        record.suspend_checkpoint = True
        record.checkpoint_attempt = record.attempt
        record.checkpoint_batch_id = batch_id
        record.version += 1
        record.updated_at = now or _now()
        return True

    def _txn_snapshot(self) -> dict[RunId, RunRecord]:
        """Shallow-copy every run row so a failed multi-store suspension can roll back.

        The in-memory suspension persister snapshots the run store (alongside the event +
        approval stores) before it marks the checkpoint, then restores on any injected failure
        so a partially-applied batch never leaves a checkpoint without its rows/events."""
        return {rid: replace(rec) for rid, rec in self._rows.items()}

    def _txn_restore(self, snapshot: dict[RunId, RunRecord]) -> None:
        self._rows = dict(snapshot)

    async def claim(
        self,
        run_id: RunId,
        *,
        worker_id: str,
        now: datetime | None = None,
        lease_seconds: int,
    ) -> RunLease | None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        now = now or _now()
        record = self._rows.get(run_id)
        if record is None or record.is_terminal:
            return None
        claimable = record.status in (RunStatus.admitted, RunStatus.queued) or (
            record.status in (RunStatus.running, RunStatus.waiting_approval)
            and record.lease_expires_at is not None
            and record.lease_expires_at <= now
        )
        if not claimable:
            return None
        # Resume (vs fresh start) when: the run is explicitly waiting_approval, an approval
        # resolution requeued it (resume_requested), OR it crashed mid-suspension — a
        # ``running`` row with a durable checkpoint marker whose lease has now expired. The
        # last case is the crash boundary: without it the reclaim would restart fresh and
        # silently discard the durable (later approved) approval bound to the source attempt.
        resume = (
            record.status is RunStatus.waiting_approval
            or record.resume_requested
            or (record.status is RunStatus.running and record.suspend_checkpoint)
        )
        token = uuid.uuid4().hex
        record.status = RunStatus.running
        record.attempt += 1
        record.version += 1
        record.worker_id = worker_id
        record.lease_token = token
        record.lease_expires_at = now + timedelta(seconds=lease_seconds)
        record.heartbeat_at = now
        record.started_at = record.started_at or now
        record.resume_requested = False
        record.updated_at = now
        return RunLease(
            run_id=run_id,
            scope_id=record.scope_id,
            org_id=record.org_id,
            token=token,
            worker_id=worker_id,
            attempt=record.attempt,
            agent_id=record.agent_id,
            session_id=record.session_id,
            lease_seconds=lease_seconds,
            resume=resume,
            max_iterations=record.max_iterations,
            token_budget=record.token_budget,
            prompt_tokens=record.prompt_tokens,
            completion_tokens=record.completion_tokens,
            iterations_used=record.iterations,
        )

    def _owned(self, lease: RunLease) -> RunRecord:
        record = self._rows.get(lease.run_id)
        if (
            record is None
            or record.lease_token != lease.token
            or record.status is not RunStatus.running
        ):
            raise RunLeaseLostError(lease.run_id)
        return record

    async def heartbeat(self, lease: RunLease, now: datetime | None = None) -> bool:
        now = now or _now()
        record = self._rows.get(lease.run_id)
        if record is None or record.lease_token != lease.token:
            return False
        if record.status is not RunStatus.running:
            return False
        record.heartbeat_at = now
        return True

    async def renew(
        self, lease: RunLease, *, lease_seconds: int, now: datetime | None = None
    ) -> bool:
        now = now or _now()
        record = self._rows.get(lease.run_id)
        if record is None or record.lease_token != lease.token:
            return False
        if record.status is not RunStatus.running:
            return False
        record.lease_expires_at = now + timedelta(seconds=lease_seconds)
        record.heartbeat_at = now
        record.updated_at = now
        return True

    def _apply_cost(self, record: RunRecord, cost: RunCost | None) -> None:
        if cost is None:
            return
        record.prompt_tokens += cost.prompt_tokens
        record.completion_tokens += cost.completion_tokens
        record.cost_usd += cost.cost_usd
        record.iterations += cost.iterations

    async def release(
        self,
        lease: RunLease,
        *,
        to_status: RunStatus,
        now: datetime | None = None,
        cost: RunCost | None = None,
    ) -> RunRecord:
        if to_status not in (RunStatus.queued, RunStatus.waiting_approval):
            raise RunStateError(f"release target must be queued/waiting_approval, got {to_status}")
        now = now or _now()
        record = self._owned(lease)
        _ensure_transition(record, to_status)
        self._apply_cost(record, cost)
        record.status = to_status
        record.version += 1
        record.worker_id = None
        record.lease_token = None
        record.lease_expires_at = None
        # The suspension intent is now durably reflected in the run status (waiting_approval)
        # or handed back to the queue; clear the crash-window marker. ``checkpoint_attempt`` is
        # preserved so the pending approval bound to the source attempt still resolves.
        record.suspend_checkpoint = False
        record.updated_at = now
        return record

    async def terminalize(
        self,
        lease: RunLease,
        *,
        status: RunStatus,
        stop_reason: str,
        now: datetime | None = None,
        cost: RunCost | None = None,
        result_ref: str | None = None,
        error_kind: str | None = None,
        error_message: str | None = None,
    ) -> RunRecord:
        if status not in TERMINAL_STATUSES:
            raise RunStateError(f"{status} is not terminal")
        now = now or _now()
        record = self._rows.get(lease.run_id)
        if record is None:
            raise RunLeaseLostError(lease.run_id)
        if record.is_terminal:
            return record  # idempotent no-op
        if record.lease_token != lease.token or record.status is not RunStatus.running:
            raise RunLeaseLostError(lease.run_id)
        _ensure_transition(record, status)
        self._apply_cost(record, cost)
        record.status = status
        record.stop_reason = stop_reason
        record.version += 1
        record.worker_id = None
        record.lease_token = None
        record.lease_expires_at = None
        record.suspend_checkpoint = False
        record.finished_at = now
        record.updated_at = now
        record.result_ref = result_ref
        record.error_kind = error_kind
        record.error_message = error_message
        return record

    async def request_control(
        self,
        run_id: RunId,
        *,
        kind: RunControlKind,
        requested_by: str,
        payload: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool:
        now = now or _now()
        record = self._rows.get(run_id)
        if record is None or record.is_terminal:
            return False
        control_id = uuid.uuid4().hex
        self._control[control_id] = RunControl(
            id=control_id,
            scope_id=record.scope_id,
            run_id=run_id,
            kind=kind,
            payload=dict(payload or {}),
            requested_by=requested_by,
            requested_at=now,
        )
        return True

    async def consume_control(
        self, run_id: RunId, *, now: datetime | None = None
    ) -> list[RunControl]:
        pending = await self.peek_control(run_id)
        await self.ack_controls([c.id for c in pending], now=now)
        return pending

    async def peek_control(self, run_id: RunId) -> list[RunControl]:
        pending = [
            c for c in self._control.values() if c.run_id == run_id and c.id not in self._consumed
        ]
        pending.sort(key=lambda c: c.requested_at)
        return pending

    async def ack_controls(self, control_ids: list[str], *, now: datetime | None = None) -> int:
        acked = 0
        for control_id in control_ids:
            if control_id in self._control and control_id not in self._consumed:
                self._consumed.add(control_id)
                acked += 1
        return acked

    async def undispatched(self, now: datetime, limit: int) -> list[str]:
        rows = [
            r for r in self._rows.values() if r.status is RunStatus.admitted and r.expires_at > now
        ]
        rows.sort(key=lambda r: r.created_at)
        return [r.id for r in rows[:limit]]

    async def redispatchable(
        self, now: datetime, limit: int, *, grace_seconds: int = 0
    ) -> list[str]:
        cutoff = now - timedelta(seconds=grace_seconds)
        rows = [
            r
            for r in self._rows.values()
            if r.status in (RunStatus.admitted, RunStatus.queued)
            and r.lease_token is None
            and r.worker_id is None
            and r.expires_at > now
            and r.updated_at <= cutoff
        ]
        rows.sort(key=lambda r: r.created_at)
        return [r.id for r in rows[:limit]]

    async def surface_pending_dispatch(
        self,
        surface: str,
        now: datetime,
        limit: int,
        *,
        grace_seconds: int = 0,
    ) -> list[str]:
        cutoff = now - timedelta(seconds=grace_seconds)
        rows = [
            r
            for r in self._rows.values()
            if r.surface == surface
            and r.status in (RunStatus.admitted, RunStatus.queued)
            and r.lease_token is None
            and r.worker_id is None
            and r.expires_at > now
            and r.created_at <= cutoff
        ]
        rows.sort(key=lambda r: r.created_at)
        return [r.id for r in rows[:limit]]

    async def reclaimable(self, now: datetime, limit: int) -> list[str]:
        rows = [
            r
            for r in self._rows.values()
            if r.status in (RunStatus.running, RunStatus.waiting_approval)
            and r.lease_expires_at is not None
            and r.lease_expires_at <= now
        ]
        rows.sort(key=lambda r: r.lease_expires_at or now)
        return [r.id for r in rows[:limit]]

    async def waiting_approval_ids(self, limit: int) -> list[str]:
        rows = [r for r in self._rows.values() if r.status is RunStatus.waiting_approval]
        rows.sort(key=lambda r: r.updated_at)
        return [r.id for r in rows[:limit]]

    async def expire_due(self, now: datetime, limit: int) -> list[str]:
        expired: list[str] = []
        rows = [r for r in self._rows.values() if not r.is_terminal and r.expires_at <= now]
        rows.sort(key=lambda r: r.expires_at)
        for record in rows[:limit]:
            record.status = RunStatus.expired
            record.stop_reason = "expired"
            record.version += 1
            record.worker_id = None
            record.lease_token = None
            record.lease_expires_at = None
            record.suspend_checkpoint = False
            record.finished_at = now
            record.updated_at = now
            expired.append(record.id)
        return expired


# ----------------------------------------------------------------------------------------
# Postgres store — the production, RLS-bound, fenced repository.
# ----------------------------------------------------------------------------------------
_RUN_COLUMNS = (
    "id, scope_id, org_id, actor, agent_id, session_id, surface, idempotency_key, status, "
    "stop_reason, attempt, version, worker_id, lease_token, lease_expires_at, heartbeat_at, "
    "max_iterations, token_budget, prompt_tokens, completion_tokens, cost_usd, result_ref, "
    "error_kind, error_message, resume_requested, prompt_persisted, iterations, fingerprint, "
    "suspend_checkpoint, checkpoint_attempt, checkpoint_batch_id, "
    "created_at, updated_at, started_at, finished_at, expires_at"
)


def _to_record(row: Mapping[Any, Any]) -> RunRecord:
    return RunRecord(
        id=row["id"],
        scope_id=row["scope_id"],
        org_id=row["org_id"],
        actor=row["actor"],
        agent_id=row["agent_id"],
        session_id=row["session_id"],
        surface=row["surface"],
        idempotency_key=row["idempotency_key"],
        status=RunStatus(row["status"]),
        stop_reason=row["stop_reason"],
        attempt=row["attempt"],
        version=row["version"],
        worker_id=row["worker_id"],
        lease_token=row["lease_token"],
        lease_expires_at=row["lease_expires_at"],
        heartbeat_at=row["heartbeat_at"],
        max_iterations=row["max_iterations"],
        token_budget=row["token_budget"],
        prompt_tokens=row["prompt_tokens"],
        completion_tokens=row["completion_tokens"],
        cost_usd=float(row["cost_usd"]),
        result_ref=row["result_ref"],
        error_kind=row["error_kind"],
        error_message=row["error_message"],
        resume_requested=bool(row["resume_requested"]),
        prompt_persisted=bool(row["prompt_persisted"]),
        iterations=row["iterations"],
        fingerprint=row["fingerprint"],
        suspend_checkpoint=bool(row["suspend_checkpoint"]),
        checkpoint_attempt=row["checkpoint_attempt"],
        checkpoint_batch_id=row["checkpoint_batch_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        expires_at=row["expires_at"],
    )


_CHECKPOINT_UPDATE = text(
    "UPDATE runs SET suspend_checkpoint = true, checkpoint_attempt = attempt, "
    "checkpoint_batch_id = :batch, version = version + 1, updated_at = :now "
    "WHERE scope_id = :scope AND id = :id AND lease_token = :token AND status = 'running' "
    "AND NOT (suspend_checkpoint AND checkpoint_attempt = attempt "
    "AND checkpoint_batch_id = :batch) RETURNING id"
)
_CHECKPOINT_CURRENT = text(
    "SELECT lease_token, status, suspend_checkpoint, checkpoint_attempt, checkpoint_batch_id, "
    "attempt FROM runs WHERE scope_id = :scope AND id = :id"
)


async def mark_checkpoint_in_transaction(
    conn: AsyncConnection,
    scope_id: ScopeId,
    lease: RunLease,
    *,
    batch_id: str = "",
    now: datetime | None = None,
) -> bool:
    """Fenced suspension-checkpoint UPDATE inside the caller's transaction (no commit here).

    Lets a suspended tool batch persist its run checkpoint (source attempt + exact batch id) in
    the *same* transaction as its approval rows + ``tool.call`` / ``approval.requested`` events
    (:func:`keel_core.run_service.execute_run`), so the whole unit commits or rolls back
    together — never a checkpoint without its batch (or a batch without its checkpoint). The
    caller owns the transaction + ``app.scope_id`` GUC. The write is fenced on
    ``lease_token`` + ``status = 'running'``: a lost/superseded lease raises
    :class:`RunLeaseLostError` (aborting — and rolling back — the whole batch); re-marking the
    same attempt + batch is an idempotent no-op."""
    now = now or _now()
    params = {
        "scope": scope_id,
        "id": lease.run_id,
        "token": lease.token,
        "batch": batch_id,
        "now": now,
    }
    row = (await conn.execute(_CHECKPOINT_UPDATE, params)).scalar_one_or_none()
    if row is not None:
        return True
    # No row updated: either the marker already reflects this attempt + batch (idempotent) or
    # the lease is lost. Distinguish so a lost lease never silently succeeds.
    current = (
        (await conn.execute(_CHECKPOINT_CURRENT, {"scope": scope_id, "id": lease.run_id}))
        .mappings()
        .one_or_none()
    )
    if (
        current is not None
        and current["lease_token"] == lease.token
        and current["status"] == "running"
        and current["suspend_checkpoint"]
        and current["checkpoint_attempt"] == current["attempt"]
        and current["checkpoint_batch_id"] == batch_id
    ):
        return True  # idempotent: the current attempt + batch is already checkpointed
    raise RunLeaseLostError(lease.run_id)


# Fenced release/terminalize assignment fragments, shared verbatim by the ``PostgresRunStore``
# methods and the in-connection helpers below so a run finalized inside a *foreign* transaction
# (e.g. the atomic patch-generation finalize) enforces the exact same status/lease/usage/iteration
# semantics as a standalone ``release``/``terminalize`` — never a divergent hand-rolled UPDATE.
_RELEASE_ASSIGNMENTS = (
    "status = :to_status, worker_id = NULL, lease_token = NULL, "
    "lease_expires_at = NULL, suspend_checkpoint = false, "
    "prompt_tokens = prompt_tokens + :ptok, "
    "completion_tokens = completion_tokens + :ctok, cost_usd = cost_usd + :cost, "
    "iterations = iterations + :iters, updated_at = :now"
)
_TERMINALIZE_ASSIGNMENTS = (
    "status = :status, stop_reason = :stop_reason, worker_id = NULL, "
    "lease_token = NULL, lease_expires_at = NULL, suspend_checkpoint = false, "
    "finished_at = :now, "
    "prompt_tokens = prompt_tokens + :ptok, "
    "completion_tokens = completion_tokens + :ctok, cost_usd = cost_usd + :cost, "
    "iterations = iterations + :iters, "
    "result_ref = :result_ref, error_kind = :error_kind, "
    "error_message = :error_message, updated_at = :now"
)


async def _fenced_run_update_in_connection(
    conn: AsyncConnection,
    scope_id: ScopeId,
    lease: RunLease,
    *,
    assignments: str,
    params: Mapping[str, Any],
    expected_status: str = "running",
) -> RunRecord:
    """Fenced run UPDATE inside the caller's transaction (no commit; caller owns the scope GUC).

    Optimistically bumps ``version`` and fences on ``scope_id`` + ``lease_token`` +
    ``status = expected_status`` exactly like :meth:`PostgresRunStore._fenced_update`, raising
    :class:`RunLeaseLostError` (which rolls back the caller's whole transaction) when a
    lost/superseded lease matches no row."""
    row = (
        (
            await conn.execute(
                text(
                    f"UPDATE runs SET {assignments}, version = version + 1 "
                    "WHERE scope_id = :scope AND id = :id AND lease_token = :token "
                    f"AND status = '{expected_status}' RETURNING {_RUN_COLUMNS}"
                ),
                {"scope": scope_id, "id": lease.run_id, "token": lease.token, **params},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise RunLeaseLostError(lease.run_id)
    return _to_record(row)


async def release_in_transaction(
    conn: AsyncConnection,
    scope_id: ScopeId,
    lease: RunLease,
    *,
    to_status: RunStatus,
    now: datetime | None = None,
    cost: RunCost | None = None,
) -> RunRecord:
    """Cooperative lease release back to the queue/waiting_approval inside the caller's transaction.

    Applies ``cost`` as a fenced cumulative delta (the run row is the authoritative charge ledger)
    and returns the updated record so a composing caller (the patch-generation transient-outage
    finalize) can mirror the run's cumulative cost onto its proposal in the SAME transaction."""
    if to_status not in (RunStatus.queued, RunStatus.waiting_approval):
        raise RunStateError(f"release target must be queued/waiting_approval, got {to_status}")
    now = now or _now()
    cost = cost or RunCost()
    return await _fenced_run_update_in_connection(
        conn,
        scope_id,
        lease,
        assignments=_RELEASE_ASSIGNMENTS,
        params={
            "to_status": to_status.value,
            "ptok": cost.prompt_tokens,
            "ctok": cost.completion_tokens,
            "cost": cost.cost_usd,
            "iters": cost.iterations,
            "now": now,
        },
    )


async def terminalize_in_transaction(
    conn: AsyncConnection,
    scope_id: ScopeId,
    lease: RunLease,
    *,
    status: RunStatus,
    stop_reason: str,
    now: datetime | None = None,
    cost: RunCost | None = None,
    result_ref: str | None = None,
    error_kind: str | None = None,
    error_message: str | None = None,
) -> RunRecord:
    """Terminalize a run (completed/failed/...) inside the caller's transaction (no commit here).

    Applies ``cost`` as a fenced cumulative delta and returns the terminal record so a composing
    caller (the atomic patch-generation success/failure finalize) can persist the proposal + its
    dispatch pointer in the SAME transaction — the run can never be terminal while the proposal
    stays ``generating`` (or vice versa). Unlike :meth:`PostgresRunStore.terminalize` there is no
    already-terminal idempotent fallback: a lost lease raises :class:`RunLeaseLostError` and rolls
    back the whole unit (the composing caller only ever finalizes a freshly-claimed running run)."""
    if status not in TERMINAL_STATUSES:
        raise RunStateError(f"{status} is not terminal")
    now = now or _now()
    cost = cost or RunCost()
    return await _fenced_run_update_in_connection(
        conn,
        scope_id,
        lease,
        assignments=_TERMINALIZE_ASSIGNMENTS,
        params={
            "status": status.value,
            "stop_reason": stop_reason,
            "now": now,
            "ptok": cost.prompt_tokens,
            "ctok": cost.completion_tokens,
            "cost": cost.cost_usd,
            "iters": cost.iterations,
            "result_ref": result_ref,
            "error_kind": error_kind,
            "error_message": error_message,
        },
    )


async def purge_scope(engine: AsyncEngine, scope_id: str) -> int:
    """Erase every durable run + control row for a scope (idempotent). Rows removed."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        await conn.execute(
            text("DELETE FROM run_control WHERE scope_id = :scope"), {"scope": scope_id}
        )
        result = await conn.execute(
            text("DELETE FROM runs WHERE scope_id = :scope"), {"scope": scope_id}
        )
    return int(result.rowcount or 0)


class PostgresRunStore:
    """Durable, scope-bound, fenced RunStore over Postgres (RLS defense-in-depth)."""

    def __init__(self, engine: AsyncEngine, scope_id: ScopeId) -> None:
        self._engine = engine
        self._scope_id = scope_id

    @property
    def scope_id(self) -> ScopeId:
        return self._scope_id

    async def create(
        self,
        *,
        run_id: RunId,
        scope_id: ScopeId,
        org_id: str,
        actor: str,
        agent_id: str,
        session_id: SessionId,
        surface: str,
        idempotency_key: str,
        budget: RunBudgetSpec,
        expires_at: datetime,
        fingerprint: str = "",
        legacy_fingerprint: str = "",
        now: datetime | None = None,
    ) -> tuple[RunRecord, bool]:
        if scope_id != self._scope_id:
            raise RunStateError("admit scope mismatch")
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            # Idempotent admission namespaced by tenant + actor: an INSERT that no-ops on the
            # unique (scope, org, actor, key) tuple so two tenants/actors cannot collide.
            inserted = (
                await conn.execute(
                    text(
                        "INSERT INTO runs (id, scope_id, org_id, actor, agent_id, session_id, "
                        "surface, idempotency_key, status, attempt, version, max_iterations, "
                        "token_budget, prompt_tokens, completion_tokens, cost_usd, fingerprint, "
                        "created_at, updated_at, expires_at) VALUES (:id, :scope, :org, :actor, "
                        ":agent, :session, :surface, :key, 'admitted', 0, 1, :max_it, "
                        ":tok_budget, :ptok, :ctok, :cost, :fingerprint, :now, :now, :expires) "
                        "ON CONFLICT (scope_id, org_id, actor, idempotency_key) DO NOTHING "
                        "RETURNING id"
                    ),
                    {
                        "id": run_id,
                        "scope": scope_id,
                        "org": org_id,
                        "actor": actor,
                        "agent": agent_id,
                        "session": session_id,
                        "surface": surface,
                        "key": idempotency_key,
                        "max_it": budget.max_iterations,
                        "tok_budget": budget.token_budget,
                        "ptok": budget.prompt_tokens,
                        "ctok": budget.completion_tokens,
                        "cost": budget.cost_usd,
                        "fingerprint": fingerprint,
                        "now": now,
                        "expires": expires_at,
                    },
                )
            ).scalar_one_or_none()
            created = inserted is not None
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_RUN_COLUMNS} FROM runs "
                            "WHERE scope_id = :scope AND org_id = :org AND actor = :actor "
                            "AND idempotency_key = :key"
                        ),
                        {
                            "scope": scope_id,
                            "org": org_id,
                            "actor": actor,
                            "key": idempotency_key,
                        },
                    )
                )
                .mappings()
                .one()
            )
        record = _to_record(row)
        # A retry that reuses the identity but mismatches the immutable fingerprint is a
        # conflict — never repaired using the caller-supplied binding/content. A run admitted
        # by a pre-model binary matches the precise legacy fingerprint, so an in-flight legacy
        # run still completes idempotently across a deploy without a model change hijacking it.
        if not created and not _fingerprint_matches(
            record.fingerprint, fingerprint, legacy_fingerprint
        ):
            raise RunAdmissionConflict(record.id)
        return record, created

    async def admit(
        self,
        *,
        run_id: RunId,
        scope_id: ScopeId,
        org_id: str,
        actor: str,
        agent_id: str,
        session_id: SessionId,
        surface: str,
        idempotency_key: str,
        budget: RunBudgetSpec,
        expires_at: datetime,
        fingerprint: str = "",
        legacy_fingerprint: str = "",
        now: datetime | None = None,
    ) -> RunRecord:
        record, _ = await self.create(
            run_id=run_id,
            scope_id=scope_id,
            org_id=org_id,
            actor=actor,
            agent_id=agent_id,
            session_id=session_id,
            surface=surface,
            idempotency_key=idempotency_key,
            budget=budget,
            expires_at=expires_at,
            fingerprint=fingerprint,
            legacy_fingerprint=legacy_fingerprint,
            now=now,
        )
        return record

    async def get(self, run_id: RunId) -> RunRecord | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_RUN_COLUMNS} FROM runs WHERE scope_id = :scope AND id = :id"
                        ),
                        {"scope": self._scope_id, "id": run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return _to_record(row) if row is not None else None

    async def mark_queued(self, run_id: RunId, *, now: datetime | None = None) -> bool:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE runs SET status = 'queued', version = version + 1, "
                    "updated_at = :now WHERE scope_id = :scope AND id = :id "
                    "AND status = 'admitted'"
                ),
                {"scope": self._scope_id, "id": run_id, "now": now},
            )
        return result.rowcount == 1

    async def mark_queued_with_dispatch_intent(
        self,
        run_id: RunId,
        scope_id: ScopeId,
        outbox: RunDispatchOutbox,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Commit the queued transition + the dispatch intent in ONE transaction.

        Both statements run on this store's engine inside a single ``begin()`` block: if the
        intent INSERT fails the ``UPDATE runs`` is rolled back with it, so the run never reaches
        ``queued`` in the durable store without a discoverable ``run_dispatch_outbox`` pointer.
        The ``outbox`` argument is accepted for interface symmetry with the in-memory store; the
        Postgres path writes the intent directly on its own connection (same database), so the
        outbox must be backed by the same engine — asserted below.
        """
        if isinstance(outbox, PostgresRunDispatchOutbox) and outbox._engine is not self._engine:
            raise RunStateError("dispatch outbox must share the run store engine for atomicity")
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE runs SET status = 'queued', version = version + 1, "
                    "updated_at = :now WHERE scope_id = :scope AND id = :id "
                    "AND status = 'admitted'"
                ),
                {"scope": self._scope_id, "id": run_id, "now": now},
            )
            if result.rowcount != 1:
                return False
            # Same transaction as the transition — commits or rolls back as one unit.
            await record_intent_in_connection(conn, run_id, scope_id, now=now)
        return True

    async def mark_prompt_persisted(self, run_id: RunId, *, now: datetime | None = None) -> bool:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE runs SET prompt_persisted = true, updated_at = :now "
                    "WHERE scope_id = :scope AND id = :id AND prompt_persisted = false"
                ),
                {"scope": self._scope_id, "id": run_id, "now": now},
            )
        return result.rowcount == 1

    async def requeue(self, run_id: RunId, *, now: datetime | None = None) -> bool:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE runs SET status = 'queued', resume_requested = true, "
                    "version = version + 1, updated_at = :now "
                    "WHERE scope_id = :scope AND id = :id AND status = 'waiting_approval'"
                ),
                {"scope": self._scope_id, "id": run_id, "now": now},
            )
        return result.rowcount == 1

    async def mark_checkpoint(
        self, lease: RunLease, *, batch_id: str = "", now: datetime | None = None
    ) -> bool:
        """Durably mark a suspension checkpoint on the ``running`` row, fenced by the lease.

        Written (atomically with the approval batch's rows/events — see
        :func:`keel_core.run_service.execute_run`) so a crash before the
        ``running -> waiting_approval`` release still leaves a marker; ``checkpoint_attempt``
        captures the current attempt as the batch's source attempt and ``checkpoint_batch_id``
        the exact suspended batch, both for approval binding. The write is fenced on
        ``lease_token`` + ``status = 'running'``; a lost lease raises rather than writing
        unfenced. Re-marking the current attempt + batch is an idempotent no-op."""
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            return await mark_checkpoint_in_transaction(
                conn, self._scope_id, lease, batch_id=batch_id, now=now
            )

    async def claim(
        self,
        run_id: RunId,
        *,
        worker_id: str,
        now: datetime | None = None,
        lease_seconds: int,
    ) -> RunLease | None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        now = now or _now()
        token = uuid.uuid4().hex
        expires = now + timedelta(seconds=lease_seconds)
        run_cols_r = ", ".join(f"r.{c}" for c in _RUN_COLUMNS.replace(" ", "").split(","))
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "WITH claimable AS ("
                            "  SELECT id, status, resume_requested, suspend_checkpoint FROM runs "
                            "  WHERE scope_id = :scope AND id = :id AND expires_at > :now AND ("
                            "    status IN ('admitted', 'queued') OR "
                            "    (status IN ('running', 'waiting_approval') "
                            "     AND lease_expires_at <= :now)"
                            "  ) FOR UPDATE"
                            ") "
                            "UPDATE runs r SET status = 'running', attempt = attempt + 1, "
                            "version = version + 1, worker_id = :worker, lease_token = :token, "
                            "lease_expires_at = :expires, heartbeat_at = :now, "
                            "started_at = COALESCE(started_at, :now), updated_at = :now, "
                            "resume_requested = false "
                            "FROM claimable c WHERE r.id = c.id "
                            f"RETURNING {run_cols_r}, "
                            "(c.status = 'waiting_approval' OR c.resume_requested OR "
                            " (c.status = 'running' AND c.suspend_checkpoint)) AS resume"
                        ),
                        {
                            "scope": self._scope_id,
                            "id": run_id,
                            "worker": worker_id,
                            "token": token,
                            "expires": expires,
                            "now": now,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        record = _to_record(row)
        return RunLease(
            run_id=record.id,
            scope_id=record.scope_id,
            org_id=record.org_id,
            token=token,
            worker_id=worker_id,
            attempt=record.attempt,
            agent_id=record.agent_id,
            session_id=record.session_id,
            lease_seconds=lease_seconds,
            resume=bool(row["resume"]),
            max_iterations=record.max_iterations,
            token_budget=record.token_budget,
            prompt_tokens=record.prompt_tokens,
            completion_tokens=record.completion_tokens,
            iterations_used=record.iterations,
        )

    async def heartbeat(self, lease: RunLease, now: datetime | None = None) -> bool:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE runs SET heartbeat_at = :now WHERE scope_id = :scope AND id = :id "
                    "AND status = 'running' AND lease_token = :token AND lease_expires_at > :now"
                ),
                {"scope": self._scope_id, "id": lease.run_id, "token": lease.token, "now": now},
            )
        return result.rowcount == 1

    async def renew(
        self, lease: RunLease, *, lease_seconds: int, now: datetime | None = None
    ) -> bool:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        now = now or _now()
        expires = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE runs SET lease_expires_at = :expires, heartbeat_at = :now, "
                    "updated_at = :now WHERE scope_id = :scope AND id = :id "
                    "AND status = 'running' AND lease_token = :token AND lease_expires_at > :now"
                ),
                {
                    "scope": self._scope_id,
                    "id": lease.run_id,
                    "token": lease.token,
                    "expires": expires,
                    "now": now,
                },
            )
        return result.rowcount == 1

    async def release(
        self,
        lease: RunLease,
        *,
        to_status: RunStatus,
        now: datetime | None = None,
        cost: RunCost | None = None,
    ) -> RunRecord:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            return await release_in_transaction(
                conn, self._scope_id, lease, to_status=to_status, now=now, cost=cost
            )

    async def terminalize(
        self,
        lease: RunLease,
        *,
        status: RunStatus,
        stop_reason: str,
        now: datetime | None = None,
        cost: RunCost | None = None,
        result_ref: str | None = None,
        error_kind: str | None = None,
        error_message: str | None = None,
    ) -> RunRecord:
        now = now or _now()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
                return await terminalize_in_transaction(
                    conn,
                    self._scope_id,
                    lease,
                    status=status,
                    stop_reason=stop_reason,
                    now=now,
                    cost=cost,
                    result_ref=result_ref,
                    error_kind=error_kind,
                    error_message=error_message,
                )
        except RunLeaseLostError:
            # Idempotent terminalization: a run already terminal returns the authoritative row.
            current = await self.get(lease.run_id)
            if current is not None and current.is_terminal:
                return current
            raise

    async def request_control(
        self,
        run_id: RunId,
        *,
        kind: RunControlKind,
        requested_by: str,
        payload: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool:
        now = now or _now()
        control_id = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT status FROM runs WHERE scope_id = :scope AND id = :id "
                            "FOR UPDATE"
                        ),
                        {"scope": self._scope_id, "id": run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None or RunStatus(row["status"]) in TERMINAL_STATUSES:
                return False
            await conn.execute(
                text(
                    "INSERT INTO run_control (id, scope_id, run_id, kind, payload, "
                    "requested_by, requested_at) VALUES (:id, :scope, :run, :kind, "
                    "CAST(:payload AS jsonb), :by, :now)"
                ),
                {
                    "id": control_id,
                    "scope": self._scope_id,
                    "run": run_id,
                    "kind": kind.value,
                    "payload": json.dumps(dict(payload or {})),
                    "by": requested_by,
                    "now": now,
                },
            )
        return True

    async def consume_control(
        self, run_id: RunId, *, now: datetime | None = None
    ) -> list[RunControl]:
        controls = await self.peek_control(run_id)
        await self.ack_controls([c.id for c in controls], now=now)
        return controls

    async def peek_control(self, run_id: RunId) -> list[RunControl]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT id, scope_id, run_id, kind, payload, requested_by, "
                            "requested_at FROM run_control "
                            "WHERE scope_id = :scope AND run_id = :run AND consumed_at IS NULL "
                            "ORDER BY requested_at"
                        ),
                        {"scope": self._scope_id, "run": run_id},
                    )
                )
                .mappings()
                .all()
            )
        return [
            RunControl(
                id=row["id"],
                scope_id=row["scope_id"],
                run_id=row["run_id"],
                kind=RunControlKind(row["kind"]),
                payload=dict(row["payload"] or {}),
                requested_by=row["requested_by"],
                requested_at=row["requested_at"],
            )
            for row in rows
        ]

    async def ack_controls(self, control_ids: list[str], *, now: datetime | None = None) -> int:
        if not control_ids:
            return 0
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE run_control SET consumed_at = :now "
                    "WHERE scope_id = :scope AND id = ANY(:ids) AND consumed_at IS NULL"
                ),
                {"scope": self._scope_id, "now": now, "ids": list(control_ids)},
            )
        return int(result.rowcount or 0)

    async def _select_ids(self, sql: str, params: Mapping[str, Any]) -> list[str]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (await conn.execute(text(sql), params)).scalars().all()
        return [str(r) for r in rows]

    async def undispatched(self, now: datetime, limit: int) -> list[str]:
        return await self._select_ids(
            "SELECT id FROM runs WHERE scope_id = :scope AND status = 'admitted' "
            "AND expires_at > :now ORDER BY created_at LIMIT :limit",
            {"scope": self._scope_id, "now": now, "limit": limit},
        )

    async def redispatchable(
        self, now: datetime, limit: int, *, grace_seconds: int = 0
    ) -> list[str]:
        cutoff = now - timedelta(seconds=grace_seconds)
        return await self._select_ids(
            "SELECT id FROM runs WHERE scope_id = :scope "
            "AND status IN ('admitted', 'queued') AND lease_token IS NULL "
            "AND worker_id IS NULL AND expires_at > :now AND updated_at <= :cutoff "
            "ORDER BY created_at LIMIT :limit",
            {"scope": self._scope_id, "now": now, "cutoff": cutoff, "limit": limit},
        )

    async def surface_pending_dispatch(
        self,
        surface: str,
        now: datetime,
        limit: int,
        *,
        grace_seconds: int = 0,
    ) -> list[str]:
        cutoff = now - timedelta(seconds=grace_seconds)
        return await self._select_ids(
            "SELECT id FROM runs WHERE scope_id = :scope AND surface = :surface "
            "AND status IN ('admitted', 'queued') AND lease_token IS NULL "
            "AND worker_id IS NULL AND expires_at > :now AND created_at <= :cutoff "
            "ORDER BY created_at LIMIT :limit",
            {
                "scope": self._scope_id,
                "surface": surface,
                "now": now,
                "cutoff": cutoff,
                "limit": limit,
            },
        )

    async def reclaimable(self, now: datetime, limit: int) -> list[str]:
        return await self._select_ids(
            "SELECT id FROM runs WHERE scope_id = :scope "
            "AND status IN ('running', 'waiting_approval') AND lease_expires_at <= :now "
            "ORDER BY lease_expires_at LIMIT :limit",
            {"scope": self._scope_id, "now": now, "limit": limit},
        )

    async def waiting_approval_ids(self, limit: int) -> list[str]:
        return await self._select_ids(
            "SELECT id FROM runs WHERE scope_id = :scope AND status = 'waiting_approval' "
            "ORDER BY updated_at LIMIT :limit",
            {"scope": self._scope_id, "limit": limit},
        )

    async def expire_due(self, now: datetime, limit: int) -> list[str]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "UPDATE runs SET status = 'expired', stop_reason = 'expired', "
                            "worker_id = NULL, lease_token = NULL, lease_expires_at = NULL, "
                            "suspend_checkpoint = false, "
                            "finished_at = :now, version = version + 1, updated_at = :now "
                            "WHERE scope_id = :scope AND id IN ("
                            "  SELECT id FROM runs WHERE scope_id = :scope "
                            "  AND status NOT IN ('completed','failed','cancelled',"
                            "'interrupted','expired') AND expires_at <= :now "
                            "  ORDER BY expires_at LIMIT :limit FOR UPDATE SKIP LOCKED"
                            ") RETURNING id"
                        ),
                        {"scope": self._scope_id, "now": now, "limit": limit},
                    )
                )
                .scalars()
                .all()
            )
        return [str(r) for r in rows]


__all__ = [
    "RunControl",
    "RunControlKind",
    "RunCost",
    "RunBudgetSpec",
    "RunLease",
    "RunLeaseLostError",
    "RunRecord",
    "RunStateError",
    "RunStatus",
    "RunStore",
    "RunSurface",
    "InMemoryRunStore",
    "PostgresRunStore",
    "TERMINAL_STATUSES",
    "action_hash",
    "can_transition",
    "mark_checkpoint_in_transaction",
    "purge_scope",
    "release_in_transaction",
    "terminalize_in_transaction",
]
