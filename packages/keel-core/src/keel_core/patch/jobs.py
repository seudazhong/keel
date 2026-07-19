"""Worker-agnostic durable patch jobs (WS-PP, P3b-0).

Controlled patch generation and writeback are durable, restart-safe, idempotent background jobs on
the existing jobs substrate: the server creates the run + proposal and enqueues one
``patch.generate`` job keyed by the proposal id; a human decision enqueues one ``patch.writeback``
job. This module owns only the payload contracts + handlers around
:class:`~keel_core.patch.coordinator.PatchCoordinator` plus a durable job-lease heartbeat; a thin
worker adapter wraps each handler in a ``JobDefinition`` (mirroring :mod:`keel_core.review.jobs`).
**No worker registration lives here** — the worker package (P3b-1) owns the registry/reconciler
wiring.

Two distinct lease keepers cooperate during generation: the coordinator's RUN-lease keeper (inside
``execute_generation``) renews the run row's fence, while the JOB-lease heartbeat here renews the
worker's job lease and, on a cooperative cancel or a lost job lease, sets a cancellation event that
is folded into the generation author interrupt — so an in-flight generation aborts without a
terminal write, and neither lease can silently expire under slow provider/transfer I/O.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
)
from sqlalchemy.exc import SQLAlchemyError

from keel_core.errors import DuplicateEventError
from keel_core.events import Event, EventType
from keel_core.jobs import (
    JobCancellationRequested,
    JobLeaseLostError,
    JobResult,
    PermanentJobError,
    RetryableJobError,
)
from keel_core.protocols import EventStore

from .coordinator import PatchCoordinator
from .errors import (
    PatchError,
    PatchLeaseLost,
    PatchProviderUnavailable,
    PatchRemoteUnavailable,
    PatchWritebackError,
)
from .models import (
    DEFAULT_MAX_DIFF_BYTES,
    DEFAULT_PATCH_COST_CEILING_USD,
    DEFAULT_PATCH_MAX_ITERATIONS,
    DEFAULT_PATCH_OUTPUT_MAX_TOKENS,
    DEFAULT_PATCH_TOKEN_BUDGET,
    PatchProposalRequest,
)

logger = logging.getLogger("keel.patch.jobs")

PATCH_GENERATE_KIND = "patch.generate"
PATCH_WRITEBACK_KIND = "patch.writeback"
# Generation replays are safe (idempotent by proposal/run + fenced atomic finalize); a small
# attempt budget absorbs transient provider/transfer outages before the run TTL/reconciler backstop.
PATCH_GENERATE_MAX_ATTEMPTS = 3
# Writeback is idempotent (branch/PR reuse) and its transient remote failures are common, so it gets
# a slightly larger attempt budget.
PATCH_WRITEBACK_MAX_ATTEMPTS = 5

# Job-lease heartbeat cadence bounds (strictly below the job lease TTL; see
# ``_heartbeat_interval``).
PATCH_JOB_HEARTBEAT_MIN_INTERVAL_SECONDS = 1.0
PATCH_JOB_HEARTBEAT_MAX_INTERVAL_SECONDS = 30.0


def patch_generate_idempotency_key(proposal_id: str) -> str:
    """A stable job idempotency key so a retried enqueue schedules at most one generation job."""
    return f"{PATCH_GENERATE_KIND}:{proposal_id}"


def patch_writeback_idempotency_key(proposal_id: str) -> str:
    """A stable job idempotency key so a retried enqueue schedules at most one writeback job."""
    return f"{PATCH_WRITEBACK_KIND}:{proposal_id}"


# --- durable generate-request metadata (reconstruction seam) -------------------------------------
# A ``patch.generate`` job carries the full authorized request (raw tainted task text + budgets),
# but a proposal row persists only a ``task_digest`` and the outbox pointer deliberately carries no
# sensitive payload. So when a proposal is admitted the full request is recorded ONCE on the run's
# append-only event log (mirroring ``ReviewCoordinator._persist_request_metadata``); the fenced
# patch reconciler reconstructs it to (re-)create a stranded generation job after a lost enqueue,
# and fails closed (never fabricates a payload) when it is absent/legacy/tampered.
PATCH_GENERATE_REQUEST_MARKER = "patch_generate_request"
PATCH_GENERATE_METADATA_VERSION = 1


async def persist_generate_metadata(
    events: EventStore,
    *,
    scope_id: str,
    payload: PatchGenerateJobPayload,
    now: datetime | None = None,
) -> None:
    """Durably record the immutable generate-job payload on the run's event log (idempotent).

    No schema migration: the payload lives in the append-only ``events`` payload keyed by the
    run id. A duplicate append (a retried/idempotent re-request) is ignored. The caller injects a
    per-scope :class:`~keel_core.protocols.EventStore` bound to the proposal's canonical
    ``scope_id`` (passed explicitly — the Protocol exposes no scope accessor) and the metadata is
    written only on the winning admission so a replayed request never rewrites it.
    """
    moment = now or datetime.now(UTC)
    event = Event(
        type=EventType.run_started,
        seq=0,
        session_id=payload.run_id,
        scope_id=scope_id,
        run_id=payload.run_id,
        ts=moment,
        payload={
            PATCH_GENERATE_REQUEST_MARKER: {
                "version": PATCH_GENERATE_METADATA_VERSION,
                "payload": payload.model_dump(mode="json"),
            },
            "dedup_key": f"patch-generate-meta:{payload.run_id}",
        },
    )
    try:
        await events.append(event)
    except DuplicateEventError:
        # A concurrent/replayed admission already recorded the immutable metadata — the winner's
        # copy is authoritative and identical, so observing the duplicate is the idempotent no-op.
        pass


async def load_generate_metadata(events: EventStore, run_id: str) -> PatchGenerateJobPayload | None:
    """Reconstruct the durably-persisted generate-job payload for ``run_id`` (or ``None``).

    Fail closed: a missing marker, an unknown/legacy metadata version, or a payload that no longer
    validates against the current strict schema returns ``None`` rather than a fabricated or
    partially-defaulted request — so a stranded re-dispatch never runs generation under a payload
    the server never admitted. The reconciler defers such an entry to the TTL backstop.
    """
    async for event in events.read(run_id):
        marker = event.payload.get(PATCH_GENERATE_REQUEST_MARKER)
        if not isinstance(marker, dict):
            continue
        if marker.get("version") != PATCH_GENERATE_METADATA_VERSION:
            logger.warning(
                "patch generate metadata version mismatch; failing closed run=%s", run_id
            )
            return None
        raw = marker.get("payload")
        if not isinstance(raw, dict):
            return None
        try:
            return PatchGenerateJobPayload.model_validate(raw)
        except ValidationError:
            logger.warning(
                "patch generate metadata failed schema validation; failing closed run=%s", run_id
            )
            return None
    return None


def _heartbeat_interval(lease_seconds: float) -> float:
    """A job-lease heartbeat interval strictly *below* ``lease_seconds`` (never at the expiry).

    Uses ``min(lease/3, cap)`` bounded to a small floor, guaranteed strictly less than the lease
    even for a tiny lease (a 1s lease yields 0.5s), so a beat can never land exactly on expiry.
    """
    if lease_seconds <= 0:
        return PATCH_JOB_HEARTBEAT_MIN_INTERVAL_SECONDS
    interval = min(lease_seconds / 3.0, PATCH_JOB_HEARTBEAT_MAX_INTERVAL_SECONDS)
    interval = max(PATCH_JOB_HEARTBEAT_MIN_INTERVAL_SECONDS, interval)
    if interval >= lease_seconds:
        interval = lease_seconds / 2.0
    return interval


@runtime_checkable
class PatchJobContext(Protocol):
    """The subset of the worker job context the patch handlers need.

    Structurally satisfied by ``keel_worker.jobs.JobContext``: ``checkpoint`` heartbeats the durable
    job lease and raises :class:`~keel_core.jobs.JobCancellationRequested` on a cooperative cancel
    (or :class:`~keel_core.jobs.JobLeaseLostError` when the lease was reclaimed)."""

    @property
    def job_id(self) -> str: ...

    @property
    def scope_id(self) -> str: ...

    @property
    def job_lease_seconds(self) -> int: ...

    async def checkpoint(self) -> None: ...


class PatchGenerateJobPayload(BaseModel):
    """The durable payload carried by a ``patch.generate`` job.

    Reconstructs the authorized :class:`~keel_core.patch.models.PatchProposalRequest` (the tainted
    development ``task`` is bounded storage-safe text) plus the ``run_id`` the server bound to the
    proposal, so the worker can resume generation idempotently after a crash/retry."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True, strict=True)

    proposal_id: StrictStr
    run_id: StrictStr
    org_id: StrictStr
    project_id: StrictStr
    actor: StrictStr
    task: StrictStr
    base_ref: StrictStr
    model: StrictStr
    idempotency_key: StrictStr
    agent_id: StrictStr | None = None
    source_ref: StrictStr = ""
    test_commands: tuple[StrictStr, ...] = ()
    max_diff_bytes: StrictInt = Field(default=DEFAULT_MAX_DIFF_BYTES)
    token_budget: StrictInt = Field(default=DEFAULT_PATCH_TOKEN_BUDGET)
    output_max_tokens: StrictInt = Field(default=DEFAULT_PATCH_OUTPUT_MAX_TOKENS)
    cost_ceiling_usd: StrictFloat = Field(default=DEFAULT_PATCH_COST_CEILING_USD)
    max_iterations: StrictInt = Field(default=DEFAULT_PATCH_MAX_ITERATIONS)

    @field_validator("test_commands", mode="before")
    @classmethod
    def _coerce_test_commands(cls, value: object) -> object:
        # A durable job payload survives a JSONB store/read round-trip, which lowers the immutable
        # ``tuple`` to a JSON ``list``. Coerce a list back to a tuple BEFORE strict validation so a
        # reconstructed/enqueued payload validates, while every element is still checked as a
        # ``StrictStr`` (a non-list, e.g. a bare string, is left untouched and fails closed).
        if isinstance(value, list):
            return tuple(value)
        return value

    def to_request(self) -> PatchProposalRequest:
        return PatchProposalRequest(
            org_id=self.org_id,
            project_id=self.project_id,
            actor=self.actor,
            task=self.task,
            base_ref=self.base_ref,
            model=self.model,
            idempotency_key=self.idempotency_key,
            agent_id=self.agent_id,
            source_ref=self.source_ref,
            test_commands=self.test_commands,
            max_diff_bytes=self.max_diff_bytes,
            token_budget=self.token_budget,
            output_max_tokens=self.output_max_tokens,
            cost_ceiling_usd=self.cost_ceiling_usd,
            max_iterations=self.max_iterations,
        )

    @classmethod
    def from_request(
        cls, request: PatchProposalRequest, *, proposal_id: str, run_id: str
    ) -> PatchGenerateJobPayload:
        return cls(
            proposal_id=proposal_id,
            run_id=run_id,
            org_id=request.org_id,
            project_id=request.project_id,
            actor=request.actor,
            task=request.task,
            base_ref=request.base_ref,
            model=request.model,
            idempotency_key=request.idempotency_key,
            agent_id=request.agent_id,
            source_ref=request.source_ref,
            test_commands=tuple(request.test_commands),
            max_diff_bytes=request.max_diff_bytes,
            token_budget=request.token_budget,
            output_max_tokens=request.output_max_tokens,
            cost_ceiling_usd=request.cost_ceiling_usd,
            max_iterations=request.max_iterations,
        )


class PatchWritebackJobPayload(BaseModel):
    """The durable payload carried by a ``patch.writeback`` job (an approved proposal to push)."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True, strict=True)

    proposal_id: StrictStr
    org_id: StrictStr

    # Reserved for a future coordinator that verifies the enqueuing decision id; unused in P3b-0 but
    # accepted so an early enqueuer can carry it without a payload-shape break.
    decided_by: StrictStr | None = None
    resumed: StrictBool = False


@dataclass
class _JobHeartbeatKeeper:
    """Periodically heartbeats the durable JOB lease; signals the operation on cancel/lease-loss.

    Distinct from the coordinator's RUN-lease keeper: this watches the worker's *job* lease via
    ``PatchJobContext.checkpoint`` (which raises ``JobCancellationRequested`` on a cooperative
    cancel and ``JobLeaseLostError`` when the job lease was reclaimed). On either signal it records
    the outcome and either sets a cancellation event (folded into the generation author interrupt)
    or cancels a bound task (writeback has no interrupt seam). A typed store error
    (``SQLAlchemyError``) is treated as a lost lease (fail closed). Checkpoint signals are never
    swallowed — every one maps to a recorded cancel/lost outcome the handler resolves
    deterministically."""

    checkpoint: Callable[[], Awaitable[None]]
    interval_seconds: float
    cancel_event: asyncio.Event | None = None
    cancelled: bool = False
    lost: bool = False
    error: BaseException | None = None
    _target: asyncio.Task[Any] | None = None
    _task: asyncio.Task[None] | None = None

    def bind(self, target: asyncio.Task[Any]) -> None:
        self._target = target

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    def _signal(self) -> None:
        if self.cancel_event is not None:
            self.cancel_event.set()
        if self._target is not None:
            self._target.cancel()

    async def _loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.interval_seconds)
                try:
                    await self.checkpoint()
                except asyncio.CancelledError:
                    raise
                except JobCancellationRequested:
                    self.cancelled = True
                    self._signal()
                    return
                except (JobLeaseLostError, SQLAlchemyError) as exc:
                    logger.warning("patch job heartbeat lost the job lease")
                    self.lost = True
                    self.error = exc
                    self._signal()
                    return
        except asyncio.CancelledError:
            raise

    async def stop(self) -> None:
        """Cancel + await the keeper deterministically.

        ``_loop`` never lets a non-``CancelledError`` escape (a checkpoint cancel/lease-loss/DB
        error is caught and recorded), so awaiting the cancelled task only raises
        ``CancelledError``.
        """
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None


class PatchJobHandlers:
    """Durable handlers that execute patch generation/writeback idempotently around the coordinator.

    A *worker* coordinator (with generation/writeback/author dependencies) is required. Each handler
    validates its payload (a malformed payload is a permanent failure), heartbeats the job lease,
    and maps the coordinator's typed outcomes onto the job framework's retry/permanent/lease
    semantics — never a broad catch, never a success-shaped fallback."""

    def __init__(self, coordinator: PatchCoordinator) -> None:
        self._coordinator = coordinator

    async def generate(self, context: PatchJobContext, raw_payload: dict[str, Any]) -> JobResult:
        try:
            payload = PatchGenerateJobPayload.model_validate(raw_payload)
            request = payload.to_request()
        except (ValidationError, PatchError) as exc:
            raise PermanentJobError(
                "invalid_patch_generate_payload", "Patch generation job payload is invalid."
            ) from exc
        # Initial cancellation/lease check before any expensive work (propagates to the worker).
        await context.checkpoint()
        cancel_event = asyncio.Event()
        keeper = _JobHeartbeatKeeper(
            checkpoint=context.checkpoint,
            interval_seconds=_heartbeat_interval(context.job_lease_seconds),
            cancel_event=cancel_event,
        )
        keeper.start()
        # A single try/finally stops the heartbeat on *every* path — typed patch errors, a
        # non-patch ``RuntimeError`` from the coordinator, an ``asyncio.CancelledError`` cancelling
        # this handler, or success — so the heartbeat task can never leak. The typed mapping runs
        # inside the try and reads the keeper's ``cancelled``/``lost`` (set before the signal that
        # aborted generation), which the later idempotent ``stop`` never clears.
        try:
            try:
                proposal = await self._coordinator.execute_generation(
                    payload.org_id,
                    payload.run_id,
                    request,
                    worker_id=context.job_id,
                    interrupt=cancel_event.is_set,
                )
            except PatchLeaseLost as exc:
                # The generation aborted on a lost fence. A cooperative job cancellation surfaces
                # the cancel path; anything else (a lost job lease, or the RUN lease being
                # reclaimed) is a lost lease. Either way the proposal/run are untouched — never a
                # terminal success.
                if keeper.cancelled:
                    raise JobCancellationRequested from exc
                raise JobLeaseLostError(context.job_id) from exc
            except (PatchProviderUnavailable, PatchRemoteUnavailable) as exc:
                # Transient upstream failure. The coordinator already released the run to the queue
                # and persisted the partial usage atomically; retry while attempts remain.
                raise RetryableJobError(
                    "patch_generation_transient", "Patch generation temporarily failed."
                ) from exc
            except PatchError as exc:
                # A permanent generation failure. The coordinator already terminalized the proposal
                # (failed) + run and retired the dispatch pointer atomically; fail permanently.
                raise PermanentJobError(
                    "patch_generation_failed", "Patch generation failed."
                ) from exc
            # Success (normal ``approval_pending``, an idempotent replay of an already
            # ``approval_pending`` proposal, or a healed ``ready``). The durable outcome is
            # authoritative and idempotent, so a heartbeat signal that raced a successful completion
            # never undoes it.
            return JobResult(
                data={
                    "proposal_id": proposal.id,
                    "run_id": proposal.run_id,
                    "status": proposal.status.value,
                    "approval_id": proposal.approval_id,
                },
                message=f"patch generation reached {proposal.status.value}",
            )
        finally:
            await keeper.stop()

    async def writeback(self, context: PatchJobContext, raw_payload: dict[str, Any]) -> JobResult:
        try:
            payload = PatchWritebackJobPayload.model_validate(raw_payload)
        except ValidationError as exc:
            raise PermanentJobError(
                "invalid_patch_writeback_payload", "Patch writeback job payload is invalid."
            ) from exc
        await context.checkpoint()
        keeper = _JobHeartbeatKeeper(
            checkpoint=context.checkpoint,
            interval_seconds=_heartbeat_interval(context.job_lease_seconds),
        )
        # Writeback has no interrupt seam (it is idempotent + retryable by design): bind the keeper
        # to the work task so a cancellation / lost job lease aborts the in-flight push and a retry
        # reuses the reserved branch/PR — never a terminal success under a superseded fence.
        work = asyncio.ensure_future(
            self._coordinator.execute_writeback(
                payload.org_id, payload.proposal_id, worker_id=context.job_id
            )
        )
        keeper.bind(work)
        keeper.start()
        try:
            proposal = await work
        except asyncio.CancelledError:
            if keeper.cancelled:
                raise JobCancellationRequested from None
            if keeper.lost:
                raise JobLeaseLostError(context.job_id) from keeper.error
            raise  # a genuine external cancellation of this handler task
        except PatchRemoteUnavailable as exc:
            raise RetryableJobError(
                "patch_writeback_transient", "Patch writeback temporarily failed."
            ) from exc
        except PatchWritebackError as exc:
            # A permanent, structural writeback refusal. The coordinator already terminalized the
            # proposal (failed) + retired the pointer with an explicit audit; fail the job.
            raise PermanentJobError("patch_writeback_failed", "Patch writeback failed.") from exc
        except PatchError as exc:
            # Any other permanent patch error (validation/state/binding): fail the job permanently.
            raise PermanentJobError("patch_writeback_failed", "Patch writeback failed.") from exc
        finally:
            await keeper.stop()
        # Success: a fresh ``draft_pr_created``, an idempotent replay of one, or a terminal
        # ``stale`` (base drift is a legitimate terminal outcome, not a job failure).
        return JobResult(
            data={
                "proposal_id": proposal.id,
                "status": proposal.status.value,
                "pr_number": proposal.pr_number,
                "remote_branch": proposal.remote_branch,
            },
            message=f"patch writeback reached {proposal.status.value}",
        )


__all__ = [
    "PATCH_GENERATE_KIND",
    "PATCH_GENERATE_MAX_ATTEMPTS",
    "PATCH_GENERATE_METADATA_VERSION",
    "PATCH_GENERATE_REQUEST_MARKER",
    "PATCH_WRITEBACK_KIND",
    "PATCH_WRITEBACK_MAX_ATTEMPTS",
    "PatchGenerateJobPayload",
    "PatchJobContext",
    "PatchJobHandlers",
    "PatchWritebackJobPayload",
    "load_generate_metadata",
    "patch_generate_idempotency_key",
    "patch_writeback_idempotency_key",
    "persist_generate_metadata",
]
