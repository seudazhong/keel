"""Unit tests for the durable patch jobs module (WS-PP, P3b-0).

Covers the worker-agnostic contracts + handlers around the coordinator, in isolation from any real
worker/provider/DB:

* strict payload validation / round-trip / idempotency keys;
* the JOB-lease heartbeat keeper (``_JobHeartbeatKeeper``): a cooperative cancel or a lost job lease
  (or a typed ``SQLAlchemyError``) records the outcome and signals the operation (cancel event for
  generation, task cancel for writeback) — never swallowed;
* handler error mapping onto the job framework's retry/permanent/lease/cancel semantics, with NO
  broad catch and NO success-shaped fallback.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from keel_core.jobs import (
    JobCancellationRequested,
    JobLeaseLostError,
    JobResult,
    PermanentJobError,
    RetryableJobError,
)
from keel_core.patch.errors import (
    PatchLeaseLost,
    PatchProviderError,
    PatchProviderUnavailable,
    PatchRemoteUnavailable,
    PatchStateError,
    PatchValidationError,
    PatchWritebackError,
)
from keel_core.patch.jobs import (
    PATCH_GENERATE_KIND,
    PATCH_GENERATE_METADATA_VERSION,
    PATCH_GENERATE_REQUEST_MARKER,
    PATCH_WRITEBACK_KIND,
    PatchGenerateJobPayload,
    PatchJobHandlers,
    PatchWritebackJobPayload,
    _JobHeartbeatKeeper,
    load_generate_metadata,
    patch_generate_idempotency_key,
    patch_writeback_idempotency_key,
    persist_generate_metadata,
)
from keel_core.patch.models import PatchProposalRequest, PatchStatus
from keel_core.state import InMemoryEventStore


def _request(idem: str = "k1") -> PatchProposalRequest:
    return PatchProposalRequest(
        org_id="o",
        project_id="p",
        actor="u",
        task="do the thing",
        base_ref="main",
        model="m",
        idempotency_key=idem,
    )


def _gen_payload_dict(**overrides: Any) -> dict[str, Any]:
    payload = PatchGenerateJobPayload.from_request(_request(), proposal_id="prop-1", run_id="run-1")
    data = payload.model_dump()
    data.update(overrides)
    return data


def _approval_pending_proposal() -> SimpleNamespace:
    # The generate handler reads only these attributes off the returned proposal.
    return SimpleNamespace(
        id="prop-1",
        run_id="run-1",
        status=PatchStatus.approval_pending,
        approval_id="appr-1",
    )


def _draft_pr_proposal(status: PatchStatus = PatchStatus.draft_pr_created) -> SimpleNamespace:
    return SimpleNamespace(id="prop-1", status=status, pr_number=101, remote_branch="keel/patch/x")


class _FakeContext:
    """A duck-typed ``PatchJobContext`` with a scriptable ``checkpoint``.

    ``error_after`` makes the Nth (1-based) ``checkpoint`` raise ``checkpoint_error`` — so the
    handler's *initial* checkpoint (call 1) can succeed while the keeper's heartbeat (call >=2)
    raises a cooperative cancel / lost lease."""

    def __init__(
        self,
        *,
        lease: int = 30,
        checkpoint_error: BaseException | None = None,
        error_after: int = 1,
    ) -> None:
        self._lease = lease
        self.checkpoint_error = checkpoint_error
        self.error_after = error_after
        self.checkpoints = 0

    @property
    def job_id(self) -> str:
        return "job-1"

    @property
    def scope_id(self) -> str:
        return "agent:o/patch"

    @property
    def job_lease_seconds(self) -> int:
        return self._lease

    async def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.checkpoint_error is not None and self.checkpoints >= self.error_after:
            raise self.checkpoint_error


class _ScriptedCoordinator:
    """A coordinator double whose execution entrypoints return or raise immediately."""

    def __init__(
        self,
        *,
        gen_result: Any = None,
        gen_error: BaseException | None = None,
        wb_result: Any = None,
        wb_error: BaseException | None = None,
    ) -> None:
        self.gen_result = gen_result
        self.gen_error = gen_error
        self.wb_result = wb_result
        self.wb_error = wb_error
        self.gen_calls = 0
        self.wb_calls = 0

    async def execute_generation(
        self,
        org_id: str,
        run_id: str,
        request: Any,
        *,
        worker_id: str,
        interrupt: Any,
        now: Any = None,
    ) -> Any:
        self.gen_calls += 1
        if self.gen_error is not None:
            raise self.gen_error
        return self.gen_result

    async def execute_writeback(
        self, org_id: str, proposal_id: str, *, worker_id: str, now: Any = None
    ) -> Any:
        self.wb_calls += 1
        if self.wb_error is not None:
            raise self.wb_error
        return self.wb_result


class _InterruptGenerationCoordinator:
    """``execute_generation`` blocks until the folded ``interrupt`` fires, then aborts as lost.

    Models a slow generation whose author interrupt is tripped by the JOB heartbeat keeper (a
    cooperative cancel or a lost job lease), which the coordinator surfaces as
    ``PatchLeaseLost``."""

    def __init__(self) -> None:
        self.gen_calls = 0

    async def execute_generation(
        self,
        org_id: str,
        run_id: str,
        request: Any,
        *,
        worker_id: str,
        interrupt: Any,
        now: Any = None,
    ) -> Any:
        self.gen_calls += 1
        for _ in range(500):
            if interrupt():
                raise PatchLeaseLost("run lease reclaimed")
            await asyncio.sleep(0.01)
        raise AssertionError("interrupt never fired")


class _BlockingWritebackCoordinator:
    """``execute_writeback`` blocks so the keeper (bound to the work task) can cancel it."""

    def __init__(self) -> None:
        self.wb_calls = 0

    async def execute_writeback(
        self, org_id: str, proposal_id: str, *, worker_id: str, now: Any = None
    ) -> Any:
        self.wb_calls += 1
        await asyncio.sleep(5)
        return _draft_pr_proposal()


class _DelayedRaiseGenerationCoordinator:
    """``execute_generation`` blocks for ``delay`` then raises the scripted error.

    ``delay`` sits comfortably above the heartbeat interval, and the keeper's shorter heartbeat
    sleep always elapses first in the same loop — so the heartbeat fires *before* the error. A
    frozen checkpoint count afterwards can then only mean the keeper was stopped (never leaked).
    """

    def __init__(self, *, exc: BaseException, delay: float) -> None:
        self._exc = exc
        self._delay = delay
        self.gen_calls = 0

    async def execute_generation(
        self,
        org_id: str,
        run_id: str,
        request: Any,
        *,
        worker_id: str,
        interrupt: Any,
        now: Any = None,
    ) -> Any:
        self.gen_calls += 1
        await asyncio.sleep(self._delay)
        raise self._exc


class _StartedThenBlockingGenerationCoordinator:
    """Signals it started, then blocks so the handler task can be cancelled."""

    def __init__(self, started: asyncio.Event) -> None:
        self._started = started
        self.gen_calls = 0

    async def execute_generation(
        self,
        org_id: str,
        run_id: str,
        request: Any,
        *,
        worker_id: str,
        interrupt: Any,
        now: Any = None,
    ) -> Any:
        self.gen_calls += 1
        self._started.set()
        await asyncio.sleep(3600)
        raise AssertionError("generation must be cancelled before completing")  # pragma: no cover


class _DelayedRaiseWritebackCoordinator:
    """``execute_writeback`` blocks for ``delay`` (above the heartbeat interval) then raises."""

    def __init__(self, *, exc: BaseException, delay: float) -> None:
        self._exc = exc
        self._delay = delay
        self.wb_calls = 0

    async def execute_writeback(
        self, org_id: str, proposal_id: str, *, worker_id: str, now: Any = None
    ) -> Any:
        self.wb_calls += 1
        await asyncio.sleep(self._delay)
        raise self._exc


def _handlers(coordinator: Any) -> PatchJobHandlers:
    return PatchJobHandlers(coordinator)  # type: ignore[arg-type]


# --- payload contracts ---------------------------------------------------------------


def test_generate_payload_round_trip_reconstructs_the_request() -> None:
    req = _request("idem-9")
    payload = PatchGenerateJobPayload.from_request(req, proposal_id="prop-9", run_id="run-9")
    assert payload.proposal_id == "prop-9" and payload.run_id == "run-9"
    rebuilt = payload.to_request()
    assert rebuilt == req  # every immutable request field survives the durable round-trip


def test_generate_payload_is_strict_and_forbids_extra_fields() -> None:
    good = _gen_payload_dict()
    # extra key rejected (no silent drop of an unexpected field)
    with pytest.raises(ValidationError):
        PatchGenerateJobPayload.model_validate({**good, "unexpected": "x"})
    # wrong type rejected under strict mode (no int->str coercion)
    with pytest.raises(ValidationError):
        PatchGenerateJobPayload.model_validate({**good, "org_id": 123})
    # missing required field rejected
    missing = dict(good)
    del missing["run_id"]
    with pytest.raises(ValidationError):
        PatchGenerateJobPayload.model_validate(missing)


def test_generate_payload_is_frozen() -> None:
    payload = PatchGenerateJobPayload.from_request(_request(), proposal_id="p", run_id="r")
    with pytest.raises(ValidationError):
        payload.run_id = "other"  # type: ignore[misc]


def test_writeback_payload_requires_ids_and_forbids_extra() -> None:
    ok = PatchWritebackJobPayload.model_validate({"proposal_id": "prop-1", "org_id": "o"})
    assert ok.proposal_id == "prop-1" and ok.org_id == "o"
    with pytest.raises(ValidationError):
        PatchWritebackJobPayload.model_validate({"proposal_id": "prop-1"})  # missing org_id
    with pytest.raises(ValidationError):
        PatchWritebackJobPayload.model_validate({"proposal_id": "prop-1", "org_id": "o", "nope": 1})


def test_idempotency_keys_are_stable_and_kind_scoped() -> None:
    assert patch_generate_idempotency_key("prop-7") == f"{PATCH_GENERATE_KIND}:prop-7"
    assert patch_writeback_idempotency_key("prop-7") == f"{PATCH_WRITEBACK_KIND}:prop-7"
    # generation and writeback keys never collide for the same proposal
    assert patch_generate_idempotency_key("p") != patch_writeback_idempotency_key("p")


# --- durable generate-request metadata (reconstruction seam) -------------------------


def _raw_metadata_event(payload: dict[str, Any], *, run_id: str = "run-1") -> Any:
    from datetime import UTC, datetime

    from keel_core.events import Event, EventType

    return Event(
        type=EventType.run_started,
        seq=0,
        session_id=run_id,
        scope_id="agent:o/patch",
        run_id=run_id,
        ts=datetime.now(UTC),
        payload={PATCH_GENERATE_REQUEST_MARKER: payload, "dedup_key": f"m:{run_id}"},
    )


async def test_generate_metadata_round_trips_through_event_log() -> None:
    events = InMemoryEventStore()
    payload = PatchGenerateJobPayload.model_validate(
        _gen_payload_dict(test_commands=["pytest -q", "ruff check ."])
    )
    await persist_generate_metadata(events, scope_id="agent:o/patch", payload=payload)
    loaded = await load_generate_metadata(events, "run-1")
    assert loaded == payload
    # A JSONB store/read lowers the immutable tuple to a list; the loaded payload is a tuple again.
    assert loaded is not None
    assert loaded.test_commands == ("pytest -q", "ruff check .")


async def test_generate_metadata_persist_is_idempotent() -> None:
    events = InMemoryEventStore()
    payload = PatchGenerateJobPayload.model_validate(_gen_payload_dict())
    await persist_generate_metadata(events, scope_id="agent:o/patch", payload=payload)
    # A replayed/idempotent re-admission is a no-op (the winner's copy is authoritative).
    await persist_generate_metadata(events, scope_id="agent:o/patch", payload=payload)
    assert await load_generate_metadata(events, "run-1") == payload


def test_generate_payload_coerces_jsonb_list_test_commands_to_tuple() -> None:
    payload = PatchGenerateJobPayload.model_validate(_gen_payload_dict(test_commands=["a", "b"]))
    assert payload.test_commands == ("a", "b")
    # A non-list (a bare string) is left untouched and fails closed under strict validation.
    with pytest.raises(ValidationError):
        PatchGenerateJobPayload.model_validate(_gen_payload_dict(test_commands="pytest"))


async def test_load_generate_metadata_missing_returns_none() -> None:
    assert await load_generate_metadata(InMemoryEventStore(), "run-absent") is None


async def test_load_generate_metadata_version_mismatch_fails_closed() -> None:
    events = InMemoryEventStore()
    good = _gen_payload_dict()
    await events.append(
        _raw_metadata_event({"version": PATCH_GENERATE_METADATA_VERSION + 1, "payload": good})
    )
    # An unknown/legacy metadata version never runs generation under a payload we cannot trust.
    assert await load_generate_metadata(events, "run-1") is None


async def test_load_generate_metadata_invalid_payload_fails_closed() -> None:
    events = InMemoryEventStore()
    bad = _gen_payload_dict()
    del bad["run_id"]  # no longer validates against the strict schema
    await events.append(
        _raw_metadata_event({"version": PATCH_GENERATE_METADATA_VERSION, "payload": bad})
    )
    assert await load_generate_metadata(events, "run-1") is None


# --- job-lease heartbeat keeper ------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_keeper_signals_cancel_event_on_cooperative_cancel() -> None:
    event = asyncio.Event()

    async def _cancel() -> None:
        raise JobCancellationRequested()

    keeper = _JobHeartbeatKeeper(checkpoint=_cancel, interval_seconds=0.01, cancel_event=event)
    keeper.start()
    try:
        await asyncio.wait_for(keeper._task, timeout=1.0)  # type: ignore[arg-type]
    finally:
        await keeper.stop()
    assert keeper.cancelled is True and keeper.lost is False
    assert event.is_set()


@pytest.mark.asyncio
async def test_heartbeat_keeper_marks_lost_and_retains_cause_on_lease_loss() -> None:
    event = asyncio.Event()
    boom = JobLeaseLostError("job-1")

    async def _lost() -> None:
        raise boom

    keeper = _JobHeartbeatKeeper(checkpoint=_lost, interval_seconds=0.01, cancel_event=event)
    keeper.start()
    try:
        await asyncio.wait_for(keeper._task, timeout=1.0)  # type: ignore[arg-type]
    finally:
        await keeper.stop()
    assert keeper.lost is True and keeper.cancelled is False
    assert keeper.error is boom and event.is_set()


@pytest.mark.asyncio
async def test_heartbeat_keeper_treats_db_error_as_lost() -> None:
    db = OperationalError("SELECT 1", {}, Exception("db down"))

    async def _db_error() -> None:
        raise db

    keeper = _JobHeartbeatKeeper(checkpoint=_db_error, interval_seconds=0.01)
    keeper.start()
    try:
        await asyncio.wait_for(keeper._task, timeout=1.0)  # type: ignore[arg-type]
    finally:
        await keeper.stop()
    assert keeper.lost is True and isinstance(keeper.error, SQLAlchemyError)


@pytest.mark.asyncio
async def test_heartbeat_keeper_cancels_bound_target() -> None:
    target = asyncio.ensure_future(asyncio.sleep(10))

    async def _cancel() -> None:
        raise JobCancellationRequested()

    keeper = _JobHeartbeatKeeper(checkpoint=_cancel, interval_seconds=0.01)
    keeper.bind(target)
    keeper.start()
    try:
        await asyncio.wait_for(keeper._task, timeout=1.0)  # type: ignore[arg-type]
        with pytest.raises(asyncio.CancelledError):
            await target
    finally:
        await keeper.stop()
    assert keeper.cancelled is True


# --- generate handler mapping --------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_invalid_payload_is_permanent() -> None:
    coord = _ScriptedCoordinator(gen_result=_approval_pending_proposal())
    handlers = _handlers(coord)
    with pytest.raises(PermanentJobError) as ei:
        await handlers.generate(_FakeContext(), {"bad": "payload"})
    assert ei.value.code == "invalid_patch_generate_payload"
    assert coord.gen_calls == 0  # never reached the coordinator


@pytest.mark.asyncio
async def test_generate_success_returns_job_result() -> None:
    coord = _ScriptedCoordinator(gen_result=_approval_pending_proposal())
    result = await _handlers(coord).generate(_FakeContext(), _gen_payload_dict())
    assert isinstance(result, JobResult)
    assert result.data["status"] == "approval_pending"
    assert result.data["proposal_id"] == "prop-1" and result.data["approval_id"] == "appr-1"
    assert coord.gen_calls == 1


@pytest.mark.asyncio
async def test_generate_transient_provider_is_retryable() -> None:
    coord = _ScriptedCoordinator(gen_error=PatchProviderUnavailable("upstream 503"))
    handlers = _handlers(coord)
    with pytest.raises(RetryableJobError) as ei:
        await handlers.generate(_FakeContext(), _gen_payload_dict())
    assert ei.value.code == "patch_generation_transient"


@pytest.mark.asyncio
async def test_generate_permanent_error_is_permanent() -> None:
    coord = _ScriptedCoordinator(gen_error=PatchValidationError("empty proposal"))
    handlers = _handlers(coord)
    with pytest.raises(PermanentJobError) as ei:
        await handlers.generate(_FakeContext(), _gen_payload_dict())
    assert ei.value.code == "patch_generation_failed"


@pytest.mark.asyncio
async def test_generate_permanent_provider_error_is_permanent() -> None:
    # A permanent PatchProviderError (cost ceiling / malformed output) is NOT retryable.
    coord = _ScriptedCoordinator(gen_error=PatchProviderError("ceiling exceeded"))
    with pytest.raises(PermanentJobError):
        await _handlers(coord).generate(_FakeContext(), _gen_payload_dict())


@pytest.mark.asyncio
async def test_generate_lease_lost_via_cooperative_cancel_maps_to_cancellation() -> None:
    # The job heartbeat observes a cooperative cancel; the folded interrupt aborts generation with
    # PatchLeaseLost, which the handler resolves to JobCancellationRequested (not a lease loss).
    coord = _InterruptGenerationCoordinator()
    ctx = _FakeContext(lease=1, checkpoint_error=JobCancellationRequested(), error_after=2)
    with pytest.raises(JobCancellationRequested):
        await asyncio.wait_for(_handlers(coord).generate(ctx, _gen_payload_dict()), timeout=5.0)
    assert coord.gen_calls == 1


@pytest.mark.asyncio
async def test_generate_lease_lost_via_lost_job_lease_maps_to_lease_lost() -> None:
    # A lost JOB lease (not a cooperative cancel) folds into the interrupt; the resulting
    # PatchLeaseLost maps to JobLeaseLostError so the framework re-dispatches under a fresh lease.
    coord = _InterruptGenerationCoordinator()
    ctx = _FakeContext(lease=1, checkpoint_error=JobLeaseLostError("job-1"), error_after=2)
    with pytest.raises(JobLeaseLostError):
        await asyncio.wait_for(_handlers(coord).generate(ctx, _gen_payload_dict()), timeout=5.0)
    assert coord.gen_calls == 1


# --- writeback handler mapping -------------------------------------------------------


@pytest.mark.asyncio
async def test_writeback_invalid_payload_is_permanent() -> None:
    coord = _ScriptedCoordinator(wb_result=_draft_pr_proposal())
    handlers = _handlers(coord)
    with pytest.raises(PermanentJobError) as ei:
        await handlers.writeback(_FakeContext(), {"only": "garbage"})
    assert ei.value.code == "invalid_patch_writeback_payload"
    assert coord.wb_calls == 0


@pytest.mark.asyncio
async def test_writeback_success_returns_job_result() -> None:
    coord = _ScriptedCoordinator(wb_result=_draft_pr_proposal())
    payload = {"proposal_id": "prop-1", "org_id": "o"}
    result = await _handlers(coord).writeback(_FakeContext(), payload)
    assert isinstance(result, JobResult)
    assert result.data["status"] == "draft_pr_created" and result.data["pr_number"] == 101
    assert coord.wb_calls == 1


@pytest.mark.asyncio
async def test_writeback_stale_terminal_is_success_not_failure() -> None:
    # Base drift resolves to a terminal ``stale`` proposal inside the coordinator (no raise) — a
    # legitimate terminal outcome the handler reports as success.
    coord = _ScriptedCoordinator(wb_result=_draft_pr_proposal(status=PatchStatus.stale))
    result = await _handlers(coord).writeback(_FakeContext(), {"proposal_id": "p", "org_id": "o"})
    assert result.data["status"] == "stale"


@pytest.mark.asyncio
async def test_writeback_remote_unavailable_is_retryable() -> None:
    coord = _ScriptedCoordinator(wb_error=PatchRemoteUnavailable("github 503"))
    handlers = _handlers(coord)
    with pytest.raises(RetryableJobError) as ei:
        await handlers.writeback(_FakeContext(), {"proposal_id": "p", "org_id": "o"})
    assert ei.value.code == "patch_writeback_transient"


@pytest.mark.asyncio
async def test_writeback_permanent_writeback_error_is_permanent() -> None:
    coord = _ScriptedCoordinator(wb_error=PatchWritebackError("verify refused"))
    handlers = _handlers(coord)
    with pytest.raises(PermanentJobError) as ei:
        await handlers.writeback(_FakeContext(), {"proposal_id": "p", "org_id": "o"})
    assert ei.value.code == "patch_writeback_failed"


@pytest.mark.asyncio
async def test_writeback_other_patch_error_is_permanent() -> None:
    # Any other permanent patch error (e.g. an illegal-state transition) fails the job permanently.
    coord = _ScriptedCoordinator(wb_error=PatchStateError("not approved"))
    with pytest.raises(PermanentJobError):
        await _handlers(coord).writeback(_FakeContext(), {"proposal_id": "p", "org_id": "o"})


@pytest.mark.asyncio
async def test_writeback_cooperative_cancel_maps_to_cancellation() -> None:
    # The heartbeat cancels the bound writeback task; the handler resolves the cancellation to
    # JobCancellationRequested (a retry reuses the reserved branch/PR).
    coord = _BlockingWritebackCoordinator()
    ctx = _FakeContext(lease=1, checkpoint_error=JobCancellationRequested(), error_after=2)
    with pytest.raises(JobCancellationRequested):
        await asyncio.wait_for(
            _handlers(coord).writeback(ctx, {"proposal_id": "p", "org_id": "o"}), timeout=5.0
        )
    assert coord.wb_calls == 1


@pytest.mark.asyncio
async def test_writeback_lost_job_lease_maps_to_lease_lost() -> None:
    coord = _BlockingWritebackCoordinator()
    ctx = _FakeContext(lease=1, checkpoint_error=JobLeaseLostError("job-1"), error_after=2)
    with pytest.raises(JobLeaseLostError):
        await asyncio.wait_for(
            _handlers(coord).writeback(ctx, {"proposal_id": "p", "org_id": "o"}), timeout=5.0
        )
    assert coord.wb_calls == 1


# --- handler keeper lifecycle: no heartbeat-task leak ---------------------------------


@pytest.mark.asyncio
async def test_generate_handler_non_patch_error_stops_keeper_and_does_not_leak() -> None:
    # A non-``PatchError`` (a bare ``RuntimeError`` from the coordinator) is caught by none of the
    # handler's typed ``except`` branches, so only the ``try/finally`` can stop the heartbeat
    # keeper. Without it the heartbeat task would leak and keep renewing the (now doomed) job lease.
    ctx = _FakeContext(lease=1)  # heartbeat interval 0.5s (< the coordinator's 0.7s delay)
    coord = _DelayedRaiseGenerationCoordinator(exc=RuntimeError("coordinator boom"), delay=0.7)
    before = asyncio.all_tasks()
    with pytest.raises(RuntimeError, match="coordinator boom"):
        await _handlers(coord).generate(ctx, _gen_payload_dict())

    # The keeper heartbeated past the handler's initial checkpoint and is now stopped: the count
    # freezes and no heartbeat task is left behind on the loop.
    assert ctx.checkpoints >= 2
    frozen = ctx.checkpoints
    await asyncio.sleep(0.6)  # span a would-be next heartbeat interval
    assert ctx.checkpoints == frozen
    assert [t for t in asyncio.all_tasks() if t not in before] == []


@pytest.mark.asyncio
async def test_generate_handler_external_cancellation_stops_keeper_and_does_not_leak() -> None:
    # Cancelling the handler task injects ``asyncio.CancelledError`` at the coordinator await — a
    # path no typed ``except`` catches. The ``finally`` must still stop the heartbeat keeper.
    ctx = _FakeContext(lease=1)
    started = asyncio.Event()
    coord = _StartedThenBlockingGenerationCoordinator(started)
    before = asyncio.all_tasks()
    task = asyncio.ensure_future(_handlers(coord).generate(ctx, _gen_payload_dict()))
    await started.wait()
    await asyncio.sleep(0.7)  # let at least one heartbeat fire past the initial checkpoint
    assert ctx.checkpoints >= 2
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    frozen = ctx.checkpoints
    await asyncio.sleep(0.6)
    assert ctx.checkpoints == frozen
    assert [t for t in asyncio.all_tasks() if t not in before] == []


@pytest.mark.asyncio
async def test_writeback_handler_non_patch_error_stops_keeper_and_does_not_leak() -> None:
    # The writeback handler already wraps its keeper in a ``finally``; confirm a non-patch
    # ``RuntimeError`` (caught by no typed branch) still stops the heartbeat with no task leak.
    ctx = _FakeContext(lease=1)
    coord = _DelayedRaiseWritebackCoordinator(exc=RuntimeError("writeback boom"), delay=0.7)
    before = asyncio.all_tasks()
    with pytest.raises(RuntimeError, match="writeback boom"):
        await _handlers(coord).writeback(ctx, {"proposal_id": "p", "org_id": "o"})

    assert ctx.checkpoints >= 2
    frozen = ctx.checkpoints
    await asyncio.sleep(0.6)
    assert ctx.checkpoints == frozen
    assert [t for t in asyncio.all_tasks() if t not in before] == []
