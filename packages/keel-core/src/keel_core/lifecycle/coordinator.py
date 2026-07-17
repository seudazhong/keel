"""Restart-safe, idempotent, resumable erasure coordinator (M3.5, WS-K).

The coordinator turns an :class:`~keel_core.lifecycle.models.ErasureRequest` into an
ordered plan of *steps*, records each step's outcome in the durable ledger, and finalizes
the request. Its guarantees:

* **Idempotent** — every step is a physical delete (safe to repeat); a re-run replays the
  ledger and skips already-``done`` steps.
* **Restart-safe / resumable** — a crash mid-run leaves earlier steps ``done``; the durable
  job retries :meth:`execute`, which resumes at the first unfinished step.
* **Observable** — every step transition is logged and recorded with a row count.
* **Honest about external systems** — an external provider/telemetry deletion that cannot
  be performed is recorded ``unsupported``/``failed`` and forces the request to finish
  ``partial``; it is never silently reported as ``completed``.

Ordering is chosen so a step's prerequisites are deleted only after it (e.g. tool-spill
paths and tombstones are captured before the events they reference are removed).
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.lifecycle.coding import CodingArtifactCleaner, ToolSpillCleaner
from keel_core.lifecycle.models import (
    INCOMPLETE_STEP_STATUSES,
    ErasureRequest,
    ErasureResult,
    ErasureStatus,
    ErasureTarget,
    ErasureTargetKind,
    StepStatus,
)
from keel_core.lifecycle.purge import ScopePurgeRepository
from keel_core.lifecycle.redis import NullRedisLifecycleCleaner
from keel_core.lifecycle.store import ErasureStore

logger = logging.getLogger("keel.core.lifecycle")


@dataclass(frozen=True)
class ExternalStepOutcome:
    """The result of attempting an external-system deletion."""

    status: StepStatus
    detail: str | None = None
    rows_affected: int = 0


@runtime_checkable
class ExternalDeletionStep(Protocol):
    """A best-effort deletion in an external system (provider logs, telemetry, ...)."""

    @property
    def name(self) -> str: ...

    async def erase(self, target: ErasureTarget) -> ExternalStepOutcome: ...


class UnsupportedExternalStep:
    """An external target with no deletion API — always ``unsupported`` (forces partial)."""

    def __init__(self, name: str, *, detail: str = "no external deletion API available") -> None:
        self._name = name
        self._detail = detail

    @property
    def name(self) -> str:
        return self._name

    async def erase(self, target: ErasureTarget) -> ExternalStepOutcome:
        return ExternalStepOutcome(StepStatus.unsupported, self._detail)


@runtime_checkable
class RedisSessionCleaner(Protocol):
    async def purge_sessions(self, session_ids: Sequence[str]) -> int: ...


@dataclass
class _PlannedStep:
    name: str
    internal: Callable[[], Awaitable[tuple[int, StepStatus, str | None]]] | None = None
    external: ExternalDeletionStep | None = None


class ErasureCoordinator:
    """Executes erasure requests against every store, filesystem, and external step."""

    def __init__(
        self,
        engine: AsyncEngine | None,
        store: ErasureStore,
        *,
        purge: ScopePurgeRepository | None = None,
        redis_cleaner: RedisSessionCleaner | None = None,
        coding_cleaner: CodingArtifactCleaner | None = None,
        spill_cleaner: ToolSpillCleaner | None = None,
        external_steps: Sequence[ExternalDeletionStep] = (),
    ) -> None:
        self._store = store
        if purge is not None:
            self._purge: ScopePurgeRepository | None = purge
        elif engine is not None:
            self._purge = ScopePurgeRepository(engine)
        else:
            self._purge = None
        self._redis = redis_cleaner or NullRedisLifecycleCleaner()
        self._coding = coding_cleaner or CodingArtifactCleaner(None)
        self._spill = spill_cleaner or ToolSpillCleaner(None)
        self._external = tuple(external_steps)

    @property
    def store(self) -> ErasureStore:
        return self._store

    async def submit(
        self,
        target: ErasureTarget,
        idempotency_key: str,
        *,
        request_id: str | None = None,
        requested_by: str | None = None,
        reason: str | None = None,
    ) -> ErasureRequest:
        """Persist (or dedupe) an erasure request. Returns the durable request row."""
        return await self._store.create(
            target,
            idempotency_key,
            request_id=request_id or uuid.uuid4().hex,
            requested_by=requested_by,
            reason=reason,
        )

    async def execute(self, request_id: str, *, current_job_id: str | None = None) -> ErasureResult:
        """Run (or resume) an erasure request to a terminal state (idempotent)."""
        request = await self._store.get(request_id)
        if request is None:
            raise LookupError(f"erasure request {request_id!r} not found")
        if request.status is ErasureStatus.completed:
            return await self._result(request_id, request.status, request.external_incomplete)

        running = await self._store.mark_running(request_id)
        if running is not None:
            request = running

        ledger = {step.step: step.status for step in await self._store.steps(request_id)}
        plan = await self._plan(request.target, current_job_id=current_job_id)

        external_incomplete = False
        for planned in plan:
            prior = ledger.get(planned.name)
            if prior in {StepStatus.done, StepStatus.skipped}:
                continue
            if planned.external is not None:
                outcome = await planned.external.erase(request.target)
                await self._store.record_step(
                    request_id,
                    planned.name,
                    outcome.status,
                    rows_affected=outcome.rows_affected,
                    detail=outcome.detail,
                )
                logger.info(
                    "erasure step scope=%s request=%s step=%s status=%s",
                    request.scope_id,
                    request_id,
                    planned.name,
                    outcome.status.value,
                )
                if outcome.status in INCOMPLETE_STEP_STATUSES:
                    external_incomplete = True
                continue
            assert planned.internal is not None
            try:
                rows, status, detail = await planned.internal()
            except Exception as exc:  # noqa: BLE001 - recorded + surfaced for retry
                await self._store.record_step(
                    request_id, planned.name, StepStatus.pending, detail=type(exc).__name__
                )
                await self._store.bump_attempt(
                    request_id, error=f"{planned.name}: {type(exc).__name__}"
                )
                logger.warning(
                    "erasure step failed scope=%s request=%s step=%s error=%s",
                    request.scope_id,
                    request_id,
                    planned.name,
                    type(exc).__name__,
                )
                raise
            await self._store.record_step(
                request_id, planned.name, status, rows_affected=rows, detail=detail
            )
            logger.info(
                "erasure step scope=%s request=%s step=%s status=%s rows=%d",
                request.scope_id,
                request_id,
                planned.name,
                status.value,
                rows,
            )

        # Re-scan the ledger so a resumed run inherits earlier external gaps.
        for step in await self._store.steps(request_id):
            if step.status in INCOMPLETE_STEP_STATUSES:
                external_incomplete = True

        final = ErasureStatus.partial if external_incomplete else ErasureStatus.completed
        await self._store.finalize(request_id, final, external_incomplete=external_incomplete)
        logger.info(
            "erasure finalized scope=%s request=%s status=%s external_incomplete=%s",
            request.scope_id,
            request_id,
            final.value,
            external_incomplete,
        )
        return await self._result(request_id, final, external_incomplete)

    async def _result(
        self, request_id: str, status: ErasureStatus, external_incomplete: bool
    ) -> ErasureResult:
        steps = await self._store.steps(request_id)
        return ErasureResult(
            request_id=request_id,
            status=status,
            external_incomplete=external_incomplete,
            steps=tuple(steps),
        )

    async def _plan(
        self, target: ErasureTarget, *, current_job_id: str | None
    ) -> list[_PlannedStep]:
        if target.kind is ErasureTargetKind.scope:
            return self._scope_plan(target, current_job_id=current_job_id)
        if target.kind is ErasureTargetKind.session:
            return self._session_plan(target)
        return self._project_plan(target)

    def _require_purge(self) -> ScopePurgeRepository:
        if self._purge is None:
            raise RuntimeError("erasure coordinator has no purge repository (no engine)")
        return self._purge

    def _scope_plan(
        self, target: ErasureTarget, *, current_job_id: str | None
    ) -> list[_PlannedStep]:
        scope = target.scope_id
        purge = self._require_purge()

        async def tombstones() -> tuple[int, StepStatus, str | None]:
            session_ids = await purge.session_ids(scope)
            for session_id in session_ids:
                await self._store.add_tombstone(session_id, request_id=None, reason="scope erasure")
            return len(session_ids), StepStatus.done, None

        async def spill() -> tuple[int, StepStatus, str | None]:
            paths = await purge.spill_paths(scope)
            removed = await self._spill.purge_paths(paths)
            return removed, StepStatus.done, None

        async def redis_streams() -> tuple[int, StepStatus, str | None]:
            sessions = await self._store.tombstoned_sessions()
            removed = await self._redis.purge_sessions(sorted(sessions))
            return removed, StepStatus.done, None

        async def coding_skip() -> tuple[int, StepStatus, str | None]:
            return 0, StepStatus.skipped, "no project reference for scope erasure"

        internal: list[tuple[str, Callable[[], Awaitable[tuple[int, StepStatus, str | None]]]]] = [
            ("tombstones", tombstones),
            ("tool_spill", spill),
            ("redis_streams", redis_streams),
            ("message_embeddings", lambda: self._wrap(purge.message_embeddings(scope))),
            ("events_and_sessions", lambda: self._wrap(purge.events_and_sessions(scope))),
            ("archival", lambda: self._wrap(purge.archival(scope))),
            ("memory", lambda: self._wrap(purge.memory(scope))),
            ("memory_proposals", lambda: self._wrap(purge.memory_proposals(scope))),
            ("consolidation_cursor", lambda: self._wrap(purge.consolidation_cursor(scope))),
            ("knowledge", lambda: self._wrap(purge.knowledge(scope))),
            ("connector_tokens", lambda: self._wrap(purge.connector_tokens(scope))),
            ("connector_outbox", lambda: self._wrap(purge.connector_outbox(scope))),
            ("oauth_states", lambda: self._wrap(purge.oauth_states(scope))),
            ("schedules", lambda: self._wrap(purge.schedules(scope))),
            ("approvals", lambda: self._wrap(purge.approvals(scope))),
            ("runs", lambda: self._wrap(purge.runs(scope))),
            (
                "jobs",
                lambda: self._wrap(purge.jobs(scope, exclude_job_id=current_job_id)),
            ),
            ("coding_artifacts", coding_skip),
        ]
        plan = [_PlannedStep(name=name, internal=fn) for name, fn in internal]
        plan.extend(self._external_steps())
        return plan

    def _session_plan(self, target: ErasureTarget) -> list[_PlannedStep]:
        scope = target.scope_id
        session_id = target.resource_id
        assert session_id is not None
        purge = self._require_purge()

        async def tombstone_session() -> tuple[int, StepStatus, str | None]:
            await self._store.add_tombstone(session_id, request_id=None, reason="session erasure")
            return 1, StepStatus.done, None

        async def spill() -> tuple[int, StepStatus, str | None]:
            paths = await purge.spill_paths(scope, session_id)
            removed = await self._spill.purge_paths(paths)
            return removed, StepStatus.done, None

        async def redis_stream() -> tuple[int, StepStatus, str | None]:
            removed = await self._redis.purge_sessions([session_id])
            return removed, StepStatus.done, None

        internal: list[tuple[str, Callable[[], Awaitable[tuple[int, StepStatus, str | None]]]]] = [
            ("tombstone_session", tombstone_session),
            ("tool_spill", spill),
            ("redis_stream", redis_stream),
            (
                "session_message_embeddings",
                lambda: self._wrap(purge.session_message_embeddings(scope, session_id)),
            ),
            ("session_events", lambda: self._wrap(purge.session_events(scope, session_id))),
        ]
        plan = [_PlannedStep(name=name, internal=fn) for name, fn in internal]
        plan.extend(self._external_steps())
        return plan

    def _project_plan(self, target: ErasureTarget) -> list[_PlannedStep]:
        project_id = target.resource_id
        assert project_id is not None

        async def coding() -> tuple[int, StepStatus, str | None]:
            removed = await self._coding.purge_project(project_id)
            if removed:
                return 1, StepStatus.done, None
            return 0, StepStatus.done, "no on-disk project state"

        plan = [_PlannedStep(name="coding_artifacts", internal=coding)]
        plan.extend(self._external_steps())
        return plan

    def _external_steps(self) -> list[_PlannedStep]:
        return [_PlannedStep(name=step.name, external=step) for step in self._external]

    @staticmethod
    async def _wrap(coro: Awaitable[int]) -> tuple[int, StepStatus, str | None]:
        rows = await coro
        return rows, StepStatus.done, None


__all__ = [
    "ErasureCoordinator",
    "ExternalDeletionStep",
    "ExternalStepOutcome",
    "RedisSessionCleaner",
    "UnsupportedExternalStep",
]
