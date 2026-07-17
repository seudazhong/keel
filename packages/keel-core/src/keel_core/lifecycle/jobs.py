"""Worker-agnostic durable erasure job handler (M3.5, WS-K).

The erasure request is executed by a durable background job (ADR-0010) so it is
restart-safe, retried with backoff, and observable through the standard jobs API. The
handler is deliberately thin: it validates the payload and delegates to the idempotent,
resumable :class:`keel_core.lifecycle.coordinator.ErasureCoordinator`. A job retry simply
re-runs :meth:`ErasureCoordinator.execute`, which resumes at the first unfinished step.

A ``partial`` result (data stores erased, an external step could not be verified) is a job
*success* — the incompleteness is recorded on the erasure request for governance, not a
worker failure. Only an internal store-cleanup error retries/fails the job; on terminal
failure the ``on_failed`` hook marks the request ``failed``.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from keel_core.jobs import (
    CancelMode,
    JobError,
    JobRecord,
    JobResult,
    PermanentJobError,
)
from keel_core.lifecycle.coordinator import ErasureCoordinator
from keel_core.lifecycle.models import ErasureStatus

ERASURE_KIND = "lifecycle.erase"
ERASURE_MAX_ATTEMPTS = 5
# Erasure must run to completion once admitted: cancelling a half-erased request would
# leave an ambiguous state, and the operation is idempotent/resumable anyway.
ERASURE_CANCEL_MODE = CancelMode.disabled

_INVALID_PAYLOAD_CODE = "invalid_erasure_job_payload"
_SCOPE_MISMATCH_CODE = "erasure_scope_mismatch"
_UNKNOWN_REQUEST_CODE = "erasure_request_not_found"


@runtime_checkable
class LifecycleJobContext(Protocol):
    """The durable-job context slice the erasure handler needs."""

    job_id: str
    scope_id: str

    async def checkpoint(self) -> None: ...


def _request_id(payload: dict[str, Any]) -> str:
    value = payload.get("request_id")
    if not isinstance(value, str) or not value.strip():
        raise PermanentJobError(
            _INVALID_PAYLOAD_CODE, "erasure job payload must carry a string request_id"
        )
    return value


class ErasureJobHandlers:
    """Durable-job handlers bound to one scope's erasure coordinator."""

    def __init__(self, coordinator: ErasureCoordinator) -> None:
        self._coordinator = coordinator

    async def erase(self, ctx: LifecycleJobContext, payload: dict[str, Any]) -> JobResult:
        request_id = _request_id(payload)
        if ctx.scope_id != self._coordinator.store.scope_id:
            raise PermanentJobError(
                _SCOPE_MISMATCH_CODE, "erasure job scope does not match the coordinator scope"
            )
        await ctx.checkpoint()
        try:
            result = await self._coordinator.execute(request_id, current_job_id=ctx.job_id)
        except LookupError as exc:
            raise PermanentJobError(_UNKNOWN_REQUEST_CODE, str(exc)) from None
        message = (
            f"Erased {result.rows_affected} rows across {len(result.steps)} steps "
            f"(status={result.status.value})."
        )
        return JobResult(
            data={
                "request_id": request_id,
                "status": result.status.value,
                "rows_affected": result.rows_affected,
                "external_incomplete": result.external_incomplete,
                "steps": {step.step: step.status.value for step in result.steps},
            },
            message=message,
        )

    async def erase_failed(self, row: JobRecord, error: JobError) -> None:
        """Mark the erasure request ``failed`` when the job exhausts its retries."""
        value = row.payload.get("request_id")
        if not isinstance(value, str) or not value.strip():
            return
        request = await self._coordinator.store.get(value)
        external_incomplete = request.external_incomplete if request is not None else False
        await self._coordinator.store.finalize(
            value, ErasureStatus.failed, external_incomplete=external_incomplete
        )


__all__ = [
    "ERASURE_CANCEL_MODE",
    "ERASURE_KIND",
    "ERASURE_MAX_ATTEMPTS",
    "ErasureJobHandlers",
    "LifecycleJobContext",
]
