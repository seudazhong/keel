"""REST API: authenticated data-erasure governance (``/v1/erasure``, WS-K, M3.5).

Additive-only under ``/v1`` (G14). Submitting an erasure requires ``admin``; reading
status requires ``operator``; retry requires ``admin``. Submission admits a durable job
(the same jobs/admission pattern as Knowledge) that runs the idempotent, resumable erasure
coordinator on the worker. A request that could not verify an external provider/telemetry
deletion finishes ``partial`` — surfaced here so an operator never mistakes it for a clean
``completed``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from keel_core.lifecycle.models import (
    ErasureRequest,
    ErasureStatus,
    ErasureStep,
    ErasureTarget,
    ErasureTargetKind,
)
from keel_core.lifecycle.service import ErasureService, ErasureStatusView
from keel_server.auth import Role, authenticate, require_role

router = APIRouter(
    prefix="/v1/erasure",
    tags=["erasure"],
    dependencies=[Depends(require_role(Role.operator))],
)


class ErasureRequestBody(BaseModel):
    """Submit an erasure request for a scope, a session, or a coding project."""

    target_kind: ErasureTargetKind = ErasureTargetKind.scope
    target_id: str | None = None
    idempotency_key: str = Field(min_length=1, max_length=200)
    reason: str | None = Field(default=None, max_length=1000)


class ErasureStepResponse(BaseModel):
    step: str
    status: str
    rows_affected: int
    detail: str | None

    @classmethod
    def from_step(cls, step: ErasureStep) -> ErasureStepResponse:
        return cls(
            step=step.step,
            status=step.status.value,
            rows_affected=step.rows_affected,
            detail=step.detail,
        )


class ErasureRequestResponse(BaseModel):
    id: str
    scope_id: str
    target_kind: str
    target_id: str | None
    status: str
    external_incomplete: bool
    attempts: int
    requested_by: str | None
    reason: str | None
    created_at: datetime | None
    updated_at: datetime | None
    completed_at: datetime | None

    @classmethod
    def from_request(cls, request: ErasureRequest) -> ErasureRequestResponse:
        return cls(
            id=request.id,
            scope_id=request.scope_id,
            target_kind=request.target_kind.value,
            target_id=request.target_id,
            status=request.status.value,
            external_incomplete=request.external_incomplete,
            attempts=request.attempts,
            requested_by=request.requested_by,
            reason=request.reason,
            created_at=request.created_at,
            updated_at=request.updated_at,
            completed_at=request.completed_at,
        )


class ErasureStatusResponse(BaseModel):
    request: ErasureRequestResponse
    job_id: str | None = None
    steps: list[ErasureStepResponse]

    @classmethod
    def from_view(
        cls, view: ErasureStatusView, *, job_id: str | None = None
    ) -> ErasureStatusResponse:
        return cls(
            request=ErasureRequestResponse.from_request(view.request),
            job_id=job_id,
            steps=[ErasureStepResponse.from_step(step) for step in view.steps],
        )


def _service(request: Request) -> ErasureService:
    service: ErasureService | None = getattr(request.app.state, "erasure", None)
    if service is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "erasure service unavailable")
    return service


@router.post(
    "/requests",
    response_model=ErasureStatusResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a data-erasure request (scope/session/project)",
    dependencies=[Depends(require_role(Role.admin))],
)
async def submit_erasure(body: ErasureRequestBody, request: Request) -> ErasureStatusResponse:
    service = _service(request)
    try:
        target = ErasureTarget(
            scope_id=service.scope_id,
            kind=body.target_kind,
            resource_id=body.target_id,
        )
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from None
    principal = authenticate(request)
    submission = await service.submit(
        target, body.idempotency_key, requested_by=principal.name, reason=body.reason
    )
    view = await service.get(submission.request.id)
    assert view is not None
    return ErasureStatusResponse.from_view(view, job_id=submission.job.id)


@router.get(
    "/requests",
    response_model=list[ErasureRequestResponse],
    summary="List erasure requests for the scope",
)
async def list_erasure_requests(
    request: Request,
    status_filter: Annotated[ErasureStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> list[ErasureRequestResponse]:
    rows = await _service(request).list(status=status_filter, limit=limit)
    return [ErasureRequestResponse.from_request(row) for row in rows]


@router.get(
    "/requests/{request_id}",
    response_model=ErasureStatusResponse,
    summary="Get an erasure request's status + step ledger",
)
async def get_erasure_request(request_id: str, request: Request) -> ErasureStatusResponse:
    view = await _service(request).get(request_id)
    if view is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "erasure request not found")
    return ErasureStatusResponse.from_view(view)


@router.post(
    "/requests/{request_id}/retry",
    response_model=ErasureStatusResponse,
    summary="Retry a failed/partial erasure request",
    dependencies=[Depends(require_role(Role.admin))],
)
async def retry_erasure_request(request_id: str, request: Request) -> ErasureStatusResponse:
    service = _service(request)
    job = await service.retry(request_id)
    view = await service.get(request_id)
    if view is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "erasure request not found")
    if job is None and view.request.status is ErasureStatus.completed:
        raise HTTPException(status.HTTP_409_CONFLICT, "erasure request already completed")
    return ErasureStatusResponse.from_view(view, job_id=None if job is None else job.id)
