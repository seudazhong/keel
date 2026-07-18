"""Authenticated read-only code-review REST API (WS-R).

Every ``/v1/projects/{project_id}/reviews`` route binds to the request actor + org
(``X-Keel-Org``) and defers fine-grained authorization to the review coordinator (read+run ==
the ``use`` capability, re-checked at worker claim time). Triggering a review is idempotent
(``Idempotency-Key`` header or generated) and durable: it creates a run + project association
and enqueues a restart-safe ``review.run`` job. No route makes a code change, a push, or a
GitHub comment — reads return the immutable, evidence-verified report artifacts only.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from keel_core.identity.models import Capability
from keel_core.projects.models import ProjectError
from keel_core.projects.service import ProjectService
from keel_core.review import (
    ReviewCoordinator,
    ReviewRequest,
    ReviewSource,
    review_idempotency_key,
)
from keel_core.review.errors import ReviewError, ReviewNotFound, ReviewValidationError
from keel_core.review.jobs import ReviewJobPayload
from keel_core.runs import RunStatus
from keel_server.identity_context import ResolvedOrg, require_org

router = APIRouter(prefix="/v1/projects/{project_id}/reviews", tags=["reviews"])


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)


class CreateReviewRequest(_Model):
    source: ReviewSource = ReviewSource.branch
    head: str = Field(min_length=1, max_length=255)
    base: str | None = Field(default=None, max_length=255)
    model: str | None = Field(default=None, max_length=128)
    agent_id: str | None = Field(default=None, max_length=128)
    max_findings: int = Field(default=50, ge=1, le=50)
    idempotency_key: str | None = Field(default=None, max_length=200)


class CreateReviewResponse(_Model):
    review_id: str
    run_id: str
    status: str
    idempotency_key: str
    created: bool


class ReviewStatusResponse(_Model):
    review_id: str
    org_id: str
    project_id: str
    run_id: str
    status: str
    source: str
    head: str
    base: str | None
    model: str
    created_at: str
    updated_at: str
    finding_count: int
    severity_counts: dict[str, int]
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    report_json_sha256: str | None
    report_markdown_sha256: str | None
    error_kind: str | None
    error_message: str | None


def _coordinator(request: Request) -> ReviewCoordinator:
    coordinator = getattr(request.app.state, "review_coordinator", None)
    if not isinstance(coordinator, ReviewCoordinator):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "review service unavailable")
    return coordinator


def _projects(request: Request) -> ProjectService:
    service = getattr(request.app.state, "projects", None)
    if not isinstance(service, ProjectService):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "projects service unavailable")
    return service


def _resolve_model(request: Request, requested: str | None) -> str:
    """Resolve + authorize the review model against the configured allowlist (never arbitrary).

    ``default_model`` is always allowed; any other model must be explicitly allowlisted. A
    caller-supplied model outside the allowlist is rejected before it can reach the provider.
    """
    settings = getattr(request.app.state, "settings", None)
    default_model = getattr(settings, "default_model", "gpt-4o-mini")
    if requested is None or not requested.strip():
        return default_model
    requested = requested.strip()
    allowed = settings.review_allowed_models if settings is not None else frozenset({default_model})
    if requested not in allowed:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "requested model is not permitted for reviews",
        )
    return requested


@dataclass(frozen=True)
class _Budget:
    token_budget: int
    output_max_tokens: int
    cost_ceiling_usd: float
    max_provider_attempts: int


def _review_budget(request: Request) -> _Budget:
    """The enforced, non-unlimited budget envelope from settings (fail-closed defaults)."""
    settings = getattr(request.app.state, "settings", None)
    return _Budget(
        token_budget=int(getattr(settings, "review_token_budget", 200_000)),
        output_max_tokens=int(getattr(settings, "review_output_max_tokens", 8_000)),
        cost_ceiling_usd=float(getattr(settings, "review_cost_ceiling_usd", 1.0)),
        max_provider_attempts=int(getattr(settings, "review_max_provider_attempts", 2)),
    )


def _idempotency_key(request: Request, body: CreateReviewRequest) -> str:
    header = (request.headers.get("idempotency-key") or "").strip()
    if header:
        return header
    if body.idempotency_key and body.idempotency_key.strip():
        return body.idempotency_key.strip()
    return uuid.uuid4().hex


@router.post("", response_model=CreateReviewResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_review(
    project_id: str,
    body: CreateReviewRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> CreateReviewResponse:
    coordinator = _coordinator(request)
    idempotency_key = _idempotency_key(request, body)
    budget = _review_budget(request)
    review_request = ReviewRequest(
        org_id=org.org_id,
        project_id=project_id,
        source=body.source,
        head=body.head,
        base=body.base,
        model=_resolve_model(request, body.model),
        agent_id=body.agent_id,
        idempotency_key=idempotency_key,
        max_findings=body.max_findings,
        token_budget=budget.token_budget,
        output_max_tokens=budget.output_max_tokens,
        cost_ceiling_usd=budget.cost_ceiling_usd,
        max_provider_attempts=budget.max_provider_attempts,
    )
    handle = await coordinator.request_review(review_request, actor=org.user_id)
    # Idempotently (re-)enqueue on EVERY request, not only first admission: a duplicate request
    # or a retried call re-drives dispatch (enqueue_once dedupes), so a run can never be
    # stranded by a lost enqueue. Enqueue failure does not fail the request — the run is durably
    # admitted and a subsequent request / reconcile re-enqueues it.
    payload = ReviewJobPayload.from_request(review_request, run_id=handle.run_id).model_dump(
        mode="json"
    )
    enqueue = getattr(request.app.state, "enqueue_review", None)
    if enqueue is not None:
        try:
            await enqueue(payload, review_idempotency_key(handle.run_id))
        except Exception:  # noqa: BLE001 — admission already durable; do not strand on enqueue
            pass
    return CreateReviewResponse(
        review_id=handle.review_id,
        run_id=handle.run_id,
        status=handle.status.value,
        idempotency_key=idempotency_key,
        created=handle.created,
    )


async def _authorize_read(request: Request, org: ResolvedOrg, project_id: str) -> None:
    await _projects(request).authorize_review(
        org.org_id, org.user_id, project_id, capability=Capability.read
    )


async def _require_review_in_project(
    request: Request, org: ResolvedOrg, project_id: str, review_id: str
) -> None:
    """404 unless ``review_id`` is a review run associated with *this* route's project.

    Prevents reading a review of project A through project B's route even when the caller can
    read both: the review must belong to the project named in the path (project_runs binding).
    """
    run_ids = await _projects(request).list_project_runs(org.org_id, org.user_id, project_id)
    if review_id not in run_ids:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "review not found")


@router.get("", response_model=list[ReviewStatusResponse])
async def list_reviews(
    project_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> list[ReviewStatusResponse]:
    await _authorize_read(request, org, project_id)
    coordinator = _coordinator(request)
    run_ids = await _projects(request).list_project_runs(org.org_id, org.user_id, project_id)
    responses: list[ReviewStatusResponse] = []
    for run_id in run_ids:
        run = await coordinator.get_run_optional(run_id)
        if run is None:
            continue
        try:
            record = await coordinator.build_review_record(run)
        except ReviewNotFound:
            # A review whose durable metadata is not (yet) available is omitted rather than
            # projected with fabricated defaults.
            continue
        responses.append(ReviewStatusResponse.model_validate(record.to_dict()))
    return responses


@router.get("/{review_id}", response_model=ReviewStatusResponse)
async def get_review(
    project_id: str,
    review_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ReviewStatusResponse:
    await _authorize_read(request, org, project_id)
    await _require_review_in_project(request, org, project_id, review_id)
    coordinator = _coordinator(request)
    run = await coordinator.get_run(review_id)
    if run.org_id != org.org_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "review not found")
    report = None
    if run.status is RunStatus.completed and run.result_ref:
        project = await _projects(request).authorize_review(
            org.org_id, org.user_id, project_id, capability=Capability.read
        )
        handle = project.active_git_handle or project.id
        report = coordinator.read_report(
            project_handle=handle, run_id=review_id, json_sha256=run.result_ref
        )
    record = await coordinator.build_review_record(run, report=report)
    return ReviewStatusResponse.model_validate(record.to_dict())


@router.get("/{review_id}/report")
async def get_review_report(
    project_id: str,
    review_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> JSONResponse:
    await _authorize_read(request, org, project_id)
    await _require_review_in_project(request, org, project_id, review_id)
    coordinator = _coordinator(request)
    run = await coordinator.get_run(review_id)
    if run.org_id != org.org_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "review not found")
    if run.status is not RunStatus.completed or not run.result_ref:
        raise HTTPException(status.HTTP_409_CONFLICT, "review is not completed")
    project = await _projects(request).authorize_review(
        org.org_id, org.user_id, project_id, capability=Capability.read
    )
    handle = project.active_git_handle or project.id
    report = coordinator.read_report(
        project_handle=handle, run_id=review_id, json_sha256=run.result_ref
    )
    return JSONResponse(content=report.to_dict())


@router.get("/{review_id}/report.md")
async def get_review_report_markdown(
    project_id: str,
    review_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> Response:
    await _authorize_read(request, org, project_id)
    await _require_review_in_project(request, org, project_id, review_id)
    coordinator = _coordinator(request)
    run = await coordinator.get_run(review_id)
    if run.org_id != org.org_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "review not found")
    if run.status is not RunStatus.completed or not run.result_ref:
        raise HTTPException(status.HTTP_409_CONFLICT, "review is not completed")
    project = await _projects(request).authorize_review(
        org.org_id, org.user_id, project_id, capability=Capability.read
    )
    handle = project.active_git_handle or project.id
    report = coordinator.read_report(
        project_handle=handle, run_id=review_id, json_sha256=run.result_ref
    )
    markdown = coordinator.read_report_markdown(
        project_handle=handle, run_id=review_id, markdown_sha256=report.markdown_sha256
    )
    return Response(content=markdown, media_type="text/markdown; charset=utf-8")


def review_http_status(exc: Exception) -> int:
    if isinstance(exc, ReviewNotFound):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, ReviewValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, ProjectError):
        from keel_server.api.projects import project_http_status

        return project_http_status(exc)
    return status.HTTP_400_BAD_REQUEST


async def _review_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=review_http_status(exc), content={"detail": str(exc)})


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ReviewError, _review_error_handler)


__all__ = ["register_exception_handlers", "review_http_status", "router"]
