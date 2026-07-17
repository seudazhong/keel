"""Authenticated managed-project + GitHub webhook REST API (M3.7, WS-P).

Every ``/v1/projects`` route binds to the request *actor* (a durable user) and an org the
user is an active member of (``X-Keel-Org``) — never the ambient ``web:local`` scope.
Authorization is the fine-grained capability model in
:mod:`keel_core.projects.service` layered under the coarse endpoint role tiers.

The GitHub webhook endpoint (``POST /v1/projects/github/webhook``) is authenticated
*independently* by an ``X-Hub-Signature-256`` HMAC over the raw body plus the installation
->org binding — it never uses the actor/org header path.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from keel_core.config import get_settings
from keel_core.errors import PermissionDenied
from keel_core.identity.models import Capability
from keel_core.projects.github.webhooks import (
    ALLOWED_EVENTS,
    WebhookVerificationError,
    parse_event,
    verify_signature,
)
from keel_core.projects.github.webhooks import (
    delivery_id as read_delivery_id,
)
from keel_core.projects.github.webhooks import (
    event_name as read_event_name,
)
from keel_core.projects.models import (
    GitHubInstallation,
    Project,
    ProjectError,
    ProjectNotFoundError,
    ProjectValidationError,
    ProjectVisibility,
    ProjectWorktree,
    RepoSyncEntry,
    WebhookStatus,
)
from keel_core.projects.service import ProjectService
from keel_server.identity_context import ResolvedOrg, require_org

router = APIRouter(prefix="/v1/projects", tags=["projects"])


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)


# --- response models -----------------------------------------------------------------


class ProjectResponse(_Model):
    id: str
    org_id: str
    slug: str
    display_name: str
    source: str
    visibility: str
    status: str
    default_branch: str
    github_repository_id: int | None
    version: int
    created_at: datetime | None
    updated_at: datetime | None

    @classmethod
    def of(cls, project: Project) -> ProjectResponse:
        return cls.model_validate(project)


class WorktreeResponse(_Model):
    id: str
    org_id: str
    project_id: str
    run_id: str
    git_ref: str
    commit_sha: str | None
    status: str
    created_at: datetime | None

    @classmethod
    def of(cls, worktree: ProjectWorktree) -> WorktreeResponse:
        return cls.model_validate(worktree)


class SyncEntryResponse(_Model):
    id: str
    project_id: str
    kind: str
    status: str
    git_ref: str | None
    before_sha: str | None
    after_sha: str | None
    delivery_id: str | None
    created_at: datetime | None

    @classmethod
    def of(cls, entry: RepoSyncEntry) -> SyncEntryResponse:
        return cls.model_validate(entry)


class InstallationResponse(_Model):
    id: str
    org_id: str
    installation_id: int
    app_id: int
    account_login: str
    account_type: str
    status: str

    @classmethod
    def of(cls, installation: GitHubInstallation) -> InstallationResponse:
        return cls.model_validate(installation)


class ProjectGrantResponse(_Model):
    id: str
    org_id: str
    agent_id: str
    resource_type: str
    resource_id: str
    capability: str
    status: str


class RunListResponse(_Model):
    run_ids: list[str]


class RunAssociationResponse(_Model):
    run_id: str
    created: bool


# --- request models ------------------------------------------------------------------


class CreateProjectRequest(_Model):
    slug: str = Field(min_length=3, max_length=40)
    display_name: str = Field(min_length=1, max_length=200)
    visibility: ProjectVisibility = ProjectVisibility.private
    default_branch: str = Field(default="main", min_length=1, max_length=255)


class ImportProjectRequest(_Model):
    slug: str = Field(min_length=3, max_length=40)
    display_name: str = Field(min_length=1, max_length=200)
    installation_id: int = Field(ge=1)
    repo_full_name: str = Field(min_length=3, max_length=255)


class UpdateProjectRequest(_Model):
    expected_version: int = Field(ge=1)
    display_name: str | None = Field(default=None, min_length=1, max_length=200)
    default_branch: str | None = Field(default=None, min_length=1, max_length=255)
    visibility: ProjectVisibility | None = None


class VersionedRequest(_Model):
    expected_version: int = Field(ge=1)


class MaterializeWorktreeRequest(_Model):
    run_id: str = Field(min_length=1, max_length=255)
    git_ref: str = Field(default="HEAD", min_length=1, max_length=255)
    agent_id: str | None = None


class AssociateRunRequest(_Model):
    run_id: str = Field(min_length=1, max_length=255)
    agent_id: str | None = None


class GrantProjectRequest(_Model):
    agent_id: str = Field(min_length=1, max_length=64)
    capability: Capability


class LinkInstallationRequest(_Model):
    installation_id: int = Field(ge=1)
    app_id: int = Field(ge=1)
    account_login: str = Field(min_length=1, max_length=100)
    account_type: str = Field(default="Organization", max_length=40)


# --- service accessor ----------------------------------------------------------------


def _service(request: Request) -> ProjectService:
    service = getattr(request.app.state, "projects", None)
    if not isinstance(service, ProjectService):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "projects service unavailable")
    return service


# --- project CRUD --------------------------------------------------------------------


@router.get("", response_model=list[ProjectResponse])
async def list_projects(
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
    include_inactive: bool = False,
) -> list[ProjectResponse]:
    projects = await _service(request).list_projects(
        org.org_id, org.user_id, include_inactive=include_inactive
    )
    return [ProjectResponse.of(p) for p in projects]


@router.post("", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def create_project(
    body: CreateProjectRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ProjectResponse:
    project = await _service(request).create_project(
        org.org_id,
        org.user_id,
        slug=body.slug,
        display_name=body.display_name,
        visibility=body.visibility,
        default_branch=body.default_branch,
    )
    return ProjectResponse.of(project)


@router.post("/import", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def import_project(
    body: ImportProjectRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ProjectResponse:
    project = await _service(request).import_github_project(
        org.org_id,
        org.user_id,
        slug=body.slug,
        display_name=body.display_name,
        installation_id=body.installation_id,
        repo_full_name=body.repo_full_name,
    )
    return ProjectResponse.of(project)


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ProjectResponse:
    project = await _service(request).get_project(org.org_id, org.user_id, project_id)
    return ProjectResponse.of(project)


@router.patch("/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: str,
    body: UpdateProjectRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ProjectResponse:
    project = await _service(request).update_project(
        org.org_id,
        org.user_id,
        project_id,
        expected_version=body.expected_version,
        display_name=body.display_name,
        default_branch=body.default_branch,
        visibility=body.visibility,
    )
    return ProjectResponse.of(project)


@router.post("/{project_id}/archive", response_model=ProjectResponse)
async def archive_project(
    project_id: str,
    body: VersionedRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ProjectResponse:
    project = await _service(request).archive_project(
        org.org_id, org.user_id, project_id, expected_version=body.expected_version
    )
    return ProjectResponse.of(project)


@router.post("/{project_id}/delete", response_model=ProjectResponse)
async def delete_project(
    project_id: str,
    body: VersionedRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ProjectResponse:
    project = await _service(request).delete_project(
        org.org_id, org.user_id, project_id, expected_version=body.expected_version
    )
    return ProjectResponse.of(project)


# --- sync ----------------------------------------------------------------------------


@router.post("/{project_id}/sync", response_model=SyncEntryResponse, status_code=202)
async def request_sync(
    project_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> SyncEntryResponse:
    entry = await _service(request).request_sync(org.org_id, org.user_id, project_id)
    return SyncEntryResponse.of(entry)


@router.get("/{project_id}/sync", response_model=list[SyncEntryResponse])
async def list_sync(
    project_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> list[SyncEntryResponse]:
    entries = await _service(request).list_sync_entries(org.org_id, org.user_id, project_id)
    return [SyncEntryResponse.of(e) for e in entries]


# --- worktrees -----------------------------------------------------------------------


@router.get("/{project_id}/worktrees", response_model=list[WorktreeResponse])
async def list_worktrees(
    project_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> list[WorktreeResponse]:
    worktrees = await _service(request).list_worktrees(org.org_id, org.user_id, project_id)
    return [WorktreeResponse.of(w) for w in worktrees]


@router.post(
    "/{project_id}/worktrees", response_model=WorktreeResponse, status_code=status.HTTP_201_CREATED
)
async def materialize_worktree(
    project_id: str,
    body: MaterializeWorktreeRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> WorktreeResponse:
    worktree = await _service(request).materialize_worktree(
        org.org_id,
        org.user_id,
        project_id,
        body.run_id,
        git_ref=body.git_ref,
        agent_id=body.agent_id,
    )
    return WorktreeResponse.of(worktree)


@router.delete("/{project_id}/worktrees/{run_id}", response_model=WorktreeResponse)
async def reclaim_worktree(
    project_id: str,
    run_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> WorktreeResponse:
    worktree = await _service(request).reclaim_worktree(org.org_id, org.user_id, project_id, run_id)
    return WorktreeResponse.of(worktree)


# --- run associations ----------------------------------------------------------------


@router.get("/{project_id}/runs", response_model=RunListResponse)
async def list_runs(
    project_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> RunListResponse:
    run_ids = await _service(request).list_project_runs(org.org_id, org.user_id, project_id)
    return RunListResponse(run_ids=run_ids)


@router.post("/{project_id}/runs", response_model=RunAssociationResponse)
async def associate_run(
    project_id: str,
    body: AssociateRunRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> RunAssociationResponse:
    run_id, created = await _service(request).associate_run(
        org.org_id, org.user_id, project_id, body.run_id, agent_id=body.agent_id
    )
    return RunAssociationResponse(run_id=run_id, created=created)


# --- grants --------------------------------------------------------------------------


@router.get("/{project_id}/grants", response_model=list[ProjectGrantResponse])
async def list_grants(
    project_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> list[ProjectGrantResponse]:
    grants = await _service(request).list_project_grants(org.org_id, org.user_id, project_id)
    return [ProjectGrantResponse.model_validate(g) for g in grants]


@router.post(
    "/{project_id}/grants", response_model=ProjectGrantResponse, status_code=status.HTTP_201_CREATED
)
async def create_grant(
    project_id: str,
    body: GrantProjectRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ProjectGrantResponse:
    grant = await _service(request).grant_project(
        org.org_id,
        org.user_id,
        project_id=project_id,
        agent_id=body.agent_id,
        capability=body.capability,
    )
    return ProjectGrantResponse.model_validate(grant)


@router.delete("/{project_id}/grants/{grant_id}", response_model=ProjectGrantResponse)
async def revoke_grant(
    project_id: str,
    grant_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> ProjectGrantResponse:
    grant = await _service(request).revoke_project_grant(org.org_id, org.user_id, grant_id)
    return ProjectGrantResponse.model_validate(grant)


# --- GitHub installations ------------------------------------------------------------


@router.get("/github/installations", response_model=list[InstallationResponse])
async def list_installations(
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> list[InstallationResponse]:
    installations = await _service(request).list_installations(org.org_id, org.user_id)
    return [InstallationResponse.of(i) for i in installations]


@router.post(
    "/github/installations",
    response_model=InstallationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def link_installation(
    body: LinkInstallationRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> InstallationResponse:
    installation = await _service(request).link_installation(
        org.org_id,
        org.user_id,
        installation_id=body.installation_id,
        app_id=body.app_id,
        account_login=body.account_login,
        account_type=body.account_type,
    )
    return InstallationResponse.of(installation)


# --- GitHub webhook (independently HMAC-authenticated; no actor/org header) -----------


@router.post("/github/webhook", status_code=status.HTTP_202_ACCEPTED)
async def github_webhook(request: Request, background: BackgroundTasks) -> dict[str, bool | str]:
    service = _service(request)
    settings = get_settings()
    raw = await request.body()
    secret = settings.github_webhook_secret.get_secret_value()
    signature = request.headers.get("X-Hub-Signature-256")
    if not secret:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "github webhook secret not configured"
        )
    if not verify_signature(secret, raw, signature):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid webhook signature")
    delivery = read_delivery_id(request.headers.get("X-GitHub-Delivery"))
    event = read_event_name(request.headers.get("X-GitHub-Event"))
    if delivery is None or event is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "missing webhook headers")
    if event not in ALLOWED_EVENTS:
        # Acknowledge but do not process (durable no-op).
        return {"accepted": True, "status": "skipped"}
    try:
        payload = await request.json()
    except Exception as exc:  # noqa: BLE001 - malformed body is a client error
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid JSON body") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "payload must be a JSON object")

    # Durable replay protection: the delivery ledger PK collapses a replay to a no-op.
    _, created = await service.store.record_delivery(
        delivery_id=delivery,
        event=event,
        installation_id=None,
        action=payload.get("action") if isinstance(payload.get("action"), str) else None,
    )
    if not created:
        return {"accepted": True, "status": "duplicate"}
    try:
        parsed = parse_event(event=event, delivery=delivery, payload=payload)
    except WebhookVerificationError as exc:
        await service.store.mark_delivery(delivery, WebhookStatus.skipped)
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "unsupported event") from exc

    async def _process() -> None:
        outcome = await service.process_webhook(parsed)
        await service.store.mark_delivery(delivery, outcome.status)

    background.add_task(_process)
    return {"accepted": True, "status": "processing"}


# --- exception handling --------------------------------------------------------------


def project_http_status(exc: Exception) -> int:
    from keel_core.projects.models import (
        ProjectConflictError,
        ProjectCrossOrgError,
        ProjectOptimisticConcurrencyError,
        ProjectQuotaExceededError,
    )

    if isinstance(exc, ProjectNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(
        exc, ProjectConflictError | ProjectOptimisticConcurrencyError | ProjectQuotaExceededError
    ):
        return status.HTTP_409_CONFLICT
    if isinstance(exc, ProjectValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, ProjectCrossOrgError | PermissionDenied):
        return status.HTTP_403_FORBIDDEN
    return status.HTTP_400_BAD_REQUEST


async def _project_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ProjectError)
    return JSONResponse(status_code=project_http_status(exc), content={"detail": str(exc)})


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ProjectError, _project_error_handler)
