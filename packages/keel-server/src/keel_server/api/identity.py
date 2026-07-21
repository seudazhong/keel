"""Authenticated identity / organization / Agent / grant REST API (M3.6, WS-L).

Every route binds to the request *actor* (a durable user) and, for org-scoped operations,
an org the user is an active member of (selected via the ``X-Keel-Org`` header) — never the
ambient ``web:local`` scope. Authorization is the fine-grained membership/ownership/grant
model in :mod:`keel_core.identity.authz`, layered under the coarse endpoint role tiers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from keel_core.errors import PermissionDenied
from keel_core.identity import (
    Agent,
    AgentAccess,
    AgentAccessLevel,
    AgentAccessPrincipalType,
    AgentKind,
    Capability,
    IdentityError,
    IdentityService,
    Membership,
    MembershipRole,
    Organization,
    ResourceGrant,
    User,
)
from keel_server.identity_context import (
    Actor,
    ResolvedOrg,
    identity_http_status,
    require_org,
    require_user,
)

router = APIRouter(prefix="/v1/identity", tags=["identity"])


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)


# --- response models -----------------------------------------------------------------


class UserResponse(_Model):
    id: str
    display_name: str
    email: str | None
    status: str
    created_at: datetime | None
    updated_at: datetime | None

    @classmethod
    def of(cls, user: User) -> UserResponse:
        return cls.model_validate(user)


class OrganizationResponse(_Model):
    id: str
    slug: str
    display_name: str
    status: str
    created_at: datetime | None
    updated_at: datetime | None

    @classmethod
    def of(cls, org: Organization) -> OrganizationResponse:
        return cls.model_validate(org)


class MembershipResponse(_Model):
    id: str
    org_id: str
    user_id: str
    role: MembershipRole
    status: str
    created_at: datetime | None
    updated_at: datetime | None

    @classmethod
    def of(cls, membership: Membership) -> MembershipResponse:
        return cls.model_validate(membership)


class OrganizationMembershipResponse(_Model):
    organization: OrganizationResponse
    membership: MembershipResponse


class MeResponse(_Model):
    user: UserResponse
    organizations: list[OrganizationMembershipResponse]


class AgentResponse(_Model):
    id: str
    org_id: str
    kind: AgentKind
    owner_user_id: str
    name: str
    persona: str
    status: str
    version: int
    created_at: datetime | None
    updated_at: datetime | None

    @classmethod
    def of(cls, agent: Agent) -> AgentResponse:
        return cls.model_validate(agent)


class GrantResponse(_Model):
    id: str
    org_id: str
    agent_id: str
    resource_type: str
    resource_id: str
    capability: Capability
    grantor_user_id: str
    status: str
    created_at: datetime | None
    updated_at: datetime | None

    @classmethod
    def of(cls, grant: ResourceGrant) -> GrantResponse:
        return cls.model_validate(grant)


class AgentAccessResponse(_Model):
    id: str
    org_id: str
    agent_id: str
    principal_type: AgentAccessPrincipalType
    principal_id: str
    level: AgentAccessLevel
    grantor_user_id: str
    status: str
    created_at: datetime | None
    updated_at: datetime | None

    @classmethod
    def of(cls, access: AgentAccess) -> AgentAccessResponse:
        return cls.model_validate(access)


# --- request models ------------------------------------------------------------------


class CreateOrganizationRequest(_Model):
    slug: str = Field(min_length=3, max_length=40)
    display_name: str = Field(min_length=1, max_length=200)


class AddMemberRequest(_Model):
    user_id: str = Field(min_length=1, max_length=200)
    role: MembershipRole


class ChangeRoleRequest(_Model):
    role: MembershipRole


class CreateAgentRequest(_Model):
    kind: AgentKind
    name: str = Field(min_length=2, max_length=64)
    persona: str = Field(default="", max_length=20_000)


class UpdateAgentRequest(_Model):
    expected_version: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=2, max_length=64)
    persona: str | None = Field(default=None, max_length=20_000)


class ArchiveAgentRequest(_Model):
    expected_version: int = Field(ge=1)


class CreateGrantRequest(_Model):
    agent_id: str = Field(min_length=1, max_length=200)
    resource_type: str = Field(min_length=1, max_length=100)
    resource_id: str = Field(min_length=1, max_length=200)
    capability: Capability


class GrantAgentAccessRequest(_Model):
    principal_type: AgentAccessPrincipalType
    principal_id: str = Field(min_length=1, max_length=300)
    level: AgentAccessLevel


def _service(request: Request) -> IdentityService:
    service = getattr(request.app.state, "identity", None)
    if not isinstance(service, IdentityService):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable")
    return service


# --- profile + organizations ---------------------------------------------------------


@router.get("/me", response_model=MeResponse)
async def get_me(request: Request, actor: Annotated[Actor, Depends(require_user)]) -> MeResponse:
    assert actor.user_id is not None
    user, orgs = await _service(request).get_profile(actor.user_id)
    return MeResponse(
        user=UserResponse.of(user),
        organizations=[
            OrganizationMembershipResponse(
                organization=OrganizationResponse.of(org),
                membership=MembershipResponse.of(membership),
            )
            for org, membership in orgs
        ],
    )


@router.get("/organizations", response_model=list[OrganizationMembershipResponse])
async def list_organizations(
    request: Request, actor: Annotated[Actor, Depends(require_user)]
) -> list[OrganizationMembershipResponse]:
    assert actor.user_id is not None
    orgs = await _service(request).list_orgs_for_user(actor.user_id)
    return [
        OrganizationMembershipResponse(
            organization=OrganizationResponse.of(org),
            membership=MembershipResponse.of(membership),
        )
        for org, membership in orgs
    ]


@router.post(
    "/organizations",
    response_model=OrganizationMembershipResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_organization(
    body: CreateOrganizationRequest,
    request: Request,
    actor: Annotated[Actor, Depends(require_user)],
) -> OrganizationMembershipResponse:
    assert actor.user_id is not None
    ctx = await _service(request).create_org(
        actor.user_id, slug=body.slug, display_name=body.display_name
    )
    return OrganizationMembershipResponse(
        organization=OrganizationResponse.of(ctx.organization),
        membership=MembershipResponse.of(ctx.membership),
    )


@router.get("/organizations/current", response_model=OrganizationMembershipResponse)
async def get_current_organization(
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> OrganizationMembershipResponse:
    return OrganizationMembershipResponse(
        organization=OrganizationResponse.of(org.context.organization),
        membership=MembershipResponse.of(org.context.membership),
    )


# --- members -------------------------------------------------------------------------


@router.get("/members", response_model=list[MembershipResponse])
async def list_members(
    request: Request, org: Annotated[ResolvedOrg, Depends(require_org)]
) -> list[MembershipResponse]:
    members = await _service(request).list_members(org.org_id, org.user_id)
    return [MembershipResponse.of(m) for m in members]


@router.post("/members", response_model=MembershipResponse, status_code=status.HTTP_201_CREATED)
async def add_member(
    body: AddMemberRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> MembershipResponse:
    membership = await _service(request).add_member(
        org.org_id, org.user_id, body.user_id, body.role
    )
    return MembershipResponse.of(membership)


@router.patch("/members/{user_id}", response_model=MembershipResponse)
async def change_member_role(
    user_id: str,
    body: ChangeRoleRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> MembershipResponse:
    membership = await _service(request).change_member_role(
        org.org_id, org.user_id, user_id, body.role
    )
    return MembershipResponse.of(membership)


@router.delete("/members/{user_id}", response_model=MembershipResponse)
async def remove_member(
    user_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> MembershipResponse:
    membership = await _service(request).remove_member(org.org_id, org.user_id, user_id)
    return MembershipResponse.of(membership)


# --- agents --------------------------------------------------------------------------


@router.get("/agents", response_model=list[AgentResponse])
async def list_agents(
    request: Request, org: Annotated[ResolvedOrg, Depends(require_org)]
) -> list[AgentResponse]:
    agents = await _service(request).list_visible_agents(org.org_id, org.user_id)
    return [AgentResponse.of(a) for a in agents]


@router.post("/agents", response_model=AgentResponse, status_code=status.HTTP_201_CREATED)
async def create_agent(
    body: CreateAgentRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> AgentResponse:
    agent = await _service(request).create_agent(
        org.org_id, org.user_id, kind=body.kind, name=body.name, persona=body.persona
    )
    return AgentResponse.of(agent)


@router.get("/agents/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> AgentResponse:
    agent = await _service(request).get_agent(org.org_id, org.user_id, agent_id)
    return AgentResponse.of(agent)


@router.patch("/agents/{agent_id}", response_model=AgentResponse)
async def update_agent(
    agent_id: str,
    body: UpdateAgentRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> AgentResponse:
    agent = await _service(request).update_agent(
        org.org_id,
        org.user_id,
        agent_id,
        expected_version=body.expected_version,
        name=body.name,
        persona=body.persona,
    )
    return AgentResponse.of(agent)


@router.post("/agents/{agent_id}/archive", response_model=AgentResponse)
async def archive_agent(
    agent_id: str,
    body: ArchiveAgentRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> AgentResponse:
    agent = await _service(request).archive_agent(
        org.org_id, org.user_id, agent_id, expected_version=body.expected_version
    )
    return AgentResponse.of(agent)


@router.post("/agents/{agent_id}/select", response_model=AgentResponse)
async def select_agent(
    agent_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> AgentResponse:
    """Compatibility bridge: resolve + authorize the persisted Agent for a future run."""
    agent = await _service(request).select_agent(org.org_id, org.user_id, agent_id)
    return AgentResponse.of(agent)


# --- agent access (team Agents; R1B) --------------------------------------------------


@router.get("/agents/{agent_id}/access", response_model=list[AgentAccessResponse])
async def list_agent_access(
    agent_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> list[AgentAccessResponse]:
    """List a team Agent's Agent Access edges (requires org admin/owner or manage access)."""
    edges = await _service(request).list_agent_access(org.org_id, org.user_id, agent_id=agent_id)
    return [AgentAccessResponse.of(e) for e in edges]


@router.post(
    "/agents/{agent_id}/access",
    response_model=AgentAccessResponse,
    status_code=status.HTTP_201_CREATED,
)
async def grant_agent_access(
    agent_id: str,
    body: GrantAgentAccessRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> AgentAccessResponse:
    """Grant (or update) a user/channel's discover/use/manage edge on a team Agent."""
    access = await _service(request).grant_agent_access(
        org.org_id,
        org.user_id,
        agent_id=agent_id,
        principal_type=body.principal_type,
        principal_id=body.principal_id,
        level=body.level,
    )
    return AgentAccessResponse.of(access)


@router.delete(
    "/agents/{agent_id}/access",
    response_model=AgentAccessResponse,
)
async def revoke_agent_access(
    agent_id: str,
    principal_type: Annotated[AgentAccessPrincipalType, Query()],
    principal_id: Annotated[str, Query(min_length=1, max_length=300)],
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> AgentAccessResponse:
    access = await _service(request).revoke_agent_access(
        org.org_id,
        org.user_id,
        agent_id=agent_id,
        principal_type=principal_type,
        principal_id=principal_id,
    )
    return AgentAccessResponse.of(access)


# --- grants --------------------------------------------------------------------------


@router.get("/grants", response_model=list[GrantResponse])
async def list_grants(
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
    agent_id: Annotated[str | None, Query()] = None,
) -> list[GrantResponse]:
    grants = await _service(request).list_grants(org.org_id, org.user_id, agent_id=agent_id)
    return [GrantResponse.of(g) for g in grants]


@router.post("/grants", response_model=GrantResponse, status_code=status.HTTP_201_CREATED)
async def create_grant(
    body: CreateGrantRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> GrantResponse:
    grant = await _service(request).grant_resource(
        org.org_id,
        org.user_id,
        agent_id=body.agent_id,
        resource_type=body.resource_type,
        resource_id=body.resource_id,
        capability=body.capability,
    )
    return GrantResponse.of(grant)


@router.delete("/grants/{grant_id}", response_model=GrantResponse)
async def revoke_grant(
    grant_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> GrantResponse:
    grant = await _service(request).revoke_grant(org.org_id, org.user_id, grant_id)
    return GrantResponse.of(grant)


# --- exception handlers --------------------------------------------------------------


async def _identity_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, IdentityError | PermissionDenied)
    http_status = identity_http_status(exc)
    return JSONResponse(status_code=http_status, content={"detail": str(exc)})


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(IdentityError, _identity_error_handler)
    app.add_exception_handler(PermissionDenied, _identity_error_handler)


__all__ = [
    "AgentAccessResponse",
    "AgentResponse",
    "GrantResponse",
    "MeResponse",
    "MembershipResponse",
    "OrganizationResponse",
    "UserResponse",
    "register_exception_handlers",
    "router",
]
