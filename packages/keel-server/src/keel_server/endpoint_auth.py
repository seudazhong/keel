"""Unified endpoint authorization: OIDC users + API keys + local preview (M3.6, WS-L).

Before M3.6 the ``/v1`` routes gated on :func:`keel_server.auth.require_role`, which resolves
only the coarse **API-key** :class:`~keel_server.auth.Principal`. A verified OIDC bearer JWT
was therefore rejected *before* the endpoint ran (an unknown API key -> 401, or, in a
key-less cloud deployment, 503), so a real human user could never reach the message/session/
run endpoints. This module replaces that with a single dependency that:

* resolves the request actor with :func:`keel_server.identity_context.resolve_actor`
  (OIDC-first, then API-key/local — a JWT that fails verification is **never** downgraded
  into open-mode admin);
* maps the actor to an :class:`EndpointPrivilege`: a **user**'s privilege comes from their
  active **org membership role** (``X-Keel-Org``), an API-key **machine**/**local** operator
  keeps its coarse role tier;
* derives the canonical per-Agent data-plane scope (``agent:<org>/<agent>``) for a user, or
  the explicit ``web:local`` scope for the non-cloud local operator — never the ambient
  data-plane scope, and never ``web:local`` shared across organizations.

Everything fails closed: a missing/invalid credential, a non-member org, an unusable Agent,
or a cloud caller with no authenticated user is denied, never granted a default admin.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status

from keel_core.errors import PermissionDenied
from keel_core.identity import IdentityService, MembershipRole, NotFoundError
from keel_core.interactive import LOCAL_PREVIEW_AGENT_ID, LOCAL_PREVIEW_ORG_ID
from keel_core.scoping import LOCAL_PREVIEW_SCOPE, ScopeValidationError, derive_agent_scope
from keel_core.types import ScopeId
from keel_server.auth import Role
from keel_server.identity_context import Actor, resolve_actor


class EndpointPrivilege(IntEnum):
    """The effective privilege an actor holds on an endpoint (higher includes lower)."""

    viewer = 1
    operator = 2
    admin = 3


# An org membership role -> the endpoint privilege it confers. A viewer may read, a member
# may operate (send messages, resolve approvals), an admin/owner may administer. Fail closed
# (a role not listed here confers nothing).
_ROLE_PRIVILEGE: dict[MembershipRole, EndpointPrivilege] = {
    MembershipRole.viewer: EndpointPrivilege.viewer,
    MembershipRole.member: EndpointPrivilege.operator,
    MembershipRole.admin: EndpointPrivilege.admin,
    MembershipRole.owner: EndpointPrivilege.admin,
}

# The coarse API-key role tier -> endpoint privilege (1:1; preserves API-key behavior).
_API_ROLE_PRIVILEGE: dict[Role, EndpointPrivilege] = {
    Role.viewer: EndpointPrivilege.viewer,
    Role.operator: EndpointPrivilege.operator,
    Role.admin: EndpointPrivilege.admin,
}


@dataclass(frozen=True)
class EndpointAuth:
    """The resolved actor + privilege + derived data-plane scope for one request."""

    actor: Actor
    privilege: EndpointPrivilege
    scope_id: ScopeId
    org_id: str | None = None
    agent_id: str | None = None

    @property
    def is_user(self) -> bool:
        return self.actor.is_user


def _cloud_mode(request: Request) -> bool:
    return bool(getattr(request.app.state, "auth_required", False))


def _identity_service(request: Request) -> IdentityService:
    service = getattr(request.app.state, "identity", None)
    if service is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable")
    return service  # type: ignore[no-any-return]


def _local_actor_id(actor: Actor) -> str:
    """A stable, never-blank actor id for a non-user (local operator / API-key machine)."""
    return f"{actor.kind.value}:{actor.display_name}"


async def _resolve_user_scope(
    request: Request, actor: Actor, org_ref: str | None, agent_ref: str | None
) -> EndpointAuth:
    """Resolve a user's org membership + selected Agent into privilege + derived scope."""
    assert actor.user_id is not None
    service = _identity_service(request)
    if not org_ref or not org_ref.strip():
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "select an organization via the X-Keel-Org header"
        )
    try:
        org_context = await service.select_org(actor.user_id, org_ref.strip())
    except NotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "organization not found") from None
    privilege = _ROLE_PRIVILEGE.get(org_context.membership.role)
    if privilege is None:
        # A membership role that confers nothing on the endpoint fails closed.
        raise HTTPException(status.HTTP_403_FORBIDDEN, "insufficient organization role")
    if not agent_ref or not agent_ref.strip():
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "select an Agent via the X-Keel-Agent header"
        )
    try:
        agent = await service.select_agent(org_context.org_id, actor.user_id, agent_ref.strip())
    except NotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found") from None
    except PermissionDenied as exc:
        raise HTTPException(status.HTTP_403_FORBIDDEN, str(exc)) from None
    try:
        scope_id = derive_agent_scope(org_context.org_id, agent.id)
    except ScopeValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from None
    return EndpointAuth(
        actor=actor,
        privilege=privilege,
        scope_id=scope_id,
        org_id=org_context.org_id,
        agent_id=agent.id,
    )


async def resolve_endpoint_auth(
    request: Request,
    actor: Annotated[Actor, Depends(resolve_actor)],
    x_keel_org: Annotated[str | None, Header(alias="X-Keel-Org")] = None,
    x_keel_agent: Annotated[str | None, Header(alias="X-Keel-Agent")] = None,
) -> EndpointAuth:
    """Resolve the request's :class:`EndpointAuth` (actor + privilege + derived scope).

    A **user** binds their org membership + selected Agent (fail closed). A non-cloud
    **local**/**machine** operator maps to the explicit ``web:local`` local-preview scope with
    its coarse API-key role. In cloud mode a non-user caller fails closed — the local-preview
    scope is never reachable on an authenticated/cloud route.
    """
    if actor.is_user:
        return await _resolve_user_scope(request, actor, x_keel_org, x_keel_agent)
    if _cloud_mode(request):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "this operation requires an authenticated user with a selected org and Agent",
        )
    privilege = _API_ROLE_PRIVILEGE.get(actor.api_role, EndpointPrivilege.viewer)
    # The non-cloud single-operator preview scope: the configured single-tenant data plane
    # (``web:local`` by default). It is never a derived per-Agent scope and never shared across
    # organizations — a cloud caller can never reach this branch.
    scope_id = (
        getattr(request.app.state, "durable_scope", LOCAL_PREVIEW_SCOPE) or LOCAL_PREVIEW_SCOPE
    )
    return EndpointAuth(
        actor=actor,
        privilege=privilege,
        scope_id=scope_id,
        org_id=LOCAL_PREVIEW_ORG_ID,
        agent_id=LOCAL_PREVIEW_AGENT_ID,
    )


def require_privilege(minimum: EndpointPrivilege):  # type: ignore[no-untyped-def]
    """A dependency that resolves :class:`EndpointAuth` and enforces ``privilege >= minimum``."""

    async def dependency(
        auth: Annotated[EndpointAuth, Depends(resolve_endpoint_auth)],
    ) -> EndpointAuth:
        if auth.privilege < minimum:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "insufficient privilege")
        return auth

    return dependency


async def require_authenticated(
    request: Request, actor: Annotated[Actor, Depends(resolve_actor)]
) -> Actor:
    """Baseline gate: the caller must be authenticated (user, API key, or local operator).

    Unlike :func:`keel_server.auth.require_role`, a verified OIDC **user** is accepted here
    rather than rejected as an unknown API key — the fix for the JWT-rejected-first defect.
    An API-key machine / local operator still resolves through the existing hashed-key path
    (:func:`resolve_actor` -> :func:`keel_server.auth.authenticate`), which fails closed in
    cloud mode with no keys. Fine-grained privilege is enforced per-route via
    :func:`require_privilege`.
    """
    return actor


__all__ = [
    "EndpointAuth",
    "EndpointPrivilege",
    "require_authenticated",
    "require_privilege",
    "resolve_endpoint_auth",
]
