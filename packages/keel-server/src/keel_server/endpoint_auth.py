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

import logging
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

logger = logging.getLogger(__name__)


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


async def _resolve_machine_scope(
    request: Request, actor: Actor, org_ref: str | None, agent_ref: str | None
) -> EndpointAuth:
    """Resolve an API-key machine credential's privilege + derived per-Agent scope.

    A **scoped** machine credential carries an explicit org/Agent binding from trusted config;
    it operates only inside that one tenant's data plane. A client-supplied ``X-Keel-Org`` /
    ``X-Keel-Agent`` header may not override that binding — a mismatch is rejected as a spoof
    (never silently honored, never widened). A **global** admin credential carries no binding
    but must still select an org/Agent per request via those headers (explicit + audited). An
    unbound, non-global credential confers no data-plane scope in cloud mode (fail closed).
    """
    service = _identity_service(request)
    bound_org = actor.machine_org_ref
    bound_agent = actor.machine_agent_ref
    if bound_org and bound_agent:
        # Reject a header that tries to point a scoped credential at a different tenant.
        if org_ref and org_ref.strip() and org_ref.strip() not in {bound_org}:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "org header does not match credential")
        if agent_ref and agent_ref.strip() and agent_ref.strip() not in {bound_agent}:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "agent header does not match credential")
        selected_org, selected_agent = bound_org, bound_agent
    elif actor.machine_global:
        # A global admin credential must explicitly select the org/Agent it is acting on.
        if not org_ref or not org_ref.strip() or not agent_ref or not agent_ref.strip():
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "a global admin credential must select an org and Agent via headers",
            )
        selected_org, selected_agent = org_ref.strip(), agent_ref.strip()
        logger.warning(
            "global admin credential %s acting on org=%s agent=%s",
            actor.display_name,
            selected_org,
            selected_agent,
        )
    else:
        # An unbound, non-global machine credential has no tenant: never an ambient scope.
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "this credential is not bound to an org and Agent",
        )
    try:
        org, agent = await service.resolve_machine_binding(selected_org, selected_agent)
    except NotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "organization or agent not found") from None
    try:
        scope_id = derive_agent_scope(org.id, agent.id)
    except ScopeValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from None
    privilege = _API_ROLE_PRIVILEGE.get(actor.api_role, EndpointPrivilege.viewer)
    return EndpointAuth(
        actor=actor,
        privilege=privilege,
        scope_id=scope_id,
        org_id=org.id,
        agent_id=agent.id,
    )


async def _resolve_legacy_machine_scope(
    request: Request,
    actor: Actor,
    org_ref: str | None,
    agent_ref: str | None,
    binding: tuple[str, str],
) -> EndpointAuth:
    """Bind a legacy (unbound, non-global) ``key:role`` credential to the configured default.

    This is the explicit, cloud-only migration path (review finding 5): with no ``org=``/
    ``agent=`` binding a pre-identity API key carries no tenant, so instead of an ambient scope
    it is pinned to the one configured default org+Agent. It is treated exactly like a scoped
    credential bound to that pair — a client-supplied ``X-Keel-Org``/``X-Keel-Agent`` that tries
    to select a *different* tenant is rejected as a spoof (never widened), and the derived scope
    is the same per-Agent data plane. Every use is audited so operators can track remaining
    unmigrated keys.
    """
    service = _identity_service(request)
    bound_org, bound_agent = binding
    if org_ref and org_ref.strip() and org_ref.strip() != bound_org:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "org header does not match credential")
    if agent_ref and agent_ref.strip() and agent_ref.strip() != bound_agent:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "agent header does not match credential")
    try:
        org, agent = await service.resolve_machine_binding(bound_org, bound_agent)
    except NotFoundError:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "legacy migration org or agent not found"
        ) from None
    try:
        scope_id = derive_agent_scope(org.id, agent.id)
    except ScopeValidationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from None
    privilege = _API_ROLE_PRIVILEGE.get(actor.api_role, EndpointPrivilege.viewer)
    logger.warning(
        "legacy machine credential %s bound to migration default org=%s agent=%s",
        actor.display_name,
        org.id,
        agent.id,
    )
    return EndpointAuth(
        actor=actor,
        privilege=privilege,
        scope_id=scope_id,
        org_id=org.id,
        agent_id=agent.id,
    )


async def resolve_endpoint_auth(
    request: Request,
    actor: Annotated[Actor, Depends(resolve_actor)],
    x_keel_org: Annotated[str | None, Header(alias="X-Keel-Org")] = None,
    x_keel_agent: Annotated[str | None, Header(alias="X-Keel-Agent")] = None,
) -> EndpointAuth:
    """Resolve the request's :class:`EndpointAuth` (actor + privilege + derived scope).

    A **user** binds their org membership + selected Agent (fail closed). A **scoped** or
    **global** API-key machine credential binds its configured (or header-selected) org/Agent
    and derives the same per-Agent scope, so an API-key client keeps working in cloud mode
    *without* ambient cross-tenant access. A non-cloud **local**/unbound-**machine** operator
    maps to the explicit ``web:local`` local-preview scope with its coarse API-key role. In
    cloud mode an unbound, non-global caller fails closed — the local-preview scope is never
    reachable on an authenticated/cloud route.
    """
    if actor.is_user:
        return await _resolve_user_scope(request, actor, x_keel_org, x_keel_agent)
    # A machine credential with an explicit binding (or the global marker) resolves its own
    # per-Agent scope in either mode — never the ambient local-preview scope.
    if actor.machine_org_ref or actor.machine_global:
        return await _resolve_machine_scope(request, actor, x_keel_org, x_keel_agent)
    if _cloud_mode(request):
        # Legacy API-key migration (review finding 5): an explicit, cloud-only default
        # org+Agent lets pre-identity bare ``key:role`` credentials keep working during
        # migration, pinned to exactly that one tenant (never an ambient scope). Absent the
        # configured pair, an unbound non-global credential still fails closed.
        legacy_binding = getattr(request.app.state, "legacy_machine_binding", None)
        if legacy_binding is not None:
            return await _resolve_legacy_machine_scope(
                request, actor, x_keel_org, x_keel_agent, legacy_binding
            )
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
