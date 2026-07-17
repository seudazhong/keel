"""Request actor context: OIDC users, API-key machines, local preview (M3.6, WS-L).

Extends the coarse API-key :class:`~keel_server.auth.Principal` with a richer *actor* that
distinguishes:

* **user** — a human authenticated by a verified OIDC bearer JWT, resolved to a durable
  :class:`~keel_core.identity.User`;
* **machine** — an API-key caller (the existing hashed-key path), with a role tier but no
  durable identity;
* **local** — the open-mode single-operator preview, mapped to a durable *local operator*
  user so identity reads/writes still bind to a real user rather than the ambient
  ``web:local`` scope.

The identity API depends on :func:`require_user` (a durable user) and :func:`require_org`
(an org the user is an active member of, selected via the ``X-Keel-Org`` header). Org
selection rejects spoofing: a non-member gets the same 404 as an unknown org. There is no
global mutable actor state — every actor is derived per-request from the credential.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status

from keel_core.errors import PermissionDenied
from keel_core.identity import (
    ConflictError,
    IdentityService,
    IdentityValidationError,
    LastOwnerError,
    NotFoundError,
    OIDCVerificationError,
    OIDCVerifier,
    OptimisticConcurrencyError,
    OrgContext,
)
from keel_server.auth import Principal, Role, authenticate


class ActorKind(StrEnum):
    user = "user"
    machine = "machine"
    local = "local"


@dataclass(frozen=True)
class Actor:
    """The authenticated caller for a request (no ambient/global state)."""

    kind: ActorKind
    api_role: Role
    display_name: str
    user_id: str | None = None
    oidc_subject: str | None = None

    @property
    def is_user(self) -> bool:
        return self.kind is ActorKind.user and self.user_id is not None


def _bearer_token(request: Request) -> str | None:
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip() or None
    return None


def _looks_like_jwt(token: str) -> bool:
    # A compact JWS has exactly three '.'-separated non-empty segments; an opaque API key
    # does not. This lets a bearer header carry either without ambiguity.
    parts = token.split(".")
    return len(parts) == 3 and all(parts)


def _identity_service(request: Request) -> IdentityService | None:
    service = getattr(request.app.state, "identity", None)
    return service if isinstance(service, IdentityService) else None


def _oidc_verifier(request: Request) -> OIDCVerifier | None:
    verifier = getattr(request.app.state, "oidc_verifier", None)
    return verifier if isinstance(verifier, OIDCVerifier) else None


async def resolve_actor(request: Request) -> Actor:
    """Resolve the request's actor from an OIDC JWT or the API-key/local path."""
    token = _bearer_token(request)
    verifier = _oidc_verifier(request)
    service = _identity_service(request)
    if verifier is not None and token is not None and _looks_like_jwt(token):
        if service is None:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable")
        try:
            claims = await verifier.verify(token)
        except OIDCVerificationError:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or expired token") from None
        try:
            user = await service.resolve_oidc_user(claims)
        except NotFoundError:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN, "authenticated subject is not provisioned"
            ) from None
        return Actor(
            kind=ActorKind.user,
            api_role=Role.operator,
            display_name=user.display_name,
            user_id=user.id,
            oidc_subject=claims.subject,
        )

    principal: Principal = authenticate(request)
    is_local = principal.name == "local"
    return Actor(
        kind=ActorKind.local if is_local else ActorKind.machine,
        api_role=principal.role,
        display_name=principal.name,
    )


def _cloud_mode(request: Request) -> bool:
    return bool(getattr(request.app.state, "auth_required", False))


async def require_user(request: Request, actor: Annotated[Actor, Depends(resolve_actor)]) -> Actor:
    """Require a durable user actor.

    OIDC users pass through. In non-cloud mode a local operator is transparently mapped to
    a durable local user so the single-operator profile can use identity APIs. In cloud
    mode a non-user caller is rejected — identity operations require OIDC.
    """
    if actor.is_user:
        return actor
    service = _identity_service(request)
    if service is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable")
    if actor.kind is ActorKind.local and not _cloud_mode(request):
        user = await service.ensure_local_user()
        return Actor(
            kind=ActorKind.user,
            api_role=actor.api_role,
            display_name=user.display_name,
            user_id=user.id,
            oidc_subject=None,
        )
    raise HTTPException(
        status.HTTP_403_FORBIDDEN,
        "identity operations require an authenticated user (configure OIDC)",
    )


@dataclass(frozen=True)
class ResolvedOrg:
    """A user actor bound to a selected, authorized organization."""

    actor: Actor
    context: OrgContext

    @property
    def user_id(self) -> str:
        assert self.actor.user_id is not None
        return self.actor.user_id

    @property
    def org_id(self) -> str:
        return self.context.org_id


async def require_org(
    request: Request,
    actor: Annotated[Actor, Depends(require_user)],
    x_keel_org: Annotated[str | None, Header(alias="X-Keel-Org")] = None,
) -> ResolvedOrg:
    """Resolve the org selected via ``X-Keel-Org`` that the user actively belongs to."""
    if not x_keel_org or not x_keel_org.strip():
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "select an organization via the X-Keel-Org header"
        )
    service = _identity_service(request)
    if service is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable")
    assert actor.user_id is not None
    try:
        context = await service.select_org(actor.user_id, x_keel_org.strip())
    except NotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "organization not found") from None
    return ResolvedOrg(actor=actor, context=context)


def identity_http_status(exc: Exception) -> int:
    """Map an identity-domain error to its HTTP status (fail closed to 403/422)."""
    if isinstance(exc, NotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, LastOwnerError | ConflictError | OptimisticConcurrencyError):
        return status.HTTP_409_CONFLICT
    if isinstance(exc, IdentityValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, PermissionDenied):
        return status.HTTP_403_FORBIDDEN
    return status.HTTP_400_BAD_REQUEST


__all__ = [
    "Actor",
    "ActorKind",
    "ResolvedOrg",
    "identity_http_status",
    "require_org",
    "require_user",
    "resolve_actor",
]
