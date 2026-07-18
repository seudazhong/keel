"""Authenticated durable IM channel-mapping admin API (WS-E/J, M3.7).

CRUD/list/status/revoke over the **org-owned** OneBot/Telegram channel mappings that bind a
provider chat identity to an exact persisted Agent + canonical scope + reply policy.

**Provisioning is platform-admin-only** (route-ownership hardening): *claiming* a global
``(provider, external_bot_id, external_chat_id)`` route (``POST``) requires a **truly global
machine admin** credential (or, self-hosted, the open-mode local operator) that explicitly
selects the target org (``X-Keel-Org``) and Agent (``agent_id``); the claim is audited. An
ordinary OIDC/org admin can **view and manage** their org's existing mappings (list/get/
revoke/disable/enable) but can never first-claim an arbitrary chat — closing the first-claim
theft where any tenant admin could bind a chat they do not control. Self-service webhook chat
verification is future work.

Route creation is **insert/claim, never last-writer-wins**: a route belongs to exactly one
org/mapping, and a conflicting claim by a different org fails closed with an opaque ``409``
(the owning org is never disclosed) without disturbing the existing route. The mapping row and
its global route claim are committed atomically (Postgres transaction) or compensated safely
(in-memory), so a concurrent cross-org race yields exactly one winner and the loser leaves no
orphan. The Agent is re-validated so a mapping can only ever bind an active Agent in the target
org (the DB composite FK is the final guard). No raw external provider secrets are ever accepted
or returned; the mapping policy is a plain capability/flag object. Revoking/disabling removes the
global route row so the webhook route is invalid immediately.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import IntegrityError

from keel_core.errors import PermissionDenied
from keel_core.identity import AuditAction, AuditEvent, IdentityService, NotFoundError
from keel_core.im_routing import (
    ImChannelMapping,
    ImChatKind,
    ImMappingStatus,
    ImMappingStore,
    ImProvider,
    ImProvisioner,
    ImReplyPolicy,
    ImRouteIndexStore,
    InMemoryImMappingStore,
    InMemoryImProvisioner,
    InMemoryImRouteIndex,
    PostgresImMappingStore,
    PostgresImProvisioner,
    PostgresImRouteIndex,
    RouteConflictError,
    StaleMappingError,
    TerminalMappingError,
)
from keel_core.scoping import derive_agent_scope
from keel_server.auth import Role
from keel_server.identity_context import (
    Actor,
    ActorKind,
    ResolvedOrg,
    require_org,
    require_org_manage,
    resolve_actor,
)

router = APIRouter(prefix="/v1/im/mappings", tags=["im-routing"])


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)


class PolicyModel(_Model):
    """The mapping reply/tool policy (never carries a secret — capability flags only)."""

    reply_enabled: bool = True
    partial_replies: bool = False
    approvals_enabled: bool = False
    allow_tools: list[str] = Field(default_factory=list)

    def to_policy(self) -> ImReplyPolicy:
        return ImReplyPolicy(
            reply_enabled=self.reply_enabled,
            partial_replies=self.partial_replies,
            approvals_enabled=self.approvals_enabled,
            allow_tools=tuple(self.allow_tools),
        )

    @classmethod
    def of(cls, policy: ImReplyPolicy) -> PolicyModel:
        return cls(
            reply_enabled=policy.reply_enabled,
            partial_replies=policy.partial_replies,
            approvals_enabled=policy.approvals_enabled,
            allow_tools=list(policy.allow_tools),
        )


class MappingCreateRequest(_Model):
    provider: ImProvider
    external_bot_id: str = Field(min_length=1, max_length=200)
    external_chat_id: str = Field(min_length=1, max_length=200)
    chat_kind: ImChatKind
    agent_id: str = Field(min_length=1, max_length=200)
    run_as_user_id: str = Field(min_length=1, max_length=200)
    policy: PolicyModel = Field(default_factory=PolicyModel)


class MappingResponse(_Model):
    id: str
    org_id: str
    provider: ImProvider
    external_bot_id: str
    external_chat_id: str
    chat_kind: ImChatKind
    agent_id: str
    scope_id: str
    run_as_user_id: str
    policy: PolicyModel
    status: ImMappingStatus
    version: int
    created_by: str
    created_at: datetime | None
    updated_at: datetime | None
    revoked_by: str
    revoked_at: datetime | None

    @classmethod
    def of(cls, mapping: ImChannelMapping) -> MappingResponse:
        return cls(
            id=mapping.id,
            org_id=mapping.org_id,
            provider=mapping.provider,
            external_bot_id=mapping.external_bot_id,
            external_chat_id=mapping.external_chat_id,
            chat_kind=mapping.chat_kind,
            agent_id=mapping.agent_id,
            scope_id=mapping.scope_id,
            run_as_user_id=mapping.run_as_user_id,
            policy=PolicyModel.of(mapping.policy),
            status=mapping.status,
            version=mapping.version,
            created_by=mapping.created_by,
            created_at=mapping.created_at,
            updated_at=mapping.updated_at,
            revoked_by=mapping.revoked_by,
            revoked_at=mapping.revoked_at,
        )


def _identity(request: Request) -> IdentityService:
    service = getattr(request.app.state, "identity", None)
    if not isinstance(service, IdentityService):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable")
    return service


def _mapping_store(request: Request, org_id: str) -> ImMappingStore:
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        return PostgresImMappingStore(engine, org_id)
    store = getattr(request.app.state, "im_mappings", None)
    if store is None:
        store = request.app.state.im_mappings = InMemoryImMappingStore()
    return store


def _route_index(request: Request) -> ImRouteIndexStore:
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        return PostgresImRouteIndex(engine)
    index = getattr(request.app.state, "im_route_index", None)
    if index is None:
        index = request.app.state.im_route_index = InMemoryImRouteIndex()
    return index


def _provisioner(request: Request, org_id: str) -> ImProvisioner:
    """The transactional (Postgres) / compensating (in-memory) mapping+route provisioner.

    The in-memory provisioner is cached on ``app.state`` so its serializing lock is shared across
    requests — the in-memory parity for the Postgres row/version lock that keeps concurrent
    enable/disable/revoke from racing the mapping status against its global route.
    """
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        return PostgresImProvisioner(engine)
    provisioner = getattr(request.app.state, "im_provisioner", None)
    if not isinstance(provisioner, InMemoryImProvisioner):
        provisioner = request.app.state.im_provisioner = InMemoryImProvisioner(
            _mapping_store(request, org_id), _route_index(request)
        )
    return provisioner


def _cloud_mode(request: Request) -> bool:
    return bool(getattr(request.app.state, "auth_required", False))


@dataclass(frozen=True)
class _Provisioner:
    """A platform-admin actor authorized to claim IM routes for an explicitly selected org."""

    actor: Actor
    org_ref: str


async def require_platform_provisioner(
    request: Request,
    actor: Annotated[Actor, Depends(resolve_actor)],
    x_keel_org: Annotated[str | None, Header(alias="X-Keel-Org")] = None,
) -> _Provisioner:
    """Gate route provisioning to a **truly global** machine admin (or open-mode local operator).

    An ordinary OIDC/org admin — even an org owner — is denied here (``403``): they may manage
    an already-provisioned mapping but can never first-claim an arbitrary chat. A global machine
    admin credential (``…:admin:global``) must explicitly select the target org via ``X-Keel-Org``.
    In a self-hosted, non-cloud deployment the single trusted local operator is the platform admin.
    """
    is_global_machine = (
        actor.kind is ActorKind.machine and actor.machine_global and actor.api_role >= Role.admin
    )
    is_local_operator = (
        actor.kind is ActorKind.local and actor.api_role >= Role.admin and not _cloud_mode(request)
    )
    if not (is_global_machine or is_local_operator):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "IM channel provisioning requires a platform admin credential",
        )
    if not x_keel_org or not x_keel_org.strip():
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "select the target organization via the X-Keel-Org header"
        )
    return _Provisioner(actor=actor, org_ref=x_keel_org.strip())


def _provision_actor_id(actor: Actor) -> str:
    """A stable, non-sensitive identifier for the claiming platform admin (for the audit)."""
    return actor.user_id or f"{actor.kind.value}:{actor.display_name}"


@router.post("", response_model=MappingResponse, status_code=status.HTTP_201_CREATED)
async def create_mapping(
    body: MappingCreateRequest,
    request: Request,
    provisioner: Annotated[_Provisioner, Depends(require_platform_provisioner)],
) -> MappingResponse:
    """Platform-admin-only: claim a global route + create the org-owned channel mapping.

    The target org (``X-Keel-Org``) and Agent (``agent_id``) are resolved as authoritative
    configuration (no membership) and must both be active; the selected **run-as** user
    (``run_as_user_id``) must be an active org member independently authorized to use that Agent
    (a personal Agent's owner, or a team Agent user) — the platform admin is only the audited
    provisioner, never the run actor. The mapping row and its global route claim are committed
    atomically; a chat already claimed by another org fails closed with an opaque ``409`` (the
    owning org is never disclosed). Reusing a **revoked** chat idempotently reprovisions the same
    row (no duplicate); an already-live chat is idempotent only for the exact same binding. The
    claim is audited.
    """
    identity = _identity(request)
    try:
        org, agent = await identity.resolve_machine_binding(provisioner.org_ref, body.agent_id)
    except NotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "organization or agent not found") from None
    try:
        await identity.authorize_im_run_as(org, agent, body.run_as_user_id)
    except NotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run-as user not found") from None
    except PermissionDenied as exc:
        raise HTTPException(status.HTTP_403_FORBIDDEN, str(exc)) from None
    mapping = ImChannelMapping(
        id=uuid.uuid4().hex,
        org_id=org.id,
        provider=body.provider,
        external_bot_id=body.external_bot_id,
        external_chat_id=body.external_chat_id,
        chat_kind=body.chat_kind,
        agent_id=agent.id,
        scope_id=derive_agent_scope(org.id, agent.id),
        policy=body.policy.to_policy(),
        status=ImMappingStatus.active,
        created_by=_provision_actor_id(provisioner.actor),
        run_as_user_id=body.run_as_user_id,
    )
    try:
        created = await _provisioner(request, org.id).provision(mapping)
    except RouteConflictError:
        # Opaque: never leak which org already owns the chat.
        raise HTTPException(status.HTTP_409_CONFLICT, "this chat is already claimed") from None
    except IntegrityError:
        # A duplicate (org, provider, bot, chat) mapping or a cross-org Agent FK violation.
        raise HTTPException(
            status.HTTP_409_CONFLICT, "a mapping for this chat already exists"
        ) from None
    identity.audit.record(
        AuditEvent(
            AuditAction.im_route_claimed,
            _provision_actor_id(provisioner.actor),
            org.id,
            created.id,
            {
                "provider": created.provider.value,
                "chat_kind": created.chat_kind.value,
                "agent_id": created.agent_id,
                "run_as": created.run_as_user_id,
                "route": created.route_key[:12],
            },
        )
    )
    return MappingResponse.of(created)


@router.get("", response_model=list[MappingResponse])
async def list_mappings(
    request: Request, org: Annotated[ResolvedOrg, Depends(require_org)]
) -> list[MappingResponse]:
    rows = await _mapping_store(request, org.org_id).list_for_org(org.org_id)
    return [MappingResponse.of(m) for m in rows]


@router.get("/{mapping_id}", response_model=MappingResponse)
async def get_mapping(
    mapping_id: str, request: Request, org: Annotated[ResolvedOrg, Depends(require_org)]
) -> MappingResponse:
    mapping = await _mapping_store(request, org.org_id).get(mapping_id)
    if mapping is None or mapping.org_id != org.org_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "mapping not found")
    return MappingResponse.of(mapping)


async def _set_status(
    request: Request,
    org: ResolvedOrg,
    mapping_id: str,
    new_status: ImMappingStatus,
    expected_version: int | None,
) -> MappingResponse:
    """Atomically transition a mapping's status **and** claim/release its global route.

    The mapping status change and the route claim/removal are one all-or-nothing transaction under
    a row/version lock (Postgres ``SELECT ... FOR UPDATE`` / an in-memory lock), so the route
    invariant (``active`` iff a route exists) is never observably broken by a concurrent
    enable/disable/revoke. A stale optimistic-version request is refused with ``409``; a transition
    out of the terminal ``revoked`` state with ``409``; enabling a chat another org has re-claimed
    fails closed with an opaque ``409`` and rolls back. The audit is recorded **after** the
    committed outcome — a no-op retry (the requested status already holds) commits nothing and is
    never audited.
    """
    before = await _mapping_store(request, org.org_id).get(mapping_id)
    try:
        updated = await _provisioner(request, org.org_id).transition(
            mapping_id,
            new_status,
            org_id=org.org_id,
            actor=org.user_id,
            expected_version=expected_version,
        )
    except RouteConflictError:
        raise HTTPException(status.HTTP_409_CONFLICT, "this chat is already claimed") from None
    except StaleMappingError:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "mapping was modified concurrently; re-read and retry"
        ) from None
    except TerminalMappingError:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "mapping is revoked; reprovision to reactivate"
        ) from None
    if updated is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "mapping not found")
    # A version bump is the ground truth for "something actually changed"; a no-op retry (the
    # requested status already held) returns the row unchanged and must never be audited.
    changed = before is None or updated.version != before.version
    if changed:
        # Audit only the committed outcome: enabling re-claimed the route; disable/revoke
        # released it.
        action = (
            AuditAction.im_route_claimed
            if new_status is ImMappingStatus.active
            else AuditAction.im_route_released
        )
        _identity(request).audit.record(
            AuditEvent(
                action,
                org.user_id,
                org.org_id,
                mapping_id,
                {"status": new_status.value, "route": updated.route_key[:12]},
            )
        )
    return MappingResponse.of(updated)


@router.post("/{mapping_id}/revoke", response_model=MappingResponse)
async def revoke_mapping(
    mapping_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org_manage)],
    expected_version: int | None = None,
) -> MappingResponse:
    return await _set_status(request, org, mapping_id, ImMappingStatus.revoked, expected_version)


@router.post("/{mapping_id}/disable", response_model=MappingResponse)
async def disable_mapping(
    mapping_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org_manage)],
    expected_version: int | None = None,
) -> MappingResponse:
    return await _set_status(request, org, mapping_id, ImMappingStatus.disabled, expected_version)


@router.post("/{mapping_id}/enable", response_model=MappingResponse)
async def enable_mapping(
    mapping_id: str,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org_manage)],
    expected_version: int | None = None,
) -> MappingResponse:
    return await _set_status(request, org, mapping_id, ImMappingStatus.active, expected_version)


__all__ = ["router"]
