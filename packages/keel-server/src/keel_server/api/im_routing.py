"""Authenticated durable IM channel-mapping admin API (WS-E/J, M3.7).

CRUD/list/status/revoke over the **org-owned** OneBot/Telegram channel mappings that bind a
provider chat identity to an exact persisted Agent + canonical scope + reply policy. Every route
binds to a request actor and an org the user is an active member of (``X-Keel-Org``); the Agent
is re-validated through :meth:`IdentityService.select_agent` so a mapping can only ever bind an
Agent in the caller's *own* org (cross-org binding is impossible — the DB composite FK is the
final guard). No raw external provider secrets are ever accepted or returned; the mapping policy
is a plain capability/flag object. Creating/updating a mapping (re)publishes its opaque row in
the global route index; revoking removes it so the webhook route is invalid immediately.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field

from keel_core.errors import PermissionDenied
from keel_core.identity import IdentityService, NotFoundError
from keel_core.im_routing import (
    ImChannelMapping,
    ImChatKind,
    ImMappingStatus,
    ImMappingStore,
    ImProvider,
    ImReplyPolicy,
    ImRouteIndexStore,
    InMemoryImMappingStore,
    InMemoryImRouteIndex,
    PostgresImMappingStore,
    PostgresImRouteIndex,
)
from keel_core.scoping import derive_agent_scope
from keel_server.identity_context import ResolvedOrg, require_org

router = APIRouter(prefix="/v1/im/mappings", tags=["im-routing"])


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)


class PolicyModel(_Model):
    """The mapping reply/tool policy (never carries a secret — capability flags only)."""

    reply_enabled: bool = True
    partial_replies: bool = False
    allow_tools: list[str] = Field(default_factory=list)

    def to_policy(self) -> ImReplyPolicy:
        return ImReplyPolicy(
            reply_enabled=self.reply_enabled,
            partial_replies=self.partial_replies,
            allow_tools=tuple(self.allow_tools),
        )

    @classmethod
    def of(cls, policy: ImReplyPolicy) -> PolicyModel:
        return cls(
            reply_enabled=policy.reply_enabled,
            partial_replies=policy.partial_replies,
            allow_tools=list(policy.allow_tools),
        )


class MappingCreateRequest(_Model):
    provider: ImProvider
    external_bot_id: str = Field(min_length=1, max_length=200)
    external_chat_id: str = Field(min_length=1, max_length=200)
    chat_kind: ImChatKind
    agent_id: str = Field(min_length=1, max_length=200)
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


async def _require_agent_name(request: Request, org: ResolvedOrg, agent_id: str) -> str:
    """Resolve + authorize the Agent in the caller's org (cross-org binding rejected)."""
    try:
        agent = await _identity(request).select_agent(org.org_id, org.user_id, agent_id)
    except NotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found") from None
    except PermissionDenied:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "agent not permitted") from None
    return agent.name


@router.post("", response_model=MappingResponse, status_code=status.HTTP_201_CREATED)
async def create_mapping(
    body: MappingCreateRequest,
    request: Request,
    org: Annotated[ResolvedOrg, Depends(require_org)],
) -> MappingResponse:
    """Create an org-owned channel mapping + publish its opaque global route index row."""
    await _require_agent_name(request, org, body.agent_id)
    mapping = ImChannelMapping(
        id=uuid.uuid4().hex,
        org_id=org.org_id,
        provider=body.provider,
        external_bot_id=body.external_bot_id,
        external_chat_id=body.external_chat_id,
        chat_kind=body.chat_kind,
        agent_id=body.agent_id,
        scope_id=derive_agent_scope(org.org_id, body.agent_id),
        policy=body.policy.to_policy(),
        status=ImMappingStatus.active,
        created_by=org.user_id,
    )
    created = await _mapping_store(request, org.org_id).create(mapping)
    await _route_index(request).put(created.route_entry())
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
    request: Request, org: ResolvedOrg, mapping_id: str, new_status: ImMappingStatus
) -> MappingResponse:
    store = _mapping_store(request, org.org_id)
    existing = await store.get(mapping_id)
    if existing is None or existing.org_id != org.org_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "mapping not found")
    updated = await store.set_status(mapping_id, new_status, actor=org.user_id)
    if updated is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "mapping not found")
    index = _route_index(request)
    if new_status is ImMappingStatus.active:
        await index.put(updated.route_entry())
    else:
        # Revoke/disable: drop the global route row so the webhook route is invalid immediately.
        await index.remove_for_mapping(mapping_id)
    return MappingResponse.of(updated)


@router.post("/{mapping_id}/revoke", response_model=MappingResponse)
async def revoke_mapping(
    mapping_id: str, request: Request, org: Annotated[ResolvedOrg, Depends(require_org)]
) -> MappingResponse:
    return await _set_status(request, org, mapping_id, ImMappingStatus.revoked)


@router.post("/{mapping_id}/disable", response_model=MappingResponse)
async def disable_mapping(
    mapping_id: str, request: Request, org: Annotated[ResolvedOrg, Depends(require_org)]
) -> MappingResponse:
    return await _set_status(request, org, mapping_id, ImMappingStatus.disabled)


@router.post("/{mapping_id}/enable", response_model=MappingResponse)
async def enable_mapping(
    mapping_id: str, request: Request, org: Annotated[ResolvedOrg, Depends(require_org)]
) -> MappingResponse:
    return await _set_status(request, org, mapping_id, ImMappingStatus.active)


__all__ = ["router"]
