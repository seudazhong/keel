"""Durable IM ingress: pre-tenant route lookup -> fail-closed resolve -> durable admission.

An inbound provider event has **already** been authenticated + replay-checked by the webhook
(:mod:`keel_server.api.gateway`) *before* any tenant is known. This module then does the
pre-tenant step: it looks the chat up in the minimal, opaque **global route index** (no
credentials, no content), fails closed on an unknown/revoked mapping (cloud) or an unmapped
local-preview chat, loads the mapping's reply policy, and admits the run through the **same**
:class:`~keel_core.run_service.DurableRunService` the Web surface uses — ``surface="im"`` with
the durable IM provider/chat context — so the worker rebuilds the untrusted IM-safe Agent and a
durable, encrypted reply. There is no process-local in-memory gateway runtime.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field

from keel_core.im_routing import (
    ImApprovalCommand,
    ImChannelMapping,
    ImChatKind,
    ImInboundContext,
    ImMappingStatus,
    ImMappingStore,
    ImProvider,
    ImReplyPolicy,
    ImRouteIndexStore,
    parse_approval_command,
    resolve_inbound_route,
    route_key,
)
from keel_core.run_service import DurableRunService
from keel_core.runs import RunSurface
from keel_core.types import ScopeId
from keel_server.gateway.onebot import OneBotEvent, wake_rule
from keel_server.gateway.telegram import (
    TelegramUpdate,
)
from keel_server.gateway.telegram import (
    parse_telegram_inbound as _tg_parse,
)

logger = logging.getLogger("keel.server.im_ingress")

# A run admitted from an IM surface acts under the authority of the **run-as** org member the
# platform admin bound the channel mapping to (its ``run_as_user_id``) — never the platform admin
# that provisioned it (``created_by`` is only an audit identity). The worker re-validates that
# run-as member's Agent-use authorization at claim time, so a revoked membership or member removal
# fails future runs closed. A mapping with no recorded run-as falls back to this service principal.
_IM_SERVICE_ACTOR = "im:service"


@dataclass(frozen=True)
class ImInbound:
    """A normalized, woke inbound IM message ready for durable admission."""

    provider: ImProvider
    external_bot_id: str
    external_chat_id: str
    external_message_id: str
    chat_kind: ImChatKind
    text: str

    @property
    def route_key(self) -> str:
        return route_key(self.provider.value, self.external_bot_id, self.external_chat_id)

    def session_id(self) -> str:
        """One durable session per chat (multi-turn conversation continuity)."""
        return f"im/{self.provider.value}/{self.external_bot_id}/{self.external_chat_id}"


# (org_id) -> a mapping store bound to that org; (scope_id) -> a DurableRunService for that scope.
MappingStoreFactory = Callable[[str], ImMappingStore]
RunServiceFactory = Callable[[ScopeId], DurableRunService]


@dataclass
class DurableImIngress:
    """Resolve an authenticated inbound IM message to a durable ``surface="im"`` admission."""

    route_index: ImRouteIndexStore
    mapping_store_factory: MappingStoreFactory
    run_service_factory: RunServiceFactory
    cloud_mode: bool
    default_model: str = ""
    _reject_unknown: Callable[[str], Awaitable[None]] | None = field(default=None)

    async def admit(self, inbound: ImInbound) -> str | None:
        """Admit ``inbound`` durably, or return ``None`` when it is dropped fail-closed.

        Resolution is fail-closed: an unknown chat (no route row) or a revoked/disabled mapping
        raises inside :func:`resolve_inbound_route` and the message is dropped (never a silent
        default binding). A resolved mapping is re-read for its current status + reply policy,
        then the run is admitted through the scoped :class:`DurableRunService` with the durable
        IM context so the worker can rebuild the safe Agent and reply target."""
        entry = await self.route_index.lookup(inbound.route_key)
        try:
            resolved = resolve_inbound_route(entry, cloud_mode=self.cloud_mode)
        except Exception:  # noqa: BLE001 - unknown/revoked mapping: fail closed, drop the event
            logger.info(
                "im ingress dropped provider=%s chat=%s (no active mapping)",
                inbound.provider.value,
                # never log the raw chat id — only its opaque route key
                inbound.route_key[:12],
            )
            return None
        mapping = await self.mapping_store_factory(resolved.org_id).get(resolved.mapping_id)
        if mapping is None or mapping.status is not ImMappingStatus.active:
            logger.info("im ingress dropped: mapping %s revoked/missing", resolved.mapping_id)
            return None
        # An IM-originated approval decision (opt-in per mapping) resolves an existing durable
        # approval instead of admitting a new run; the exact approval/attempt/action-hash + org
        # binding is re-validated by resolve_approval (stale/replay denied).
        if mapping.policy.approvals_enabled:
            command = parse_approval_command(inbound.text)
            if command is not None:
                await self._resolve_approval(resolved.scope_id, resolved.org_id, mapping, command)
                return None
        context = self._context(inbound, mapping)
        actor = mapping.run_as_user_id or _IM_SERVICE_ACTOR
        service = self.run_service_factory(resolved.scope_id)
        result = await service.admit(
            org_id=resolved.org_id,
            actor=actor,
            agent_id=resolved.agent_id,
            session_id=inbound.session_id(),
            surface=RunSurface.im.value,
            content=inbound.text,
            idempotency_key=self._idempotency_key(inbound),
            model=self.default_model or None,
            admission_extra=context.to_admission_extra(),
        )
        logger.info(
            "im ingress admitted provider=%s org=%s agent=%s run=%s",
            inbound.provider.value,
            resolved.org_id,
            resolved.agent_id,
            result.run_id,
        )
        return result.run_id

    @staticmethod
    def _context(inbound: ImInbound, mapping: ImChannelMapping) -> ImInboundContext:
        policy = mapping.policy if isinstance(mapping.policy, ImReplyPolicy) else ImReplyPolicy()
        return ImInboundContext(
            provider=inbound.provider,
            external_bot_id=inbound.external_bot_id,
            external_chat_id=inbound.external_chat_id,
            external_message_id=inbound.external_message_id,
            chat_kind=inbound.chat_kind,
            mapping_id=mapping.id,
            policy=policy,
            mapping_version=mapping.version,
            run_as_user_id=mapping.run_as_user_id,
            agent_id=mapping.agent_id,
            scope_id=mapping.scope_id,
        )

    @staticmethod
    def _idempotency_key(inbound: ImInbound) -> str:
        """At-most-once admission per inbound message (a duplicate webhook dedups on this)."""
        return (
            f"{inbound.provider.value}:{inbound.external_bot_id}:"
            f"{inbound.external_chat_id}:{inbound.external_message_id}"
        )

    async def _resolve_approval(
        self,
        scope_id: ScopeId,
        org_id: str,
        mapping: ImChannelMapping,
        command: ImApprovalCommand,
    ) -> None:
        """Resolve a durable approval from an IM command under the mapping's run-as authority.

        The decision resolves as the mapping's **run-as** org member (``run_as_user_id`` — the same
        actor an IM run binds), so the durable ``resolve_approval`` owner check passes; every
        other binding (org, current run attempt, recomputed action hash, terminal state) is
        verified there, denying a stale/replayed or cross-org command."""
        actor = mapping.run_as_user_id or _IM_SERVICE_ACTOR
        service = self.run_service_factory(scope_id)
        ok = await service.resolve_approval(
            command.approval_id,
            approved=command.approved,
            resolved_by=f"im:{mapping.provider.value}",
            actor=actor,
            org_id=org_id,
        )
        logger.info(
            "im approval command mapping=%s approval=%s approved=%s resolved=%s",
            mapping.id,
            command.approval_id,
            command.approved,
            ok,
        )


__all__ = [
    "DurableImIngress",
    "ImInbound",
    "MappingStoreFactory",
    "RunServiceFactory",
    "parse_onebot_inbound",
    "parse_telegram_inbound",
]


def parse_onebot_inbound(
    payload: Mapping[str, object], *, self_id: int | None, prefixes: tuple[str, ...]
) -> ImInbound | None:
    """Parse + wake-gate a OneBot v11 event into a normalized :class:`ImInbound` (None = ignore)."""
    event = OneBotEvent.model_validate(payload)
    if event.post_type != "message":
        return None
    decision = wake_rule(event, self_id=self_id, prefixes=prefixes)
    if not decision.woke or not decision.text:
        return None
    is_group = event.message_type == "group"
    chat_id = event.group_id if is_group else event.user_id
    bot = event.self_id if event.self_id is not None else self_id
    raw_message_id = payload.get("message_id")
    if raw_message_id is None or str(raw_message_id) == "":
        # No stable per-message id: reject rather than admit with an empty id, which would
        # collapse unrelated messages onto the same admission idempotency key (dedupe them).
        return None
    return ImInbound(
        provider=ImProvider.onebot,
        external_bot_id=str(bot) if bot is not None else "",
        external_chat_id=str(chat_id) if chat_id is not None else "",
        external_message_id=str(raw_message_id),
        chat_kind=ImChatKind.group if is_group else ImChatKind.personal,
        text=decision.text,
    )


def _parse_telegram_woke(
    payload: Mapping[str, object], *, bot_username: str | None, prefixes: tuple[str, ...]
) -> str | None:
    """The woke, wake-token-stripped text of a Telegram update (None = ignore/no wake)."""
    message = _tg_parse(dict(payload), bot_username=bot_username, prefixes=prefixes)
    return message.text if message is not None else None


def parse_telegram_inbound(
    payload: Mapping[str, object],
    *,
    bot_id: str,
    bot_username: str | None,
    prefixes: tuple[str, ...],
) -> ImInbound | None:
    """Parse + wake-gate a Telegram update into a normalized :class:`ImInbound` (None = ignore)."""
    woke = _parse_telegram_woke(payload, bot_username=bot_username, prefixes=prefixes)
    if woke is None:
        return None
    message = TelegramUpdate.model_validate(payload).message
    if message is None:
        return None
    chat = message.chat
    is_group = chat.type != "private"
    if not message.message_id:
        # An id-less update has no stable dedupe key: reject it rather than admit with an empty
        # message id, which would collapse unrelated messages onto one admission idempotency key.
        return None
    return ImInbound(
        provider=ImProvider.telegram,
        external_bot_id=bot_id,
        external_chat_id=str(chat.id),
        external_message_id=str(message.message_id),
        chat_kind=ImChatKind.group if is_group else ImChatKind.personal,
        text=woke,
    )
