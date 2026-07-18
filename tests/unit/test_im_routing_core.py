"""Durable IM routing core: mapping resolution, safe agent, reply outbox (WS-E/J, M3.7).

Unit coverage (no Postgres/Redis) for the durable OneBot/Telegram routing primitives: the
opaque global route index + fail-closed resolution, the untrusted IM-safe permission policy
(a malicious prompt can never reach write/shell), encrypted reply payloads, and the idempotent
reply outbox + fenced global reply-dispatch index.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from keel_core.im_routing import (
    IM_SAFE_TOOLS,
    ImChannelMapping,
    ImChatKind,
    ImMappingStatus,
    ImProvider,
    ImReplyIntent,
    ImReplyKind,
    ImReplyPolicy,
    ImReplyStatus,
    InMemoryImMappingStore,
    InMemoryImReplyDispatchIndex,
    InMemoryImReplyStore,
    InMemoryImRouteIndex,
    RevokedMappingError,
    UnknownMappingError,
    build_im_safe_agent,
    decrypt_reply_payload,
    encrypt_reply_payload,
    im_safe_permissions,
    reply_idempotency_key,
    resolve_inbound_route,
    route_key,
)
from keel_core.protocols import ToolContext
from keel_core.secrets import KeyRing
from keel_core.types import ContentTaint, PermissionDecision, TrustLevel

_NOW = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)


def _mapping(**overrides: object) -> ImChannelMapping:
    base: dict[str, object] = {
        "id": "map-1",
        "org_id": "org-a",
        "provider": ImProvider.telegram,
        "external_bot_id": "bot-9",
        "external_chat_id": "chat-42",
        "chat_kind": ImChatKind.group,
        "agent_id": "agent-1",
        "scope_id": "agent:org-a/agent-1",
    }
    base.update(overrides)
    return ImChannelMapping(**base)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- route key


def test_route_key_is_stable_and_opaque() -> None:
    key = route_key("telegram", "bot-9", "chat-42")
    # Deterministic, 64-hex SHA-256, and holds none of the plaintext ids.
    assert key == route_key("telegram", "bot-9", "chat-42")
    assert len(key) == 64
    assert "bot-9" not in key and "chat-42" not in key and "telegram" not in key
    # A different chat / bot / provider yields a different key.
    assert key != route_key("telegram", "bot-9", "chat-43")
    assert key != route_key("onebot", "bot-9", "chat-42")


# --------------------------------------------------------------------------- resolution


async def test_unknown_mapping_fails_closed_in_cloud_and_local() -> None:
    for cloud in (True, False):
        with pytest.raises(UnknownMappingError):
            resolve_inbound_route(None, cloud_mode=cloud)


async def test_revoked_and_disabled_mapping_fail_closed() -> None:
    for status in (ImMappingStatus.revoked, ImMappingStatus.disabled):
        entry = _mapping(status=status).route_entry()
        with pytest.raises(RevokedMappingError):
            resolve_inbound_route(entry, cloud_mode=True)


async def test_active_mapping_resolves() -> None:
    entry = _mapping().route_entry()
    resolved = resolve_inbound_route(entry, cloud_mode=True)
    assert resolved.scope_id == "agent:org-a/agent-1"
    assert resolved.agent_id == "agent-1"
    assert resolved.reply_allowed is True


async def test_route_index_lookup_round_trip() -> None:
    index = InMemoryImRouteIndex()
    mapping = _mapping()
    await index.put(mapping.route_entry())
    key = route_key("telegram", "bot-9", "chat-42")
    found = await index.lookup(key)
    assert found is not None
    assert found.mapping_id == "map-1"
    # A revoke that removes the route makes the chat unknown → fail closed.
    await index.remove_for_mapping("map-1")
    assert await index.lookup(key) is None
    with pytest.raises(UnknownMappingError):
        resolve_inbound_route(await index.lookup(key), cloud_mode=True)


# --------------------------------------------------------------------------- safe agent


def _ctx() -> ToolContext:
    return ToolContext(
        scope_id="agent:org-a/agent-1",
        session_id="s1",
        trust=TrustLevel.untrusted,
        content_taint=ContentTaint.tainted,
    )


async def test_malicious_prompt_cannot_reach_write_or_shell() -> None:
    perms = im_safe_permissions(ImReplyPolicy())
    for allowed in IM_SAFE_TOOLS:
        assert perms.evaluate(allowed, {}, _ctx()) is PermissionDecision.allow
    for forbidden in ("write", "edit", "shell", "feishu_reply", "gmail_send"):
        assert perms.evaluate(forbidden, {}, _ctx()) is PermissionDecision.deny


async def test_read_only_extras_allowed_and_policy_tools_are_ask() -> None:
    perms = im_safe_permissions(
        ImReplyPolicy(allow_tools=("write",)), read_only_extra=("knowledge_search",)
    )
    assert perms.evaluate("knowledge_search", {}, _ctx()) is PermissionDecision.allow
    # An explicitly approved otherwise-forbidden tool is downgraded to ask (durable approval),
    # never silently allowed.
    assert perms.evaluate("write", {}, _ctx()) is PermissionDecision.ask
    # A non-approved mutating tool is still denied.
    assert perms.evaluate("shell", {}, _ctx()) is PermissionDecision.deny


def test_build_im_safe_agent_is_untrusted_and_read_only() -> None:
    agent = build_im_safe_agent(
        scope_id="agent:org-a/agent-1",
        model="gpt-4o-mini",
        agent_id="agent-1",
        name="Support",
        chat_kind=ImChatKind.group,
        read_only_extra=("knowledge_search",),
    )
    assert agent.scope.trust is TrustLevel.untrusted
    assert set(agent.toolset) == set(IM_SAFE_TOOLS) | {"knowledge_search"}
    assert "write" not in agent.toolset and "shell" not in agent.toolset


def test_policy_approved_tool_added_to_toolset() -> None:
    agent = build_im_safe_agent(
        scope_id="agent:org-a/agent-1",
        model="m",
        agent_id="a",
        name="n",
        chat_kind=ImChatKind.personal,
        policy=ImReplyPolicy(allow_tools=("write",)),
    )
    assert "write" in agent.toolset


# --------------------------------------------------------------------------- reply payload


def _keyring() -> KeyRing:
    return KeyRing({"v1": "unit-test-secret"}, "v1")


def test_reply_payload_encrypts_at_rest() -> None:
    ring = _keyring()
    secret = encrypt_reply_payload(ring, "hello human")
    assert "hello human" not in secret.ciphertext
    intent = _intent(key_id=secret.key_id, ciphertext=secret.ciphertext)
    assert decrypt_reply_payload(ring, intent) == "hello human"


def _intent(**overrides: object) -> ImReplyIntent:
    base: dict[str, object] = {
        "id": "reply-1",
        "scope_id": "agent:org-a/agent-1",
        "run_id": "run-1",
        "org_id": "org-a",
        "provider": ImProvider.telegram,
        "external_bot_id": "bot-9",
        "external_chat_id": "chat-42",
        "external_message_id": "msg-7",
        "chat_kind": ImChatKind.group,
        "reply_kind": ImReplyKind.final,
        "idempotency_key": reply_idempotency_key(
            run_id="run-1",
            provider="telegram",
            external_bot_id="bot-9",
            external_chat_id="chat-42",
            external_message_id="msg-7",
            kind="final",
        ),
        "key_id": "v1",
        "ciphertext": "x",
    }
    base.update(overrides)
    return ImReplyIntent(**base)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- reply outbox


async def test_reply_intent_is_idempotent() -> None:
    store = InMemoryImReplyStore()
    first, created1 = await store.record_intent(_intent())
    second, created2 = await store.record_intent(_intent(id="reply-2"))
    assert created1 is True and created2 is False
    # The second record collapses to the first row (same idempotency key).
    assert second.id == first.id == "reply-1"


async def test_reply_claim_is_fenced_and_marks_sent() -> None:
    store = InMemoryImReplyStore()
    await store.record_intent(_intent())
    claimed = await store.claim("reply-1", worker_id="w1", lease_seconds=60, now=_NOW)
    assert claimed is not None and claimed.status is ImReplyStatus.leased
    # A second concurrent claim under a live lease is refused (fencing).
    assert await store.claim("reply-1", worker_id="w2", lease_seconds=60, now=_NOW) is None
    # A stale fence token cannot mark sent.
    assert await store.mark_sent("reply-1", lease_token="stale", provider_message_id="p") is False
    ok = await store.mark_sent(
        "reply-1", lease_token=claimed.lease_token, provider_message_id="pm-1"
    )
    assert ok is True
    final = await store.get("reply-1")
    assert final is not None and final.status is ImReplyStatus.sent
    assert final.provider_message_id == "pm-1"
    # A sent reply is never re-claimed (no duplicate user-visible reply).
    assert await store.claim("reply-1", worker_id="w1", lease_seconds=60, now=_NOW) is None


async def test_reply_failure_reschedules_for_retry() -> None:
    store = InMemoryImReplyStore()
    await store.record_intent(_intent())
    claimed = await store.claim("reply-1", worker_id="w1", lease_seconds=60, now=_NOW)
    assert claimed is not None
    ok = await store.mark_failed(
        "reply-1",
        lease_token=claimed.lease_token,
        error="boom",
        retry_delay_seconds=30,
        now=_NOW,
    )
    assert ok is True
    after = await store.get("reply-1")
    assert after is not None and after.status is ImReplyStatus.pending
    # Reclaimable after the lease is released (restart-safe retry).
    reclaim = await store.claim(
        "reply-1", worker_id="w2", lease_seconds=60, now=_NOW + timedelta(seconds=31)
    )
    assert reclaim is not None


# --------------------------------------------------------------------------- dispatch index


async def test_reply_dispatch_index_leases_and_retires() -> None:
    index = InMemoryImReplyDispatchIndex()
    await index.record("reply-1", "agent:org-a/agent-1", now=_NOW)
    claimed = await index.claim_due(worker_id="w1", now=_NOW)
    assert [i.reply_id for i in claimed] == ["reply-1"]
    # A second worker sees nothing while the lease is live (no double send).
    assert await index.claim_due(worker_id="w2", now=_NOW) == []
    assert await index.active_scopes() == {"agent:org-a/agent-1"}
    # Terminal reply → retire the pointer.
    await index.remove("reply-1")
    assert await index.active_scopes() == set()


async def test_reply_dispatch_reschedule_is_reclaimable() -> None:
    index = InMemoryImReplyDispatchIndex()
    await index.record("reply-1", "agent:org-a/agent-1", now=_NOW)
    await index.claim_due(worker_id="w1", now=_NOW)
    await index.reschedule("reply-1", delay_seconds=30, now=_NOW)
    assert await index.claim_due(worker_id="w2", now=_NOW) == []
    later = await index.claim_due(worker_id="w2", now=_NOW + timedelta(seconds=31))
    assert [i.reply_id for i in later] == ["reply-1"]


# --------------------------------------------------------------------------- mapping store


async def test_mapping_revoke_bumps_version_and_status() -> None:
    store = InMemoryImMappingStore()
    await store.create(_mapping())
    revoked = await store.set_status(
        "map-1", ImMappingStatus.revoked, actor="admin@org", now=_NOW
    )
    assert revoked is not None
    assert revoked.status is ImMappingStatus.revoked
    assert revoked.version == 2
    assert revoked.revoked_by == "admin@org" and revoked.revoked_at == _NOW
    # The republished route entry now fails closed on lookup.
    with pytest.raises(RevokedMappingError):
        resolve_inbound_route(revoked.route_entry(), cloud_mode=True)


async def test_mapping_list_is_org_scoped() -> None:
    store = InMemoryImMappingStore()
    await store.create(_mapping(id="m1", org_id="org-a"))
    await store.create(_mapping(id="m2", org_id="org-b", external_chat_id="c2"))
    assert {m.id for m in await store.list_for_org("org-a")} == {"m1"}
    assert {m.id for m in await store.list_for_org("org-b")} == {"m2"}
