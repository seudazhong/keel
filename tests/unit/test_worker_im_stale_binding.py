"""Claim-time + reply-time revalidation of an IM run's immutable channel binding (WS-M).

A durable IM run pins an *immutable admission binding* (mapping id + version, run-as org member,
provider/bot/chat + kind, bound Agent + scope, and the safe-policy fingerprint) at admission. The
worker must re-validate that binding against the **current** active mapping both when it claims the
run and again before it delivers the terminal reply, so a mapping mutated between queueing and
execution fails the stale run closed — no provider/model/tool effect, no reply.

These tests queue an IM run and then, before the worker claims it, independently: revoke the
mapping, reprovision the same row under a new run-as, reduce the allowed-tool policy, and change the
chat target — each must terminally fail the run with no reply persisted or sent. The exact,
unchanged mapping still completes and delivers. A separate case revokes the mapping *after* the run
completes (reply already persisted) and proves the restart-safe sender denies delivery.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from keel_core.approvals import InMemoryApprovalStore
from keel_core.identity import IdentityService, InMemoryIdentityStore, LoggingAuditSink
from keel_core.identity.models import (
    AgentAccessLevel,
    AgentAccessPrincipalType,
    AgentKind,
    MembershipRole,
)
from keel_core.im_routing import (
    ImChannelMapping,
    ImChatKind,
    ImInboundContext,
    ImMappingStatus,
    ImProvider,
    ImReplyPolicy,
    ImReplyStatus,
    InMemoryImMappingStore,
    InMemoryImReplyDispatchIndex,
    InMemoryImReplyStore,
)
from keel_core.loop import admit
from keel_core.protocols import ProviderChunk
from keel_core.run_service import DurableRunService
from keel_core.runs import InMemoryRunStore, RunStatus, RunSurface
from keel_core.scoping import derive_agent_scope
from keel_core.secrets import KeyRing
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.tools import UnavailableExecutionEnvironment
from keel_core.types import FinishReason
from keel_worker.runs import run_interactive, send_im_replies_tick


class _RecordingSender:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def send(self, intent: Any, text_payload: str) -> str:
        self.sent.append((intent.external_chat_id, text_payload))
        return f"pmid-{len(self.sent)}"


def _keyring() -> KeyRing:
    return KeyRing({"v1": "unit-test-secret"}, "v1")


async def _identity_with_team_agent_and_members() -> tuple[IdentityService, str, str, str, str]:
    """An org with a team Agent, its run-as member, and a *second* member (an alternate run-as)."""
    svc = IdentityService(InMemoryIdentityStore(), audit=LoggingAuditSink())
    owner = await svc.ensure_local_user()
    org = await svc.create_org(owner.id, slug="acme", display_name="Acme")
    agent = await svc.create_agent(
        org.org_id, owner.id, kind=AgentKind.team, name="Support", persona="Be helpful."
    )
    runner = await svc.store.create_user(display_name="Runner", email=None)
    await svc.add_member(org.org_id, owner.id, runner.id, MembershipRole.member)
    other = await svc.store.create_user(display_name="Other", email=None)
    await svc.add_member(org.org_id, owner.id, other.id, MembershipRole.member)
    # R1B: both run-as candidates need an active Agent Access edge on the team Agent.
    for user_id in (runner.id, other.id):
        await svc.grant_agent_access(
            org.org_id,
            owner.id,
            agent_id=agent.id,
            principal_type=AgentAccessPrincipalType.user,
            principal_id=user_id,
            level=AgentAccessLevel.use,
        )
    return svc, org.org_id, runner.id, other.id, agent.id


def _mapping(
    *, org_id: str, agent_id: str, scope_id: str, run_as: str, policy: ImReplyPolicy
) -> ImChannelMapping:
    return ImChannelMapping(
        id="map-1",
        org_id=org_id,
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        chat_kind=ImChatKind.group,
        agent_id=agent_id,
        scope_id=scope_id,
        policy=policy,
        status=ImMappingStatus.active,
        version=1,
        created_by="admin-machine",
        run_as_user_id=run_as,
    )


def _context_for(mapping: ImChannelMapping) -> ImInboundContext:
    """The admission binding the ingress records for ``mapping`` (mirrors durable._context)."""
    return ImInboundContext(
        provider=mapping.provider,
        external_bot_id=mapping.external_bot_id,
        external_chat_id=mapping.external_chat_id,
        external_message_id="msg-7",
        chat_kind=mapping.chat_kind,
        mapping_id=mapping.id,
        policy=mapping.policy,
        mapping_version=mapping.version,
        run_as_user_id=mapping.run_as_user_id,
        agent_id=mapping.agent_id,
        scope_id=mapping.scope_id,
    )


async def _admit_im(
    runs: InMemoryRunStore,
    events: InMemoryEventStore,
    *,
    scope_id: str,
    org_id: str,
    actor: str,
    agent_id: str,
    context: ImInboundContext,
) -> str:
    async def _enqueue(run_id: str) -> None:
        return None

    service = DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=InMemoryApprovalStore(),
        scope_id=scope_id,
        enqueue=_enqueue,
        admit_fn=admit,
    )
    result = await service.admit(
        org_id=org_id,
        actor=actor,
        agent_id=agent_id,
        session_id="chat-4242",
        surface=RunSurface.im.value,
        content="hello bot",
        idempotency_key="im-k1",
        admission_extra=context.to_admission_extra(),
    )
    return result.run_id


def _ctx(
    runs: InMemoryRunStore,
    events: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    identity: IdentityService,
    provider: ScriptedProviderGateway,
    *,
    scope_id: str,
    mappings: InMemoryImMappingStore,
    replies: InMemoryImReplyStore,
    dispatch: InMemoryImReplyDispatchIndex,
    sender: _RecordingSender,
) -> dict[str, Any]:
    return {
        "durable_scope": scope_id,
        "runs": runs,
        "store": events,
        "approvals": approvals,
        "provider": provider,
        "execution_environment": UnavailableExecutionEnvironment(),
        "identity": identity,
        "engine": None,
        "embedder": None,
        "im_mappings": mappings,
        "im_replies": replies,
        "im_reply_dispatch": dispatch,
        "keyring": _keyring(),
        "im_senders": {ImProvider.telegram: sender},
    }


async def _setup(
    policy: ImReplyPolicy | None = None,
) -> tuple[
    dict[str, Any],
    str,
    InMemoryImMappingStore,
    ImChannelMapping,
    _RecordingSender,
    InMemoryImReplyDispatchIndex,
    InMemoryImReplyStore,
    str,
]:
    svc, org_id, runner_id, _other_id, agent_id = await _identity_with_team_agent_and_members()
    scope_id = derive_agent_scope(org_id, agent_id)
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    mappings = InMemoryImMappingStore()
    replies, dispatch, sender = (
        InMemoryImReplyStore(),
        InMemoryImReplyDispatchIndex(),
        _RecordingSender(),
    )
    mapping = _mapping(
        org_id=org_id,
        agent_id=agent_id,
        scope_id=scope_id,
        run_as=runner_id,
        policy=policy or ImReplyPolicy(reply_enabled=True),
    )
    agent = await svc.get_agent_for_admission(org_id, agent_id)
    assert agent is not None
    await svc.grant_agent_access(
        org_id,
        agent.owner_user_id,
        agent_id=agent_id,
        principal_type=AgentAccessPrincipalType.channel,
        principal_id=mapping.route_key,
        level=AgentAccessLevel.use,
    )
    await mappings.create(mapping)
    run_id = await _admit_im(
        runs,
        events,
        scope_id=scope_id,
        org_id=org_id,
        actor=runner_id,
        agent_id=agent_id,
        context=_context_for(mapping),
    )
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="the answer is 42", finish_reason=FinishReason.end_turn)]]
    )
    ctx = _ctx(
        runs,
        events,
        approvals,
        svc,
        provider,
        scope_id=scope_id,
        mappings=mappings,
        replies=replies,
        dispatch=dispatch,
        sender=sender,
    )
    return ctx, scope_id, mappings, mapping, sender, dispatch, replies, run_id


async def _assert_denied(ctx: dict[str, Any], run_id: str, scope_id: str) -> None:
    sender: _RecordingSender = ctx["im_senders"][ImProvider.telegram]
    dispatch: InMemoryImReplyDispatchIndex = ctx["im_reply_dispatch"]
    assert await run_interactive(ctx, run_id, scope_id) == RunStatus.failed.value
    # No provider/model/tool effect leaked into a reply, and nothing is queued to send.
    assert await dispatch.active_scopes() == set()
    assert await send_im_replies_tick(ctx) == 0
    assert sender.sent == []


async def test_exact_unchanged_mapping_completes_and_replies() -> None:
    ctx, scope_id, _mappings, _mapping, sender, _dispatch, _replies, run_id = await _setup()
    assert await run_interactive(ctx, run_id, scope_id) == RunStatus.completed.value
    assert await send_im_replies_tick(ctx) == 1
    assert sender.sent == [("4242", "the answer is 42")]


async def test_revoked_mapping_before_claim_fails_closed() -> None:
    ctx, scope_id, mappings, mapping, _sender, _dispatch, _replies, run_id = await _setup()
    await mappings.set_status(mapping.id, ImMappingStatus.revoked, actor="admin")
    await _assert_denied(ctx, run_id, scope_id)


async def test_revoked_channel_access_before_claim_fails_closed() -> None:
    ctx, scope_id, _mappings, mapping, _sender, _dispatch, _replies, run_id = await _setup()
    svc: IdentityService = ctx["identity"]
    agent = await svc.get_agent_for_admission(mapping.org_id, mapping.agent_id)
    assert agent is not None
    await svc.revoke_agent_access(
        mapping.org_id,
        agent.owner_user_id,
        agent_id=mapping.agent_id,
        principal_type=AgentAccessPrincipalType.channel,
        principal_id=mapping.route_key,
    )
    await _assert_denied(ctx, run_id, scope_id)


async def test_reprovision_new_run_as_before_claim_fails_closed() -> None:
    ctx, scope_id, mappings, mapping, _sender, _dispatch, _replies, run_id = await _setup()
    # Reprovision the *same* row under a new run-as member and an advanced version.
    rebound = replace(mapping, run_as_user_id="other-member", version=mapping.version + 1)
    await mappings.create(rebound)
    await _assert_denied(ctx, run_id, scope_id)


async def test_reduced_tool_policy_before_claim_fails_closed() -> None:
    # Admitted with an explicit allow-list; the mapping then tightens it (a security change).
    ctx, scope_id, mappings, mapping, _sender, _dispatch, _replies, run_id = await _setup(
        policy=ImReplyPolicy(reply_enabled=True, allow_tools=("shell",))
    )
    tightened = replace(
        mapping,
        policy=ImReplyPolicy(reply_enabled=True, allow_tools=()),
        version=mapping.version + 1,
    )
    await mappings.create(tightened)
    await _assert_denied(ctx, run_id, scope_id)


async def test_changed_chat_target_before_claim_fails_closed() -> None:
    ctx, scope_id, mappings, mapping, _sender, _dispatch, _replies, run_id = await _setup()
    rebound = replace(mapping, external_chat_id="9999", version=mapping.version + 1)
    await mappings.create(rebound)
    await _assert_denied(ctx, run_id, scope_id)


async def test_revoke_after_completion_denies_the_terminal_reply() -> None:
    ctx, scope_id, mappings, mapping, sender, dispatch, replies, run_id = await _setup()
    # The run completes while the mapping is still the exact active binding: reply is persisted.
    assert await run_interactive(ctx, run_id, scope_id) == RunStatus.completed.value
    assert await dispatch.active_scopes() == {scope_id}
    # The mapping is revoked before delivery: the restart-safe sender must fail closed (no send),
    # terminally deny the durable reply, and retire its dispatch pointer (never resurrected).
    await mappings.set_status(mapping.id, ImMappingStatus.revoked, actor="admin")
    assert await send_im_replies_tick(ctx) == 0
    assert sender.sent == []
    assert await dispatch.active_scopes() == set()
    stored = next(iter(replies._by_id.values()))  # noqa: SLF001 - test introspection
    assert stored.status is ImReplyStatus.failed
