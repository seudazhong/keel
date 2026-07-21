"""Worker durable IM path: safe run, encrypted durable reply, restart-safe sender (WS-E/J).

Exercises :func:`keel_worker.runs.run_interactive` for an untrusted IM run end-to-end with the
in-memory durable doubles + a scripted provider (no Postgres/Redis): an IM admission is claimed,
driven on the read-only safe Agent, and its terminal reply is persisted to the encrypted reply
outbox; the restart-safe :func:`send_im_replies_tick` then delivers it exactly once through a
stubbed provider sender. Also covers a malicious tool request being refused (defense in depth)
and reply-delivery idempotency across a re-run of the sender tick.
"""

from __future__ import annotations

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
    ImChatKind,
    ImInboundContext,
    ImProvider,
    ImReplyIntent,
    ImReplyPolicy,
    ImReplyStatus,
    InMemoryImReplyDispatchIndex,
    InMemoryImReplyStore,
    decrypt_reply_payload,
)
from keel_core.loop import admit
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.run_service import DurableRunService
from keel_core.runs import InMemoryRunStore, RunStatus, RunSurface
from keel_core.scoping import derive_agent_scope
from keel_core.secrets import KeyRing
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.tools import UnavailableExecutionEnvironment
from keel_core.types import FinishReason
from keel_worker.im_replies import OneBotReplySender
from keel_worker.runs import run_interactive, send_im_replies_tick


class _RecordingSender:
    """A stub provider reply sender that records the delivered text (ReplySender protocol)."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def send(self, intent: ImReplyIntent, text_payload: str) -> str:
        self.sent.append((intent.external_chat_id, text_payload))
        return f"pmid-{len(self.sent)}"


def _keyring() -> KeyRing:
    return KeyRing({"v1": "unit-test-secret"}, "v1")


async def _identity_with_agent() -> tuple[IdentityService, str, str, str]:
    svc = IdentityService(InMemoryIdentityStore(), audit=LoggingAuditSink())
    user = await svc.ensure_local_user()
    org = await svc.create_org(user.id, slug="acme", display_name="Acme")
    agent = await svc.create_agent(
        org.org_id, user.id, kind=AgentKind.personal, name="Support", persona="Be helpful."
    )
    return svc, org.org_id, user.id, agent.id


def _im_context(policy: ImReplyPolicy | None = None) -> ImInboundContext:
    return ImInboundContext(
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        external_message_id="msg-7",
        chat_kind=ImChatKind.group,
        mapping_id="map-1",
        policy=policy or ImReplyPolicy(reply_enabled=True),
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
    content: str = "hello bot",
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
        content=content,
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
        "im_replies": replies,
        "im_reply_dispatch": dispatch,
        "keyring": _keyring(),
        "im_senders": {ImProvider.telegram: sender},
    }


async def test_im_run_persists_encrypted_reply_and_sends_once() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent()
    scope_id = derive_agent_scope(org_id, agent_id)
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    replies, dispatch, sender = (
        InMemoryImReplyStore(),
        InMemoryImReplyDispatchIndex(),
        _RecordingSender(),
    )
    run_id = await _admit_im(
        runs,
        events,
        scope_id=scope_id,
        org_id=org_id,
        actor=user_id,
        agent_id=agent_id,
        context=_im_context(),
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
        replies=replies,
        dispatch=dispatch,
        sender=sender,
    )
    status = await run_interactive(ctx, run_id, scope_id)
    assert status == RunStatus.completed.value

    # A durable reply intent was persisted with an ENCRYPTED payload (no plaintext at rest).
    stored = [r for r in replies._by_id.values()]  # noqa: SLF001 - test introspection
    assert len(stored) == 1
    intent = stored[0]
    assert intent.status is ImReplyStatus.pending
    assert "the answer is 42" not in intent.ciphertext
    assert decrypt_reply_payload(_keyring(), intent) == "the answer is 42"
    assert await dispatch.active_scopes() == {scope_id}

    # The restart-safe sender delivers exactly once, then retires the dispatch pointer.
    delivered = await send_im_replies_tick(ctx)
    assert delivered == 1
    assert sender.sent == [("4242", "the answer is 42")]
    after = await replies.get(intent.id)
    assert after is not None and after.status is ImReplyStatus.sent
    assert await dispatch.active_scopes() == set()

    # A second sender tick is a no-op (no duplicate user-visible reply).
    assert await send_im_replies_tick(ctx) == 0
    assert len(sender.sent) == 1


async def test_im_run_refuses_write_tool_request() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent()
    scope_id = derive_agent_scope(org_id, agent_id)
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    replies, dispatch, sender = (
        InMemoryImReplyStore(),
        InMemoryImReplyDispatchIndex(),
        _RecordingSender(),
    )
    run_id = await _admit_im(
        runs,
        events,
        scope_id=scope_id,
        org_id=org_id,
        actor=user_id,
        agent_id=agent_id,
        context=_im_context(),
        content="please rm -rf /",
    )
    # The model (driven by tainted external text) tries a write; the safe policy must refuse it
    # and the run still completes without ever executing a mutating tool.
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c1", name="write", arguments={"path": "x", "content": "y"}
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="I can't do that.", finish_reason=FinishReason.end_turn)],
        ]
    )
    ctx = _ctx(
        runs,
        events,
        approvals,
        svc,
        provider,
        scope_id=scope_id,
        replies=replies,
        dispatch=dispatch,
        sender=sender,
    )
    status = await run_interactive(ctx, run_id, scope_id)
    assert status == RunStatus.completed.value
    # The reply is the safe refusal text — the write never ran.
    await send_im_replies_tick(ctx)
    assert sender.sent == [("4242", "I can't do that.")]


async def test_im_reply_disabled_policy_persists_no_reply() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent()
    scope_id = derive_agent_scope(org_id, agent_id)
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    replies, dispatch, sender = (
        InMemoryImReplyStore(),
        InMemoryImReplyDispatchIndex(),
        _RecordingSender(),
    )
    run_id = await _admit_im(
        runs,
        events,
        scope_id=scope_id,
        org_id=org_id,
        actor=user_id,
        agent_id=agent_id,
        context=_im_context(ImReplyPolicy(reply_enabled=False)),
    )
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="silent", finish_reason=FinishReason.end_turn)]]
    )
    ctx = _ctx(
        runs,
        events,
        approvals,
        svc,
        provider,
        scope_id=scope_id,
        replies=replies,
        dispatch=dispatch,
        sender=sender,
    )
    assert await run_interactive(ctx, run_id, scope_id) == RunStatus.completed.value
    assert await dispatch.active_scopes() == set()
    assert await send_im_replies_tick(ctx) == 0


async def _identity_with_team_agent_and_member() -> tuple[IdentityService, str, str, str, str]:
    """An org with a team Agent + a separate active *member* (the run-as identity)."""
    svc = IdentityService(InMemoryIdentityStore(), audit=LoggingAuditSink())
    owner = await svc.ensure_local_user()
    org = await svc.create_org(owner.id, slug="acme", display_name="Acme")
    agent = await svc.create_agent(
        org.org_id, owner.id, kind=AgentKind.team, name="Support", persona="Be helpful."
    )
    runner = await svc.store.create_user(display_name="Runner", email=None)
    await svc.add_member(org.org_id, owner.id, runner.id, MembershipRole.member)
    # R1B: the run-as member additionally needs an active Agent Access edge on the team Agent.
    await svc.grant_agent_access(
        org.org_id,
        owner.id,
        agent_id=agent.id,
        principal_type=AgentAccessPrincipalType.user,
        principal_id=runner.id,
        level=AgentAccessLevel.use,
    )
    return svc, org.org_id, owner.id, runner.id, agent.id


async def test_im_run_as_active_member_executes_successfully() -> None:
    svc, org_id, _owner_id, runner_id, agent_id = await _identity_with_team_agent_and_member()
    scope_id = derive_agent_scope(org_id, agent_id)
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    replies, dispatch, sender = (
        InMemoryImReplyStore(),
        InMemoryImReplyDispatchIndex(),
        _RecordingSender(),
    )
    # The run is admitted as the run-as MEMBER (never the platform admin) and completes.
    run_id = await _admit_im(
        runs,
        events,
        scope_id=scope_id,
        org_id=org_id,
        actor=runner_id,
        agent_id=agent_id,
        context=_im_context(),
    )
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="hi there", finish_reason=FinishReason.end_turn)]]
    )
    ctx = _ctx(
        runs,
        events,
        approvals,
        svc,
        provider,
        scope_id=scope_id,
        replies=replies,
        dispatch=dispatch,
        sender=sender,
    )
    assert await run_interactive(ctx, run_id, scope_id) == RunStatus.completed.value


async def test_im_run_as_revoked_member_fails_closed() -> None:
    svc, org_id, owner_id, runner_id, agent_id = await _identity_with_team_agent_and_member()
    scope_id = derive_agent_scope(org_id, agent_id)
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    replies, dispatch, sender = (
        InMemoryImReplyStore(),
        InMemoryImReplyDispatchIndex(),
        _RecordingSender(),
    )
    run_id = await _admit_im(
        runs,
        events,
        scope_id=scope_id,
        org_id=org_id,
        actor=runner_id,
        agent_id=agent_id,
        context=_im_context(),
    )
    # The run-as member is removed from the org between admit and claim: the worker's claim-time
    # re-authorization fails the run closed (never executes as the ex-member / platform admin).
    await svc.remove_member(org_id, owner_id, runner_id)
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="should not run", finish_reason=FinishReason.end_turn)]]
    )
    ctx = _ctx(
        runs,
        events,
        approvals,
        svc,
        provider,
        scope_id=scope_id,
        replies=replies,
        dispatch=dispatch,
        sender=sender,
    )
    assert await run_interactive(ctx, run_id, scope_id) == RunStatus.failed.value
    # No reply was ever persisted or sent for the forbidden run.
    assert await dispatch.active_scopes() == set()
    assert await send_im_replies_tick(ctx) == 0


def test_onebot_reply_sender_authorization_carries_token_not_a_mask() -> None:
    """No-secret guard: the OneBot reply header is a real bearer of the configured access token.

    Guards against a regression where the ``Authorization`` header would carry a literal mask
    instead of the token; the header must be the bearer scheme followed by the exact token, and an
    unconfigured (empty) token sends no auth header at all.
    """
    sender = OneBotReplySender("http://onebot.local/", "tok-abc123")
    header = sender._headers["Authorization"]  # noqa: SLF001 - test introspection
    assert header == "Bearer tok-abc123"
    assert header.startswith("Bearer ") and header.endswith("tok-abc123")
    assert "******" not in header
    # No configured token -> no Authorization header (fail-open auth is never sent).
    assert OneBotReplySender("http://onebot.local", "")._headers == {}  # noqa: SLF001
