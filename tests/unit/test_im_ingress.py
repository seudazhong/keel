"""Durable IM ingress: pre-tenant route lookup, fail-closed resolve, durable admission (WS-E/J).

Unit coverage (no Postgres/Redis) for :mod:`keel_server.gateway.durable`: an authenticated
inbound message is resolved through the opaque global route index and admitted as a durable
``surface="im"`` run carrying the IM provider/chat context; an unknown or revoked mapping is
dropped fail-closed; and the OneBot/Telegram parsers wake-gate + normalize provider payloads.
"""

from __future__ import annotations

from keel_core.agent_config_snapshot import ResourceGrantSnapshot
from keel_core.approvals import InMemoryApprovalStore
from keel_core.identity import (
    AgentKind,
    Capability,
    IdentityService,
    InMemoryIdentityStore,
    LoggingAuditSink,
)
from keel_core.im_routing import (
    ImChannelMapping,
    ImChatKind,
    ImInboundContext,
    ImMappingStatus,
    ImProvider,
    ImReplyPolicy,
    InMemoryImMappingStore,
    InMemoryImRouteIndex,
    im_context_in_log,
)
from keel_core.loop import admit
from keel_core.run_service import DurableRunService
from keel_core.runs import InMemoryRunStore, RunStatus, RunSurface
from keel_core.state import InMemoryEventStore
from keel_server.gateway.durable import (
    DurableImIngress,
    ImInbound,
    parse_onebot_inbound,
    parse_telegram_inbound,
)


class _Harness:
    """A durable-run substrate that records the admitted run per scope for assertions."""

    def __init__(self, *, identity: IdentityService | None = None) -> None:
        self.route_index = InMemoryImRouteIndex()
        self.mappings = InMemoryImMappingStore()
        self.runs: dict[str, InMemoryRunStore] = {}
        self.events: dict[str, InMemoryEventStore] = {}
        self.enqueued: list[tuple[str, str]] = []
        self.identity = identity

    def run_service(self, scope_id: str) -> DurableRunService:
        runs = self.runs.setdefault(scope_id, InMemoryRunStore())
        events = self.events.setdefault(scope_id, InMemoryEventStore())

        async def _enqueue(run_id: str) -> None:
            self.enqueued.append((run_id, scope_id))

        return DurableRunService(
            run_store=runs,
            event_store=events,
            approvals=InMemoryApprovalStore(),
            scope_id=scope_id,
            enqueue=_enqueue,
            admit_fn=admit,
        )

    def ingress(self, *, cloud_mode: bool = True) -> DurableImIngress:
        return DurableImIngress(
            route_index=self.route_index,
            mapping_store_factory=lambda _org: self.mappings,
            run_service_factory=self.run_service,
            cloud_mode=cloud_mode,
            default_model="gpt-4o-mini",
            identity=self.identity,
        )


async def _publish_mapping(
    h: _Harness,
    *,
    status: ImMappingStatus = ImMappingStatus.active,
    reply_enabled: bool = True,
    policy: ImReplyPolicy | None = None,
) -> ImChannelMapping:
    mapping = ImChannelMapping(
        id="map-1",
        org_id="org-a",
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        chat_kind=ImChatKind.group,
        agent_id="agent-1",
        scope_id="agent:org-a/agent-1",
        policy=policy or ImReplyPolicy(reply_enabled=reply_enabled),
        status=status,
        created_by="user-admin",
        run_as_user_id="user-member",
    )
    await h.mappings.create(mapping)
    await h.route_index.put(mapping.route_entry())
    return mapping


def _inbound(text: str = "hello bot") -> ImInbound:
    return ImInbound(
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        external_message_id="msg-7",
        chat_kind=ImChatKind.group,
        text=text,
    )


async def test_ingress_admits_durable_im_run_with_context() -> None:
    h = _Harness()
    await _publish_mapping(h)
    run_id = await h.ingress().admit(_inbound())
    assert run_id is not None
    # The run was admitted into the mapped Agent's scope as an IM surface, under the mapping's
    # run-as org member (run_as_user_id), NOT the platform admin (created_by), and enqueued.
    runs = h.runs["agent:org-a/agent-1"]
    record = await runs.get(run_id)
    assert record is not None
    assert record.surface == RunSurface.im.value
    assert record.org_id == "org-a" and record.agent_id == "agent-1"
    assert record.actor == "user-member"
    assert record.status in {RunStatus.queued, RunStatus.admitted}
    assert h.enqueued == [(run_id, "agent:org-a/agent-1")]
    # The durable IM mapping admission binding rides on the admission event: provider/chat target,
    # policy, and the immutable binding identity (mapping id + version, run-as, Agent, scope).
    ctx = await im_context_in_log(h.events["agent:org-a/agent-1"], record.session_id, run_id)
    assert ctx == ImInboundContext(
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        external_message_id="msg-7",
        chat_kind=ImChatKind.group,
        mapping_id="map-1",
        policy=ImReplyPolicy(reply_enabled=True),
        mapping_version=1,
        run_as_user_id="user-member",
        agent_id="agent-1",
        scope_id="agent:org-a/agent-1",
    )


async def test_ingress_snapshots_the_mapped_agent_at_admission() -> None:
    """IM admission snapshots the mapped Agent's *current* name/persona/version (R1B): the
    worker never has to defer that (mutable) lookup to execution time."""
    identity = IdentityService(InMemoryIdentityStore(), audit=LoggingAuditSink())
    user = await identity.ensure_local_user()
    org = await identity.create_org(user.id, slug="org-a", display_name="Org A")
    agent = await identity.create_agent(
        org.org_id, user.id, kind=AgentKind.team, name="Field Agent", persona="Be terse."
    )
    await identity.grant_resource(
        org.org_id,
        user.id,
        agent_id=agent.id,
        resource_type="knowledge_base",
        resource_id="kb-1",
        capability=Capability.read,
    )
    h = _Harness(identity=identity)
    mapping = ImChannelMapping(
        id="map-1",
        org_id=org.org_id,
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        chat_kind=ImChatKind.group,
        agent_id=agent.id,
        scope_id=f"agent:{org.org_id}/{agent.id}",
        policy=ImReplyPolicy(reply_enabled=True),
        status=ImMappingStatus.active,
        created_by=user.id,
        run_as_user_id=user.id,
    )
    await h.mappings.create(mapping)
    await h.route_index.put(mapping.route_entry())
    run_id = await h.ingress().admit(_inbound())
    assert run_id is not None
    runs = h.runs[f"agent:{org.org_id}/{agent.id}"]
    record = await runs.get(run_id)
    assert record is not None
    snapshot = record.snapshot
    assert snapshot is not None
    assert snapshot.agent_id == agent.id
    assert snapshot.agent_version == agent.version
    assert snapshot.agent_name == "Field Agent"
    assert snapshot.persona == "Be terse."
    assert snapshot.permission_profile == "im_safe"
    assert snapshot.resource_grants == (ResourceGrantSnapshot("knowledge_base", "kb-1", "read"),)


async def test_ingress_without_identity_falls_back_to_an_id_only_snapshot() -> None:
    """No identity service wired (in-memory preview): the snapshot still captures at least the
    admitted agent id/model — never crashes admission."""
    h = _Harness(identity=None)
    await _publish_mapping(h)
    run_id = await h.ingress().admit(_inbound())
    assert run_id is not None
    runs = h.runs["agent:org-a/agent-1"]
    record = await runs.get(run_id)
    assert record is not None
    snapshot = record.snapshot
    assert snapshot is not None
    assert snapshot.agent_id == "agent-1"
    assert snapshot.model == "gpt-4o-mini"
    assert snapshot.resource_grants == ()


async def test_ingress_snapshot_uses_resolved_readonly_extra_tools() -> None:
    h = _Harness(identity=None)
    await _publish_mapping(h)

    async def tool_names(_scope_id: str) -> tuple[str, ...]:
        return ("read", "session_search", "kb_search")

    ingress = DurableImIngress(
        route_index=h.route_index,
        mapping_store_factory=lambda _org: h.mappings,
        run_service_factory=h.run_service,
        cloud_mode=True,
        default_model="gpt-4o-mini",
        snapshot_tool_names=tool_names,
    )
    run_id = await ingress.admit(_inbound())
    assert run_id is not None
    record = await h.runs["agent:org-a/agent-1"].get(run_id)
    assert record is not None and record.snapshot is not None
    assert record.snapshot.tools == ("kb_search", "read", "session_search")


async def test_ingress_is_idempotent_per_message() -> None:
    h = _Harness()
    await _publish_mapping(h)
    first = await h.ingress().admit(_inbound())
    second = await h.ingress().admit(_inbound())
    # A duplicate webhook for the same message dedups on the admission idempotency key.
    assert first == second
    assert h.enqueued == [(first, "agent:org-a/agent-1")]


async def test_ingress_unknown_mapping_fails_closed() -> None:
    h = _Harness()  # no mapping published
    assert await h.ingress(cloud_mode=True).admit(_inbound()) is None
    assert h.enqueued == []


async def test_ingress_revoked_mapping_fails_closed() -> None:
    h = _Harness()
    await _publish_mapping(h, status=ImMappingStatus.revoked)
    assert await h.ingress().admit(_inbound()) is None
    assert h.enqueued == []


async def test_ingress_different_chat_is_unmapped() -> None:
    h = _Harness()
    await _publish_mapping(h)
    other = ImInbound(
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="9999",  # a different chat has no mapping
        external_message_id="m",
        chat_kind=ImChatKind.group,
        text="hi",
    )
    assert await h.ingress().admit(other) is None


async def test_ingress_approval_command_resolves_when_enabled() -> None:
    h = _Harness()
    await _publish_mapping(h, policy=ImReplyPolicy(reply_enabled=True, approvals_enabled=True))
    cmd = ImInbound(
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        external_message_id="m-approve",
        chat_kind=ImChatKind.group,
        text="approve appr-1",
    )
    # An approval command routes to resolve_approval, not a new run admission.
    assert await h.ingress().admit(cmd) is None
    assert h.enqueued == []


async def test_ingress_approval_command_is_a_normal_message_when_disabled() -> None:
    h = _Harness()
    # approvals_enabled defaults False → "approve X" is just a chat message that admits a run.
    await _publish_mapping(h, policy=ImReplyPolicy(reply_enabled=True))
    cmd = ImInbound(
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        external_message_id="m-x",
        chat_kind=ImChatKind.group,
        text="approve appr-1",
    )
    run_id = await h.ingress().admit(cmd)
    assert run_id is not None
    assert h.enqueued == [(run_id, "agent:org-a/agent-1")]


# --------------------------------------------------------------------------- parsers


def test_parse_approval_command() -> None:
    from keel_core.im_routing import parse_approval_command

    approve = parse_approval_command("approve abc123")
    assert approve is not None and approve.approved is True and approve.approval_id == "abc123"
    reject = parse_approval_command("reject abc123")
    assert reject is not None and reject.approved is False
    yes = parse_approval_command("yes id-9")
    assert yes is not None and yes.approval_id == "id-9"
    # Ordinary chat is never a command.
    assert parse_approval_command("please approve my request now") is None
    assert parse_approval_command("approve") is None
    assert parse_approval_command("hello") is None


# --------------------------------------------------------------------------- parsers


def test_parse_onebot_group_requires_wake() -> None:
    payload = {
        "post_type": "message",
        "message_type": "group",
        "group_id": 42,
        "user_id": 7,
        "self_id": 100,
        "message_id": 555,
        "raw_message": "hello there",  # no wake token in a group
    }
    assert parse_onebot_inbound(payload, self_id=100, prefixes=("/keel",)) is None
    payload["raw_message"] = "/keel status"
    inbound = parse_onebot_inbound(payload, self_id=100, prefixes=("/keel",))
    assert inbound is not None
    assert inbound.provider is ImProvider.onebot
    assert inbound.external_bot_id == "100"
    assert inbound.external_chat_id == "42"
    assert inbound.external_message_id == "555"
    assert inbound.chat_kind is ImChatKind.group
    assert inbound.text == "status"


def test_parse_onebot_private_always_wakes() -> None:
    payload = {
        "post_type": "message",
        "message_type": "private",
        "user_id": 7,
        "self_id": 100,
        "message_id": 9,
        "raw_message": "hi",
    }
    inbound = parse_onebot_inbound(payload, self_id=100, prefixes=("/keel",))
    assert inbound is not None
    assert inbound.chat_kind is ImChatKind.personal
    assert inbound.external_chat_id == "7"


def test_parse_telegram_private_and_group() -> None:
    private = {"message": {"message_id": 11, "text": "hi", "chat": {"id": 5, "type": "private"}}}
    inbound = parse_telegram_inbound(private, bot_id="mybot", bot_username="mybot", prefixes=())
    assert inbound is not None
    assert inbound.chat_kind is ImChatKind.personal
    assert inbound.external_chat_id == "5"
    assert inbound.external_bot_id == "mybot"
    assert inbound.external_message_id == "11"

    group = {
        "message": {"message_id": 12, "text": "@mybot help", "chat": {"id": -100, "type": "group"}}
    }
    inbound2 = parse_telegram_inbound(group, bot_id="mybot", bot_username="mybot", prefixes=())
    assert inbound2 is not None
    assert inbound2.chat_kind is ImChatKind.group
    assert inbound2.text == "help"
    # A group message with no mention/prefix is ignored.
    quiet = {
        "message": {"message_id": 13, "text": "chatter", "chat": {"id": -100, "type": "group"}}
    }
    assert parse_telegram_inbound(quiet, bot_id="mybot", bot_username="mybot", prefixes=()) is None
