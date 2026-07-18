"""Live-Postgres claim-time + reply-time revalidation of an IM run's immutable channel binding.

A durable IM run pins an immutable admission binding (mapping id + version, run-as org member,
provider/bot/chat + kind, bound Agent + scope, and the safe-policy fingerprint) at admission. The
worker job body :func:`keel_worker.runs.run_interactive` re-reads the *current*
``im_channel_mappings`` row when it claims the run and again (via :func:`send_im_replies_tick`)
before delivering the reply, and fails the run closed if the mapping no longer matches every
admitted field exactly.

Against a live database this queues an IM run and then, before the worker claims it, independently:
revokes the mapping, reprovisions the same row under a new run-as, reduces the allowed-tool policy,
and changes the chat target — each terminally fails the run with no reply persisted or delivered.
The exact unchanged mapping completes and delivers, and a mapping revoked *after* completion (reply
already persisted) is denied delivery by the restart-safe sender.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import PostgresApprovalStore
from keel_core.im_routing import (
    ImChannelMapping,
    ImChatKind,
    ImInboundContext,
    ImMappingStatus,
    ImProvider,
    ImReplyPolicy,
    ImReplyStatus,
    PostgresImProvisioner,
    PostgresImReplyStore,
)
from keel_core.loop import admit
from keel_core.protocols import ProviderChunk
from keel_core.run_service import DurableRunService
from keel_core.runs import PostgresRunStore, RunStatus, RunSurface
from keel_core.secrets import KeyRing
from keel_core.state import PostgresEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.tools import UnavailableExecutionEnvironment
from keel_core.types import FinishReason
from keel_worker.runs import run_interactive, send_im_replies_tick

pytestmark = pytest.mark.integration

_ORG = "org-a"
_AGENT = "agent-1"
_RUNNER = "u1"
_OTHER = "u2"
_SCOPE = f"agent:{_ORG}/{_AGENT}"
_KEY = KeyRing({"v1": "integration-secret"}, "v1")


class _RecordingSender:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def send(self, intent: Any, text_payload: str) -> str:
        self.sent.append((intent.external_chat_id, text_payload))
        return f"pmid-{len(self.sent)}"


async def _seed(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO users(id,display_name,status) VALUES('u1','Runner','active')")
        )
        await conn.execute(
            text("INSERT INTO users(id,display_name,status) VALUES('u2','Other','active')")
        )
        await conn.execute(
            text(
                "INSERT INTO organizations(id,slug,display_name,status) VALUES(:o,:o,:o,'active')"
            ),
            {"o": _ORG},
        )
        for uid in (_RUNNER, _OTHER):
            await conn.execute(
                text(
                    "INSERT INTO memberships(id,org_id,user_id,role,status) "
                    "VALUES(:id,:o,:u,'member','active')"
                ),
                {"id": f"mem-{uid}", "o": _ORG, "u": uid},
            )
        await conn.execute(
            text(
                "INSERT INTO agents(id,org_id,kind,owner_user_id,name,status,version) "
                "VALUES(:a,:o,'team','u1','Support','active',1)"
            ),
            {"a": _AGENT, "o": _ORG},
        )


def _mapping(*, run_as: str, policy: ImReplyPolicy) -> ImChannelMapping:
    return ImChannelMapping(
        id="map-1",
        org_id=_ORG,
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        chat_kind=ImChatKind.group,
        agent_id=_AGENT,
        scope_id=_SCOPE,
        policy=policy,
        run_as_user_id=run_as,
    )


def _context_for(mapping: ImChannelMapping) -> ImInboundContext:
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


def _ctx(
    engine: AsyncEngine, provider: ScriptedProviderGateway, sender: _RecordingSender
) -> dict[str, Any]:
    return {
        "durable_scope": _SCOPE,
        "runs": PostgresRunStore(engine, _SCOPE),
        "store": PostgresEventStore(engine, _SCOPE),
        "approvals": PostgresApprovalStore(engine, _SCOPE),
        "provider": provider,
        "execution_environment": UnavailableExecutionEnvironment(),
        "identity": None,
        "engine": engine,
        "embedder": None,
        "keyring": _KEY,
        "im_senders": {ImProvider.telegram: sender},
    }


async def _admit(engine: AsyncEngine, context: ImInboundContext) -> str:
    async def _enqueue(_run_id: str) -> None:
        return None

    service = DurableRunService(
        run_store=PostgresRunStore(engine, _SCOPE),
        event_store=PostgresEventStore(engine, _SCOPE),
        approvals=PostgresApprovalStore(engine, _SCOPE),
        scope_id=_SCOPE,
        enqueue=_enqueue,
        admit_fn=admit,
    )
    result = await service.admit(
        org_id=_ORG,
        actor=_RUNNER,
        agent_id=_AGENT,
        session_id="chat-4242",
        surface=RunSurface.im.value,
        content="hello bot",
        idempotency_key="im-k1",
        admission_extra=context.to_admission_extra(),
    )
    return result.run_id


async def _provision(
    engine: AsyncEngine, *, run_as: str = _RUNNER, policy: ImReplyPolicy | None = None
) -> ImChannelMapping:
    return await PostgresImProvisioner(engine).provision(
        _mapping(run_as=run_as, policy=policy or ImReplyPolicy(reply_enabled=True))
    )


async def _reprovision(
    engine: AsyncEngine,
    mapping_id: str,
    *,
    run_as: str = _RUNNER,
    policy: ImReplyPolicy | None = None,
) -> None:
    """Revoke then re-provision the same chat in place (a run-as / policy re-bind)."""
    provisioner = PostgresImProvisioner(engine)
    await provisioner.transition(mapping_id, ImMappingStatus.revoked, org_id=_ORG, actor="admin")
    await provisioner.provision(
        _mapping(run_as=run_as, policy=policy or ImReplyPolicy(reply_enabled=True))
    )


async def _rebind_chat(engine: AsyncEngine, chat: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text(
                "UPDATE im_channel_mappings SET external_chat_id = :c, version = version + 1 "
                "WHERE id = 'map-1'"
            ),
            {"c": chat},
        )


def _provider() -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [[ProviderChunk(delta="the answer is 42", finish_reason=FinishReason.end_turn)]]
    )


async def _reply_id(engine: AsyncEngine) -> str | None:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        rid = await conn.scalar(
            text("SELECT id FROM im_reply_intents WHERE scope_id = :s"), {"s": _SCOPE}
        )
    return str(rid) if rid is not None else None


async def _assert_denied_before_claim(engine: AsyncEngine, run_id: str) -> None:
    sender = _RecordingSender()
    ctx = _ctx(engine, _provider(), sender)
    assert await run_interactive(ctx, run_id, _SCOPE) == RunStatus.failed.value
    assert await send_im_replies_tick(ctx) == 0
    assert sender.sent == []
    assert await _reply_id(engine) is None  # no reply persisted for the forbidden run


async def test_exact_unchanged_mapping_completes_and_delivers(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    await _seed(engine)
    created = await _provision(engine)
    run_id = await _admit(engine, _context_for(created))
    sender = _RecordingSender()
    ctx = _ctx(engine, _provider(), sender)
    assert await run_interactive(ctx, run_id, _SCOPE) == RunStatus.completed.value
    assert await send_im_replies_tick(ctx) == 1
    assert sender.sent == [("4242", "the answer is 42")]


async def test_revoked_mapping_before_claim_fails_closed(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    await _seed(engine)
    created = await _provision(engine)
    run_id = await _admit(engine, _context_for(created))
    await PostgresImProvisioner(engine).transition(
        created.id, ImMappingStatus.revoked, org_id=_ORG, actor="admin"
    )
    await _assert_denied_before_claim(engine, run_id)


async def test_reprovision_new_run_as_before_claim_fails_closed(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    await _seed(engine)
    created = await _provision(engine)
    run_id = await _admit(engine, _context_for(created))
    await _reprovision(engine, created.id, run_as=_OTHER)  # same row, new run-as, new version
    await _assert_denied_before_claim(engine, run_id)


async def test_reduced_tool_policy_before_claim_fails_closed(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    await _seed(engine)
    created = await _provision(
        engine, policy=ImReplyPolicy(reply_enabled=True, allow_tools=("shell",))
    )
    run_id = await _admit(engine, _context_for(created))
    await _reprovision(engine, created.id, policy=ImReplyPolicy(reply_enabled=True, allow_tools=()))
    await _assert_denied_before_claim(engine, run_id)


async def test_changed_chat_target_before_claim_fails_closed(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    await _seed(engine)
    created = await _provision(engine)
    run_id = await _admit(engine, _context_for(created))
    await _rebind_chat(engine, "9999")
    await _assert_denied_before_claim(engine, run_id)


async def test_revoke_after_completion_denies_terminal_reply(migrated_db: AsyncEngine) -> None:
    engine = migrated_db
    await _seed(engine)
    created = await _provision(engine)
    run_id = await _admit(engine, _context_for(created))
    sender = _RecordingSender()
    ctx = _ctx(engine, _provider(), sender)
    # The run completes while the mapping is still the exact active binding (reply persisted).
    assert await run_interactive(ctx, run_id, _SCOPE) == RunStatus.completed.value
    reply_id = await _reply_id(engine)
    assert reply_id is not None
    # Revoke before delivery: the restart-safe sender must fail closed and never send.
    await PostgresImProvisioner(engine).transition(
        created.id, ImMappingStatus.revoked, org_id=_ORG, actor="admin"
    )
    assert await send_im_replies_tick(ctx) == 0
    assert sender.sent == []
    stored = await PostgresImReplyStore(engine, _SCOPE).get(reply_id)
    assert stored is not None and stored.status is ImReplyStatus.failed


async def test_queued_run_survives_repeated_active_enable(migrated_db: AsyncEngine) -> None:
    """A harmless retried ``enable`` on an already-active mapping never bumps its version, so a
    queued run's admitted binding fingerprint (pinned to the mapping's version at admission)
    still matches at claim time and the run completes normally."""
    engine = migrated_db
    await _seed(engine)
    created = await _provision(engine)
    run_id = await _admit(engine, _context_for(created))

    # A retried "enable" (the mapping is already active) is a true no-op: no version bump.
    reenabled = await PostgresImProvisioner(engine).transition(
        created.id, ImMappingStatus.active, org_id=_ORG, actor="admin"
    )
    assert reenabled is not None and reenabled.version == created.version

    sender = _RecordingSender()
    ctx = _ctx(engine, _provider(), sender)
    assert await run_interactive(ctx, run_id, _SCOPE) == RunStatus.completed.value
    assert await send_im_replies_tick(ctx) == 1
    assert sender.sent == [("4242", "the answer is 42")]


async def test_terminal_reply_survives_noop_status_retry(migrated_db: AsyncEngine) -> None:
    """A retried no-op status transition after completion never invalidates the persisted,
    not-yet-delivered terminal reply (no version bump means the reply-time revalidation still
    matches the binding it was admitted/persisted under)."""
    engine = migrated_db
    await _seed(engine)
    created = await _provision(engine)
    run_id = await _admit(engine, _context_for(created))
    sender = _RecordingSender()
    ctx = _ctx(engine, _provider(), sender)
    assert await run_interactive(ctx, run_id, _SCOPE) == RunStatus.completed.value
    reply_id = await _reply_id(engine)
    assert reply_id is not None

    # A harmless retried "enable" on the still-active mapping before delivery is a no-op.
    noop = await PostgresImProvisioner(engine).transition(
        created.id, ImMappingStatus.active, org_id=_ORG, actor="admin"
    )
    assert noop is not None and noop.version == created.version

    assert await send_im_replies_tick(ctx) == 1
    assert sender.sent == [("4242", "the answer is 42")]
    stored = await PostgresImReplyStore(engine, _SCOPE).get(reply_id)
    assert stored is not None and stored.status is ImReplyStatus.sent
