"""Worker-owned interactive run routing: Agent-profile parity + claim-time visibility.

Exercises :func:`keel_worker.runs.run_interactive` end-to-end with the in-memory durable
doubles + a scripted provider (no Postgres/Redis): a durable admission is claimed and driven
to completion as the *persisted* selected Agent, and an Agent revoked between admit and claim
fails the run closed. (Postgres concurrency/isolation is proven in the integration suite.)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from keel_core.approvals import InMemoryApprovalStore
from keel_core.identity import IdentityService, InMemoryIdentityStore, LoggingAuditSink
from keel_core.identity.models import AgentKind
from keel_core.interactive import LOCAL_PREVIEW_ORG_ID
from keel_core.loop import admit
from keel_core.protocols import ProviderChunk
from keel_core.run_service import DurableRunService
from keel_core.runs import InMemoryRunStore, RunRecord, RunStatus, RunSurface
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.tools import UnavailableExecutionEnvironment
from keel_core.types import FinishReason
from keel_worker.runs import _resolve_agent_profile, _visibility_check, run_interactive

_SCOPE = "web:local"


async def _identity_with_agent(
    *, name: str = "Helper", persona: str = "You are a helper."
) -> tuple[IdentityService, str, str, str]:
    """A durable identity with one org + owner + personal Agent. Returns (svc, org, user, agent)."""
    svc = IdentityService(InMemoryIdentityStore(), audit=LoggingAuditSink())
    user = await svc.ensure_local_user()
    org = await svc.create_org(user.id, slug="acme", display_name="Acme")
    agent = await svc.create_agent(
        org.org_id, user.id, kind=AgentKind.personal, name=name, persona=persona
    )
    return svc, org.org_id, user.id, agent.id


async def _admit(
    runs: InMemoryRunStore,
    events: InMemoryEventStore,
    *,
    org_id: str,
    actor: str,
    agent_id: str,
) -> str:
    enqueued: list[str] = []

    async def _enqueue(run_id: str) -> None:
        enqueued.append(run_id)

    service = DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=InMemoryApprovalStore(),
        scope_id=_SCOPE,
        enqueue=_enqueue,
        admit_fn=admit,
    )
    result = await service.admit(
        org_id=org_id,
        actor=actor,
        agent_id=agent_id,
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="hi",
        idempotency_key="k1",
    )
    return result.run_id


def _ctx(
    runs: InMemoryRunStore,
    events: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    identity: IdentityService | None,
    provider: ScriptedProviderGateway,
) -> dict[str, Any]:
    return {
        "durable_scope": _SCOPE,
        "runs": runs,
        "store": events,
        "approvals": approvals,
        "provider": provider,
        "execution_environment": UnavailableExecutionEnvironment(),
        "identity": identity,
        "engine": None,
        "embedder": None,
    }


def _record(org_id: str, actor: str, agent_id: str) -> RunRecord:
    now = datetime.now(UTC)
    return RunRecord(
        id="r",
        scope_id=_SCOPE,
        org_id=org_id,
        actor=actor,
        agent_id=agent_id,
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key="k",
        status=RunStatus.queued,
        attempt=0,
        version=1,
        max_iterations=20,
        token_budget=None,
        prompt_tokens=0,
        completion_tokens=0,
        cost_usd=0.0,
        created_at=now,
        updated_at=now,
        expires_at=now + timedelta(hours=1),
    )


async def test_run_interactive_runs_as_persisted_agent() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent()
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    run_id = await _admit(runs, events, org_id=org_id, actor=user_id, agent_id=agent_id)
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
    )
    ctx = _ctx(runs, events, approvals, svc, provider)
    status = await run_interactive(ctx, run_id, _SCOPE)
    assert status == RunStatus.completed.value
    record = await runs.get(run_id)
    assert record is not None and record.status is RunStatus.completed
    # The run executed as the persisted, still-visible Agent (not the local-preview default).
    assert record.agent_id == agent_id


async def test_run_interactive_fails_closed_when_agent_revoked() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent()
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    run_id = await _admit(runs, events, org_id=org_id, actor=user_id, agent_id=agent_id)
    # Revoke the Agent (archive) between admission and worker claim.
    agent = await svc.get_agent(org_id, user_id, agent_id)
    await svc.archive_agent(org_id, user_id, agent_id, expected_version=agent.version)
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="unused", finish_reason=FinishReason.end_turn)]]
    )  # must never be consumed: the run fails closed before running
    ctx = _ctx(runs, events, approvals, svc, provider)
    status = await run_interactive(ctx, run_id, _SCOPE)
    assert status == RunStatus.failed.value
    record = await runs.get(run_id)
    assert record is not None and record.status is RunStatus.failed
    assert record.error_kind == "agent_forbidden"


async def test_resolve_agent_profile_local_preview_uses_defaults() -> None:
    # A local-preview run (or a worker with no identity service) uses the local-preview profile.
    record = _record(LOCAL_PREVIEW_ORG_ID, "local:local", "web")
    assert await _resolve_agent_profile(None, record) == ("web", "Keel Web", "")
    svc, *_ = await _identity_with_agent()
    assert await _resolve_agent_profile(svc, record) == ("web", "Keel Web", "")


async def test_resolve_agent_profile_loads_persisted_profile() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent(name="Scout", persona="Scout it.")
    record = _record(org_id, user_id, agent_id)
    assert await _resolve_agent_profile(svc, record) == (agent_id, "Scout", "Scout it.")


async def test_visibility_check_denies_non_member() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent()
    check = _visibility_check(svc)
    assert check is not None
    assert await check(_record(org_id, user_id, agent_id)) is True
    # A different actor (not a member / not the owner) is denied.
    assert await check(_record(org_id, "user-other", agent_id)) is False
    # No identity service -> no check (local-preview single tenant).
    assert _visibility_check(None) is None
