"""Worker-owned interactive run routing: Agent-profile parity + claim-time visibility.

Exercises :func:`keel_worker.runs.run_interactive` end-to-end with the in-memory durable
doubles + a scripted provider (no Postgres/Redis): a durable admission is claimed and driven
to completion as the *persisted* selected Agent, and an Agent revoked between admit and claim
fails the run closed. (Postgres concurrency/isolation is proven in the integration suite.)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from keel_core.agent_config_snapshot import AgentConfigSnapshot
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
    snapshot: AgentConfigSnapshot | None = None,
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
        snapshot=snapshot,
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


class _CapturingProvider:
    """Wraps a provider, recording every request it streams (assert on model/messages)."""

    def __init__(self, inner: ScriptedProviderGateway) -> None:
        self._inner = inner
        self.requests: list[Any] = []

    def stream(self, request: Any) -> Any:
        self.requests.append(request)
        return self._inner.stream(request)


async def test_run_interactive_pins_snapshot_persona_model_despite_later_mutation() -> None:
    """A run admitted with an explicit snapshot executes with ITS captured name/persona/model —
    never the Agent's later-mutated fields (R1B; INVARIANTS.md C8). Claim-time visibility is
    still (successfully) re-authorized; only the *content* of the profile is pinned."""
    svc, org_id, user_id, agent_id = await _identity_with_agent(name="Scout", persona="Scout it.")
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    agent = await svc.get_agent(org_id, user_id, agent_id)
    snapshot = AgentConfigSnapshot(
        agent_id=agent_id,
        agent_version=agent.version,
        agent_name=agent.name,
        persona=agent.persona,
        model="pinned-model",
    )
    run_id = await _admit(
        runs, events, org_id=org_id, actor=user_id, agent_id=agent_id, snapshot=snapshot
    )
    # Mutate the Agent's persona/name AFTER admission but BEFORE claim/execution.
    await svc.update_agent(
        org_id,
        user_id,
        agent_id,
        expected_version=agent.version,
        name="Renamed",
        persona="a completely different persona",
    )
    inner = ScriptedProviderGateway(
        [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
    )
    provider = _CapturingProvider(inner)
    ctx = _ctx(runs, events, approvals, svc, provider)
    status = await run_interactive(ctx, run_id, _SCOPE)
    assert status == RunStatus.completed.value
    assert provider.requests, "the provider must have been called"
    request = provider.requests[0]
    # The pinned (admitted) model — never settings' process default.
    assert request.model == "pinned-model"
    system_texts = [
        str(m.get("content", "")) for m in request.messages if m.get("role") == "system"
    ]
    assert any("Scout it." in text for text in system_texts)
    assert not any("different persona" in text for text in system_texts)


async def test_resolve_agent_profile_local_preview_uses_defaults() -> None:
    # A local-preview run (or a worker with no identity service) uses the local-preview profile.
    record = _record(LOCAL_PREVIEW_ORG_ID, "local:local", "web")
    assert await _resolve_agent_profile(None, record, None) == ("web", "Keel Web", "")
    svc, *_ = await _identity_with_agent()
    assert await _resolve_agent_profile(svc, record, None) == ("web", "Keel Web", "")


async def test_resolve_agent_profile_loads_persisted_profile() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent(name="Scout", persona="Scout it.")
    record = _record(org_id, user_id, agent_id)
    assert await _resolve_agent_profile(svc, record, None) == (agent_id, "Scout", "Scout it.")


async def test_resolve_agent_profile_prefers_the_persisted_snapshot() -> None:
    """A run admitted with a snapshot always uses ITS name/persona (R1B, INVARIANTS.md C8) —
    never the Agent's current (possibly since-mutated) live profile, even when an identity
    service *could* resolve a different current name/persona."""
    svc, org_id, user_id, agent_id = await _identity_with_agent(name="Scout", persona="Scout it.")
    record = _record(org_id, user_id, agent_id)
    snapshot = AgentConfigSnapshot(agent_id=agent_id, agent_name="Old Name", persona="old persona")
    assert await _resolve_agent_profile(svc, record, snapshot) == (
        agent_id,
        "Old Name",
        "old persona",
    )


async def test_visibility_check_denies_non_member() -> None:
    svc, org_id, user_id, agent_id = await _identity_with_agent()
    check = _visibility_check(svc)
    assert check is not None
    assert await check(_record(org_id, user_id, agent_id)) is True
    # A different actor (not a member / not the owner) is denied.
    assert await check(_record(org_id, "user-other", agent_id)) is False
    # No identity service -> no check (local-preview single tenant).
    assert _visibility_check(None) is None
