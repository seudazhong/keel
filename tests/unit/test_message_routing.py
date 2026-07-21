"""Web message routing: durable admission binding (cloud org/Agent + local-preview) (M3.6).

Exercises the production ``POST /v1/sessions/{id}/messages`` wrapper: a cloud user binds
their selected org + persisted Agent (re-authorized), the open-mode local operator maps to
the explicit local-preview profile, and cloud fails closed for a caller with no authenticated
user + selected org/Agent. Uses the real FastAPI app with in-memory durable stores injected.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from keel_core.approvals import InMemoryApprovalStore
from keel_core.errors import PermissionDenied
from keel_core.identity import Capability, MembershipRole, NotFoundError
from keel_core.runs import InMemoryRunStore, RunStatus
from keel_server.app import create_app
from keel_server.auth import Principal, Role, hash_api_key
from keel_server.identity_context import Actor, ActorKind, resolve_actor

_SCOPE = "web:local"


def _run[T](coro: Awaitable[T]) -> T:
    return asyncio.run(coro)  # type: ignore[arg-type]


class _FakeRuntime:
    def interrupt_run(self, run_id: str) -> bool:
        return False


def _app(
    runs: InMemoryRunStore,
    enqueued: list[tuple[str, tuple[object, ...]]],
    *,
    cloud: bool = False,
    api_keys: dict[str, Principal] | None = None,
    shared: bool = True,
) -> TestClient:
    app = create_app()
    app.state.runs = runs
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = _SCOPE
    app.state.engine = None
    # A shared durable substrate is simulated with in-memory doubles unless a test opts out to
    # exercise the memory-mode denial (M3.6, item 2).
    app.state.shared_run_substrate = shared
    app.state.runtime = _FakeRuntime()
    app.state.auth_required = cloud
    app.state.api_keys = api_keys or {}

    async def _enqueue(name: str, *args: object, **_options: object) -> None:
        enqueued.append((name, args))

    app.state.enqueue = _enqueue
    return TestClient(app)


@dataclass
class _Org:
    org_id: str

    @property
    def membership(self) -> _Membership:
        return _Membership()


@dataclass
class _Membership:
    role: MembershipRole = MembershipRole.member


@dataclass
class _Agent:
    id: str
    version: int = 1
    name: str = "Agent"
    persona: str = ""


class _FakeIdentity:
    """Grants ``member_of`` orgs and ``uses`` agents; everything else fails closed."""

    def __init__(
        self,
        member_of: set[str],
        agents: dict[str, str],
        *,
        denied: set[str] | None = None,
        agent_profiles: dict[str, _Agent] | None = None,
        grants: list[object] | None = None,
    ) -> None:
        self._member_of = member_of
        self._agents = agents  # agent_ref -> agent_id
        self._denied = denied or set()  # agent_refs the actor may see but not use
        self._agent_profiles = agent_profiles or {}  # agent_id -> full _Agent (name/persona/ver)
        self._grants = grants or []

    async def select_org(self, user_id: str, org_ref: str) -> _Org:
        if org_ref in self._member_of:
            return _Org(org_id=org_ref)
        raise NotFoundError("organization not found")

    async def select_agent(self, org_id: str, user_id: str, agent_ref: str) -> _Agent:
        if agent_ref in self._denied:
            raise PermissionDenied("agent not permitted")
        if agent_ref in self._agents:
            agent_id = self._agents[agent_ref]
            return self._agent_profiles.get(agent_id, _Agent(id=agent_id))
        raise NotFoundError("agent not found")

    async def active_resource_grants(self, org_id: str, agent_id: str) -> list[object]:
        return self._grants


@dataclass
class _Grant:
    resource_type: str
    resource_id: str
    capability: Capability


def _as_user(client: TestClient) -> None:
    async def _actor() -> Actor:
        return Actor(
            kind=ActorKind.user,
            api_role=Role.operator,
            display_name="alice",
            user_id="user-alice",
        )

    cast(FastAPI, client.app).dependency_overrides[resolve_actor] = _actor


def test_local_preview_admission_binds_local_profile() -> None:
    runs = InMemoryRunStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []
    client = _app(runs, enqueued)
    resp = client.post("/v1/sessions/s1/messages", json={"content": "hi"})
    assert resp.status_code == 202
    run_id = resp.json()["run_id"]
    assert enqueued == [("run_interactive", (run_id, _SCOPE))]
    record = _run(runs.get(run_id))
    assert record is not None
    assert (record.org_id, record.actor, record.agent_id) == ("local", "local:local", "web")
    # Local preview creates an explicit local snapshot (no persisted Agent record exists).
    snapshot = record.snapshot
    assert snapshot is not None
    assert snapshot.agent_id == "web"
    assert snapshot.permission_profile == "local_preview"
    assert snapshot.resource_grants == ()


def test_cloud_user_admission_binds_org_and_agent() -> None:
    runs = InMemoryRunStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []
    client = _app(runs, enqueued)
    _as_user(client)
    cast(FastAPI, client.app).state.identity = _FakeIdentity(
        {"org-A"},
        {"agent-ref": "agent-1"},
        agent_profiles={
            "agent-1": _Agent(id="agent-1", version=3, name="Scout", persona="Be terse.")
        },
        grants=[_Grant("knowledge_base", "kb-1", Capability.read)],
    )
    resp = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"X-Keel-Org": "org-A", "X-Keel-Agent": "agent-ref"},
    )
    assert resp.status_code == 202
    record = _run(runs.get(resp.json()["run_id"]))
    assert record is not None
    assert (record.org_id, record.actor, record.agent_id) == ("org-A", "user-alice", "agent-1")
    assert record.status is RunStatus.queued
    # The snapshot reflects the *resolved* Agent (name/persona/version) + its active grants.
    snapshot = record.snapshot
    assert snapshot is not None
    assert snapshot.agent_id == "agent-1"
    assert snapshot.agent_version == 3
    assert snapshot.agent_name == "Scout"
    assert snapshot.persona == "Be terse."
    assert snapshot.permission_profile == "default"
    assert len(snapshot.resource_grants) == 1
    grant = snapshot.resource_grants[0]
    assert (grant.resource_type, grant.resource_id, grant.capability) == (
        "knowledge_base",
        "kb-1",
        "read",
    )


def test_cloud_user_requires_org_header() -> None:
    client = _app(InMemoryRunStore(), [])
    _as_user(client)
    cast(FastAPI, client.app).state.identity = _FakeIdentity({"org-A"}, {"agent-ref": "agent-1"})
    resp = client.post(
        "/v1/sessions/s1/messages", json={"content": "hi"}, headers={"X-Keel-Agent": "agent-ref"}
    )
    assert resp.status_code == 400


def test_cloud_user_requires_agent_header() -> None:
    client = _app(InMemoryRunStore(), [])
    _as_user(client)
    cast(FastAPI, client.app).state.identity = _FakeIdentity({"org-A"}, {"agent-ref": "agent-1"})
    resp = client.post(
        "/v1/sessions/s1/messages", json={"content": "hi"}, headers={"X-Keel-Org": "org-A"}
    )
    assert resp.status_code == 400


def test_cloud_user_non_member_org_is_404() -> None:
    client = _app(InMemoryRunStore(), [])
    _as_user(client)
    cast(FastAPI, client.app).state.identity = _FakeIdentity(set(), {"agent-ref": "agent-1"})
    resp = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"X-Keel-Org": "org-B", "X-Keel-Agent": "agent-ref"},
    )
    assert resp.status_code == 404


def test_cloud_user_unauthorized_agent_is_403() -> None:
    client = _app(InMemoryRunStore(), [])
    _as_user(client)
    cast(FastAPI, client.app).state.identity = _FakeIdentity({"org-A"}, {}, denied={"agent-ref"})
    resp = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"X-Keel-Org": "org-A", "X-Keel-Agent": "agent-ref"},
    )
    assert resp.status_code == 403


def test_cloud_mode_machine_actor_denied() -> None:
    # In cloud mode an API-key machine (no durable user + selected org/Agent) fails closed:
    # the local-preview profile is never reachable on an authenticated/cloud route.
    keys = {hash_api_key("k-op"): Principal(name="machine", role=Role.operator)}
    client = _app(InMemoryRunStore(), [], cloud=True, api_keys=keys)
    resp = client.post(
        "/v1/sessions/s1/messages", json={"content": "hi"}, headers={"X-API-Key": "k-op"}
    )
    assert resp.status_code == 403


def test_admission_conflict_is_409() -> None:
    runs = InMemoryRunStore()
    client = _app(runs, [])
    headers = {"Idempotency-Key": "dup"}
    first = client.post("/v1/sessions/s1/messages", json={"content": "hi"}, headers=headers)
    assert first.status_code == 202
    # Same identity, different content -> immutable fingerprint mismatch -> conflict.
    second = client.post("/v1/sessions/s1/messages", json={"content": "other"}, headers=headers)
    assert second.status_code == 409
