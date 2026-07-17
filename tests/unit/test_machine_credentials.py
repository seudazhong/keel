"""Scoped machine API credentials (M3.6 review finding 1).

Cloud mode must keep API-key clients working *without* ambient cross-tenant access: a machine
credential is either bound to exactly one org/Agent (a scoped credential) or is an explicit
global admin that must select the org/Agent per request. These tests exercise operator/admin/
viewer tiers, bound/unbound credentials, spoofed headers, and cross-org isolation through the
unified ``EndpointAuth`` dependency (no Postgres/Redis).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable

from fastapi.testclient import TestClient

from keel_core.approvals import InMemoryApprovalStore
from keel_core.identity import IdentityService, InMemoryIdentityStore, LoggingAuditSink
from keel_core.identity.models import AgentKind
from keel_core.runs import InMemoryRunStore
from keel_core.scoping import derive_agent_scope
from keel_server.auth import parse_api_keys


def _run[T](coro: Awaitable[T]) -> T:
    return asyncio.run(coro)  # type: ignore[arg-type]


class _FakeRuntime:
    model = "github_copilot/claude-sonnet-4.5"

    def interrupt_run(self, run_id: str) -> bool:
        return False


def _seed_two_orgs() -> tuple[IdentityService, dict[str, str]]:
    svc = IdentityService(
        InMemoryIdentityStore(), audit=LoggingAuditSink(), allow_jit_provisioning=True
    )
    owner = _run(svc.store.create_user(display_name="Owner", email=None))
    org_a = _run(svc.create_org(owner.id, slug="acme", display_name="Acme"))
    org_b = _run(svc.create_org(owner.id, slug="globex", display_name="Globex"))
    agent_a = _run(
        svc.create_agent(org_a.org_id, owner.id, kind=AgentKind.team, name="Agent Acme", persona="")
    )
    agent_b = _run(
        svc.create_agent(
            org_b.org_id, owner.id, kind=AgentKind.team, name="Agent Globex", persona=""
        )
    )
    return svc, {
        "org_a": org_a.org_id,
        "agent_a": agent_a.id,
        "org_b": org_b.org_id,
        "agent_b": agent_b.id,
    }


def _client(svc: IdentityService, api_keys: str) -> tuple[TestClient, InMemoryRunStore, list]:
    from keel_server.app import create_app

    runs = InMemoryRunStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []

    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        enqueued.append((name, args))

    app = create_app()
    app.state.runs = runs
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = "web:local"
    app.state.engine = None
    app.state.shared_run_substrate = True
    app.state.runtime = _FakeRuntime()
    app.state.auth_required = True  # cloud mode: no open-mode fallback
    app.state.api_keys = parse_api_keys(api_keys)
    app.state.identity = svc
    app.state.oidc_verifier = None
    app.state.enqueue = _enqueue
    return TestClient(app), runs, enqueued


def test_scoped_operator_credential_admits_into_its_bound_scope() -> None:
    svc, ids = _seed_two_orgs()
    keys = f"opkey:operator:org={ids['org_a']}:agent={ids['agent_a']}"
    client, runs, enqueued = _client(svc, keys)

    resp = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"X-API-Key": "opkey"},
    )
    assert resp.status_code == 202
    record = _run(runs.get(resp.json()["run_id"]))
    assert record is not None
    assert record.scope_id == derive_agent_scope(ids["org_a"], ids["agent_a"])
    assert record.org_id == ids["org_a"] and record.agent_id == ids["agent_a"]
    assert enqueued and enqueued[0][0] == "run_interactive"


def test_viewer_credential_cannot_send_messages() -> None:
    svc, ids = _seed_two_orgs()
    keys = f"vkey:viewer:org={ids['org_a']}:agent={ids['agent_a']}"
    client, _runs, _enqueued = _client(svc, keys)
    resp = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"X-API-Key": "vkey"},
    )
    assert resp.status_code == 403  # operator privilege required


def test_spoofed_org_header_is_rejected() -> None:
    svc, ids = _seed_two_orgs()
    keys = f"opkey:operator:org={ids['org_a']}:agent={ids['agent_a']}"
    client, _runs, _enqueued = _client(svc, keys)
    resp = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"X-API-Key": "opkey", "X-Keel-Org": ids["org_b"]},
    )
    assert resp.status_code == 403  # header cannot re-point a scoped credential


def test_unbound_machine_credential_denied_in_cloud() -> None:
    svc, _ids = _seed_two_orgs()
    client, _runs, _enqueued = _client(svc, "plain:admin")
    resp = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"X-API-Key": "plain"},
    )
    assert resp.status_code == 403  # no ambient cross-tenant access


def test_cross_org_credentials_are_isolated() -> None:
    svc, ids = _seed_two_orgs()
    keys = (
        f"akey:operator:org={ids['org_a']}:agent={ids['agent_a']},"
        f"bkey:operator:org={ids['org_b']}:agent={ids['agent_b']}"
    )
    client, runs, _enqueued = _client(svc, keys)
    r_a = client.post(
        "/v1/sessions/shared/messages",
        json={"content": "a"},
        headers={"X-API-Key": "akey"},
    )
    r_b = client.post(
        "/v1/sessions/shared/messages",
        json={"content": "b"},
        headers={"X-API-Key": "bkey"},
    )
    assert r_a.status_code == 202 and r_b.status_code == 202
    rec_a = _run(runs.get(r_a.json()["run_id"]))
    rec_b = _run(runs.get(r_b.json()["run_id"]))
    assert rec_a is not None and rec_b is not None
    # Identical external session id, two isolated per-Agent scopes.
    assert rec_a.scope_id == derive_agent_scope(ids["org_a"], ids["agent_a"])
    assert rec_b.scope_id == derive_agent_scope(ids["org_b"], ids["agent_b"])
    assert rec_a.scope_id != rec_b.scope_id


def test_global_admin_requires_selected_org_and_agent() -> None:
    svc, ids = _seed_two_orgs()
    client, runs, _enqueued = _client(svc, "gkey:admin:global")

    missing = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"X-API-Key": "gkey"},
    )
    assert missing.status_code == 400  # must select org + Agent

    ok = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={
            "X-API-Key": "gkey",
            "X-Keel-Org": ids["org_a"],
            "X-Keel-Agent": ids["agent_a"],
        },
    )
    assert ok.status_code == 202
    record = _run(runs.get(ok.json()["run_id"]))
    assert record is not None
    assert record.scope_id == derive_agent_scope(ids["org_a"], ids["agent_a"])
