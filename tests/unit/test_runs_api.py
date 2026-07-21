"""Production API tests for the durable-run routes: org authz + approval routing (M3.6).

These exercise the real FastAPI app (``create_app``) with the in-memory durable stores
injected onto ``app.state`` — the production ``/v1`` wrappers, not just the service layer.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

import keel_server.api.v1 as v1
from keel_core.agent_config_snapshot import AgentConfigSnapshot
from keel_core.approvals import InMemoryApprovalStore
from keel_core.identity import MembershipRole, NotFoundError
from keel_core.runs import InMemoryRunStore, RunBudgetSpec, RunStatus, RunSurface, action_hash
from keel_server.app import create_app
from keel_server.auth import Role
from keel_server.identity_context import Actor, ActorKind, resolve_actor

_SCOPE = "web:local"


def _run[T](coro: Awaitable[T]) -> T:
    """Drive a setup/verify coroutine to completion (the stores are plain in-memory dicts)."""
    return asyncio.run(coro)  # type: ignore[arg-type]


class _FakeRuntime:
    def interrupt_run(self, run_id: str) -> bool:
        return False


def _app_with_state(
    runs: InMemoryRunStore,
    approvals: InMemoryApprovalStore,
    enqueued: list[tuple[str, tuple[object, ...]]],
) -> TestClient:
    app = create_app()
    app.state.runs = runs
    app.state.durable_approvals = approvals
    app.state.durable_scope = _SCOPE
    app.state.engine = None
    app.state.runtime = _FakeRuntime()

    async def _enqueue(name: str, *args: object, **_options: object) -> None:
        enqueued.append((name, args))

    app.state.enqueue = _enqueue
    return TestClient(app)


async def _admit_run(
    runs: InMemoryRunStore, run_id: str, *, org_id: str = "org-1", scope_id: str = _SCOPE
) -> None:
    await runs.admit(
        run_id=run_id,
        scope_id=scope_id,
        org_id=org_id,
        actor="local:local",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key=run_id,
        budget=RunBudgetSpec(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        snapshot=AgentConfigSnapshot(agent_id="agent-1"),
    )


async def _suspend_run(
    runs: InMemoryRunStore, run_id: str, *, org_id: str = "org-1", scope_id: str = _SCOPE
) -> None:
    await _admit_run(runs, run_id, org_id=org_id, scope_id=scope_id)
    await runs.mark_queued(run_id)
    lease = await runs.claim(run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None
    await runs.release(lease, to_status=RunStatus.waiting_approval)


def test_get_run_status_local_preview_allowed() -> None:
    runs = InMemoryRunStore()
    client = _app_with_state(runs, InMemoryApprovalStore(), [])
    _run(_admit_run(runs, "run-1"))
    resp = client.get("/v1/runs/run-1")
    assert resp.status_code == 200
    assert resp.json()["status"] == "admitted"


def test_get_run_cross_org_user_is_404(monkeypatch: Any) -> None:
    runs = InMemoryRunStore()
    client = _app_with_state(runs, InMemoryApprovalStore(), [])
    _run(_admit_run(runs, "run-1", org_id="org-A"))

    # A real user actor who is not a member of the run's org must get 404 (no info leak).
    async def _user_actor(_request: object) -> Actor:
        return Actor(
            kind=ActorKind.user,
            api_role=Role.operator,
            display_name="alice",
            user_id="user-alice",
        )

    class _DenyingIdentity:
        async def select_org(self, user_id: str, org_ref: str) -> Any:
            raise NotFoundError("organization not found")

    monkeypatch.setattr(v1, "resolve_actor", _user_actor)
    cast(FastAPI, client.app).state.identity = _DenyingIdentity()
    resp = client.get("/v1/runs/run-1")
    assert resp.status_code == 404


def test_steer_records_durable_control() -> None:
    runs = InMemoryRunStore()
    client = _app_with_state(runs, InMemoryApprovalStore(), [])
    _run(_admit_run(runs, "run-1"))
    resp = client.post("/v1/runs/run-1/steer", json={"text": "use staging"})
    assert resp.status_code == 200 and resp.json() == {"ok": True}
    pending = _run(runs.peek_control("run-1"))
    assert [c.kind.value for c in pending] == ["steer"]


def test_durable_interactive_approval_routes_to_run_interactive() -> None:
    runs = InMemoryRunStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []
    client = _app_with_state(runs, approvals, enqueued)
    _run(_suspend_run(runs, "run-1"))
    approval_id = _run(
        approvals.create_pending(
            scope_id=_SCOPE,
            run_id="run-1",
            session_id="sess-1",
            tool="email.send",
            args={"to": "z@x"},
            call_id="c1",
            idempotency_key="i1",
            reason="first_use",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            org_id="org-1",
            actor="local:local",
            action_hash=action_hash("email.send", {"to": "z@x"}),
            run_attempt=1,
        )
    )
    resp = client.post(f"/v1/approvals/{approval_id}/approve")
    assert resp.status_code == 200 and resp.json() == {"ok": True}
    # The durable interactive path enqueues run_interactive and requeues the run.
    assert enqueued == [("run_interactive", ("run-1", _SCOPE))]
    record = _run(runs.get("run-1"))
    assert record is not None and record.status is RunStatus.queued and record.resume_requested


def test_legacy_approval_routes_to_resume_run() -> None:
    runs = InMemoryRunStore()  # empty: this approval's run is NOT a durable interactive run
    approvals = InMemoryApprovalStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []
    client = _app_with_state(runs, approvals, enqueued)
    approval_id = _run(
        approvals.create_pending(
            scope_id=_SCOPE,
            run_id="legacy-run",
            session_id="sched-sess",
            tool="email.send",
            args={},
            call_id="c1",
            idempotency_key="i1",
            reason="first_use",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
    )
    resp = client.post(f"/v1/approvals/{approval_id}/approve")
    assert resp.status_code == 200 and resp.json() == {"ok": True}
    # The legacy scheduled/digest path keeps the existing resume_run behavior.
    assert enqueued == [("resume_run", ("sched-sess", "legacy-run", _SCOPE))]


async def _suspend_run_for_user(
    runs: InMemoryRunStore,
    approvals: InMemoryApprovalStore,
    run_id: str,
    *,
    org_id: str,
    actor: str,
) -> str:
    """Admit + suspend a run owned by ``actor`` with a bound pending approval (attempt 1)."""
    await runs.admit(
        run_id=run_id,
        scope_id=_SCOPE,
        org_id=org_id,
        actor=actor,
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key=run_id,
        budget=RunBudgetSpec(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        snapshot=AgentConfigSnapshot(agent_id="agent-1"),
    )
    await runs.mark_queued(run_id)
    lease = await runs.claim(run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None
    await runs.release(lease, to_status=RunStatus.waiting_approval)
    return await approvals.create_pending(
        scope_id=_SCOPE,
        run_id=run_id,
        session_id="sess-1",
        tool="email.send",
        args={"to": "z@x"},
        call_id="c1",
        idempotency_key=f"i-{run_id}",
        reason="first_use",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        org_id=org_id,
        actor=actor,
        action_hash=action_hash("email.send", {"to": "z@x"}),
        run_attempt=1,
    )


def test_cross_user_same_org_resolution_denied_via_api(monkeypatch: Any) -> None:
    # A run owned by user-alice (org-1); a DIFFERENT user (bob), also an org-1 member, must
    # not resolve alice's approval — the API passes bob's stable actor and the service denies.
    runs = InMemoryRunStore()
    approvals = InMemoryApprovalStore()
    client = _app_with_state(runs, approvals, [])
    approval_id = _run(
        _suspend_run_for_user(runs, approvals, "run-1", org_id="org-1", actor="user-alice")
    )

    async def _bob(_request: object) -> Actor:
        return Actor(
            kind=ActorKind.user,
            api_role=Role.operator,
            display_name="bob",
            user_id="user-bob",
        )

    class _Org1Member:
        async def select_org(self, user_id: str, org_ref: str) -> Any:
            if org_ref == "org-1":
                return object()
            raise NotFoundError("organization not found")

    monkeypatch.setattr(v1, "resolve_actor", _bob)
    cast(FastAPI, client.app).state.identity = _Org1Member()
    # bob is an org member (authorize passes) but not the owner: the service denies.
    resp = client.post(f"/v1/approvals/{approval_id}/approve")
    assert resp.status_code == 200 and resp.json() == {"ok": False}
    record = _run(runs.get("run-1"))
    assert record is not None and record.status is RunStatus.waiting_approval  # still suspended


def test_conflicting_decision_via_api_is_rejected() -> None:
    # Local-preview operator approves, then a conflicting reject must be rejected (ok: False).
    runs = InMemoryRunStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []
    client = _app_with_state(runs, approvals, enqueued)
    approval_id = _run(
        _suspend_run_for_user(runs, approvals, "run-1", org_id="org-1", actor="local:local")
    )
    assert client.post(f"/v1/approvals/{approval_id}/approve").json() == {"ok": True}
    # A duplicate approve is idempotent; a conflicting reject is rejected.
    assert client.post(f"/v1/approvals/{approval_id}/approve").json() == {"ok": True}
    assert client.post(f"/v1/approvals/{approval_id}/reject").json() == {"ok": False}
    assert _run(approvals.get(approval_id)).status == "granted"  # type: ignore[union-attr]


def test_legacy_approval_routes_to_resume_run_after_new_helpers() -> None:
    # (Kept minimal: re-verifies the legacy path is untouched by the new binding checks.)
    runs = InMemoryRunStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []
    client = _app_with_state(runs, approvals, enqueued)
    approval_id = _run(
        approvals.create_pending(
            scope_id=_SCOPE,
            run_id="legacy-run",
            session_id="sched-sess",
            tool="email.send",
            args={},
            call_id="c1",
            idempotency_key="i1",
            reason="first_use",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
    )
    assert client.post(f"/v1/approvals/{approval_id}/approve").json() == {"ok": True}
    assert enqueued == [("resume_run", ("sched-sess", "legacy-run", _SCOPE))]


async def _seed_interactive_approval(
    approvals: InMemoryApprovalStore, run_id: str, *, org_id: str, to: str, scope_id: str = _SCOPE
) -> str:
    return await approvals.create_pending(
        scope_id=scope_id,
        run_id=run_id,
        session_id="sess-1",
        tool="email.send",
        args={"to": to},
        call_id=f"c-{run_id}",
        idempotency_key=f"i-{run_id}",
        reason="first_use",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        org_id=org_id,
        actor="user-1",
        action_hash=action_hash("email.send", {"to": to}),
        run_attempt=1,
    )


def _override_user(client: TestClient, identity: Any) -> None:
    """Route the endpoint auth through a durable user + a fake identity (per-scope)."""

    async def _user_actor() -> Actor:
        return Actor(
            kind=ActorKind.user,
            api_role=Role.operator,
            display_name="alice",
            user_id="user-alice",
        )

    cast(FastAPI, client.app).dependency_overrides[resolve_actor] = _user_actor
    cast(FastAPI, client.app).state.identity = identity


@dataclass
class _MemberEdge:
    role: MembershipRole = MembershipRole.member


@dataclass
class _MemberOrg:
    _org: str

    @property
    def org_id(self) -> str:
        return self._org

    @property
    def membership(self) -> _MemberEdge:
        return _MemberEdge()


@dataclass
class _AgentRef:
    id: str


class _ScopedIdentity:
    """A fake identity granting one org (as ``member``) + one agent, deriving a scope."""

    def __init__(self, org: str, agent_ref: str, agent_id: str) -> None:
        self._org = org
        self._agent_ref = agent_ref
        self._agent_id = agent_id

    async def select_org(self, user_id: str, org_ref: str) -> Any:
        if org_ref == self._org:
            return _MemberOrg(self._org)
        raise NotFoundError("organization not found")

    async def select_agent(self, org_id: str, user_id: str, agent_ref: str) -> Any:
        if agent_ref == self._agent_ref:
            return _AgentRef(self._agent_id)
        raise NotFoundError("agent not found")


def test_list_approvals_isolates_durable_interactive_across_orgs() -> None:
    # Two orgs' durable interactive approvals live in their own derived per-Agent scopes; a
    # user who selects org-A's scope must never see org-B's tool/args (M3.6, item 3/8).
    from keel_core.scoping import derive_agent_scope

    runs = InMemoryRunStore()
    approvals = InMemoryApprovalStore()
    client = _app_with_state(runs, approvals, [])
    scope_a = derive_agent_scope("org-A", "agent-A")
    scope_b = derive_agent_scope("org-B", "agent-B")
    _run(_suspend_run(runs, "run-A", org_id="org-A", scope_id=scope_a))
    _run(_suspend_run(runs, "run-B", org_id="org-B", scope_id=scope_b))
    aid_a = _run(
        _seed_interactive_approval(approvals, "run-A", org_id="org-A", to="a@x", scope_id=scope_a)
    )
    _run(_seed_interactive_approval(approvals, "run-B", org_id="org-B", to="b@x", scope_id=scope_b))

    _override_user(client, _ScopedIdentity("org-A", "agent-A", "agent-A"))
    listed = client.get(
        "/v1/approvals?status=pending",
        headers={"X-Keel-Org": "org-A", "X-Keel-Agent": "agent-A"},
    ).json()
    assert [a["id"] for a in listed] == [aid_a]  # only org-A's scope; org-B never exposed
    assert listed[0]["origin"] == "interactive" and listed[0]["org_id"] == "org-A"


def test_list_approvals_legacy_isolated_to_local_operator() -> None:
    # A legacy local-preview approval (no durable run row) is visible to the local operator,
    # labeled, and never surfaced to a cloud user's per-Agent scope.
    runs = InMemoryRunStore()
    approvals = InMemoryApprovalStore()
    client = _app_with_state(runs, approvals, [])
    aid = _run(
        approvals.create_pending(
            scope_id=_SCOPE,
            run_id="legacy-run",
            session_id="sched-sess",
            tool="email.send",
            args={"to": "finance@external.example"},
            call_id="c1",
            idempotency_key="i1",
            reason="tainted",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
    )
    # Default open-mode actor is the local operator: sees the legacy approval, labeled.
    listed_local = client.get("/v1/approvals?status=pending").json()
    assert [a["id"] for a in listed_local] == [aid]
    assert listed_local[0]["origin"] == "local-preview"

    # A cloud user's derived scope never contains the legacy web:local approval.
    _override_user(client, _ScopedIdentity("org-A", "agent-A", "agent-A"))
    assert (
        client.get(
            "/v1/approvals?status=pending",
            headers={"X-Keel-Org": "org-A", "X-Keel-Agent": "agent-A"},
        ).json()
        == []
    )
