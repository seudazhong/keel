"""Production API tests for the durable-run routes: org authz + approval routing (M3.6).

These exercise the real FastAPI app (``create_app``) with the in-memory durable stores
injected onto ``app.state`` — the production ``/v1`` wrappers, not just the service layer.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

import keel_server.api.v1 as v1
from keel_core.approvals import InMemoryApprovalStore
from keel_core.identity import NotFoundError
from keel_core.runs import InMemoryRunStore, RunBudgetSpec, RunStatus, RunSurface, action_hash
from keel_server.app import create_app
from keel_server.auth import Role
from keel_server.identity_context import Actor, ActorKind

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


async def _admit_run(runs: InMemoryRunStore, run_id: str, *, org_id: str = "org-1") -> None:
    await runs.admit(
        run_id=run_id,
        scope_id=_SCOPE,
        org_id=org_id,
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key=run_id,
        budget=RunBudgetSpec(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )


async def _suspend_run(runs: InMemoryRunStore, run_id: str, *, org_id: str = "org-1") -> None:
    await _admit_run(runs, run_id, org_id=org_id)
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
            actor="user-1",
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
