"""Review-finding coverage for durable Web routing (M3.6): auth, isolation, dispatch, model.

Unit-level (no Postgres/Redis) coverage for the future-run-routing review findings:

* real OIDC message admission through the unified auth dependency (finding 1);
* cloud memory-mode admission denial — worker-owned admission requires a shared substrate
  (finding 2);
* per-scope workspace isolation + fail-closed sandbox namespace (finding 3);
* enqueue-failure returns the accepted run (202 + dispatch_pending) and a retry is idempotent
  (finding 4);
* the model selected at admission is captured durably and consumed by the worker (finding 5).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any

from fastapi.testclient import TestClient
from oidc_helpers import make_rsa_key, sign_token

import keel_worker.runs as worker_runs
from keel_core.agents import AgentSpec
from keel_core.approvals import InMemoryApprovalStore
from keel_core.config import Settings
from keel_core.identity import (
    IdentityService,
    InMemoryIdentityStore,
    LoggingAuditSink,
    OIDCConfig,
    OIDCVerifier,
    StaticJWKSProvider,
)
from keel_core.identity.models import AgentKind
from keel_core.loop import admission_model_in_log, admit
from keel_core.protocols import ProviderChunk
from keel_core.run_service import DurableRunService
from keel_core.runs import InMemoryRunStore, RunStatus, RunSurface
from keel_core.scoping import derive_agent_scope
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.tools import (
    ReadRequest,
    UnavailableExecutionEnvironment,
    WriteRequest,
    build_scoped_execution_environment,
)
from keel_core.tools.rpc import ExecutionOperation, ExecutionRpcRequest
from keel_core.types import FinishReason
from keel_sandbox.service import ExecutorAdmissionPolicy

_SCOPE = "web:local"


def _run[T](coro: Awaitable[T]) -> T:
    return asyncio.run(coro)  # type: ignore[arg-type]


class _FakeRuntime:
    model = "github_copilot/claude-sonnet-4.5"

    def interrupt_run(self, run_id: str) -> bool:
        return False


# --- finding 1: real OIDC message admission through the unified auth dependency ----------


def test_real_oidc_user_admits_message() -> None:
    from keel_server.app import create_app

    key = make_rsa_key()
    verifier = OIDCVerifier(
        OIDCConfig(issuer="https://issuer.example", audiences=frozenset({"keel"})),
        StaticJWKSProvider([key.jwk]),
    )
    svc = IdentityService(
        InMemoryIdentityStore(), audit=LoggingAuditSink(), allow_jit_provisioning=True
    )
    # Provision the OIDC subject as a real user, then give them an org + persisted Agent.
    claims = _run(verifier.verify(sign_token(key, subject="sub-1", email="a@b.com")))
    user = _run(svc.resolve_oidc_user(claims))
    org = _run(svc.create_org(user.id, slug="acme", display_name="Acme"))
    agent = _run(
        svc.create_agent(org.org_id, user.id, kind=AgentKind.personal, name="Scout", persona="")
    )

    runs = InMemoryRunStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []

    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        enqueued.append((name, args))

    app = create_app()
    app.state.runs = runs
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = _SCOPE
    app.state.engine = None
    app.state.shared_run_substrate = True
    app.state.runtime = _FakeRuntime()
    app.state.auth_required = True  # cloud: OIDC required, no open-mode fallback
    app.state.api_keys = {}
    app.state.identity = svc
    app.state.oidc_verifier = verifier
    app.state.enqueue = _enqueue
    client = TestClient(app)

    token = sign_token(key, subject="sub-1", email="a@b.com")
    resp = client.post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={
            "Authorization": f"Bearer {token}",
            "X-Keel-Org": org.org_id,
            "X-Keel-Agent": agent.id,
        },
    )
    assert resp.status_code == 202
    record = _run(runs.get(resp.json()["run_id"]))
    assert record is not None
    # Bound to the derived per-Agent scope + the durable user + the persisted Agent.
    assert record.scope_id == derive_agent_scope(org.org_id, agent.id)
    assert record.org_id == org.org_id and record.actor == user.id and record.agent_id == agent.id


def test_cloud_request_without_user_fails_closed() -> None:
    from keel_server.app import create_app

    app = create_app()
    app.state.runs = InMemoryRunStore()
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = _SCOPE
    app.state.engine = None
    app.state.shared_run_substrate = True
    app.state.runtime = _FakeRuntime()
    app.state.auth_required = True
    app.state.api_keys = {}
    app.state.identity = IdentityService(InMemoryIdentityStore(), audit=LoggingAuditSink())
    app.state.oidc_verifier = None

    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        return None

    app.state.enqueue = _enqueue
    resp = TestClient(app).post("/v1/sessions/s1/messages", json={"content": "hi"})
    # No verified user, no local-preview fallback in cloud mode -> fail closed.
    assert resp.status_code in (401, 403, 503)


# --- finding 2: cloud memory-mode admission denial (shared substrate required) -----------


def test_memory_mode_admission_denied_503() -> None:
    from keel_server.app import create_app

    app = create_app()
    app.state.runs = InMemoryRunStore()
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = _SCOPE
    app.state.engine = None
    app.state.shared_run_substrate = False  # in-memory/process-local: worker cannot see runs
    app.state.runtime = _FakeRuntime()
    app.state.auth_required = False
    app.state.api_keys = {}

    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        return None

    app.state.enqueue = _enqueue
    resp = TestClient(app).post("/v1/sessions/s1/messages", json={"content": "hi"})
    assert resp.status_code == 503


# --- finding 3: per-scope workspace isolation + fail-closed sandbox namespace ------------


def test_scoped_local_workspaces_are_isolated(tmp_path: Any) -> None:
    settings = Settings(
        execution_backend="unsafe-local-dev",
        trusted_preview_allow_unsafe_execution=True,
        trusted_preview_shell_workspace_sanitized=True,
    )
    scope_a = derive_agent_scope("org_a", "agt_1")
    scope_b = derive_agent_scope("org_b", "agt_1")
    env_a = build_scoped_execution_environment(
        settings, tmp_path, service="worker", scope_id=scope_a
    )
    env_b = build_scoped_execution_environment(
        settings, tmp_path, service="worker", scope_id=scope_b
    )
    assert _run(env_a.write(WriteRequest("secret.txt", "org-a-only"))).ok
    # Org B's scoped workspace cannot see org A's file (no shared writable workspace).
    assert not _run(env_b.read(ReadRequest("secret.txt"))).ok
    read_a = _run(env_a.read(ReadRequest("secret.txt")))
    assert read_a.ok and "org-a-only" in read_a.output


def test_scoped_environment_fails_closed_when_unavailable(tmp_path: Any) -> None:
    scope = derive_agent_scope("org_a", "agt_1")
    # Unsafe execution not allowed -> fail closed (never a shared writable workspace).
    unsafe_off = Settings(
        execution_backend="unsafe-local-dev", trusted_preview_allow_unsafe_execution=False
    )
    denied = build_scoped_execution_environment(
        unsafe_off,
        tmp_path,
        service="worker",
        scope_id=scope,
    )
    assert isinstance(denied, UnavailableExecutionEnvironment)
    # Unknown backend -> fail closed.
    unknown = build_scoped_execution_environment(
        Settings(execution_backend="nope"), tmp_path, service="worker", scope_id=scope
    )
    assert isinstance(unknown, UnavailableExecutionEnvironment)


def test_sandbox_denies_scoped_namespace_when_unsupported() -> None:
    req = ExecutionRpcRequest(operation=ExecutionOperation.list, workspace="ws_deadbeef")
    # A sandbox that cannot provision a scoped workspace fails file/shell closed (never shares).
    denial = ExecutorAdmissionPolicy(scoped_workspaces_supported=False).admit(req)
    assert denial is not None and denial.ok is False
    # A sandbox that can provision scoped workspaces admits the namespaced request.
    assert ExecutorAdmissionPolicy(scoped_workspaces_supported=True).admit(req) is None
    # A malformed namespace is denied even when scoped workspaces are supported.
    bad = ExecutionRpcRequest(operation=ExecutionOperation.list, workspace="../evil")
    assert ExecutorAdmissionPolicy(scoped_workspaces_supported=True).admit(bad) is not None


# --- finding 4: enqueue failure returns the accepted run; retry is idempotent ------------


def _admit_service(runs: InMemoryRunStore, events: InMemoryEventStore, enqueue: Any) -> Any:
    return DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=InMemoryApprovalStore(),
        scope_id=_SCOPE,
        enqueue=enqueue,
        admit_fn=admit,
    )


def test_enqueue_failure_keeps_run_and_retry_is_idempotent() -> None:
    runs, events = InMemoryRunStore(), InMemoryEventStore()

    async def _boom(_run_id: str) -> None:
        raise RuntimeError("queue unavailable")

    service = _admit_service(runs, events, _boom)

    async def _scenario() -> None:
        result = await service.admit(
            org_id="org-1",
            actor="user-1",
            agent_id="agent-1",
            session_id="s1",
            surface=RunSurface.web.value,
            content="hi",
            idempotency_key="k1",
        )
        # The durable admission committed despite the enqueue failure: accepted, dispatch-pending.
        assert result.dispatch_pending is True
        record = await runs.get(result.run_id)
        assert record is not None and record.status is RunStatus.queued
        # A retry with the same idempotency key returns the SAME run (no duplicate, no 500).
        retry = await service.admit(
            org_id="org-1",
            actor="user-1",
            agent_id="agent-1",
            session_id="s1",
            surface=RunSurface.web.value,
            content="hi",
            idempotency_key="k1",
        )
        assert retry.run_id == result.run_id

    _run(_scenario())


def test_endpoint_enqueue_failure_returns_202_dispatch_pending() -> None:
    from keel_server.app import create_app

    app = create_app()
    app.state.runs = InMemoryRunStore()
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = _SCOPE
    app.state.engine = None
    app.state.shared_run_substrate = True
    app.state.runtime = _FakeRuntime()
    app.state.auth_required = False
    app.state.api_keys = {}

    async def _boom(name: str, *args: object, **_o: object) -> None:
        raise RuntimeError("queue unavailable")

    app.state.enqueue = _boom
    resp = TestClient(app).post(
        "/v1/sessions/s1/messages",
        json={"content": "hi"},
        headers={"Idempotency-Key": "req-1"},
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["dispatch_pending"] is True
    assert body["idempotency_key"] == "req-1"  # echoed for correlation


# --- finding 5: the admitted model is captured durably and consumed by the worker --------


def test_admission_captures_model_and_worker_consumes_it(monkeypatch: Any) -> None:
    runs, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()

    async def _enqueue(_run_id: str) -> None:
        return None

    service = _admit_service_with_approvals(runs, events, approvals, _enqueue)
    run_id = _run(
        service.admit(
            org_id="local",
            actor="local:local",
            agent_id="web",
            session_id="s1",
            surface=RunSurface.web.value,
            content="hi",
            idempotency_key="k1",
            model="github_copilot/gpt-4.1",
        )
    ).run_id
    # The selected model is captured in the durable admission event (no schema migration).
    assert _run(admission_model_in_log(events, "s1", run_id)) == "github_copilot/gpt-4.1"

    captured: dict[str, str] = {}
    from keel_core.interactive import build_interactive_agent

    def _capture(*, model: str, **kwargs: Any) -> AgentSpec:
        captured["model"] = model
        return build_interactive_agent(model=model, **kwargs)

    monkeypatch.setattr(worker_runs, "build_interactive_agent", _capture)
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
    )
    ctx = {
        "durable_scope": _SCOPE,
        "runs": runs,
        "store": events,
        "approvals": approvals,
        "provider": provider,
        "execution_environment": UnavailableExecutionEnvironment(),
        "identity": None,
        "engine": None,
        "embedder": None,
    }
    status = _run(worker_runs.run_interactive(ctx, run_id, _SCOPE))
    assert status == RunStatus.completed.value
    # The worker executed with the ADMITTED model, not its process default.
    assert captured["model"] == "github_copilot/gpt-4.1"


def _admit_service_with_approvals(
    runs: InMemoryRunStore,
    events: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    enqueue: Any,
) -> DurableRunService:
    return DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=approvals,
        scope_id=_SCOPE,
        enqueue=enqueue,
        admit_fn=admit,
    )
