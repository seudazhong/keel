"""Server route tests (no external services): a fake runtime drives the HTTP layer."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine

from keel_core.approvals import InMemoryApprovalStore
from keel_core.config import Settings
from keel_core.embeddings import FakeEmbedder
from keel_core.events import Event, EventType
from keel_core.jobs import InMemoryJobStore, PostgresJobStore
from keel_core.knowledge.service import KnowledgeService
from keel_core.runs import InMemoryRunStore, RunStatus
from keel_server.app import (
    _build_job_store,
    _build_knowledge_service,
    _enqueue_arq,
    create_app,
)

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


class _FakeRuntime:
    """Stand-in for AgentRuntime that records calls and replays canned events."""

    def __init__(self, events: list[Event] | None = None) -> None:
        self._events = events or []
        self.admitted: list[tuple[str, str]] = []
        self.resolved: list[tuple[str, bool]] = []

    async def admit_and_run(self, session_id: str, content: str) -> str:
        self.admitted.append((session_id, content))
        return "run-abc"

    def resolve_approval(self, approval_id: str, approved: bool) -> bool:
        self.resolved.append((approval_id, approved))
        return approval_id == "known"

    async def tail(self, session_id: str, after: int | None = None) -> AsyncIterator[Event]:
        for event in self._events:
            yield event


def _client(runtime: _FakeRuntime) -> TestClient:
    app = create_app()
    app.state.runtime = runtime  # inject without running the real lifespan
    return TestClient(app)


def test_create_message_admits_durable_run() -> None:
    # The default Web path durably admits + dispatches a worker-owned run (no in-process task):
    # it persists a run row, enqueues run_interactive, and returns the run_id (response shape
    # preserved). Open mode binds the explicit local-preview org/actor/Agent profile.
    runs = InMemoryRunStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []

    async def _enqueue(name: str, *args: object, **_options: object) -> None:
        enqueued.append((name, args))

    app = create_app()
    app.state.runtime = _FakeRuntime()
    app.state.runs = runs
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = "web:local"
    app.state.engine = None
    app.state.shared_run_substrate = True  # simulate a shared durable substrate with doubles
    app.state.enqueue = _enqueue
    client = TestClient(app)

    resp = client.post("/v1/sessions/s1/messages", json={"content": "hi"})
    assert resp.status_code == 202
    body = resp.json()
    assert body["session_id"] == "s1" and body["accepted"] is True
    run_id = body["run_id"]
    assert enqueued == [("run_interactive", (run_id, "web:local"))]
    # The durable run row is bound to the local-preview compatibility profile.
    record = asyncio.run(runs.get(run_id))
    assert record is not None
    assert record.org_id == "local" and record.actor == "local:local"
    assert record.agent_id == "web" and record.status is RunStatus.queued


def test_create_message_requires_queue() -> None:
    # Admission fails closed (503) with no live queue — never an in-process fallback.
    app = create_app()
    app.state.runtime = _FakeRuntime()
    app.state.runs = InMemoryRunStore()
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = "web:local"
    app.state.engine = None
    app.state.enqueue = None
    client = TestClient(app)
    resp = client.post("/v1/sessions/s1/messages", json={"content": "hi"})
    assert resp.status_code == 503


def test_create_message_idempotent_admission() -> None:
    # A retried message with the same Idempotency-Key admits the run exactly once.
    runs = InMemoryRunStore()
    enqueued: list[tuple[str, tuple[object, ...]]] = []

    async def _enqueue(name: str, *args: object, **_options: object) -> None:
        enqueued.append((name, args))

    app = create_app()
    app.state.runtime = _FakeRuntime()
    app.state.runs = runs
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = "web:local"
    app.state.engine = None
    app.state.shared_run_substrate = True  # simulate a shared durable substrate with doubles
    app.state.enqueue = _enqueue
    client = TestClient(app)

    headers = {"Idempotency-Key": "req-1"}
    first = client.post("/v1/sessions/s1/messages", json={"content": "hi"}, headers=headers)
    second = client.post("/v1/sessions/s1/messages", json={"content": "hi"}, headers=headers)
    assert first.status_code == 202 and second.status_code == 202
    assert first.json()["run_id"] == second.json()["run_id"]  # same run, no duplicate


def _readiness_app(*, shared: bool, queue: bool) -> TestClient:
    app = create_app()

    class _FakeConn:
        async def __aenter__(self) -> _FakeConn:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def execute(self, *_a: object, **_k: object) -> None:
            return None

    class _FakeEngine:
        def connect(self) -> _FakeConn:
            return _FakeConn()

    class _FakeRedis:
        async def ping(self) -> bool:
            return True

    app.state.engine = _FakeEngine()
    app.state.redis = _FakeRedis()
    app.state.shared_run_substrate = shared
    app.state.enqueue = (lambda *a, **k: None) if queue else None
    return TestClient(app)


def test_readiness_degraded_when_admission_cannot_execute() -> None:
    # 200-ready while every message would 503 is a lie: with no shared substrate / queue the
    # default durable admission path cannot execute, so readiness must be degraded (item 7).
    client = _readiness_app(shared=False, queue=False)
    resp = client.get("/readiness")
    assert resp.status_code == 503
    body = resp.json()
    assert body["ready"] is False
    assert body["checks"]["run_admission"] == "degraded"


def test_readiness_degraded_when_queue_missing() -> None:
    client = _readiness_app(shared=True, queue=False)
    resp = client.get("/readiness")
    assert resp.status_code == 503
    assert resp.json()["checks"]["run_queue"] == "unavailable"


def test_readiness_ready_when_admission_available() -> None:
    client = _readiness_app(shared=True, queue=True)
    resp = client.get("/readiness")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ready"] is True
    assert body["checks"]["run_admission"] == "ready"


def test_resolve_unknown_approval_is_404() -> None:
    runtime = _FakeRuntime()
    client = _client(runtime)
    resp = client.post("/v1/approvals/xyz", json={"approval_id": "xyz", "decision": "allow"})
    assert resp.status_code == 404
    assert runtime.resolved == [("xyz", True)]


def test_resolve_known_approval_ok() -> None:
    runtime = _FakeRuntime()
    client = _client(runtime)
    resp = client.post("/v1/approvals/known", json={"approval_id": "known", "decision": "deny"})
    assert resp.status_code == 200
    assert resp.json() == {"resolved": True, "approved": False}


def _event(seq: int, etype: EventType, payload: dict[str, object]) -> Event:
    return Event(
        type=etype,
        seq=seq,
        session_id="s1",
        scope_id="web:local",
        ts=datetime.now(UTC),
        payload=payload,
    )


def test_sse_stream_emits_events() -> None:
    events = [
        _event(1, EventType.run_started, {"agent": "web"}),
        _event(2, EventType.message_token, {"role": "assistant", "text": "hi there"}),
        _event(3, EventType.run_ended, {"reason": "completed"}),
    ]
    client = _client(_FakeRuntime(events))
    with client.stream("GET", "/v1/sessions/s1/events") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        body = "".join(resp.iter_text())
    assert "run.started" in body
    assert "hi there" in body
    assert "run.ended" in body
    assert "id: 2" in body  # SSE ids carry the replay cursor (seq)


def test_index_serves_web_ui() -> None:
    client = _client(_FakeRuntime())
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "EventSource" in resp.text  # the SSE client is wired in the page


def test_onebot_webhook_accepts_and_dispatches() -> None:
    handled: list[dict[str, object]] = []

    class _FakeGateway:
        async def handle(self, payload: dict[str, object]) -> None:
            handled.append(payload)

    app = create_app()
    app.state.onebot_gateway = _FakeGateway()
    client = TestClient(app)
    resp = client.post(
        "/v1/gateway/onebot",
        json={"post_type": "message", "message_type": "private", "user_id": 1, "raw_message": "hi"},
    )
    assert resp.status_code == 202
    # BackgroundTasks run after the response; TestClient waits for them.
    assert handled and handled[0]["raw_message"] == "hi"


def test_onebot_webhook_503_without_gateway() -> None:
    client = TestClient(create_app())  # lifespan not run -> no gateway configured
    resp = client.post("/v1/gateway/onebot", json={"post_type": "message"})
    assert resp.status_code == 503


def test_telegram_webhook_accepts_and_dispatches() -> None:
    handled: list[dict[str, object]] = []

    class _FakeGateway:
        async def handle(self, payload: dict[str, object]) -> None:
            handled.append(payload)

    app = create_app()
    app.state.telegram_gateway = _FakeGateway()
    client = TestClient(app)
    resp = client.post(
        "/v1/gateway/telegram",
        json={"message": {"text": "hi", "chat": {"id": 7, "type": "private"}}},
    )
    assert resp.status_code == 202
    assert handled and handled[0]["message"]["text"] == "hi"  # type: ignore[index]


def test_telegram_webhook_503_without_gateway() -> None:
    client = TestClient(create_app())  # lifespan not run -> no gateway configured
    resp = client.post("/v1/gateway/telegram", json={"message": {}})
    assert resp.status_code == 503


async def test_server_builds_scope_bound_job_store_for_both_profiles() -> None:
    settings = Settings()
    memory = _build_job_store(None, "web:local", settings)
    assert isinstance(memory, InMemoryJobStore)
    assert memory.scope_id == "web:local"

    engine = create_async_engine("postgresql+psycopg://localhost:5432/keel_test")
    assert engine.url.database == "keel_test"
    try:
        postgres = _build_job_store(engine, "web:local", settings)
        assert isinstance(postgres, PostgresJobStore)
        assert postgres.scope_id == "web:local"
    finally:
        await engine.dispose()


async def test_server_builds_knowledge_service_only_for_postgres() -> None:
    settings = Settings(
        embedding_model="fake/server",
        embedding_dim=3,
    )
    memory_jobs = _build_job_store(None, "web:local", settings)
    assert (
        _build_knowledge_service(
            None,
            "web:local",
            settings,
            memory_jobs,
            embedder=FakeEmbedder(dim=3, model="fake/server"),
        )
        is None
    )

    engine = create_async_engine("postgresql+psycopg://localhost:5432/keel_test")
    try:
        postgres_jobs = _build_job_store(engine, "web:local", settings)
        service = _build_knowledge_service(
            engine,
            "web:local",
            settings,
            postgres_jobs,
            embedder=FakeEmbedder(dim=3, model="fake/server"),
        )
        assert isinstance(service, KnowledgeService)
        assert service.scope_id == "web:local"
    finally:
        await engine.dispose()


def test_knowledge_routes_are_unavailable_without_lifespan_state() -> None:
    response = TestClient(create_app()).get("/v1/knowledge-bases")
    assert response.status_code == 503


async def test_server_enqueue_adapter_forwards_keyword_options() -> None:
    seen: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    class Pool:
        async def enqueue_job(self, name: str, *args: object, **options: object) -> None:
            seen.append((name, args, options))

    await _enqueue_arq(Pool(), "run_job", "web:local", "job_1", _defer_until=_NOW)
    assert seen == [("run_job", ("web:local", "job_1"), {"_defer_until": _NOW})]
