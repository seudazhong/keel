"""Server route tests (no external services): a fake runtime drives the HTTP layer."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from keel_core.events import Event, EventType
from keel_server.app import create_app


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


def test_create_message_launches_run() -> None:
    runtime = _FakeRuntime()
    client = _client(runtime)
    resp = client.post("/v1/sessions/s1/messages", json={"content": "hi"})
    assert resp.status_code == 202
    body = resp.json()
    assert body["session_id"] == "s1" and body["run_id"] == "run-abc"
    assert runtime.admitted == [("s1", "hi")]


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
