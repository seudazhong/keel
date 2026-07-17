"""Authenticated SSE performs durable replay then tails live worker events (finding 5).

The per-Agent scoped stream must not close immediately after replay: it keeps the connection
open and tails newly-appended durable events until the run ends (or the client disconnects),
honoring ``Last-Event-ID`` with no missed/duplicate events. These are unit-level (no Redis).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from fastapi.testclient import TestClient

import keel_server.api.v1 as v1
from keel_core.approvals import InMemoryApprovalStore
from keel_core.events import Event, EventType
from keel_core.identity import IdentityService, InMemoryIdentityStore, LoggingAuditSink
from keel_core.identity.models import AgentKind
from keel_core.runs import InMemoryRunStore
from keel_server.auth import parse_api_keys


def _event(seq: int, etype: EventType, scope: str) -> Event:
    return Event(
        type=etype,
        seq=seq,
        session_id="s1",
        scope_id=scope,
        ts=datetime.now(UTC),
        payload={"i": seq},
    )


class _GrowingStore:
    """A durable store double that gains a terminal event after replay (simulates the worker)."""

    def __init__(self, scope: str) -> None:
        self._scope = scope
        self._events: list[Event] = [
            _event(1, EventType.run_started, scope),
            _event(2, EventType.message_token, scope),
        ]
        self._reads = 0

    def read(self, session_id: str, after: int | None = None) -> AsyncIterator[Event]:
        return self._read(after)

    async def _read(self, after: int | None) -> AsyncIterator[Event]:
        self._reads += 1
        # After the replay + a poll, the "worker" appends the terminal event so the tail closes.
        if self._reads >= 3 and not any(e.type is EventType.run_ended for e in self._events):
            self._events.append(_event(3, EventType.run_ended, self._scope))
        for event in self._events:
            if after is None or event.seq > after:
                yield event


def _scoped_client(store: _GrowingStore) -> TestClient:
    from keel_server.app import create_app

    svc = IdentityService(
        InMemoryIdentityStore(), audit=LoggingAuditSink(), allow_jit_provisioning=True
    )

    async def _seed() -> tuple[str, str]:
        owner = await svc.store.create_user(display_name="Owner", email=None)
        org = await svc.create_org(owner.id, slug="acme", display_name="Acme")
        agent = await svc.create_agent(
            org.org_id, owner.id, kind=AgentKind.team, name="Agent One", persona=""
        )
        return org.org_id, agent.id

    org_id, agent_id = asyncio.run(_seed())

    app = create_app()
    app.state.engine = None
    app.state.events = store
    app.state.runs = InMemoryRunStore()
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = "web:local"
    app.state.auth_required = True
    app.state.identity = svc
    app.state.oidc_verifier = None
    app.state.api_keys = parse_api_keys(f"vkey:viewer:org={org_id}:agent={agent_id}")
    return TestClient(app)


def test_sse_replays_then_tails_until_terminal(monkeypatch) -> None:
    monkeypatch.setattr(v1, "_SSE_POLL_INTERVAL_SECONDS", 0.01)
    store = _GrowingStore("agent-scope")
    client = _scoped_client(store)
    resp = client.get("/v1/sessions/s1/events", headers={"X-API-Key": "vkey"})
    assert resp.status_code == 200
    body = resp.text
    # Replay (run.started + message_token) AND the event the worker appended after replay.
    assert "run.started" in body
    assert "run.ended" in body  # tail did not close immediately after replay
    # Each durable event appears exactly once (no missed/duplicate), and each carries its id.
    assert body.count("run.started") == 1
    assert body.count("run.ended") == 1
    assert "id: 1" in body and "id: 2" in body and "id: 3" in body


def test_resume_cursor_prefers_last_event_id() -> None:
    from starlette.requests import Request

    def _req(headers: dict[str, str]) -> Request:
        raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
        return Request({"type": "http", "headers": raw})

    assert v1._resume_cursor(_req({"last-event-id": "7"}), None) == 7
    assert v1._resume_cursor(_req({"last-event-id": "7"}), 3) == 7  # header wins over after
    assert v1._resume_cursor(_req({}), 3) == 3  # no header -> after
    assert v1._resume_cursor(_req({"last-event-id": "nope"}), 3) == 3  # invalid -> after


class _RecordingRuntime:
    """A local-preview runtime double that records the resume cursor its tail was given."""

    def __init__(self) -> None:
        self.tail_after: list[int | None] = []

    def tail(self, session_id: str, after: int | None) -> AsyncIterator[Event]:
        self.tail_after.append(after)

        async def _gen() -> AsyncIterator[Event]:
            yield _event(9, EventType.run_ended, "web:local")

        return _gen()


def test_local_preview_stream_honors_last_event_id() -> None:
    # Finding 6: the local-preview live path must apply the resume cursor too, so a reconnect
    # with Last-Event-ID resumes exactly where it dropped rather than replaying from the start.
    from keel_server.app import create_app

    runtime = _RecordingRuntime()
    app = create_app()
    app.state.engine = None
    app.state.runtime = runtime
    app.state.runs = InMemoryRunStore()
    app.state.durable_approvals = InMemoryApprovalStore()
    app.state.durable_scope = "web:local"
    app.state.auth_required = False  # non-cloud local preview -> web:local scope
    app.state.api_keys = parse_api_keys("vkey:viewer")
    client = TestClient(app)

    resp = client.get(
        "/v1/sessions/s1/events",
        headers={"X-API-Key": "vkey", "Last-Event-ID": "5"},
    )
    assert resp.status_code == 200
    # The Last-Event-ID header (5) is the cursor handed to the live tail, not the default None.
    assert runtime.tail_after == [5]
