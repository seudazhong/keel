"""Contract freeze tests: event vocabulary, models, Protocols, REST v0 shape."""

from __future__ import annotations

from datetime import UTC, datetime

from keel_core.agents import AgentSpec, Scope
from keel_core.errors import CrossScopeError
from keel_core.events import Event, EventType, RunEndedPayload
from keel_core.protocols import Tool, ToolContext, ToolResult
from keel_core.types import ScopeKind, StopReason, TrustLevel

# --- event vocabulary v0 -------------------------------------------------------

EVENT_VOCABULARY_V0 = {
    "run.started",
    "run.ended",
    "turn.started",
    "turn.ended",
    "message.token",
    "message.thinking",
    "tool.call",
    "tool.result",
    "approval.requested",
    "approval.resolved",
    "error",
}


def test_event_vocabulary_is_frozen() -> None:
    assert {member.value for member in EventType} == EVENT_VOCABULARY_V0


def test_event_roundtrip_and_version_default() -> None:
    event = Event(
        type=EventType.run_ended,
        seq=1,
        session_id="s1",
        scope_id="sc1",
        ts=datetime.now(UTC),
        payload=RunEndedPayload(reason=StopReason.completed).model_dump(),
    )
    restored = Event.model_validate_json(event.model_dump_json())
    assert restored.version == 1  # G3: default schema version
    assert restored.type is EventType.run_ended
    assert restored.scope_id == "sc1"  # ADR-0009: scope carried on every event


# --- agent + scope model -------------------------------------------------------


def test_agent_spec_defaults() -> None:
    spec = AgentSpec(
        id="a1",
        name="Personal",
        model="openai/gpt-x",
        scope=Scope(id="u:1", kind=ScopeKind.personal, trust=TrustLevel.trusted),
    )
    assert spec.max_iterations == 40
    assert spec.connectors == []
    assert spec.scope.kind is ScopeKind.personal


# --- protocols -----------------------------------------------------------------


class _EchoTool:
    name = "echo"
    description = "Echo the input."

    def input_schema(self) -> dict[str, object]:
        return {"type": "object"}

    async def run(self, args: dict[str, object], ctx: ToolContext) -> ToolResult:
        return ToolResult(ok=True, output="")


def test_tool_is_runtime_checkable() -> None:
    assert isinstance(_EchoTool(), Tool)


def test_cross_scope_error_carries_scopes() -> None:
    err = CrossScopeError("actor", "resource")
    assert err.actor_scope == "actor"
    assert err.resource_scope == "resource"


# --- REST v0 contract shape ----------------------------------------------------


def test_openapi_freezes_v1_contract() -> None:
    from keel_server.app import create_app

    schema = create_app().openapi()
    paths = schema["paths"]

    assert "post" in paths["/v1/sessions/{session_id}/messages"]
    assert "get" in paths["/v1/sessions/{session_id}/events"]
    assert "post" in paths["/v1/approvals/{approval_id}"]

    # Every non-probe API route is namespaced under /v1 (additive-only, G14).
    # "/" is the web UI (excluded from the schema), so it never appears here.
    non_probe = [p for p in paths if p not in ("/health", "/readiness", "/")]
    assert non_probe and all(p.startswith("/v1") for p in non_probe)


def test_http_health_and_v1_requires_runtime() -> None:
    from fastapi.testclient import TestClient

    from keel_server.app import create_app

    client = TestClient(create_app())

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["service"] == "keel-server"

    # The contract route is implemented; without a started runtime it fails closed.
    resp = client.post("/v1/sessions/s1/messages", json={"content": "hi"})
    assert resp.status_code == 503
