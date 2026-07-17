"""SDK typed surface tests."""

from __future__ import annotations

import json

import httpx

from keel_sdk import CreateMessageRequest, KeelClient


async def test_client_uses_typed_message_dtos() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/sessions/session-1/messages"
        assert json.loads(request.content) == {"content": "hello"}
        return httpx.Response(
            202,
            json={"session_id": "session-1", "run_id": "run-1", "accepted": True},
        )

    async with httpx.AsyncClient(
        base_url="https://keel.test",
        transport=httpx.MockTransport(handler),
    ) as transport:
        client = KeelClient("https://ignored.test", client=transport)
        response = await client.create_message("session-1", CreateMessageRequest(content="hello"))

    assert response.session_id == "session-1"
    assert response.run_id == "run-1"


async def test_client_sends_bearer_api_key_without_exposing_it() -> None:
    observed_authorization: list[str | None] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        observed_authorization.append(request.headers.get("Authorization"))
        return httpx.Response(
            202,
            json={"session_id": "session-1", "run_id": "run-1", "accepted": True},
        )

    api_key = "test-key"
    async with httpx.AsyncClient(
        base_url="https://keel.test",
        transport=httpx.MockTransport(handler),
    ) as transport:
        client = KeelClient("https://ignored.test", api_key=api_key, client=transport)
        await client.create_message("session-1", CreateMessageRequest(content="hello"))

    assert observed_authorization == [f"Bearer {api_key}"]


async def test_client_identity_methods_send_org_header() -> None:
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["org"] = request.headers.get("X-Keel-Org")
        return httpx.Response(
            201,
            json={
                "id": "agt_1",
                "org_id": "org_1",
                "kind": "team",
                "owner_user_id": "usr_1",
                "name": "Bot",
                "persona": "",
                "status": "active",
                "version": 1,
            },
        )

    from keel_sdk import CreateAgentRequest

    async with httpx.AsyncClient(
        base_url="https://keel.test", transport=httpx.MockTransport(handler)
    ) as transport:
        client = KeelClient("https://ignored.test", client=transport)
        agent = await client.create_agent("org_1", CreateAgentRequest(kind="team", name="Bot"))

    assert agent.id == "agt_1"
    assert seen == {"path": "/v1/identity/agents", "org": "org_1"}
