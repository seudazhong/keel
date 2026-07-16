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
