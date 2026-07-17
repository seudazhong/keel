"""Gateway webhook verification + replay integration (ASGI) tests (M3.3)."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

from keel_core.config import get_settings
from keel_core.webhooks import InMemoryWebhookReplayStore
from keel_server.api.gateway import router


class _FakeGateway:
    def __init__(self) -> None:
        self.handled: list[dict] = []

    async def handle(self, payload: dict) -> None:
        self.handled.append(payload)


@pytest_asyncio.fixture
async def gw_app() -> AsyncIterator[tuple[httpx.AsyncClient, FastAPI]]:
    app = FastAPI()
    app.include_router(router)
    app.state.onebot_gateway = _FakeGateway()
    app.state.telegram_gateway = _FakeGateway()
    app.state.webhook_replay_store = InMemoryWebhookReplayStore()
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, app


def _sign(secret: str, body: bytes) -> str:
    return "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()


async def test_onebot_rejects_bad_signature(
    gw_app: tuple[httpx.AsyncClient, FastAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, app = gw_app
    monkeypatch.setenv("KEEL_ONEBOT_SIGNING_SECRET", "s3cret")
    get_settings.cache_clear()
    try:
        body = {"post_type": "message", "message_type": "private", "user_id": 7, "raw_message": "h"}
        raw = json.dumps(body).encode()
        json_ct = {"Content-Type": "application/json"}
        # No signature -> 401.
        assert (
            await client.post("/v1/gateway/onebot", content=raw, headers=json_ct)
        ).status_code == 401
        # Valid signature -> accepted + dispatched.
        resp = await client.post(
            "/v1/gateway/onebot",
            content=raw,
            headers={**json_ct, "X-Signature": _sign("s3cret", raw)},
        )
        assert resp.status_code == 202 and resp.json()["accepted"] is True
        assert app.state.onebot_gateway.handled  # dispatched once
        # Exact replay -> dropped (accepted=False), not re-dispatched.
        replay = await client.post(
            "/v1/gateway/onebot",
            content=raw,
            headers={**json_ct, "X-Signature": _sign("s3cret", raw)},
        )
        assert replay.json()["accepted"] is False
        assert len(app.state.onebot_gateway.handled) == 1
    finally:
        get_settings.cache_clear()


async def test_telegram_secret_and_replay(
    gw_app: tuple[httpx.AsyncClient, FastAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, app = gw_app
    monkeypatch.setenv("KEEL_TELEGRAM_WEBHOOK_SECRET", "tok")
    get_settings.cache_clear()
    try:
        payload = {
            "update_id": 100,
            "message": {"text": "hi", "chat": {"id": 5, "type": "private"}},
        }
        # Wrong secret header -> 401.
        bad = await client.post(
            "/v1/gateway/telegram",
            json=payload,
            headers={"X-Telegram-Bot-Api-Secret-Token": "nope"},
        )
        assert bad.status_code == 401
        ok = await client.post(
            "/v1/gateway/telegram",
            json=payload,
            headers={"X-Telegram-Bot-Api-Secret-Token": "tok"},
        )
        assert ok.status_code == 202 and ok.json()["accepted"] is True
        # Replay of the same update_id -> dropped.
        replay = await client.post(
            "/v1/gateway/telegram",
            json=payload,
            headers={"X-Telegram-Bot-Api-Secret-Token": "tok"},
        )
        assert replay.json()["accepted"] is False
        assert len(app.state.telegram_gateway.handled) == 1
    finally:
        get_settings.cache_clear()


async def test_no_secret_configured_allows_dispatch(
    gw_app: tuple[httpx.AsyncClient, FastAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a configured secret and cloud mode off, webhooks stay open (local dev)."""
    client, app = gw_app
    monkeypatch.delenv("KEEL_ONEBOT_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("KEEL_CLOUD_MODE", raising=False)
    get_settings.cache_clear()
    try:
        body = {"post_type": "message", "message_type": "private", "user_id": 7, "raw_message": "h"}
        resp = await client.post("/v1/gateway/onebot", json=body)
        assert resp.status_code == 202 and resp.json()["accepted"] is True
    finally:
        get_settings.cache_clear()


async def test_cloud_mode_requires_secret(
    gw_app: tuple[httpx.AsyncClient, FastAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cloud mode fails closed when no signing secret is configured (503)."""
    client, app = gw_app
    monkeypatch.delenv("KEEL_ONEBOT_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("KEEL_CLOUD_MODE", "1")
    get_settings.cache_clear()
    try:
        body = {"post_type": "message", "message_type": "private", "user_id": 7, "raw_message": "h"}
        resp = await client.post("/v1/gateway/onebot", json=body)
        assert resp.status_code == 503
    finally:
        get_settings.cache_clear()
