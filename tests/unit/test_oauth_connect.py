"""In-browser Gmail OAuth connect flow: consent redirect + CSRF-checked callback."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

from keel_core.config import get_settings
from keel_server.api.oauth import router


@pytest_asyncio.fixture
async def oauth_client() -> AsyncIterator[httpx.AsyncClient]:
    app = FastAPI()
    app.include_router(router)
    app.state.durable_scope = "web:local"
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_callback_rejects_bad_state(oauth_client: httpx.AsyncClient) -> None:
    resp = await oauth_client.get(
        "/v1/connectors/gmail/callback", params={"state": "nope", "code": "x"}
    )
    assert resp.status_code == 400


async def test_connect_redirects_to_google(
    oauth_client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client_json = tmp_path / "client.json"
    client_json.write_text(
        json.dumps(
            {
                "installed": {
                    "client_id": "cid.apps.googleusercontent.com",
                    "client_secret": "sec",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["http://localhost"],
                }
            }
        )
    )
    monkeypatch.setenv("KEEL_GMAIL_CLIENT_SECRETS_PATH", str(client_json))
    get_settings.cache_clear()
    try:
        resp = await oauth_client.get("/v1/connectors/gmail/connect", follow_redirects=False)
        assert resp.status_code in (302, 307)
        assert "accounts.google.com" in resp.headers["location"]
        assert "gmail" in resp.headers["location"]
    finally:
        get_settings.cache_clear()
