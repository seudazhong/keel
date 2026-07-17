"""In-browser Gmail OAuth connect flow: consent redirect + CSRF-checked callback."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

from keel_core.config import get_settings
from keel_core.oauth_state import InMemoryOAuthStateStore
from keel_server.api.oauth import router
from keel_server.auth import parse_api_keys


@pytest_asyncio.fixture
async def oauth_client() -> AsyncIterator[httpx.AsyncClient]:
    app = FastAPI()
    app.include_router(router)
    app.state.durable_scope = "web:local"
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _write_client_secrets(tmp_path: Path) -> Path:
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
    return client_json


async def test_callback_rejects_bad_state(oauth_client: httpx.AsyncClient) -> None:
    resp = await oauth_client.get(
        "/v1/connectors/gmail/callback", params={"state": "nope", "code": "x"}
    )
    assert resp.status_code == 400


async def test_connect_redirects_to_google(
    oauth_client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client_json = _write_client_secrets(tmp_path)
    monkeypatch.setenv("KEEL_GMAIL_CLIENT_SECRETS_PATH", str(client_json))
    get_settings.cache_clear()
    try:
        resp = await oauth_client.get("/v1/connectors/gmail/connect", follow_redirects=False)
        assert resp.status_code in (302, 307)
        assert "accounts.google.com" in resp.headers["location"]
        assert "gmail" in resp.headers["location"]
    finally:
        get_settings.cache_clear()


@pytest_asyncio.fixture
async def keyed_oauth_app(tmp_path: Path) -> AsyncIterator[tuple[httpx.AsyncClient, FastAPI]]:
    """An app with API-key auth enabled and a durable (in-memory) OAuth state store."""
    app = FastAPI()
    app.include_router(router)
    app.state.durable_scope = "web:local"
    app.state.api_keys = parse_api_keys("op-key:operator,vw-key:viewer")
    app.state.oauth_state_store = InMemoryOAuthStateStore()
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, app


async def test_connect_requires_auth_when_keys_configured(
    keyed_oauth_app: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    """With auth enabled, unauthenticated / viewer initiation is rejected before any flow."""
    client, _ = keyed_oauth_app
    # No key -> 401 (missing), before any Google flow / state creation.
    missing = await client.get("/v1/connectors/gmail/connect", follow_redirects=False)
    assert missing.status_code == 401
    # Viewer role is below operator -> 403.
    viewer = await client.get(
        "/v1/connectors/gmail/connect",
        headers={"X-API-Key": "vw-key"},
        follow_redirects=False,
    )
    assert viewer.status_code == 403


async def test_operator_can_initiate_and_create_one_time_state(
    keyed_oauth_app: tuple[httpx.AsyncClient, FastAPI],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An operator initiation redirects to Google and persists a consumable one-time state."""
    client, app = keyed_oauth_app
    monkeypatch.setenv("KEEL_GMAIL_CLIENT_SECRETS_PATH", str(_write_client_secrets(tmp_path)))
    get_settings.cache_clear()
    try:
        resp = await client.get(
            "/v1/connectors/gmail/connect",
            headers={"X-API-Key": "op-key"},
            follow_redirects=False,
        )
        assert resp.status_code in (302, 307)
        location = resp.headers["location"]
        assert "accounts.google.com" in location
        state = parse_qs(urlparse(location).query)["state"][0]
        # The authenticated initiation created the durable one-time state the callback needs.
        consumed = await app.state.oauth_state_store.consume(state)
        assert consumed is not None
        assert consumed.scope_id == "web:local" and consumed.connector_id == "gmail"
    finally:
        get_settings.cache_clear()
