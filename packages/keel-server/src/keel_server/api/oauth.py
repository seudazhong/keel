"""In-browser OAuth (authorization-code) connect flow for Gmail (WS-G / A2e).

``GET /v1/connectors/gmail/connect`` builds a Google consent URL (server callback as the
redirect) and 302s the browser to Google; ``GET /v1/connectors/gmail/callback`` exchanges
the code and stores ``Credentials.to_json()`` via the scope-bound, encrypted token store.
CSRF ``state`` is validated against a **durable, expiring, one-time** store (Postgres when
an engine is configured, else in-memory) so a legitimate callback survives a restart or a
second replica while a replayed/unknown state is rejected. Requires the OAuth client JSON
(``KEEL_GMAIL_CLIENT_SECRETS_PATH``) + ``KEEL_SECRET_KEY`` on the server.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from keel_core.config import get_settings
from keel_core.gmail import GMAIL_CONNECTOR_ID, GMAIL_SCOPES
from keel_core.oauth_state import (
    InMemoryOAuthStateStore,
    OAuthState,
    PostgresOAuthStateStore,
)
from keel_core.secrets import keyring_from_settings
from keel_core.tokens import PostgresTokenStore

router = APIRouter(prefix="/v1/connectors/gmail", tags=["oauth"])

_SUCCESS_HTML = (
    "<!doctype html><meta charset=utf-8>"
    "<body style='font:16px system-ui;padding:40px'>"
    "✅ Gmail 已连接。可关闭此标签页并返回 Keel 的 Connectors 页面刷新。"
    "<script>setTimeout(()=>window.close(),1500)</script></body>"
)


def _oauth_state_store(request: Request) -> Any:
    """The durable OAuth state store (Postgres when an engine is wired, else in-memory).

    Cached on ``app.state`` so the in-memory fallback is shared across the connect and
    callback requests within one process.
    """
    store = getattr(request.app.state, "oauth_state_store", None)
    if store is not None:
        return store
    settings = get_settings()
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        store = PostgresOAuthStateStore(engine, ttl_seconds=settings.oauth_state_ttl_seconds)
    else:
        store = InMemoryOAuthStateStore(ttl_seconds=settings.oauth_state_ttl_seconds)
    request.app.state.oauth_state_store = store
    return store


def _flow(redirect_uri: str) -> Any:
    from google_auth_oauthlib.flow import Flow

    settings = get_settings()
    return Flow.from_client_secrets_file(
        settings.gmail_client_secrets_path, scopes=list(GMAIL_SCOPES), redirect_uri=redirect_uri
    )


def _callback_uri(request: Request) -> str:
    return str(request.url_for("gmail_oauth_callback"))


@router.get("/connect", summary="Start the Gmail in-browser OAuth connect flow")
async def gmail_oauth_connect(request: Request) -> RedirectResponse:
    """Redirect the browser to Google's consent screen (server callback registered)."""
    flow = _flow(_callback_uri(request))
    auth_url, state = flow.authorization_url(
        access_type="offline", prompt="consent", include_granted_scopes="true"
    )
    scope = getattr(request.app.state, "durable_scope", "web:local")
    await _oauth_state_store(request).put(state, scope, GMAIL_CONNECTOR_ID)
    return RedirectResponse(auth_url)


@router.get("/callback", name="gmail_oauth_callback", summary="OAuth callback: store the token")
async def gmail_oauth_callback(
    request: Request, state: str = Query(...), code: str | None = Query(None)
) -> HTMLResponse:
    """Exchange the authorization code and store the connector token for the scope."""
    consumed: OAuthState | None = await _oauth_state_store(request).consume(state)
    if consumed is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid or expired oauth state")
    engine = getattr(request.app.state, "engine", None)
    if engine is None or not code:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "missing code or datastore")

    flow = _flow(_callback_uri(request))
    flow.fetch_token(code=code)
    store = PostgresTokenStore(engine, consumed.scope_id, keyring_from_settings(get_settings()))
    await store.put(consumed.connector_id, flow.credentials.to_json())
    return HTMLResponse(_SUCCESS_HTML)
