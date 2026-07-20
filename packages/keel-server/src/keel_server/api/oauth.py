"""In-browser OAuth (authorization-code) connect flow for Gmail (WS-G / A2e).

``GET /v1/connectors/gmail/connect`` builds a Google consent URL (server callback as the
redirect) and 302s the browser to Google; it requires **operator** auth when API keys are
configured, since it mints the durable CSRF ``state``. ``GET /v1/connectors/gmail/callback``
exchanges the code and stores ``Credentials.to_json()`` via the scope-bound, encrypted
token store. The callback is intentionally unauthenticated (the browser carries no API
key); its trust is anchored in a **durable, expiring, one-time** ``state`` that only the
authenticated ``/connect`` could have created — validated against the store (Postgres when
an engine is configured, else in-memory) and consumed (deleted) on use, so a replayed or
unknown state is rejected. Requires the OAuth client JSON
(``KEEL_GMAIL_CLIENT_SECRETS_PATH``) + ``KEEL_SECRET_KEY`` on the server.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
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
from keel_server.api.connectors import ConnectUrlResponse
from keel_server.endpoint_auth import (
    EndpointAuth,
    EndpointPrivilege,
    require_privilege,
)

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


def _flow(redirect_uri: str, *, code_verifier: str | None = None) -> Any:
    from google_auth_oauthlib.flow import Flow

    settings = get_settings()
    return Flow.from_client_secrets_file(
        settings.gmail_client_secrets_path,
        scopes=list(GMAIL_SCOPES),
        redirect_uri=redirect_uri,
        code_verifier=code_verifier,
        autogenerate_code_verifier=code_verifier is None,
    )


def _callback_uri(request: Request) -> str:
    return str(request.url_for("gmail_oauth_callback"))


def _cloud_mode(request: Request) -> bool:
    """Whether the deployment runs in cloud (auth-required) mode."""
    return bool(getattr(request.app.state, "auth_required", False))


async def _mint_consent_url(request: Request, auth: EndpointAuth) -> str:
    """Build the Google consent URL and persist the one-time, scope-bound CSRF state.

    Shared by the JSON ``POST /connect-url`` (the cloud-safe path a browser calls with auth
    headers via ``fetch``) and the legacy ``GET /connect`` redirect (local preview only). The
    state is bound to the caller's **canonical per-Agent data-plane scope** (``auth.scope_id``),
    never the app-global ``web:local`` singleton, so the token the callback stores lands in
    exactly the org/Agent that initiated the connect.
    """
    flow = _flow(_callback_uri(request))
    auth_url, state = flow.authorization_url(
        access_type="offline", prompt="consent", include_granted_scopes="true"
    )
    verifier = getattr(flow, "code_verifier", None)
    metadata = (
        {"_pkce_code_verifier": str(verifier)} if isinstance(verifier, str) and verifier else {}
    )
    await _oauth_state_store(request).put(
        state,
        auth.scope_id,
        GMAIL_CONNECTOR_ID,
        metadata,
    )
    return str(auth_url)


@router.post(
    "/connect-url",
    summary="Create the Gmail OAuth consent URL (authenticated JSON; browser opens it)",
    response_model=ConnectUrlResponse,
)
async def gmail_oauth_connect_url(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> ConnectUrlResponse:
    """Return only the Google consent URL after minting a one-time, scope-bound CSRF state.

    This is the cloud-correct entry point: a browser cannot attach ``Authorization`` /
    ``X-API-Key`` / ``X-Keel-Org`` / ``X-Keel-Agent`` headers to a top-level navigation, so the
    authenticated ``GET /connect`` redirect below is unusable in a cloud deployment. Instead the
    SPA calls this endpoint with its auth headers (``fetch``), receives the consent URL, and
    opens it with ``window.open`` — the interactive redirect to Google then happens client-side
    while the trust decision (mint the state) stays behind full endpoint auth. The callback
    remains state-bound and unauthenticated.
    """
    return ConnectUrlResponse(url=await _mint_consent_url(request, auth))


@router.get(
    "/connect",
    summary="Start the Gmail in-browser OAuth connect flow (local preview only)",
)
async def gmail_oauth_connect(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> RedirectResponse:
    """Redirect the browser to Google's consent screen (local-preview single operator only).

    Requires at least **operator** privilege via the unified endpoint auth: initiating a
    connect flow mints the durable one-time CSRF ``state`` that the (necessarily
    unauthenticated) callback consumes, so this endpoint is the trust anchor of the flow and
    must not be reachable by an unauthenticated/insufficient-role caller.

    In **cloud mode** this GET fails closed: a browser navigation cannot carry the auth headers
    that bind the connect to a specific org/Agent, so honoring it would either be unauthenticated
    or silently bind to the wrong (ambient) scope. Cloud callers must use the authenticated
    ``POST /connect-url`` above and open the returned URL. The GET remains only for the non-cloud
    local-preview single operator, where there is one tenant and no header-borne scope selection.
    """
    if _cloud_mode(request):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "use POST /v1/connectors/gmail/connect-url (an authenticated JSON call) in cloud mode",
        )
    return RedirectResponse(await _mint_consent_url(request, auth))


@router.get("/callback", name="gmail_oauth_callback", summary="OAuth callback: store the token")
async def gmail_oauth_callback(
    request: Request, state: str = Query(...), code: str | None = Query(None)
) -> HTMLResponse:
    """Exchange the authorization code and store the connector token for the scope.

    Intentionally unauthenticated: the browser returning from Google carries no API key.
    Trust is instead anchored in the ``state`` — an unguessable, single-use, expiring
    token that only the **operator-authenticated** ``/connect`` above could have created
    and persisted (durable :class:`~keel_core.oauth_state.PostgresOAuthStateStore`).
    :meth:`consume` deletes the row as it validates it, so a stolen/replayed ``state`` is
    rejected. Without a matching live ``state`` the request is refused before any token
    exchange, so the callback cannot be driven by an anonymous caller.
    """
    consumed: OAuthState | None = await _oauth_state_store(request).consume(state)
    if consumed is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid or expired oauth state")
    engine = getattr(request.app.state, "engine", None)
    if engine is None or not code:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "missing code or datastore")

    verifier = consumed.metadata.get("_pkce_code_verifier", "").strip()
    if not verifier:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "missing OAuth PKCE verifier")
    flow = _flow(_callback_uri(request), code_verifier=verifier)
    flow.fetch_token(code=code)
    store = PostgresTokenStore(engine, consumed.scope_id, keyring_from_settings(get_settings()))
    await store.put(consumed.connector_id, flow.credentials.to_json())
    return HTMLResponse(_SUCCESS_HTML)
