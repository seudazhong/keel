"""Manifest-driven connector catalog, setup, lifecycle, sync, and ingress routes."""

from __future__ import annotations

import html
from typing import Annotated, Any, cast
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

from keel_core.config import get_settings
from keel_core.connector_contracts import (
    ConnectorAuthenticationError,
    ConnectorError,
    ConnectorIngressRequest,
    ConnectorSetupArtifact,
    ConnectorSetupArtifactKind,
    ConnectorUnsupportedError,
)
from keel_core.connector_credentials import ConnectorCredentialStore
from keel_core.connector_registry import (
    ConnectorProviderUnavailableError,
    get_connector_registry,
)
from keel_core.connector_repository import (
    InMemoryConnectorRepository,
    PostgresConnectorRepository,
)
from keel_core.connector_service import ConnectorService, artifact_to_dict
from keel_core.oauth_state import InMemoryOAuthStateStore, OAuthState, PostgresOAuthStateStore
from keel_core.outbox import purge_connector as purge_outbound_connector
from keel_core.secrets import SecretsError, keyring_from_settings
from keel_core.tokens import PostgresTokenStore, delete_token, list_connected
from keel_server.endpoint_auth import (
    EndpointAuth,
    EndpointPrivilege,
    require_privilege,
)

router = APIRouter(prefix="/v1/connectors", tags=["connectors"])


class SetupRequest(BaseModel):
    values: dict[str, str] = Field(default_factory=dict)


class ResourceSelectionRequest(BaseModel):
    external_ids: list[str] = Field(default_factory=list)


class TargetConfigurationRequest(BaseModel):
    targets: dict[str, str | None] = Field(default_factory=dict)


class SyncRequest(BaseModel):
    idempotency_key: str | None = None


def _default_scope(request: Request) -> str:
    """The app-global single-tenant scope (``web:local``) for the unauthenticated ingress path."""
    return str(getattr(request.app.state, "durable_scope", "web:local"))


def _repository(request: Request, scope: str) -> Any:
    """The connector repository bound to ``scope``.

    Reuses the app-global ``connector_repository`` only when it already targets ``scope`` (the
    single-tenant ``web:local`` deployment and the test doubles), otherwise builds a scope-bound
    repository so an authenticated per-Agent call never reads another scope's bindings.
    """
    repository = getattr(request.app.state, "connector_repository", None)
    if repository is not None and getattr(repository, "scope_id", None) == scope:
        return repository
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        return PostgresConnectorRepository(engine, scope)
    if repository is not None:
        return repository
    repository = InMemoryConnectorRepository(scope)
    request.app.state.connector_repository = repository
    return repository


def _credential_store(request: Request, scope: str) -> ConnectorCredentialStore | None:
    configured = getattr(request.app.state, "connector_credentials", None)
    if configured is not None:
        return cast(ConnectorCredentialStore, configured)
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        return None
    settings = get_settings()
    if not settings.secret_key and not settings.secret_keys:
        return None
    try:
        token_store = PostgresTokenStore(
            engine,
            scope,
            keyring_from_settings(settings),
        )
    except SecretsError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "encrypted connector credential storage is unavailable",
        ) from exc
    return ConnectorCredentialStore(token_store)


def _service(request: Request, scope: str) -> ConnectorService:
    """Resolve a connector service bound to the caller's derived data-plane ``scope``.

    In a full deployment the app installs a ``connector_scope_factory`` that builds every
    scope-bound collaborator (repository, encrypted credentials, job store, durable change sink,
    typed-target validator) from the canonical ``auth.scope_id`` — so an authenticated management
    call binds the caller's per-Agent scope, never the app-global ``web:local`` singleton. Lite
    apps and unit doubles (no factory) fall back to the app-global collaborators, which target the
    same ``web:local`` scope the caller resolves to.
    """
    factory = getattr(request.app.state, "connector_scope_factory", None)
    if factory is not None:
        return cast(ConnectorService, factory(scope))

    async def dispatch(dispatch_scope: str, job_id: str) -> None:
        enqueue = getattr(request.app.state, "enqueue", None)
        if enqueue is not None:
            await enqueue("run_job", dispatch_scope, job_id)

    engine = getattr(request.app.state, "engine", None)

    async def delete_credential(connector_id: str) -> bool:
        if engine is None:
            return False
        return await delete_token(engine, scope, connector_id)

    async def purge_outbound(connector_id: str) -> int:
        if engine is None:
            return 0
        return await purge_outbound_connector(engine, scope, connector_id)

    return ConnectorService(
        _registry(request),
        _repository(request, scope),
        credentials=_credential_store(request, scope),
        jobs=getattr(request.app.state, "jobs", None),
        dispatch_job=dispatch,
        dispatch_outbox=getattr(request.app.state, "job_dispatch_outbox", None),
        schedule_index=getattr(request.app.state, "connector_schedule_index", None),
        webhook_route_store=getattr(request.app.state, "connector_webhook_route_store", None),
        change_sink=getattr(request.app.state, "connector_change_sink", None),
        target_validator=getattr(request.app.state, "connector_target_validator", None),
        delete_credential=delete_credential,
        purge_outbound=purge_outbound,
    )


def _registry(request: Request) -> Any:
    return getattr(request.app.state, "connector_registry", None) or get_connector_registry()


def _provider(connector_id: str, request: Request) -> Any:
    try:
        return _registry(request).create(connector_id)
    except KeyError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connector") from exc
    except ConnectorProviderUnavailableError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc


def _connector_auth_state_store(request: Request) -> Any:
    store = getattr(request.app.state, "oauth_state_store", None)
    if store is not None:
        return store
    settings = get_settings()
    engine = getattr(request.app.state, "engine", None)
    store = (
        PostgresOAuthStateStore(engine, ttl_seconds=settings.oauth_state_ttl_seconds)
        if engine is not None
        else InMemoryOAuthStateStore(ttl_seconds=settings.oauth_state_ttl_seconds)
    )
    request.app.state.oauth_state_store = store
    return store


def _callback_url(request: Request, connector_id: str) -> str:
    return str(request.url_for("connector_callback", connector_id=connector_id))


def _connector_callback_base_url(request: Request, connector_id: str) -> str:
    webhook_url = str(request.url_for("connector_webhook", connector_id=connector_id))
    return webhook_url.removesuffix("/webhook")


@router.get(
    "",
    summary="List connector manifests and scope status",
)
async def list_connectors(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> list[dict[str, Any]]:
    legacy: dict[str, Any] = {}
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        legacy = {
            item.connector_id: item.updated_at
            for item in await list_connected(engine, auth.scope_id)
        }
    return await _service(request, auth.scope_id).catalog(legacy)


def _cloud_mode(request: Request) -> bool:
    """Whether the deployment runs in cloud (auth-required) mode."""
    return bool(getattr(request.app.state, "auth_required", False))


class ConnectUrlResponse(BaseModel):
    """The provider consent URL for the browser to open in a new tab/window."""

    url: str


async def _mint_connect_url(request: Request, connector_id: str, auth: EndpointAuth) -> str:
    """Build the provider consent URL and persist the one-time, scope-bound CSRF state.

    Shared by the JSON ``POST /connect-url`` (the cloud-safe path a browser calls with auth
    headers via ``fetch``) and the legacy ``GET /connect`` redirect (local preview only). The
    state is bound to the caller's canonical per-Agent data-plane scope (``auth.scope_id``),
    never the app-global ``web:local`` singleton, so the token the callback stores lands in
    exactly the org/Agent that initiated the connect.
    """
    provider = _provider(connector_id, request)
    if provider.manifest.auth_action is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"{provider.manifest.id} does not declare browser authorization",
        )
    try:
        start = await _service(request, auth.scope_id).begin_auth(
            connector_id,
            _callback_url(request, connector_id),
        )
    except ConnectorUnsupportedError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except ConnectorAuthenticationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    parsed = urlsplit(start.url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or not start.state.strip():
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "connector authorization start returned invalid instructions",
        )
    await _connector_auth_state_store(request).put(
        start.state,
        auth.scope_id,
        connector_id,
        start.metadata,
    )
    return start.url


@router.post(
    "/{connector_id}/connect-url",
    summary="Create the connector consent URL (authenticated JSON; browser opens it)",
    response_model=ConnectUrlResponse,
)
async def connector_connect_url(
    connector_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> ConnectUrlResponse:
    """Return only the provider consent URL after minting a one-time, scope-bound CSRF state.

    This is the cloud-correct entry point: a browser cannot attach ``Authorization`` /
    ``X-API-Key`` / ``X-Keel-Org`` / ``X-Keel-Agent`` headers to a top-level navigation, so the
    authenticated ``GET /connect`` redirect below is unusable in a cloud deployment. Instead the
    SPA calls this endpoint with its auth headers (``fetch``), receives the consent URL, and opens
    it with ``window.open`` — the interactive redirect to the provider then happens client-side
    while the trust decision (mint the state) stays behind full endpoint auth. The callback
    remains state-bound and unauthenticated.
    """
    return ConnectUrlResponse(url=await _mint_connect_url(request, connector_id, auth))


@router.get(
    "/{connector_id}/connect",
    summary="Start browser-based connector authorization (local preview only)",
)
async def connector_connect(
    connector_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> RedirectResponse:
    """Redirect the browser to the provider consent screen (local-preview single operator only).

    In **cloud mode** this GET fails closed: a browser navigation cannot carry the auth headers
    that bind the connect to a specific org/Agent, so honoring it would either be unauthenticated
    or silently bind to the wrong (ambient) scope. Cloud callers must use the authenticated
    ``POST /connect-url`` above and open the returned URL. The GET remains only for the non-cloud
    local-preview single operator, where there is one tenant and no header-borne scope selection.
    """
    if _cloud_mode(request):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "use POST /v1/connectors/{connector_id}/connect-url (an authenticated JSON call) "
            "in cloud mode",
        )
    return RedirectResponse(await _mint_connect_url(request, connector_id, auth))


@router.get(
    "/{connector_id}/callback",
    name="connector_callback",
    summary="Complete browser-based connector authorization",
)
async def connector_callback(
    connector_id: str,
    request: Request,
    state: str = Query(...),
) -> HTMLResponse:
    provider = _provider(connector_id, request)
    action = provider.manifest.auth_action
    if action is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"{provider.manifest.id} does not declare browser authorization",
        )
    state_values = request.query_params.getlist("state")
    if len(state_values) != 1 or len(state.encode("utf-8")) > 4096:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid callback state")
    parameters: dict[str, str] = {"state": state}
    for field in action.callback_parameters:
        values = request.query_params.getlist(field.id)
        if len(values) > 1:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"duplicate callback parameter: {field.id}",
            )
        value = values[0].strip() if values else ""
        if len(value.encode("utf-8")) > 4096:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"callback parameter is too large: {field.id}",
            )
        if field.required and not value:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"missing callback parameter: {field.id}",
            )
        if value:
            parameters[field.id] = value
    consumed: OAuthState | None = await _connector_auth_state_store(request).consume(state)
    if consumed is None or consumed.connector_id != connector_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid or expired authorization state")
    parameters.update(consumed.metadata)
    try:
        # The callback is intentionally unauthenticated (the browser returning from the provider
        # carries no API key/JWT). Trust and scope are anchored in the one-time ``state`` minted by
        # the operator-authenticated ``/connect`` above, so the token is stored into exactly the
        # canonical scope that initiated the connect (``consumed.scope_id``), never ``web:local``.
        outcome = await _service(request, consumed.scope_id).complete_auth(
            connector_id,
            _callback_url(request, connector_id),
            parameters,
        )
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    except ConnectorUnsupportedError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except ConnectorAuthenticationError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    label = html.escape(provider.manifest.name)
    artifacts = "".join(_artifact_html(item) for item in outcome.artifacts)
    close_script = (
        ""
        if any(item.kind is ConnectorSetupArtifactKind.secret for item in outcome.artifacts)
        else "<script>setTimeout(()=>window.close(),1500)</script>"
    )
    state_message = "已连接" if outcome.binding.status.value == "connected" else "授权状态已保存"
    return HTMLResponse(
        "<!doctype html><meta charset=utf-8>"
        "<body style='font:16px system-ui;padding:40px'>"
        f"✅ {label} {state_message}。可关闭此标签页并返回 Keel 的 Connectors 页面刷新。"
        f"{artifacts}"
        f"{close_script}</body>",
        headers={
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
            "Referrer-Policy": "no-referrer",
        },
    )


@router.post(
    "/{connector_id}/setup",
    summary="Run manifest-declared connector setup",
)
async def connector_setup(
    connector_id: str,
    body: SetupRequest,
    request: Request,
    response: Response,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        outcome = await _service(request, auth.scope_id).setup(
            connector_id,
            body.values,
            callback_base_url=_connector_callback_base_url(request, connector_id),
        )
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    except ConnectorUnsupportedError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return {
        "ok": True,
        "binding_id": outcome.binding.id,
        "status": outcome.binding.status.value,
        "artifacts": [artifact_to_dict(item) for item in outcome.artifacts],
    }


@router.put(
    "/{connector_id}/targets",
    summary="Configure typed connector destination and trigger targets",
)
async def configure_connector_targets(
    connector_id: str,
    body: TargetConfigurationRequest,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        targets = await _service(request, auth.scope_id).configure_targets(
            connector_id, body.targets
        )
    except LookupError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    return {
        "ok": True,
        "targets": {item.kind.value: item.target_id for item in targets},
    }


@router.get(
    "/{connector_id}/resources",
    summary="List selectable connector resources",
)
async def connector_resources(
    connector_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
    refresh: bool = Query(True),
) -> list[dict[str, Any]]:
    _provider(connector_id, request)
    try:
        if refresh:
            return await _service(request, auth.scope_id).refresh_resources(connector_id)
        return [
            {
                "id": item.id,
                "external_id": item.external_id,
                "kind": item.kind,
                "display_name": item.display_name,
                "url": item.url,
                "selected": item.selected,
                "config": item.config,
            }
            for item in await _repository(request, auth.scope_id).list_resources(connector_id)
        ]
    except (LookupError, RuntimeError) as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc


@router.put(
    "/{connector_id}/resources",
    summary="Select connector resources",
)
async def select_connector_resources(
    connector_id: str,
    body: ResourceSelectionRequest,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        changed = await _service(request, auth.scope_id).select_resources(
            connector_id, set(body.external_ids)
        )
    except (LookupError, ValueError) as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    return {"ok": True, "changed": changed}


@router.post(
    "/{connector_id}/sync",
    summary="Queue a durable connector sync",
)
async def sync_connector(
    connector_id: str,
    body: SyncRequest,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        job = await _service(request, auth.scope_id).enqueue_sync(
            connector_id, idempotency_key=body.idempotency_key
        )
    except (LookupError, ValueError) as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    return {"ok": True, "job_id": job.id, "status": job.status.value}


@router.get(
    "/{connector_id}/health",
    summary="Check connector health",
)
async def connector_health(
    connector_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        health = await _service(request, auth.scope_id).health(connector_id)
    except LookupError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    return {
        "status": health.status.value,
        "checked_at": health.checked_at.isoformat(),
        "message": health.message,
        "retryable": health.retryable,
    }


@router.delete(
    "/{connector_id}",
    summary="Revoke connector credentials and binding",
)
async def revoke_connector(
    connector_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    try:
        return {"ok": await _service(request, auth.scope_id).revoke(connector_id)}
    except KeyError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connector") from exc
    except (ConnectorError, LookupError, RuntimeError, ValueError) as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, "connector revoke failed; credentials were retained"
        ) from exc


@router.delete(
    "/{connector_id}/purge",
    summary="Revoke and purge connector-owned state",
)
async def revoke_and_purge_connector(
    connector_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    try:
        return {"ok": await _service(request, auth.scope_id).revoke(connector_id, purge=True)}
    except KeyError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connector") from exc
    except (ConnectorError, LookupError, RuntimeError, ValueError) as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            "connector revoke-and-purge failed; credentials were retained",
        ) from exc


@router.delete(
    "/{connector_id}/local",
    summary="Forget local connector credentials and binding without remote revoke",
)
async def forget_local_connector(
    connector_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    try:
        return {"ok": await _service(request, auth.scope_id).revoke(connector_id, local_only=True)}
    except (LookupError, RuntimeError, ValueError) as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "connector local forget failed; credentials were retained",
        ) from exc


@router.delete(
    "/{connector_id}/purge/local",
    summary="Force local purge without loading or revoking the remote provider",
)
async def force_local_purge_connector(
    connector_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    try:
        return {
            "ok": await _service(request, auth.scope_id).revoke(
                connector_id,
                purge=True,
                local_only=True,
            )
        }
    except (LookupError, RuntimeError, ValueError) as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "connector forced local purge failed; credentials were retained",
        ) from exc


def _webhook_route_store(request: Request) -> Any:
    return getattr(request.app.state, "connector_webhook_route_store", None)


async def _ingress_in_scope(connector_id: str, request: Request, scope: str) -> Response:
    """Run the provider webhook ingress bound to the resolved ``scope``.

    The provider-specific signature / endpoint-token / replay verification runs inside
    ``service.ingress`` against ``scope``'s bound credential, so resolving the route token only
    selects *which* scope's connector service handles the delivery — it is never itself an
    authorization.
    """
    _provider(connector_id, request)
    body = await request.body()
    if len(body) > 1_048_576:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "webhook body is too large")
    try:
        query: dict[str, tuple[str, ...]] = {
            key: tuple(request.query_params.getlist(key)) for key in request.query_params
        }
        outcome = await _service(request, scope).ingress(
            connector_id,
            ConnectorIngressRequest(
                method=request.method,
                query=query,
                headers={key.lower(): value for key, value in request.headers.items()},
                body=body,
                public_url=str(request.url),
            ),
        )
    except ConnectorUnsupportedError as exc:
        raise HTTPException(status.HTTP_405_METHOD_NOT_ALLOWED, str(exc)) from exc
    except ConnectorAuthenticationError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except (LookupError, RuntimeError) as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    return Response(
        content=outcome.response.body,
        status_code=outcome.response.status_code,
        headers={
            **dict(outcome.response.headers),
            "Content-Type": outcome.response.content_type,
        },
    )


async def _dispatch_connector_webhook(connector_id: str, request: Request) -> Response:
    """Legacy, tokenless webhook ingress bound to the single-tenant ``web:local`` scope.

    Retained for backward compatibility with local-preview single-tenant deployments (documented).
    In **cloud mode** it fails closed with an opaque 404: a delivery carries no scope, so honoring
    it would either be scopeless or silently bind to ``web:local`` — cloud callers must use the
    routed ``/webhook/r/{route_token}`` capability minted at setup instead.
    """
    if _cloud_mode(request):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connector webhook route")
    return await _ingress_in_scope(connector_id, request, _default_scope(request))


async def _dispatch_routed_webhook(
    connector_id: str, route_token: str, request: Request
) -> Response:
    """Resolve a high-entropy webhook route token globally, then ingress in its bound scope.

    The token maps to ``(scope_id, connector_id, binding_id)`` in the global routing capability
    table. An unknown token, or a token whose connector does not match the request path (a
    cross-provider / cross-scope mismatch), fails closed with an opaque 404 so a valid scope is not
    enumerable. The bound scope's connector service then performs the provider-specific
    verification.
    """
    store = _webhook_route_store(request)
    if store is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connector webhook route")
    route = await store.resolve(route_token)
    if route is None or route.connector_id != connector_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connector webhook route")
    return await _ingress_in_scope(connector_id, request, route.scope_id)


# The provider webhook accepts GET (verification challenges), POST, and PUT (deliveries). These
# are registered as three explicit routes rather than a single multi-method ``api_route`` so each
# operation gets a stable, unique OpenAPI ``operationId`` (a multi-method route emits one shared,
# hash-order-dependent id, which is both invalid OpenAPI and non-deterministic across processes).
# ``name="connector_webhook"`` on one route keeps ``url_for('connector_webhook', ...)`` resolvable.
@router.get(
    "/{connector_id}/webhook",
    name="connector_webhook",
    summary="Verify a provider webhook",
    operation_id="connector_webhook_get",
)
async def connector_webhook_get(connector_id: str, request: Request) -> Response:
    return await _dispatch_connector_webhook(connector_id, request)


@router.post(
    "/{connector_id}/webhook",
    summary="Dispatch a provider webhook delivery",
    operation_id="connector_webhook_post",
)
async def connector_webhook_post(connector_id: str, request: Request) -> Response:
    return await _dispatch_connector_webhook(connector_id, request)


@router.put(
    "/{connector_id}/webhook",
    summary="Dispatch a provider webhook delivery",
    operation_id="connector_webhook_put",
)
async def connector_webhook_put(connector_id: str, request: Request) -> Response:
    return await _dispatch_connector_webhook(connector_id, request)


# Routed (scope-bound) webhook: the URL handed to the provider at setup embeds a high-entropy
# route token so an auth-headerless delivery resolves to the exact org/Agent scope + binding. The
# ``r/{route_token}`` segment mirrors the ``callback_base_url`` the service threads into providers
# (which build ``{callback_base_url}/webhook``), so the delivered path is
# ``/v1/connectors/{connector_id}/r/{route_token}/webhook``.
@router.get(
    "/{connector_id}/r/{route_token}/webhook",
    name="connector_routed_webhook",
    summary="Verify a scope-routed provider webhook",
    operation_id="connector_routed_webhook_get",
)
async def connector_routed_webhook_get(
    connector_id: str, route_token: str, request: Request
) -> Response:
    return await _dispatch_routed_webhook(connector_id, route_token, request)


@router.post(
    "/{connector_id}/r/{route_token}/webhook",
    summary="Dispatch a scope-routed provider webhook delivery",
    operation_id="connector_routed_webhook_post",
)
async def connector_routed_webhook_post(
    connector_id: str, route_token: str, request: Request
) -> Response:
    return await _dispatch_routed_webhook(connector_id, route_token, request)


@router.put(
    "/{connector_id}/r/{route_token}/webhook",
    summary="Dispatch a scope-routed provider webhook delivery",
    operation_id="connector_routed_webhook_put",
)
async def connector_routed_webhook_put(
    connector_id: str, route_token: str, request: Request
) -> Response:
    return await _dispatch_routed_webhook(connector_id, route_token, request)


# Some providers (e.g. a GitHub App manifest) register both a webhook and a post-install callback
# derived from the same routed ``callback_base_url``; the callback still anchors trust in the
# one-time OAuth state, so this routed alias delegates to the standard callback handler.
@router.get(
    "/{connector_id}/r/{route_token}/callback",
    name="connector_routed_callback",
    summary="Complete browser-based connector authorization (scope-routed alias)",
    operation_id="connector_routed_callback_get",
)
async def connector_routed_callback(
    connector_id: str, route_token: str, request: Request, state: str = Query(...)
) -> HTMLResponse:
    return await connector_callback(connector_id, request, state)


def _artifact_html(artifact: ConnectorSetupArtifact) -> str:
    label = html.escape(artifact.label)
    value = html.escape(artifact.value)
    if artifact.kind is ConnectorSetupArtifactKind.url:
        parsed = urlsplit(artifact.value)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return (
                f"<p><strong>{label}:</strong> <a rel='noreferrer' href='{value}'>{value}</a></p>"
            )
    return f"<p><strong>{label}:</strong> <code>{value}</code></p>"


__all__ = ["router"]
