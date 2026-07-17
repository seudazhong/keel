"""Manifest-driven connector catalog, setup, lifecycle, sync, and ingress routes."""

from __future__ import annotations

import html
from typing import Any, cast
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
from keel_server.auth import Role, require_role

router = APIRouter(prefix="/v1/connectors", tags=["connectors"])


class SetupRequest(BaseModel):
    values: dict[str, str] = Field(default_factory=dict)


class ResourceSelectionRequest(BaseModel):
    external_ids: list[str] = Field(default_factory=list)


class TargetConfigurationRequest(BaseModel):
    targets: dict[str, str | None] = Field(default_factory=dict)


class SyncRequest(BaseModel):
    idempotency_key: str | None = None


def _scope(request: Request) -> str:
    return str(getattr(request.app.state, "durable_scope", "web:local"))


def _repository(request: Request) -> Any:
    repository = getattr(request.app.state, "connector_repository", None)
    if repository is not None:
        return repository
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        repository = PostgresConnectorRepository(engine, _scope(request))
    else:
        repository = InMemoryConnectorRepository(_scope(request))
    request.app.state.connector_repository = repository
    return repository


def _credential_store(request: Request) -> ConnectorCredentialStore | None:
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
            _scope(request),
            keyring_from_settings(settings),
        )
    except SecretsError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "encrypted connector credential storage is unavailable",
        ) from exc
    return ConnectorCredentialStore(token_store)


def _service(request: Request) -> ConnectorService:
    async def dispatch(scope_id: str, job_id: str) -> None:
        enqueue = getattr(request.app.state, "enqueue", None)
        if enqueue is not None:
            await enqueue("run_job", scope_id, job_id)

    engine = getattr(request.app.state, "engine", None)

    async def delete_credential(connector_id: str) -> bool:
        if engine is None:
            return False
        return await delete_token(engine, _scope(request), connector_id)

    async def purge_outbound(connector_id: str) -> int:
        if engine is None:
            return 0
        return await purge_outbound_connector(engine, _scope(request), connector_id)

    return ConnectorService(
        _registry(request),
        _repository(request),
        credentials=_credential_store(request),
        jobs=getattr(request.app.state, "jobs", None),
        dispatch_job=dispatch,
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
    dependencies=[Depends(require_role(Role.viewer))],
)
async def list_connectors(request: Request) -> list[dict[str, Any]]:
    legacy: dict[str, Any] = {}
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        legacy = {
            item.connector_id: item.updated_at
            for item in await list_connected(engine, _scope(request))
        }
    return await _service(request).catalog(legacy)


@router.get(
    "/{connector_id}/connect",
    summary="Start browser-based connector authorization",
    dependencies=[Depends(require_role(Role.operator))],
)
async def connector_connect(connector_id: str, request: Request) -> RedirectResponse:
    provider = _provider(connector_id, request)
    if provider.manifest.auth_action is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"{provider.manifest.id} does not declare browser authorization",
        )
    try:
        start = await _service(request).begin_auth(
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
    await _connector_auth_state_store(request).put(start.state, _scope(request), connector_id)
    return RedirectResponse(start.url)


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
    try:
        outcome = await _service(request).complete_auth(
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
    state_message = (
        "已连接"
        if outcome.binding.status.value == "connected"
        else "授权状态已保存"
    )
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
    dependencies=[Depends(require_role(Role.operator))],
)
async def connector_setup(
    connector_id: str,
    body: SetupRequest,
    request: Request,
    response: Response,
) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        outcome = await _service(request).setup(
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
    dependencies=[Depends(require_role(Role.operator))],
)
async def configure_connector_targets(
    connector_id: str,
    body: TargetConfigurationRequest,
    request: Request,
) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        targets = await _service(request).configure_targets(connector_id, body.targets)
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
    dependencies=[Depends(require_role(Role.viewer))],
)
async def connector_resources(
    connector_id: str, request: Request, refresh: bool = Query(True)
) -> list[dict[str, Any]]:
    _provider(connector_id, request)
    try:
        if refresh:
            return await _service(request).refresh_resources(connector_id)
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
            for item in await _repository(request).list_resources(connector_id)
        ]
    except (LookupError, RuntimeError) as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc


@router.put(
    "/{connector_id}/resources",
    summary="Select connector resources",
    dependencies=[Depends(require_role(Role.operator))],
)
async def select_connector_resources(
    connector_id: str, body: ResourceSelectionRequest, request: Request
) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        changed = await _service(request).select_resources(connector_id, set(body.external_ids))
    except (LookupError, ValueError) as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    return {"ok": True, "changed": changed}


@router.post(
    "/{connector_id}/sync",
    summary="Queue a durable connector sync",
    dependencies=[Depends(require_role(Role.operator))],
)
async def sync_connector(connector_id: str, body: SyncRequest, request: Request) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        job = await _service(request).enqueue_sync(
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
    dependencies=[Depends(require_role(Role.viewer))],
)
async def connector_health(connector_id: str, request: Request) -> dict[str, Any]:
    _provider(connector_id, request)
    try:
        health = await _service(request).health(connector_id)
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
    dependencies=[Depends(require_role(Role.operator))],
)
async def revoke_connector(connector_id: str, request: Request) -> dict[str, bool]:
    try:
        return {"ok": await _service(request).revoke(connector_id)}
    except KeyError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connector") from exc
    except (ConnectorError, LookupError, RuntimeError, ValueError) as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, "connector revoke failed; credentials were retained"
        ) from exc


@router.delete(
    "/{connector_id}/purge",
    summary="Revoke and purge connector-owned state",
    dependencies=[Depends(require_role(Role.operator))],
)
async def revoke_and_purge_connector(connector_id: str, request: Request) -> dict[str, bool]:
    try:
        return {"ok": await _service(request).revoke(connector_id, purge=True)}
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
    dependencies=[Depends(require_role(Role.operator))],
)
async def forget_local_connector(connector_id: str, request: Request) -> dict[str, bool]:
    try:
        return {"ok": await _service(request).revoke(connector_id, local_only=True)}
    except (LookupError, RuntimeError, ValueError) as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "connector local forget failed; credentials were retained",
        ) from exc


@router.delete(
    "/{connector_id}/purge/local",
    summary="Force local purge without loading or revoking the remote provider",
    dependencies=[Depends(require_role(Role.operator))],
)
async def force_local_purge_connector(connector_id: str, request: Request) -> dict[str, bool]:
    try:
        return {
            "ok": await _service(request).revoke(
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


@router.api_route(
    "/{connector_id}/webhook",
    methods=["GET", "POST", "PUT"],
    summary="Dispatch or verify a provider webhook",
)
async def connector_webhook(connector_id: str, request: Request) -> Response:
    _provider(connector_id, request)
    body = await request.body()
    if len(body) > 1_048_576:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "webhook body is too large")
    try:
        query: dict[str, tuple[str, ...]] = {
            key: tuple(request.query_params.getlist(key))
            for key in request.query_params
        }
        outcome = await _service(request).ingress(
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
