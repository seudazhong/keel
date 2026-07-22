"""REST API: the durable Effect ledger (R1B, C4/C5) — read + operator-authorized
reconciliation/retry requests.

Additive-only under ``/v1`` (G14). Listing/reading an Effect requires ``viewer``;
requesting reconciliation or confirming retry eligibility requires ``operator`` (these
touch a durable outbound-mutation record, not merely read it). No endpoint here ever
executes a provider mutation itself — that stays owned by
:class:`~keel_core.connectors.ConnectorTool` inside a run. ``/reconcile`` drives one
on-demand provider reconciliation attempt for a single ``unknown`` Effect (bypassing the
worker cron's batch cadence); ``/retry`` only confirms/reports retry eligibility — the
actual retried mutation happens the next time the owning tool is invoked with the same
idempotency key (the API has no copy of the original call's arguments/credentials to
safely re-execute it out of band).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

from keel_core.config import get_settings
from keel_core.connector_contracts import (
    ConnectorActionContext,
    ConnectorReconciliationOutcome,
    ConnectorReconciliationRequest,
    reconciliation_result,
)
from keel_core.connector_credentials import ConnectorCredentialStore
from keel_core.connector_registry import ConnectorProviderUnavailableError, get_connector_registry
from keel_core.connector_repository import PostgresConnectorRepository
from keel_core.effect_outbox import PostgresEffectReconciliationOutbox
from keel_core.effect_store import EffectStore, InMemoryEffectStore, PostgresEffectStore
from keel_core.effects import EffectRecord, EffectStatus, is_retryable
from keel_core.secrets import SecretsError, keyring_from_settings
from keel_core.tokens import PostgresTokenStore
from keel_server.endpoint_auth import EndpointAuth, EndpointPrivilege, require_privilege

router = APIRouter(prefix="/v1/effects", tags=["effects"])


class EffectResponse(BaseModel):
    """An Effect ledger row — never carries a secret; ``canonical_args`` is either the
    call's canonical JSON or a ``sha256:<hex>`` safe digest (see
    ``keel_core.effects.canonical_args_or_digest``)."""

    id: str
    org_id: str
    agent_id: str
    actor_id: str
    run_id: str
    tool_name: str
    provider: str
    resource_id: str
    action_name: str
    action_hash: str
    idempotency_key: str
    canonical_args: str
    status: str
    attempt: int
    provider_ref: str
    result: str
    error: str
    reconciliation_attempts: int
    reconciled_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record: EffectRecord) -> EffectResponse:
        return cls(
            id=record.id,
            org_id=record.org_id,
            agent_id=record.agent_id,
            actor_id=record.actor_id,
            run_id=record.run_id,
            tool_name=record.tool_name,
            provider=record.provider,
            resource_id=record.resource_id,
            action_name=record.action_name,
            action_hash=record.action_hash,
            idempotency_key=record.idempotency_key,
            canonical_args=record.canonical_args,
            status=record.status.value,
            attempt=record.attempt,
            provider_ref=record.provider_ref,
            result=record.result,
            error=record.error,
            reconciliation_attempts=record.reconciliation_attempts,
            reconciled_at=record.reconciled_at,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


class RetryEligibilityResponse(BaseModel):
    effect: EffectResponse
    retryable: bool
    detail: str


def _effect_store(request: Request, scope_id: str) -> EffectStore:
    configured = getattr(request.app.state, "effect_store", None)
    if configured is not None and getattr(configured, "scope_id", scope_id) == scope_id:
        return configured  # type: ignore[no-any-return]
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        return InMemoryEffectStore()
    outbox = getattr(request.app.state, "effect_reconciliation_outbox", None) or (
        PostgresEffectReconciliationOutbox(engine)
    )
    return PostgresEffectStore(engine, outbox)


async def _get_or_404(request: Request, scope_id: str, effect_id: str) -> EffectRecord:
    record = await _effect_store(request, scope_id).get(scope_id, effect_id)
    if record is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "effect not found")
    return record


async def _reconciliation_context(request: Request, scope_id: str) -> ConnectorActionContext | None:
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        return None
    settings = get_settings()
    if not settings.secret_key and not settings.secret_keys:
        return None
    try:
        keyring = keyring_from_settings(settings)
    except SecretsError:
        return None
    repository = PostgresConnectorRepository(engine, scope_id)
    token_store = PostgresTokenStore(engine, scope_id, keyring)
    credential_store = ConnectorCredentialStore(token_store)
    return ConnectorActionContext.with_repository(
        scope_id,
        repository,
        credential_store=token_store,
        envelope_credential_store=credential_store,
    )


@router.get(
    "",
    response_model=list[EffectResponse],
    summary="List Effects for the caller's scope",
)
async def list_effects(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
    status_filter: Annotated[EffectStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[EffectResponse]:
    rows = await _effect_store(request, auth.scope_id).list_for_scope(
        auth.scope_id, status=status_filter, limit=limit
    )
    return [EffectResponse.from_record(row) for row in rows]


@router.get(
    "/{effect_id}",
    response_model=EffectResponse,
    summary="Get one Effect",
)
async def get_effect(
    effect_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> EffectResponse:
    record = await _get_or_404(request, auth.scope_id, effect_id)
    return EffectResponse.from_record(record)


@router.post(
    "/{effect_id}/reconcile",
    response_model=EffectResponse,
    summary="Request one on-demand provider reconciliation attempt for an unknown Effect",
    dependencies=[Depends(require_privilege(EndpointPrivilege.operator))],
)
async def reconcile_effect(
    effect_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> EffectResponse:
    store = _effect_store(request, auth.scope_id)
    record = await _get_or_404(request, auth.scope_id, effect_id)
    if record.status is not EffectStatus.unknown:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"effect is {record.status.value!r}, not unknown — nothing to reconcile",
        )
    context = await _reconciliation_context(request, auth.scope_id)
    registry = getattr(request.app.state, "connector_registry", None) or get_connector_registry()
    reconciler = None
    if context is not None:
        try:
            provider = registry.create(record.provider)
        except (KeyError, ConnectorProviderUnavailableError):
            provider = None
        if provider is not None:
            reconciler = provider.build_reconciler(context)
    if reconciler is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            f"{record.provider!r} has no reconciliation capability for this effect; "
            "it remains unknown pending a capable provider or operator action",
        )
    resolution = reconciliation_result(
        await reconciler.reconcile(
            ConnectorReconciliationRequest(
                scope_id=auth.scope_id,
                connector_id=record.provider,
                action_name=record.action_name,
                idempotency_key=record.idempotency_key,
                resource_id=record.resource_id,
                provider_ref=record.provider_ref,
                canonical_args=record.canonical_args,
                unknown_since=record.updated_at,
            )
        )
    )
    try:
        if resolution.outcome is ConnectorReconciliationOutcome.confirmed:
            updated = await store.reconcile_confirmed(
                auth.scope_id,
                record.id,
                provider_ref=resolution.provider_ref or record.provider_ref,
                result=resolution.result or record.result,
            )
        elif resolution.outcome is ConnectorReconciliationOutcome.absent:
            updated = await store.reconcile_absent(auth.scope_id, record.id)
        else:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"{record.provider!r} could not prove confirmed/absent for this effect; "
                "it remains unknown",
            )
    except LookupError:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "effect was already resolved by another reconciliation attempt",
        ) from None
    return EffectResponse.from_record(updated)


@router.post(
    "/{effect_id}/retry",
    response_model=RetryEligibilityResponse,
    summary="Confirm retry eligibility for a failed/reconciled-absent Effect",
    dependencies=[Depends(require_privilege(EndpointPrivilege.operator))],
)
async def retry_effect(
    effect_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> RetryEligibilityResponse:
    """Report whether ``effect_id`` may be retried now.

    The API never re-executes the original connector action itself (it does not hold the
    call's arguments or credentials out of band) — a legitimately eligible Effect is
    retried by invoking the owning tool again with the same ``idempotency_key``; this
    endpoint's role is authorization + a truthful, explicit eligibility signal (never a
    silent no-op that could be mistaken for an actual resend)."""
    record = await _get_or_404(request, auth.scope_id, effect_id)
    eligible = is_retryable(record.status)
    if record.status is EffectStatus.unknown:
        detail = "blocked: outcome is unknown pending reconciliation (C4)"
    elif eligible:
        detail = "eligible: invoke the owning tool again with the same idempotency_key"
    else:
        detail = f"not retryable from status {record.status.value!r}"
    return RetryEligibilityResponse(
        effect=EffectResponse.from_record(record), retryable=eligible, detail=detail
    )


__all__ = ["router"]
