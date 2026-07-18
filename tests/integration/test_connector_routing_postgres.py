"""Postgres coverage for cross-scope connector jobs + webhook routing (M3.6 findings 1 + webhook).

Live-Postgres checks that: the global connector schedule index and webhook routing capability are
readable across scopes (no RLS); an Agent-scoped connector sync records a dispatch intent in the
global ``job_dispatch_outbox`` keyed by that scope; and a minted webhook route resolves back to its
exact scope + binding while a revoked binding drops its route.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAuthKind,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCapability,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorSyncResult,
)
from keel_core.connector_registry import ConnectorRegistration, ConnectorRegistry
from keel_core.connector_repository import PostgresConnectorRepository
from keel_core.connector_schedule_index import PostgresConnectorScheduleIndex
from keel_core.connector_service import CONNECTOR_SYNC_JOB_KIND, ConnectorService
from keel_core.connector_webhook_routes import (
    PostgresConnectorWebhookRouteStore,
    mint_route_token,
)
from keel_core.job_dispatch import PostgresJobDispatchOutbox
from keel_core.jobs import JobLimits, PostgresJobStore

pytestmark = pytest.mark.integration

_SCOPE_A = "agent:orga/ag1"
_SCOPE_B = "agent:orgb/ag2"


class _SyncProvider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="pg_sync_fixture",
        name="PG sync fixture",
        description="postgres cross-scope sync fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
    )

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        return ConnectorSyncResult()


def _registry() -> ConnectorRegistry:
    return ConnectorRegistry(
        (ConnectorRegistration(_SyncProvider.manifest, _SyncProvider, "tests.pg_sync"),)
    )


async def test_schedule_index_is_readable_across_scopes(migrated_db: AsyncEngine) -> None:
    index = PostgresConnectorScheduleIndex(migrated_db)
    await index.record(_SCOPE_A)
    await index.record(_SCOPE_B)
    assert await index.active_scopes() == {_SCOPE_A, _SCOPE_B}
    await index.discard(_SCOPE_A)
    assert await index.active_scopes() == {_SCOPE_B}


async def test_webhook_route_resolves_across_scopes_and_replaces(migrated_db: AsyncEngine) -> None:
    store = PostgresConnectorWebhookRouteStore(migrated_db)
    first = mint_route_token()
    await store.put(first, _SCOPE_A, "pg_sync_fixture", "binding-1", "connected")
    route = await store.resolve(first)
    assert route is not None and route.scope_id == _SCOPE_A
    # Re-running setup rotates the token; only one route per (scope, connector) survives.
    second = mint_route_token()
    await store.put(second, _SCOPE_A, "pg_sync_fixture", "binding-1", "connected")
    assert await store.resolve(first) is None
    assert (await store.resolve(second)) is not None
    # Revoking a binding drops its route (fails closed).
    await store.delete_for_connector(_SCOPE_A, "pg_sync_fixture")
    assert await store.resolve(second) is None


async def _connected_binding(engine: AsyncEngine, scope: str) -> PostgresConnectorRepository:
    repository = PostgresConnectorRepository(engine, scope)
    await repository.upsert_binding(
        "pg_sync_fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    return repository


async def test_agent_scoped_sync_records_dispatch_intent(migrated_db: AsyncEngine) -> None:
    settings = Settings()
    outbox = PostgresJobDispatchOutbox(migrated_db)
    repository = await _connected_binding(migrated_db, _SCOPE_A)
    service = ConnectorService(
        _registry(),
        repository,
        jobs=PostgresJobStore(migrated_db, _SCOPE_A, limits=JobLimits.from_settings(settings)),
        dispatch_outbox=outbox,
    )
    job = await service.enqueue_sync("pg_sync_fixture")
    # The connector job carries a cross-scope dispatch intent keyed by its own Agent scope.
    now = datetime.now(UTC)
    intents = await outbox.claim_due(worker_id="w1", now=now)
    assert [(i.job_id, i.scope_id, i.kind) for i in intents] == [
        (job.id, _SCOPE_A, CONNECTOR_SYNC_JOB_KIND)
    ]


async def test_webhook_route_store_carries_no_credential_columns(migrated_db: AsyncEngine) -> None:
    # The routing capability table holds only routing metadata — never a credential/secret column.
    async with migrated_db.connect() as conn:
        columns = {
            row.column_name
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'connector_webhook_routes'"
                    )
                )
            ).all()
        }
    assert columns == {
        "route_token",
        "scope_id",
        "connector_id",
        "binding_id",
        "status",
        "created_at",
        "updated_at",
    }
