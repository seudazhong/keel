"""The worker wires one generic connector sync kind, independent of provider ids."""

from __future__ import annotations

from keel_core.config import Settings
from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAuthKind,
    ConnectorBinding,
    ConnectorCapability,
    ConnectorCursor,
    ConnectorManifest,
    ConnectorResource,
    ConnectorSyncResult,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connector_registry import ConnectorRegistration, ConnectorRegistry
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connector_service import CONNECTOR_SYNC_JOB_KIND, ConnectorService
from keel_worker.connectors import register_connector_jobs
from keel_worker.jobs import JobRegistry


class Provider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="worker_fixture",
        name="Worker fixture",
        description="test",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
    )

    async def sync(
        self,
        binding: ConnectorBinding,
        resources: tuple[ConnectorResource, ...],
        cursor: ConnectorCursor | None,
        credential: CredentialEnvelope | None,
    ) -> ConnectorSyncResult:
        return ConnectorSyncResult()


def test_register_connector_jobs_adds_one_provider_agnostic_kind() -> None:
    provider_registry = ConnectorRegistry(
        (ConnectorRegistration(Provider.manifest, Provider, "tests.worker_fixture"),)
    )
    service = ConnectorService(
        provider_registry,
        InMemoryConnectorRepository("scope:test"),
    )
    jobs = JobRegistry()
    register_connector_jobs(jobs, service, Settings())
    assert jobs.kinds() == (CONNECTOR_SYNC_JOB_KIND,)
