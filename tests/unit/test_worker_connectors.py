"""The worker wires one generic connector sync kind, independent of provider ids."""

from __future__ import annotations

from typing import Any

from keel_core.config import Settings
from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAction,
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
    ConnectorActionManifest,
    ConnectorActionSemantics,
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
from keel_core.protocols import ToolContext
from keel_worker.connectors import register_connector_jobs
from keel_worker.jobs import JobRegistry
from keel_worker.main import _connector_actions

ACTION = ConnectorActionManifest(
    name="calendar_create",
    description="Create a calendar event.",
    input_schema={
        "type": "object",
        "properties": {"idempotency_key": {"type": "string"}},
    },
    semantics=ConnectorActionSemantics.outbound,
    idempotency=ConnectorActionIdempotency.required,
    approval=ConnectorActionApproval.tainted,
)


class Provider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="worker_fixture",
        name="Worker fixture",
        description="test",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        actions=(ACTION,),
    )

    async def sync(
        self,
        binding: ConnectorBinding,
        resources: tuple[ConnectorResource, ...],
        cursors: tuple[ConnectorCursor, ...],
        credential: CredentialEnvelope | None,
    ) -> ConnectorSyncResult:
        return ConnectorSyncResult()

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
        async def create(args: dict[str, Any], tool_context: ToolContext) -> str:
            return "created"

        return (ConnectorAction(ACTION, create),)


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


def test_worker_discovers_provider_local_actions_without_provider_branches() -> None:
    provider_registry = ConnectorRegistry(
        (ConnectorRegistration(Provider.manifest, Provider, "tests.worker_fixture"),)
    )
    actions = _connector_actions(
        {"connector_registry": provider_registry},
        Settings(),
        "scope:test",
    )
    assert [action.manifest.name for action in actions] == ["calendar_create"]
    assert actions[0].manifest.idempotency is ConnectorActionIdempotency.required
    assert actions[0].manifest.approval is ConnectorActionApproval.tainted
