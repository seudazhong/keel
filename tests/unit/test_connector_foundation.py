"""Shared connector contracts, discovery, persistence, sync, and replay primitives."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAuthKind,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorCursor,
    ConnectorEvent,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressResult,
    ConnectorManifest,
    ConnectorProvenance,
    ConnectorResource,
    ConnectorSyncResult,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connector_registry import (
    ConnectorRegistration,
    ConnectorRegistry,
    discover_connector_registry,
)
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connector_service import (
    ConnectorChangeSink,
    ConnectorService,
    DurableConnectorChangeSink,
)
from keel_core.knowledge.models import (
    CreateKnowledgeDocumentCommand,
    DeleteKnowledgeCommand,
    UpdateKnowledgeDocumentCommand,
)
from keel_core.types import ContentTaint


def test_builtin_registry_discovers_gmail_deterministically() -> None:
    first = discover_connector_registry()
    second = discover_connector_registry()
    assert [item.id for item in first.manifests()] == ["gmail"]
    assert first.manifests() == second.manifests()
    assert first.create("gmail").manifest.id == "gmail"


def test_registry_rejects_duplicate_ids() -> None:
    manifest = ConnectorManifest(
        id="duplicate",
        name="Duplicate",
        description="test",
        auth_kind=ConnectorAuthKind.secret,
        capabilities=(ConnectorCapability.read,),
    )

    class Provider(BaseConnectorProvider):
        pass

    Provider.manifest = manifest
    factory = Provider
    with pytest.raises(ValueError, match="duplicate connector id"):
        ConnectorRegistry(
            (
                ConnectorRegistration(manifest, factory, "provider.a"),
                ConnectorRegistration(manifest, factory, "provider.b"),
            )
        )


def test_credential_envelope_is_versioned_and_legacy_safe() -> None:
    envelope = CredentialEnvelope("secret", {"api_key": "value"})
    encoded = envelope.serialize()
    assert CredentialEnvelope.parse(encoded) == envelope
    assert CredentialEnvelope.parse('{"token":"legacy-provider-json"}') is None
    with pytest.raises(ValueError, match="unsupported"):
        CredentialEnvelope("secret", {}, version=2)


class _Provider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="fixture",
        name="Fixture",
        description="test provider",
        auth_kind=ConnectorAuthKind.secret,
        capabilities=(ConnectorCapability.sync, ConnectorCapability.webhook),
    )

    async def sync(
        self,
        binding: ConnectorBinding,
        resources: tuple[ConnectorResource, ...],
        cursor: ConnectorCursor | None,
        credential: CredentialEnvelope | None,
    ) -> ConnectorSyncResult:
        return ConnectorSyncResult(
            changes=(
                ConnectorChange(
                    ConnectorChangeKind.upsert,
                    ConnectorProvenance(
                        connector_id="fixture",
                        binding_id=binding.id,
                        external_resource_id="doc-1",
                        source_url="https://example.invalid/doc-1",
                        revision="r1",
                    ),
                    title="Document",
                    content="untrusted external content",
                ),
            ),
            cursor="next",
        )

    async def ingress(
        self, headers: dict[str, str], body: bytes, binding: ConnectorBinding
    ) -> ConnectorIngressResult:
        if headers.get("x-signature") != "valid":
            raise ValueError("invalid signature")
        return ConnectorIngressResult(
            "delivery-1",
            (
                ConnectorChange(
                    ConnectorChangeKind.event,
                    ConnectorProvenance("fixture", binding.id, "event-1", event_id="event-1"),
                    event=ConnectorEvent(
                        "fixture.event",
                        ConnectorProvenance(
                            "fixture", binding.id, "event-1", event_id="event-1"
                        ),
                        {"summary": "changed"},
                    ),
                ),
            ),
        )

    async def health(
        self, binding: ConnectorBinding, credential: CredentialEnvelope | None
    ) -> ConnectorHealth:
        return ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC))


class _Sink(ConnectorChangeSink):
    def __init__(self) -> None:
        self.changes: list[ConnectorChange] = []

    async def apply(self, change: ConnectorChange) -> None:
        self.changes.append(change)


def _service() -> tuple[ConnectorService, InMemoryConnectorRepository, _Sink]:
    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                _Provider.manifest,
                _Provider,
                "tests.fixture",
            ),
        )
    )
    repository = InMemoryConnectorRepository("scope:a")
    sink = _Sink()
    return ConnectorService(registry, repository, change_sink=sink), repository, sink


async def test_repository_rejects_plaintext_secret_metadata() -> None:
    repository = InMemoryConnectorRepository("scope:a")
    with pytest.raises(ValueError, match="plaintext secrets"):
        await repository.upsert_binding(
            "fixture",
            ConnectorBindingDraft(metadata={"nested": {"access_token": "nope"}}),
            ConnectorBindingStatus.connected,
        )


async def test_sync_normalizes_taint_provenance_and_cursor() -> None:
    service, repository, sink = _service()
    binding = await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    assert await service.sync("fixture", binding.id) == 1
    assert sink.changes[0].taint is ContentTaint.tainted
    cursor = await repository.get_cursor("fixture", binding.id, "default")
    assert cursor is not None and cursor.value == "next"


async def test_ingress_verifies_before_claim_and_replays_once() -> None:
    service, repository, sink = _service()
    await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    with pytest.raises(ValueError, match="signature"):
        await service.ingress("fixture", {"x-signature": "invalid"}, b"body")
    assert await service.ingress("fixture", {"x-signature": "valid"}, b"body") == (True, 1)
    assert await service.ingress("fixture", {"x-signature": "valid"}, b"body") == (False, 0)
    with pytest.raises(ValueError, match="different payload"):
        await service.ingress("fixture", {"x-signature": "valid"}, b"different")
    assert len(sink.changes) == 1


async def test_failed_delivery_can_be_retried() -> None:
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    assert await repository.claim_delivery("fixture", binding.id, "delivery", "a" * 64)
    await repository.finish_delivery(
        "fixture",
        "delivery",
        error_code="temporary",
        error_summary="retryable",
    )
    assert await repository.claim_delivery("fixture", binding.id, "delivery", "a" * 64)


async def test_sync_rejects_clean_external_change() -> None:
    class CleanProvider(_Provider):
        async def sync(
            self,
            binding: ConnectorBinding,
            resources: tuple[ConnectorResource, ...],
            cursor: ConnectorCursor | None,
            credential: CredentialEnvelope | None,
        ) -> ConnectorSyncResult:
            return ConnectorSyncResult(
                changes=(
                    ConnectorChange(
                        ConnectorChangeKind.delete,
                        ConnectorProvenance("fixture", binding.id, "doc"),
                        taint=ContentTaint.clean,
                    ),
                )
            )

    registry = ConnectorRegistry(
        (ConnectorRegistration(_Provider.manifest, CleanProvider, "tests.clean"),)
    )
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    with pytest.raises(ValueError, match="must be tainted"):
        await ConnectorService(registry, repository, change_sink=_Sink()).sync(
            "fixture", binding.id
        )


async def test_durable_change_sink_reuses_knowledge_and_trigger_admission() -> None:
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "fixture",
        ConnectorBindingDraft(
            metadata={"knowledge_base_id": "kb-1", "trigger_session_id": "session-1"}
        ),
        ConnectorBindingStatus.connected,
    )
    knowledge_calls: list[tuple[str, str]] = []

    class Knowledge:
        async def create_document(
            self,
            kb_id: str,
            command: CreateKnowledgeDocumentCommand,
            idempotency_key: str,
        ) -> Any:
            knowledge_calls.append(("create", kb_id))
            return SimpleNamespace(document=SimpleNamespace(id="document-1"))

        async def update_document(
            self,
            kb_id: str,
            document_id: str,
            command: UpdateKnowledgeDocumentCommand,
            idempotency_key: str,
        ) -> Any:
            knowledge_calls.append(("update", document_id))
            return object()

        async def delete_document(
            self,
            kb_id: str,
            document_id: str,
            command: DeleteKnowledgeCommand,
            idempotency_key: str,
        ) -> Any:
            knowledge_calls.append(("delete", document_id))
            return object()

    admitted: list[tuple[str, str, str]] = []

    async def admit_event(session_id: str, content: str, run_id: str) -> None:
        admitted.append((session_id, content, run_id))

    sink = DurableConnectorChangeSink(
        repository,
        knowledge=Knowledge(),
        admit_event=admit_event,
    )
    provenance = ConnectorProvenance("fixture", binding.id, "doc-1", revision="r1")
    await sink.apply(
        ConnectorChange(
            ConnectorChangeKind.upsert,
            provenance,
            title="Document",
            content="external content",
            mime_type="text/plain",
        )
    )
    resource = (await repository.list_resources("fixture"))[0]
    assert resource.config["knowledge_document_id"] == "document-1"
    await sink.apply(ConnectorChange(ConnectorChangeKind.delete, provenance))
    assert knowledge_calls == [("create", "kb-1"), ("delete", "document-1")]

    event = ConnectorEvent("push", provenance, {"summary": "changed"})
    await sink.apply(
        ConnectorChange(
            ConnectorChangeKind.event,
            provenance,
            event=event,
        )
    )
    assert admitted[0][0] == "session-1"
    assert '"taint":"tainted"' in admitted[0][1]
    assert admitted[0][2].startswith("connector:")
