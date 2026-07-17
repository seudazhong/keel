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
    ConnectorCursorUpdate,
    ConnectorEvent,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressResult,
    ConnectorItemDraft,
    ConnectorManifest,
    ConnectorProvenance,
    ConnectorResource,
    ConnectorSyncResult,
    ConnectorTargetField,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connector_registry import (
    ConnectorDiscoveryFailure,
    ConnectorProviderUnavailableError,
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
        cursors: tuple[ConnectorCursor, ...],
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
            cursor_updates=(
                ConnectorCursorUpdate("messages", "next"),
                ConnectorCursorUpdate("labels", "labels-next"),
            ),
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
                        ConnectorProvenance("fixture", binding.id, "event-1", event_id="event-1"),
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
    cursor = await repository.get_cursor("fixture", binding.id, "messages")
    assert cursor is not None and cursor.value == "next"
    assert (await repository.get_cursor("fixture", binding.id, "labels")) is not None


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
            cursors: tuple[ConnectorCursor, ...],
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
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    await repository.replace_targets(
        "fixture",
        binding.id,
        {
            ConnectorTargetKind.knowledge: "kb-1",
            ConnectorTargetKind.trigger_routine: "routine-1",
        },
    )
    knowledge_calls: list[tuple[str, str]] = []

    class Knowledge:
        async def get_base(self, kb_id: str) -> Any:
            return object()

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

    async def resolve_trigger(kind: ConnectorTargetKind, target_id: str) -> str:
        assert kind is ConnectorTargetKind.trigger_routine
        assert target_id == "routine-1"
        return "session-1"

    sink = DurableConnectorChangeSink(
        repository,
        knowledge=Knowledge(),
        admit_event=admit_event,
        resolve_trigger=resolve_trigger,
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
    item = (await repository.list_items("fixture"))[0]
    assert item.destination_kind is ConnectorTargetKind.knowledge
    assert item.destination_target_id == "kb-1"
    assert item.destination_id == "document-1"
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


async def test_sync_requires_manifest_targets_before_provider_work() -> None:
    called = False
    instantiated = False
    manifest = ConnectorManifest(
        id="targeted",
        name="Targeted",
        description="targeted fixture",
        auth_kind=ConnectorAuthKind.secret,
        capabilities=(ConnectorCapability.sync,),
        target_fields=(ConnectorTargetField(ConnectorTargetKind.knowledge, "Knowledge Base"),),
    )

    class Provider(BaseConnectorProvider):
        def __init__(self) -> None:
            nonlocal instantiated
            instantiated = True

        async def sync(
            self,
            binding: ConnectorBinding,
            resources: tuple[ConnectorResource, ...],
            cursors: tuple[ConnectorCursor, ...],
            credential: CredentialEnvelope | None,
        ) -> ConnectorSyncResult:
            nonlocal called
            called = True
            return ConnectorSyncResult()

    Provider.manifest = manifest
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "targeted", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    service = ConnectorService(
        ConnectorRegistry((ConnectorRegistration(manifest, Provider, "tests.targeted"),)),
        repository,
    )
    with pytest.raises(RuntimeError, match="configured targets"):
        await service.sync("targeted", binding.id)
    assert called is False
    assert instantiated is False
    await repository.replace_targets(
        "targeted",
        binding.id,
        {ConnectorTargetKind.knowledge: "kb-1"},
    )
    await repository.upsert_items(
        "targeted",
        binding.id,
        (
            ConnectorItemDraft(
                "doc",
                "document",
                "Document",
                destination_kind=ConnectorTargetKind.knowledge,
                destination_target_id="kb-1",
                destination_id="document-1",
            ),
        ),
    )
    with pytest.raises(ValueError, match="disconnect and purge"):
        await service.configure_targets("targeted", {"knowledge": "kb-2"})
    assert instantiated is False


async def test_purge_handoff_precedes_mapping_removal_and_is_idempotent() -> None:
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    await repository.replace_targets(
        "fixture",
        binding.id,
        {ConnectorTargetKind.knowledge: "kb-1"},
    )
    await repository.upsert_items(
        "fixture",
        binding.id,
        (
            ConnectorItemDraft(
                "doc-1",
                "knowledge_document",
                "Document",
                destination_kind=ConnectorTargetKind.knowledge,
                destination_target_id="kb-1",
                destination_id="document-1",
            ),
        ),
    )
    calls: list[str] = []

    class Knowledge:
        async def get_base(self, kb_id: str) -> Any:
            return object()

        async def create_document(
            self,
            kb_id: str,
            command: CreateKnowledgeDocumentCommand,
            idempotency_key: str,
        ) -> Any:
            raise AssertionError

        async def update_document(
            self,
            kb_id: str,
            document_id: str,
            command: UpdateKnowledgeDocumentCommand,
            idempotency_key: str,
        ) -> Any:
            raise AssertionError

        async def delete_document(
            self,
            kb_id: str,
            document_id: str,
            command: DeleteKnowledgeCommand,
            idempotency_key: str,
        ) -> Any:
            assert await repository.list_items("fixture")
            calls.append(idempotency_key)
            return object()

    sink = DurableConnectorChangeSink(repository, knowledge=Knowledge())
    service = ConnectorService(
        ConnectorRegistry((ConnectorRegistration(_Provider.manifest, _Provider, "tests.fixture"),)),
        repository,
        change_sink=sink,
    )
    assert await service.revoke("fixture", purge=True)
    assert await repository.list_items("fixture") == []
    assert len(calls) == 1
    with pytest.raises(LookupError, match="not configured"):
        await service.sync("fixture", binding.id)
    assert not await service.revoke("fixture", purge=True)


async def test_purge_fails_closed_without_knowledge_handoff() -> None:
    service, repository, _ = _service()
    binding = await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    await repository.upsert_items(
        "fixture",
        binding.id,
        (ConnectorItemDraft("doc", "document", "Document"),),
    )
    with pytest.raises(RuntimeError, match="purge service is unavailable"):
        await service.revoke("fixture", purge=True)
    assert await repository.get_binding("fixture") is not None
    assert await repository.list_items("fixture")


async def test_catalog_isolates_broken_provider_factory() -> None:
    healthy = ConnectorManifest(
        id="healthy",
        name="Healthy",
        description="healthy",
        auth_kind=ConnectorAuthKind.secret,
        capabilities=(ConnectorCapability.read,),
    )
    broken = ConnectorManifest(
        id="broken",
        name="Broken",
        description="broken",
        auth_kind=ConnectorAuthKind.secret,
        capabilities=(ConnectorCapability.read,),
    )
    created: list[str] = []

    class Healthy(BaseConnectorProvider):
        def __init__(self) -> None:
            created.append("healthy")

    Healthy.manifest = healthy

    def missing_dependency() -> None:
        raise ModuleNotFoundError("missing optional", name="broken_sdk")

    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                broken,
                Healthy,
                "tests.broken",
                availability=missing_dependency,
            ),
            ConnectorRegistration(healthy, Healthy, "tests.healthy"),
        ),
        (
            ConnectorDiscoveryFailure(
                "import_broken",
                "tests.import_broken",
                "Connector 'import_broken' is unavailable: missing import_sdk",
            ),
        ),
    )
    rows = await ConnectorService(registry, InMemoryConnectorRepository("scope:a")).catalog()
    assert created == []
    by_id = {row["id"]: row for row in rows}
    assert by_id["healthy"]["available"] is True
    assert by_id["broken"]["available"] is False
    assert by_id["import_broken"]["available"] is False
    with pytest.raises(ConnectorProviderUnavailableError, match="import_sdk"):
        registry.create("import_broken")
    with pytest.raises(ConnectorProviderUnavailableError, match="broken_sdk"):
        registry.create("broken")
