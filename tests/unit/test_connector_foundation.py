"""Shared connector contracts, discovery, persistence, sync, and replay primitives."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAction,
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
    ConnectorActionManifest,
    ConnectorActionSemantics,
    ConnectorAuthKind,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorCredentialUpdate,
    ConnectorCursorUpdate,
    ConnectorEvent,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressRequest,
    ConnectorIngressResponse,
    ConnectorIngressResult,
    ConnectorItemDraft,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorProvenance,
    ConnectorResourceDraft,
    ConnectorResourceRefreshMode,
    ConnectorResourceResult,
    ConnectorSetupResult,
    ConnectorStateUpdate,
    ConnectorSyncResult,
    ConnectorTargetField,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import ConnectorCredentialStore, CredentialEnvelope
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
from keel_core.protocols import ToolContext
from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import InMemoryTokenStore
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
    envelope = CredentialEnvelope("secret", {"api_key": "super-secret-value"})
    encoded = envelope.serialize()
    assert CredentialEnvelope.parse(encoded) == envelope
    assert "super-secret-value" not in repr(envelope)
    assert "super-secret-value" not in repr(
        ConnectorOperationContext(
            "scope:a",
            "fixture",
            credential=envelope,
            credential_version=1,
        )
    )
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

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        assert context.binding is not None
        return ConnectorSyncResult(
            changes=(
                ConnectorChange(
                    ConnectorChangeKind.upsert,
                    ConnectorProvenance(
                        connector_id="fixture",
                        binding_id=context.binding.id,
                        external_resource_id="doc-1",
                        source_url="https://example.invalid/doc-1",
                        revision="r1",
                    ),
                    title="Document",
                    content="untrusted external content",
                ),
            ),
            state=ConnectorStateUpdate(
                cursor_updates=(
                    ConnectorCursorUpdate("messages", "next"),
                    ConnectorCursorUpdate("labels", "labels-next"),
                ),
            ),
        )

    async def ingress(
        self, context: ConnectorOperationContext, request: ConnectorIngressRequest
    ) -> ConnectorIngressResult:
        assert context.binding is not None
        if request.headers.get("x-signature") != "valid":
            raise ValueError("invalid signature")
        return ConnectorIngressResult(
            ConnectorIngressResponse(
                status_code=202,
                content_type="application/json",
                headers={"x-provider-result": "accepted"},
                body=b'{"ok":true}',
            ),
            delivery_id="delivery-1",
            payload_hash=hashlib.sha256(request.body).hexdigest(),
            changes=(
                ConnectorChange(
                    ConnectorChangeKind.event,
                    ConnectorProvenance(
                        "fixture", context.binding.id, "event-1", event_id="event-1"
                    ),
                    event=ConnectorEvent(
                        "fixture.event",
                        ConnectorProvenance(
                            "fixture", context.binding.id, "event-1", event_id="event-1"
                        ),
                        {"summary": "changed"},
                    ),
                ),
            ),
        )

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
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


async def test_health_does_not_promote_staged_binding_to_connected() -> None:
    repository = InMemoryConnectorRepository("scope:a")
    await repository.upsert_binding(
        "fixture",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.configured,
    )
    updated = await repository.record_health(
        "fixture",
        ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC)),
    )
    assert updated is not None
    assert updated.status is ConnectorBindingStatus.configured


async def test_setup_restores_prior_credential_when_binding_commit_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = InMemoryConnectorRepository("scope:a")
    await repository.upsert_binding(
        "fixture",
        ConnectorBindingDraft(display_name="Before"),
        ConnectorBindingStatus.configured,
    )
    credentials = ConnectorCredentialStore(
        InMemoryTokenStore("scope:a", EnvelopeCipher("key"))
    )
    await credentials.put("fixture", CredentialEnvelope("secret", {"value": "before"}))
    service = ConnectorService(
        ConnectorRegistry(
            (ConnectorRegistration(_Provider.manifest, _Provider, "tests.fixture"),)
        ),
        repository,
        credentials=credentials,
    )

    async def fail_upsert(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("binding write failed")

    monkeypatch.setattr(repository, "upsert_binding", fail_upsert)
    with pytest.raises(RuntimeError, match="binding write failed"):
        await service.save_setup(
            "fixture",
            ConnectorSetupResult(
                ConnectorBindingDraft(display_name="After"),
                CredentialEnvelope("secret", {"value": "after"}),
                status=ConnectorBindingStatus.connected,
            ),
        )
    stored = await credentials.get("fixture")
    assert stored is not None
    assert stored.values == {"value": "before"}
    binding = await repository.get_binding("fixture")
    assert binding is not None
    assert binding.display_name == "Before"
    assert binding.status is ConnectorBindingStatus.configured


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
    invalid = ConnectorIngressRequest(
        "POST",
        {},
        {"x-signature": "invalid"},
        b"body",
        "https://keel.example/v1/connectors/fixture/webhook",
    )
    valid = ConnectorIngressRequest(
        "POST",
        {},
        {"x-signature": "valid"},
        b"body",
        "https://keel.example/v1/connectors/fixture/webhook",
    )
    with pytest.raises(ValueError, match="signature"):
        await service.ingress("fixture", invalid)
    first = await service.ingress("fixture", valid)
    replay = await service.ingress("fixture", valid)
    assert (first.accepted, first.changes, first.response.status_code) == (True, 1, 202)
    assert (replay.accepted, replay.changes) == (False, 0)
    with pytest.raises(ValueError, match="different payload"):
        await service.ingress(
            "fixture",
            ConnectorIngressRequest(
                "POST",
                {},
                {"x-signature": "valid"},
                b"different",
                "https://keel.example/v1/connectors/fixture/webhook",
            ),
        )
    assert len(sink.changes) == 1


async def test_ingress_challenge_returns_before_delivery_claim() -> None:
    class ChallengeProvider(_Provider):
        async def ingress(
            self,
            context: ConnectorOperationContext,
            request: ConnectorIngressRequest,
        ) -> ConnectorIngressResult:
            assert request.method == "POST"
            assert request.query == {"validationToken": ("challenge",)}
            assert request.public_url.endswith("?validationToken=challenge")
            assert context.credential is not None
            return ConnectorIngressResult(
                ConnectorIngressResponse(
                    status_code=200,
                    content_type="text/plain",
                    headers={"cache-control": "no-store"},
                    body=b"challenge",
                )
            )

    registry = ConnectorRegistry(
        (ConnectorRegistration(_Provider.manifest, ChallengeProvider, "tests.challenge"),)
    )
    repository = InMemoryConnectorRepository("scope:a")
    await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    credentials = ConnectorCredentialStore(
        InMemoryTokenStore("scope:a", EnvelopeCipher("key"))
    )
    await credentials.put("fixture", CredentialEnvelope("secret", {"value": "hidden"}))
    service = ConnectorService(registry, repository, credentials=credentials)
    outcome = await service.ingress(
        "fixture",
        ConnectorIngressRequest(
            "POST",
            {"validationToken": ("challenge",)},
            {},
            b"",
            "https://keel.example/v1/connectors/fixture/webhook?validationToken=challenge",
        ),
    )
    assert outcome.response.body == b"challenge"
    assert outcome.accepted is False


def test_ingress_response_rejects_unsafe_hop_by_hop_headers() -> None:
    with pytest.raises(ValueError, match="unsafe header"):
        ConnectorIngressResponse(headers={"Transfer-Encoding": "chunked"})


def test_ingress_request_repr_excludes_auth_material() -> None:
    request = ConnectorIngressRequest(
        "POST",
        {"validationToken": ("secret-query",)},
        {"authorization": "secret-header"},
        b"secret-body",
        "https://keel.example/webhook?validationToken=secret-url",
    )
    rendered = repr(request)
    assert "secret-query" not in rendered
    assert "secret-header" not in rendered
    assert "secret-body" not in rendered
    assert "secret-url" not in rendered


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
        async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
            assert context.binding is not None
            return ConnectorSyncResult(
                changes=(
                    ConnectorChange(
                        ConnectorChangeKind.delete,
                        ConnectorProvenance("fixture", context.binding.id, "doc"),
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
            created = sum(1 for operation, _ in knowledge_calls if operation == "create")
            return SimpleNamespace(document=SimpleNamespace(id=f"document-{created}"))

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
    assert await repository.list_items("fixture") == []
    await sink.apply(
        ConnectorChange(
            ConnectorChangeKind.upsert,
            ConnectorProvenance("fixture", binding.id, "doc-1", revision="r2"),
            title="Document recreated",
            content="new content",
            mime_type="text/plain",
        )
    )
    recreated = (await repository.list_items("fixture"))[0]
    assert recreated.destination_id == "document-2"
    assert knowledge_calls[-1] == ("create", "kb-1")

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

        async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
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


async def test_local_forget_survives_unavailable_provider_but_remote_revoke_fails_closed() -> None:
    manifest = ConnectorManifest(
        id="optional",
        name="Optional",
        description="optional provider",
        auth_kind=ConnectorAuthKind.oauth,
        capabilities=(ConnectorCapability.sync,),
    )

    class Provider(BaseConnectorProvider):
        pass

    Provider.manifest = manifest

    def unavailable() -> None:
        raise ModuleNotFoundError("missing optional", name="optional_sdk")

    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                manifest,
                Provider,
                "tests.optional",
                availability=unavailable,
            ),
        )
    )
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "optional", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    resource = (
        await repository.upsert_resources(
            "optional",
            binding.id,
            (ConnectorResourceDraft("calendar", "calendar", "Calendar", selected=True),),
        )
    )[0]
    await repository.upsert_items(
        "optional",
        binding.id,
        (ConnectorItemDraft("event", "event", "Event", resource_id=resource.id),),
    )
    await repository.put_cursor("optional", binding.id, "events", "next", resource_id=resource.id)
    token_store = InMemoryTokenStore("scope:a", EnvelopeCipher("key"))
    credentials = ConnectorCredentialStore(token_store)
    await credentials.put("optional", CredentialEnvelope("oauth", {"refresh_token": "secret"}))

    service = ConnectorService(
        registry,
        repository,
        credentials=credentials,
    )
    with pytest.raises(ConnectorProviderUnavailableError, match="optional_sdk"):
        await service.revoke("optional", purge=True)
    assert await repository.get_binding("optional") is not None
    assert await credentials.get("optional") is not None

    assert await service.revoke("optional", purge=True, local_only=True)
    assert await repository.get_binding("optional") is None
    assert await credentials.get("optional") is None


async def test_remote_revoke_receives_full_state_and_failure_retains_local_data() -> None:
    manifest = ConnectorManifest(
        id="subscription",
        name="Subscription",
        description="subscription provider",
        auth_kind=ConnectorAuthKind.oauth,
        capabilities=(ConnectorCapability.webhook, ConnectorCapability.resources),
    )
    fail = True
    captured: list[ConnectorOperationContext] = []

    class Provider(BaseConnectorProvider):
        async def revoke(self, context: ConnectorOperationContext) -> None:
            captured.append(context)
            if fail:
                raise RuntimeError("upstream revoke failed")

    Provider.manifest = manifest
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "subscription", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    resource = (
        await repository.upsert_resources(
            "subscription",
            binding.id,
            (ConnectorResourceDraft("repo", "repository", "Repo", selected=True),),
        )
    )[0]
    await repository.replace_targets(
        "subscription",
        binding.id,
        {ConnectorTargetKind.trigger_session: "session"},
    )
    await repository.upsert_items(
        "subscription",
        binding.id,
        (ConnectorItemDraft("watch", "watch", "Watch", resource_id=resource.id),),
    )
    await repository.put_cursor(
        "subscription", binding.id, "watch", "next", resource_id=resource.id
    )
    credentials = ConnectorCredentialStore(InMemoryTokenStore("scope:a", EnvelopeCipher("key")))
    await credentials.put(
        "subscription",
        CredentialEnvelope("oauth", {"refresh_token": "secret"}),
    )
    service = ConnectorService(
        ConnectorRegistry((ConnectorRegistration(manifest, Provider, "tests.subscription"),)),
        repository,
        credentials=credentials,
    )

    with pytest.raises(RuntimeError, match="upstream revoke failed"):
        await service.revoke("subscription")
    state = captured[0]
    assert state.binding == binding
    assert state.credential is not None
    assert state.resources == (resource,)
    assert len(state.targets) == len(state.items) == len(state.cursors) == 1
    assert await repository.get_binding("subscription") is not None
    assert await credentials.get("subscription") is not None

    fail = False
    assert await service.revoke("subscription")
    assert await repository.get_binding("subscription") is None
    assert await credentials.get("subscription") is None


async def test_sync_persists_versioned_credentials_and_binding_state() -> None:
    manifest = ConnectorManifest(
        id="rotating",
        name="Rotating",
        description="rotating provider",
        auth_kind=ConnectorAuthKind.oauth,
        capabilities=(ConnectorCapability.sync,),
    )

    class Provider(BaseConnectorProvider):
        async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
            assert context.credential is not None
            assert context.credential.values["access_token"] == "old"
            assert context.credential_version == 1
            return ConnectorSyncResult(
                state=ConnectorStateUpdate(
                    credential=ConnectorCredentialUpdate(
                        CredentialEnvelope("oauth", {"access_token": "rotated"}),
                        expected_version=context.credential_version,
                    ),
                    binding_metadata={"subscription_revision": 2},
                    cursor_updates=(ConnectorCursorUpdate("events", "next"),),
                )
            )

    Provider.manifest = manifest
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "rotating",
        ConnectorBindingDraft(metadata={"subscription_revision": 1}),
        ConnectorBindingStatus.connected,
    )
    token_store = InMemoryTokenStore("scope:a", EnvelopeCipher("key"))
    credentials = ConnectorCredentialStore(token_store)
    await credentials.put("rotating", CredentialEnvelope("oauth", {"access_token": "old"}))
    service = ConnectorService(
        ConnectorRegistry((ConnectorRegistration(manifest, Provider, "tests.rotating"),)),
        repository,
        credentials=credentials,
    )

    assert await service.sync("rotating", binding.id) == 0
    stored = await credentials.get_versioned("rotating")
    assert stored is not None
    assert stored.version == 2
    assert stored.envelope.values["access_token"] == "rotated"
    updated = await repository.get_binding("rotating")
    assert updated is not None
    assert updated.metadata == {"subscription_revision": 2}
    assert await repository.get_cursor("rotating", binding.id, "events") is not None


async def test_resource_refresh_prunes_only_authoritative_missing_state() -> None:
    manifest = ConnectorManifest(
        id="resources",
        name="Resources",
        description="resource provider",
        auth_kind=ConnectorAuthKind.secret,
        capabilities=(ConnectorCapability.resources,),
    )
    responses = iter(
        (
            ConnectorResourceResult(
                (ConnectorResourceDraft("a", "calendar", "A"),),
                ConnectorResourceRefreshMode.authoritative,
            ),
            ConnectorResourceResult(
                (ConnectorResourceDraft("c", "calendar", "C"),),
                ConnectorResourceRefreshMode.incremental,
            ),
        )
    )

    class Provider(BaseConnectorProvider):
        async def list_resources(
            self, context: ConnectorOperationContext
        ) -> ConnectorResourceResult:
            return next(responses)

    Provider.manifest = manifest
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "resources", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    initial = await repository.upsert_resources(
        "resources",
        binding.id,
        (
            ConnectorResourceDraft("a", "calendar", "A", selected=True),
            ConnectorResourceDraft("b", "calendar", "B", selected=True),
        ),
    )
    resource_b = next(item for item in initial if item.external_id == "b")
    await repository.upsert_items(
        "resources",
        binding.id,
        (ConnectorItemDraft("event-b", "event", "Event B", resource_id=resource_b.id),),
    )
    await repository.put_cursor(
        "resources", binding.id, "events", "next-b", resource_id=resource_b.id
    )
    await repository.put_cursor("resources", binding.id, "global", "global")
    other_scope = InMemoryConnectorRepository("scope:b")
    other_binding = await other_scope.upsert_binding(
        "resources", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    await other_scope.upsert_resources(
        "resources",
        other_binding.id,
        (ConnectorResourceDraft("b", "calendar", "Other B", selected=True),),
    )
    service = ConnectorService(
        ConnectorRegistry((ConnectorRegistration(manifest, Provider, "tests.resources"),)),
        repository,
    )

    await service.refresh_resources("resources")
    assert [item.external_id for item in await repository.list_resources("resources")] == ["a"]
    assert await repository.list_items("resources") == []
    assert [
        (item.resource_id, item.stream)
        for item in await repository.list_cursors("resources", binding.id)
    ] == [(None, "global")]

    await service.refresh_resources("resources")
    assert [item.external_id for item in await repository.list_resources("resources")] == [
        "a",
        "c",
    ]
    assert [item.external_id for item in await other_scope.list_resources("resources")] == ["b"]


async def test_provider_action_context_enforces_scope_and_selected_resources() -> None:
    action_manifest = ConnectorActionManifest(
        name="calendar_create",
        description="Create an event in a selected calendar.",
        input_schema={"type": "object", "properties": {"calendar": {"type": "string"}}},
        semantics=ConnectorActionSemantics.outbound,
        idempotency=ConnectorActionIdempotency.required,
        approval=ConnectorActionApproval.tainted,
    )
    manifest = ConnectorManifest(
        id="action_fixture",
        name="Action fixture",
        description="action provider",
        auth_kind=ConnectorAuthKind.oauth,
        capabilities=(ConnectorCapability.write, ConnectorCapability.resources),
        actions=(action_manifest,),
    )

    class Provider(BaseConnectorProvider):
        def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
            async def create(arguments: dict[str, Any], tool_context: ToolContext) -> str:
                resource = await context.require_selected_resource(
                    "action_fixture",
                    str(arguments["calendar"]),
                )
                state = await context.load_state("action_fixture")
                return (
                    f"{resource.external_id}:{len(state.targets)}:"
                    f"{len(state.items)}:{len(state.cursors)}"
                )

            return (ConnectorAction(action_manifest, create),)

    Provider.manifest = manifest
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        "action_fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    resources = await repository.upsert_resources(
        "action_fixture",
        binding.id,
        (
            ConnectorResourceDraft("selected", "calendar", "Selected", selected=True),
            ConnectorResourceDraft("blocked", "calendar", "Blocked"),
        ),
    )
    selected = next(item for item in resources if item.external_id == "selected")
    blocked = next(item for item in resources if item.external_id == "blocked")
    await repository.replace_targets(
        "action_fixture",
        binding.id,
        {ConnectorTargetKind.trigger_session: "session-1"},
    )
    await repository.upsert_items(
        "action_fixture",
        binding.id,
        (
            ConnectorItemDraft("selected-item", "event", "Selected", resource_id=selected.id),
            ConnectorItemDraft("blocked-item", "event", "Blocked", resource_id=blocked.id),
        ),
    )
    await repository.put_cursor(
        "action_fixture", binding.id, "events", "selected", resource_id=selected.id
    )
    await repository.put_cursor(
        "action_fixture", binding.id, "events", "blocked", resource_id=blocked.id
    )
    registry = ConnectorRegistry(
        (ConnectorRegistration(manifest, Provider, "tests.action_fixture"),)
    )
    actions = registry.build_actions(ConnectorActionContext.with_repository("scope:a", repository))
    action = actions[0].action
    assert (
        await action(
            {"calendar": "selected"},
            ToolContext(scope_id="scope:a", session_id="session"),
        )
        == "selected:1:1:1"
    )
    with pytest.raises(PermissionError, match="not selected"):
        await action(
            {"calendar": "blocked"},
            ToolContext(scope_id="scope:a", session_id="session"),
        )
    with pytest.raises(PermissionError, match="cannot cross"):
        await action(
            {"calendar": "selected"},
            ToolContext(scope_id="scope:b", session_id="session"),
        )
    with pytest.raises(ValueError, match="crosses its scope"):
        ConnectorActionContext.with_repository(
            "scope:a",
            InMemoryConnectorRepository("scope:b"),
        )
