"""Provider-agnostic connector manifests, state, sync, and ingress contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connectors import ActionFn
from keel_core.outbox import OutboundIdempotencyStore
from keel_core.types import ContentTaint


class ConnectorAuthKind(StrEnum):
    oauth = "oauth"
    github_app = "github_app"
    secret = "secret"
    app_credentials = "app_credentials"
    webhook = "webhook"
    url = "url"


class ConnectorCapability(StrEnum):
    read = "read"
    write = "write"
    sync = "sync"
    webhook = "webhook"
    resources = "resources"


class ConnectorBindingStatus(StrEnum):
    configured = "configured"
    connected = "connected"
    error = "error"
    revoked = "revoked"


class ConnectorHealthStatus(StrEnum):
    unconfigured = "unconfigured"
    healthy = "healthy"
    degraded = "degraded"
    error = "error"


class ConnectorChangeKind(StrEnum):
    upsert = "upsert"
    delete = "delete"
    event = "event"


class ConnectorSetupArtifactKind(StrEnum):
    instruction = "instruction"
    url = "url"
    secret = "secret"


class ConnectorTargetKind(StrEnum):
    knowledge = "knowledge"
    trigger_session = "trigger_session"
    trigger_routine = "trigger_routine"


class ConnectorActionSemantics(StrEnum):
    read = "read"
    outbound = "outbound"


class ConnectorActionIdempotency(StrEnum):
    none = "none"
    optional = "optional"
    required = "required"


class ConnectorActionApproval(StrEnum):
    none = "none"
    tainted = "tainted"


@dataclass(frozen=True, slots=True)
class ConnectorSetupField:
    id: str
    label: str
    required: bool = True
    secret: bool = False
    input_type: str = "text"
    help_text: str | None = None

    def __post_init__(self) -> None:
        if not self.id.strip() or not self.label.strip():
            raise ValueError("connector setup field id and label must not be blank")
        if self.secret and self.input_type != "password":
            object.__setattr__(self, "input_type", "password")


@dataclass(frozen=True, slots=True)
class ConnectorCallbackParameter:
    id: str
    required: bool = True

    def __post_init__(self) -> None:
        if not self.id.strip() or not self.id.replace("_", "").isalnum():
            raise ValueError("connector callback parameter id is invalid")


@dataclass(frozen=True, slots=True)
class ConnectorAuthAction:
    label: str = "Connect"
    callback_parameters: tuple[ConnectorCallbackParameter, ...] = ()

    def __post_init__(self) -> None:
        if not self.label.strip():
            raise ValueError("connector auth action label must not be blank")
        ids = [item.id for item in self.callback_parameters]
        if len(set(ids)) != len(ids):
            raise ValueError("connector auth action has duplicate callback parameters")


@dataclass(frozen=True, slots=True)
class ConnectorTargetField:
    kind: ConnectorTargetKind
    label: str
    required: bool = True
    help_text: str | None = None

    def __post_init__(self) -> None:
        if not self.label.strip():
            raise ValueError("connector target label must not be blank")


@dataclass(frozen=True, slots=True)
class ConnectorActionManifest:
    name: str
    description: str
    input_schema: Mapping[str, Any]
    semantics: ConnectorActionSemantics
    idempotency: ConnectorActionIdempotency = ConnectorActionIdempotency.none
    approval: ConnectorActionApproval = ConnectorActionApproval.none

    def __post_init__(self) -> None:
        if not self.name or len(self.name) > 64:
            raise ValueError("connector action name must contain 1-64 characters")
        if any(not (ch.isalnum() or ch in "_-") for ch in self.name):
            raise ValueError("connector action name contains unsupported characters")
        if not self.description.strip():
            raise ValueError("connector action description must not be blank")
        if self.semantics is ConnectorActionSemantics.read:
            if self.idempotency is not ConnectorActionIdempotency.none:
                raise ValueError("read connector actions must not declare idempotency")
            if self.approval is not ConnectorActionApproval.none:
                raise ValueError("read connector actions must not require outbound approval")
        if (
            self.semantics is ConnectorActionSemantics.outbound
            and self.approval is not ConnectorActionApproval.tainted
        ):
            raise ValueError("outbound connector actions must require tainted-content approval")


@dataclass(frozen=True, slots=True)
class ConnectorManifest:
    id: str
    name: str
    description: str
    auth_kind: ConnectorAuthKind
    capabilities: tuple[ConnectorCapability, ...]
    icon: str = "🔌"
    scopes: tuple[str, ...] = ()
    setup_fields: tuple[ConnectorSetupField, ...] = ()
    auth_action: ConnectorAuthAction | None = None
    setup_action_label: str = "Save"
    resource_label: str | None = None
    target_fields: tuple[ConnectorTargetField, ...] = ()
    actions: tuple[ConnectorActionManifest, ...] = ()

    def __post_init__(self) -> None:
        connector_id = self.id.strip()
        if not connector_id or connector_id.lower() != connector_id:
            raise ValueError("connector id must be a non-empty lowercase identifier")
        if any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for ch in connector_id):
            raise ValueError("connector id contains unsupported characters")
        if not self.name.strip():
            raise ValueError("connector name must not be blank")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError(f"connector {connector_id!r} has duplicate capabilities")
        field_ids = [item.id for item in self.setup_fields]
        if len(set(field_ids)) != len(field_ids):
            raise ValueError(f"connector {connector_id!r} has duplicate setup field ids")
        if not self.setup_action_label.strip():
            raise ValueError("connector setup action label must not be blank")
        target_kinds = [item.kind for item in self.target_fields]
        if len(set(target_kinds)) != len(target_kinds):
            raise ValueError(f"connector {connector_id!r} has duplicate target fields")
        action_names = [item.name for item in self.actions]
        if len(set(action_names)) != len(action_names):
            raise ValueError(f"connector {connector_id!r} has duplicate action names")


@dataclass(frozen=True, slots=True)
class ConnectorBinding:
    id: str
    scope_id: str
    connector_id: str
    status: ConnectorBindingStatus
    display_name: str | None = None
    external_account_id: str | None = None
    external_tenant_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    last_success_at: datetime | None = None
    error_code: str | None = None
    error_summary: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ConnectorBindingDraft:
    display_name: str | None = None
    external_account_id: str | None = None
    external_tenant_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConnectorResource:
    id: str
    scope_id: str
    connector_id: str
    binding_id: str
    external_id: str
    kind: str
    display_name: str
    url: str | None = None
    selected: bool = False
    config: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ConnectorResourceDraft:
    external_id: str
    kind: str
    display_name: str
    url: str | None = None
    selected: bool = False
    config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConnectorItem:
    id: str
    scope_id: str
    connector_id: str
    binding_id: str
    external_id: str
    kind: str
    display_name: str
    url: str | None = None
    resource_id: str | None = None
    destination_kind: ConnectorTargetKind | None = None
    destination_target_id: str | None = None
    destination_id: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        present = (
            self.destination_kind is not None,
            self.destination_target_id is not None,
            self.destination_id is not None,
        )
        if any(present) and not all(present):
            raise ValueError("connector item destination mapping is incomplete")


@dataclass(frozen=True, slots=True)
class ConnectorItemDraft:
    external_id: str
    kind: str
    display_name: str
    url: str | None = None
    resource_id: str | None = None
    destination_kind: ConnectorTargetKind | None = None
    destination_target_id: str | None = None
    destination_id: str | None = None
    config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        present = (
            self.destination_kind is not None,
            self.destination_target_id is not None,
            self.destination_id is not None,
        )
        if any(present) and not all(present):
            raise ValueError("connector item destination mapping is incomplete")


@dataclass(frozen=True, slots=True)
class ConnectorBindingTarget:
    id: str
    scope_id: str
    connector_id: str
    binding_id: str
    kind: ConnectorTargetKind
    target_id: str
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ConnectorCursor:
    id: str
    scope_id: str
    connector_id: str
    binding_id: str
    stream: str
    value: str
    resource_id: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    revision: str | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ConnectorCursorUpdate:
    stream: str
    value: str
    resource_id: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    revision: str | None = None

    def __post_init__(self) -> None:
        if not self.stream.strip():
            raise ValueError("connector cursor stream must not be blank")


@dataclass(frozen=True, slots=True)
class ConnectorHealth:
    status: ConnectorHealthStatus
    checked_at: datetime
    message: str | None = None
    retryable: bool = False


@dataclass(frozen=True, slots=True)
class ConnectorProvenance:
    connector_id: str
    binding_id: str
    external_resource_id: str
    source_url: str | None = None
    revision: str | None = None
    event_id: str | None = None


@dataclass(frozen=True, slots=True)
class ConnectorEvent:
    type: str
    provenance: ConnectorProvenance
    payload: dict[str, Any]
    taint: ContentTaint = ContentTaint.tainted


@dataclass(frozen=True, slots=True)
class ConnectorChange:
    kind: ConnectorChangeKind
    provenance: ConnectorProvenance
    title: str | None = None
    content: str | None = None
    mime_type: str | None = None
    event: ConnectorEvent | None = None
    taint: ContentTaint = ContentTaint.tainted


@dataclass(frozen=True, slots=True)
class ConnectorAuthStart:
    url: str
    state: str


@dataclass(frozen=True, slots=True)
class ConnectorSetupArtifact:
    kind: ConnectorSetupArtifactKind
    label: str
    value: str

    def __post_init__(self) -> None:
        if not self.label.strip() or not self.value:
            raise ValueError("connector setup artifact label and value must not be blank")


@dataclass(frozen=True, slots=True)
class ConnectorSetupResult:
    binding: ConnectorBindingDraft
    credential: CredentialEnvelope | None = None
    artifacts: tuple[ConnectorSetupArtifact, ...] = ()


@dataclass(frozen=True, slots=True)
class ConnectorSyncResult:
    changes: tuple[ConnectorChange, ...] = ()
    cursor_updates: tuple[ConnectorCursorUpdate, ...] = ()


@dataclass(frozen=True, slots=True)
class ConnectorAction:
    manifest: ConnectorActionManifest
    action: ActionFn


@dataclass(frozen=True, slots=True)
class ConnectorActionContext:
    scope_id: str
    credential_store: Any | None = None
    idempotency_store: OutboundIdempotencyStore | None = None


@dataclass(frozen=True, slots=True)
class ConnectorIngressResult:
    delivery_id: str
    changes: tuple[ConnectorChange, ...] = ()


class ConnectorError(Exception):
    pass


class ConnectorUnsupportedError(ConnectorError):
    pass


class ConnectorAuthenticationError(ConnectorError):
    pass


class ConnectorUnavailableError(ConnectorError):
    pass


@runtime_checkable
class ConnectorProvider(Protocol):
    manifest: ConnectorManifest

    def enabled(self) -> bool: ...

    async def begin_auth(self, callback_url: str) -> ConnectorAuthStart: ...

    async def complete_auth(
        self, callback_url: str, parameters: dict[str, str]
    ) -> ConnectorSetupResult: ...

    async def setup(self, values: dict[str, str]) -> ConnectorSetupResult: ...

    async def list_resources(
        self, binding: ConnectorBinding, credential: CredentialEnvelope | None
    ) -> tuple[ConnectorResourceDraft, ...]: ...

    async def sync(
        self,
        binding: ConnectorBinding,
        resources: tuple[ConnectorResource, ...],
        cursors: tuple[ConnectorCursor, ...],
        credential: CredentialEnvelope | None,
    ) -> ConnectorSyncResult: ...

    async def health(
        self, binding: ConnectorBinding, credential: CredentialEnvelope | None
    ) -> ConnectorHealth: ...

    async def revoke(self, credential: CredentialEnvelope | None) -> None: ...

    async def ingress(
        self, headers: dict[str, str], body: bytes, binding: ConnectorBinding
    ) -> ConnectorIngressResult: ...

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]: ...


class BaseConnectorProvider:
    """Fail-closed defaults for provider operations a manifest does not advertise."""

    manifest: ConnectorManifest

    def enabled(self) -> bool:
        return True

    async def begin_auth(self, callback_url: str) -> ConnectorAuthStart:
        raise ConnectorUnsupportedError(f"{self.manifest.id} does not support browser auth")

    async def complete_auth(
        self, callback_url: str, parameters: dict[str, str]
    ) -> ConnectorSetupResult:
        raise ConnectorUnsupportedError(f"{self.manifest.id} does not support auth callbacks")

    async def setup(self, values: dict[str, str]) -> ConnectorSetupResult:
        raise ConnectorUnsupportedError(f"{self.manifest.id} does not support manual setup")

    async def list_resources(
        self, binding: ConnectorBinding, credential: CredentialEnvelope | None
    ) -> tuple[ConnectorResourceDraft, ...]:
        if ConnectorCapability.resources not in self.manifest.capabilities:
            return ()
        raise ConnectorUnsupportedError(f"{self.manifest.id} resource listing is not implemented")

    async def sync(
        self,
        binding: ConnectorBinding,
        resources: tuple[ConnectorResource, ...],
        cursors: tuple[ConnectorCursor, ...],
        credential: CredentialEnvelope | None,
    ) -> ConnectorSyncResult:
        raise ConnectorUnsupportedError(f"{self.manifest.id} sync is not implemented")

    async def health(
        self, binding: ConnectorBinding, credential: CredentialEnvelope | None
    ) -> ConnectorHealth:
        raise ConnectorUnsupportedError(f"{self.manifest.id} health is not implemented")

    async def revoke(self, credential: CredentialEnvelope | None) -> None:
        if self.manifest.auth_kind in {
            ConnectorAuthKind.oauth,
            ConnectorAuthKind.github_app,
        }:
            raise ConnectorUnsupportedError(f"{self.manifest.id} remote revoke is not implemented")
        return None

    async def ingress(
        self, headers: dict[str, str], body: bytes, binding: ConnectorBinding
    ) -> ConnectorIngressResult:
        raise ConnectorUnsupportedError(f"{self.manifest.id} webhook ingress is not implemented")

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
        if self.manifest.actions:
            raise ConnectorUnsupportedError(
                f"{self.manifest.id} action factory wiring is not implemented"
            )
        return ()


@runtime_checkable
class ConnectorProviderFactory(Protocol):
    def __call__(self) -> ConnectorProvider: ...


__all__ = [
    "BaseConnectorProvider",
    "ConnectorAction",
    "ConnectorActionApproval",
    "ConnectorActionContext",
    "ConnectorActionIdempotency",
    "ConnectorActionManifest",
    "ConnectorActionSemantics",
    "ConnectorAuthAction",
    "ConnectorAuthKind",
    "ConnectorAuthenticationError",
    "ConnectorAuthStart",
    "ConnectorBinding",
    "ConnectorBindingDraft",
    "ConnectorBindingStatus",
    "ConnectorBindingTarget",
    "ConnectorCallbackParameter",
    "ConnectorCapability",
    "ConnectorChange",
    "ConnectorChangeKind",
    "ConnectorCursor",
    "ConnectorCursorUpdate",
    "ConnectorError",
    "ConnectorEvent",
    "ConnectorHealth",
    "ConnectorHealthStatus",
    "ConnectorIngressResult",
    "ConnectorItem",
    "ConnectorItemDraft",
    "ConnectorManifest",
    "ConnectorProvider",
    "ConnectorProviderFactory",
    "ConnectorProvenance",
    "ConnectorResource",
    "ConnectorResourceDraft",
    "ConnectorSetupArtifact",
    "ConnectorSetupArtifactKind",
    "ConnectorSetupField",
    "ConnectorSetupResult",
    "ConnectorSyncResult",
    "ConnectorTargetField",
    "ConnectorTargetKind",
    "ConnectorUnsupportedError",
    "ConnectorUnavailableError",
]
