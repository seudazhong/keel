"""Deterministic discovery of built-in connector providers."""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from types import ModuleType
from typing import Any

from keel_core.connector_contracts import (
    ConnectorAction,
    ConnectorActionContext,
    ConnectorManifest,
    ConnectorProvider,
    ConnectorProviderFactory,
    ConnectorUnavailableError,
)
from keel_core.connectors import ActionFn
from keel_core.protocols import ToolContext


def _enabled() -> bool:
    return True


@dataclass(frozen=True, slots=True)
class ConnectorRegistration:
    manifest: ConnectorManifest
    factory: ConnectorProviderFactory
    module_name: str
    enabled: Callable[[], bool] = _enabled
    availability: Callable[[], None] | None = None


@dataclass(frozen=True, slots=True)
class ConnectorProviderStatus:
    available: bool
    enabled: bool
    error: str | None = None


class ConnectorProviderUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ConnectorDiscoveryFailure:
    connector_id: str
    module_name: str
    error: str


class ConnectorRegistry:
    def __init__(
        self,
        registrations: tuple[ConnectorRegistration, ...],
        failures: tuple[ConnectorDiscoveryFailure, ...] = (),
    ) -> None:
        ordered = sorted(registrations, key=lambda item: item.manifest.id)
        by_id: dict[str, ConnectorRegistration] = {}
        for item in ordered:
            connector_id = item.manifest.id
            if connector_id in by_id:
                prior = by_id[connector_id]
                raise ValueError(
                    f"duplicate connector id {connector_id!r}: "
                    f"{prior.module_name} and {item.module_name}"
                )
            by_id[connector_id] = item
        self._ordered = tuple(ordered)
        self._by_id = by_id
        self._failures = {item.connector_id: item for item in failures}
        overlap = set(self._failures) & set(self._by_id)
        if overlap:
            raise ValueError(
                f"connector discovery failures overlap registrations: {', '.join(sorted(overlap))}"
            )

    def manifests(self) -> tuple[ConnectorManifest, ...]:
        return tuple(item.manifest for item in self._ordered)

    def get(self, connector_id: str) -> ConnectorRegistration | None:
        return self._by_id.get(connector_id)

    def discovery_failures(self) -> tuple[ConnectorDiscoveryFailure, ...]:
        return tuple(self._failures[key] for key in sorted(self._failures))

    def create(self, connector_id: str) -> ConnectorProvider:
        registration = self.get(connector_id)
        if registration is None:
            failure = self._failures.get(connector_id)
            if failure is not None:
                raise ConnectorProviderUnavailableError(failure.error)
            raise KeyError(connector_id)
        try:
            if registration.enabled() and registration.availability is not None:
                registration.availability()
            provider = registration.factory()
        except ImportError as exc:
            dependency = exc.name or "optional dependency"
            raise ConnectorProviderUnavailableError(
                f"connector {connector_id!r} is unavailable: missing {dependency}"
            ) from exc
        except ConnectorUnavailableError as exc:
            raise ConnectorProviderUnavailableError(str(exc)) from exc
        if provider.manifest != registration.manifest:
            raise ValueError(
                f"connector factory in {registration.module_name} returned a mismatched manifest"
            )
        return provider

    def status(self, connector_id: str) -> ConnectorProviderStatus:
        registration = self.get(connector_id)
        if registration is None:
            failure = self._failures.get(connector_id)
            if failure is not None:
                return ConnectorProviderStatus(False, False, failure.error)
            raise KeyError(connector_id)
        try:
            enabled = registration.enabled()
            if enabled and registration.availability is not None:
                registration.availability()
        except ImportError as exc:
            dependency = exc.name or "optional dependency"
            return ConnectorProviderStatus(
                available=False,
                enabled=False,
                error=f"Missing optional dependency: {dependency}",
            )
        except ConnectorUnavailableError as exc:
            return ConnectorProviderStatus(False, False, str(exc))
        return ConnectorProviderStatus(available=True, enabled=enabled)

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
        if context.credential_store is None:
            return ()
        actions: dict[str, ConnectorAction] = {}
        for manifest in self.manifests():
            status = self.status(manifest.id)
            if not status.available or not status.enabled or not manifest.actions:
                continue
            try:
                provider = self.create(manifest.id)
            except ConnectorProviderUnavailableError:
                continue
            declared = {item.name: item for item in manifest.actions}
            for action in provider.build_actions(context):
                expected = declared.get(action.manifest.name)
                if expected is None or expected != action.manifest:
                    raise ValueError(
                        f"connector {manifest.id!r} returned an undeclared action "
                        f"{action.manifest.name!r}"
                    )
                if action.manifest.name in actions:
                    raise ValueError(f"duplicate connector action {action.manifest.name!r}")
                actions[action.manifest.name] = ConnectorAction(
                    action.manifest,
                    _scope_bound_action(action.action, context.scope_id),
                    connector_id=manifest.id,
                )
        return tuple(actions[name] for name in sorted(actions))


def _scope_bound_action(action: ActionFn, scope_id: str) -> ActionFn:
    async def invoke(arguments: dict[str, Any], tool_context: ToolContext) -> str:
        if tool_context.scope_id != scope_id:
            raise PermissionError("connector action cannot cross its configured scope")
        return await action(arguments, tool_context)

    return invoke


def _registration_from_module(module: ModuleType) -> ConnectorRegistration | None:
    manifest = getattr(module, "manifest", None)
    factory = getattr(module, "factory", None)
    if manifest is None and factory is None:
        return None
    if not isinstance(manifest, ConnectorManifest) or not callable(factory):
        raise ValueError(
            f"{module.__name__} must export ConnectorManifest 'manifest' and 'factory'"
        )
    enabled = getattr(module, "enabled", _enabled)
    availability = getattr(module, "availability", None)
    if not callable(enabled) or (availability is not None and not callable(availability)):
        raise ValueError(f"{module.__name__} connector status hooks must be callable")
    return ConnectorRegistration(
        manifest=manifest,
        factory=factory,
        module_name=module.__name__,
        enabled=enabled,
        availability=availability,
    )


def discover_connector_registry() -> ConnectorRegistry:
    package = importlib.import_module("keel_core.connector_providers")
    names = sorted(
        item.name
        for item in pkgutil.iter_modules(package.__path__, f"{package.__name__}.")
        if not item.name.rsplit(".", 1)[-1].startswith("_")
    )
    registrations: list[ConnectorRegistration] = []
    failures: list[ConnectorDiscoveryFailure] = []
    for name in names:
        try:
            module = importlib.import_module(name)
        except ImportError as exc:
            connector_id = name.rsplit(".", 1)[-1].replace("-", "_").lower()
            dependency = exc.name or "optional dependency"
            failures.append(
                ConnectorDiscoveryFailure(
                    connector_id,
                    name,
                    f"Connector {connector_id!r} is unavailable: missing {dependency}",
                )
            )
            continue
        registration = _registration_from_module(module)
        if registration is not None:
            registrations.append(registration)
    return ConnectorRegistry(tuple(registrations), tuple(failures))


@lru_cache(maxsize=1)
def get_connector_registry() -> ConnectorRegistry:
    return discover_connector_registry()


__all__ = [
    "ConnectorDiscoveryFailure",
    "ConnectorProviderStatus",
    "ConnectorProviderUnavailableError",
    "ConnectorRegistration",
    "ConnectorRegistry",
    "discover_connector_registry",
    "get_connector_registry",
]
