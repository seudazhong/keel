"""Deterministic discovery of built-in connector providers."""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass
from functools import lru_cache
from types import ModuleType

from keel_core.connector_contracts import (
    ConnectorManifest,
    ConnectorProvider,
    ConnectorProviderFactory,
)


@dataclass(frozen=True, slots=True)
class ConnectorRegistration:
    manifest: ConnectorManifest
    factory: ConnectorProviderFactory
    module_name: str


class ConnectorRegistry:
    def __init__(self, registrations: tuple[ConnectorRegistration, ...]) -> None:
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
            provider = item.factory()
            if provider.manifest != item.manifest:
                raise ValueError(
                    f"connector factory in {item.module_name} returned a mismatched manifest"
                )
            by_id[connector_id] = item
        self._ordered = tuple(ordered)
        self._by_id = by_id

    def manifests(self) -> tuple[ConnectorManifest, ...]:
        return tuple(item.manifest for item in self._ordered)

    def get(self, connector_id: str) -> ConnectorRegistration | None:
        return self._by_id.get(connector_id)

    def create(self, connector_id: str) -> ConnectorProvider:
        registration = self.get(connector_id)
        if registration is None:
            raise KeyError(connector_id)
        return registration.factory()


def _registration_from_module(module: ModuleType) -> ConnectorRegistration | None:
    manifest = getattr(module, "manifest", None)
    factory = getattr(module, "factory", None)
    if manifest is None and factory is None:
        return None
    if not isinstance(manifest, ConnectorManifest) or not callable(factory):
        raise ValueError(
            f"{module.__name__} must export ConnectorManifest 'manifest' and 'factory'"
        )
    return ConnectorRegistration(manifest=manifest, factory=factory, module_name=module.__name__)


def discover_connector_registry() -> ConnectorRegistry:
    package = importlib.import_module("keel_core.connector_providers")
    names = sorted(
        item.name
        for item in pkgutil.iter_modules(package.__path__, f"{package.__name__}.")
        if not item.name.rsplit(".", 1)[-1].startswith("_")
    )
    registrations: list[ConnectorRegistration] = []
    for name in names:
        registration = _registration_from_module(importlib.import_module(name))
        if registration is not None:
            registrations.append(registration)
    return ConnectorRegistry(tuple(registrations))


@lru_cache(maxsize=1)
def get_connector_registry() -> ConnectorRegistry:
    return discover_connector_registry()


__all__ = [
    "ConnectorRegistration",
    "ConnectorRegistry",
    "discover_connector_registry",
    "get_connector_registry",
]
