"""Versioned connector credential envelopes stored inside encrypted token rows."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

_MARKER = "keel_connector_credential"
_VERSION = 1


@dataclass(frozen=True, slots=True)
class CredentialEnvelope:
    """A typed plaintext payload that is encrypted by the existing token store."""

    kind: str
    values: dict[str, Any] = field(repr=False)
    version: int = _VERSION

    def __post_init__(self) -> None:
        if self.version != _VERSION:
            raise ValueError(f"unsupported connector credential envelope version: {self.version}")
        if not self.kind.strip():
            raise ValueError("credential kind must not be blank")

    def serialize(self) -> str:
        return json.dumps(
            {
                _MARKER: self.version,
                "kind": self.kind,
                "values": self.values,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def parse(cls, value: str) -> CredentialEnvelope | None:
        """Parse an envelope, returning ``None`` for legacy unversioned token payloads."""
        try:
            raw = json.loads(value)
        except (TypeError, ValueError):
            return None
        if not isinstance(raw, dict) or _MARKER not in raw:
            return None
        version = raw.get(_MARKER)
        kind = raw.get("kind")
        values = raw.get("values")
        if (
            not isinstance(version, int)
            or not isinstance(kind, str)
            or not isinstance(values, dict)
        ):
            raise ValueError("invalid connector credential envelope")
        return cls(kind=kind, values=values, version=version)


@dataclass(frozen=True, slots=True)
class VersionedCredential:
    envelope: CredentialEnvelope = field(repr=False)
    version: int


class VersionedTokenStore(Protocol):
    async def put(self, connector_id: str, secret: str) -> None: ...

    async def get(self, connector_id: str) -> str | None: ...

    async def get_versioned(self, connector_id: str) -> tuple[str, int] | None: ...

    async def put_if_version(
        self, connector_id: str, secret: str, expected_version: int
    ) -> int | None: ...

    async def delete(self, connector_id: str) -> None: ...


class ConnectorCredentialStore:
    """Envelope-aware adapter over the existing encrypted connector token store."""

    def __init__(self, token_store: VersionedTokenStore) -> None:
        self._tokens = token_store

    async def put(self, connector_id: str, envelope: CredentialEnvelope) -> None:
        await self._tokens.put(connector_id, envelope.serialize())

    async def get(self, connector_id: str) -> CredentialEnvelope | None:
        encoded = await self._tokens.get(connector_id)
        if encoded is None:
            return None
        return _parse_stored_credential(encoded)

    async def get_versioned(self, connector_id: str) -> VersionedCredential | None:
        stored = await self._tokens.get_versioned(connector_id)
        if stored is None:
            return None
        encoded, version = stored
        return VersionedCredential(_parse_stored_credential(encoded), version)

    async def put_if_version(
        self,
        connector_id: str,
        envelope: CredentialEnvelope,
        expected_version: int,
    ) -> int | None:
        return await self._tokens.put_if_version(
            connector_id,
            envelope.serialize(),
            expected_version,
        )

    async def delete(self, connector_id: str) -> None:
        await self._tokens.delete(connector_id)


def _parse_stored_credential(encoded: str) -> CredentialEnvelope:
    parsed = CredentialEnvelope.parse(encoded)
    if parsed is None:
        return CredentialEnvelope(kind="legacy", values={"value": encoded})
    return parsed


def unwrap_legacy_or_enveloped(value: str, *, expected_kind: str) -> tuple[str, bool]:
    """Return a legacy string or an envelope's JSON values for provider client libraries."""
    envelope = CredentialEnvelope.parse(value)
    if envelope is None:
        return value, False
    if envelope.kind != expected_kind:
        raise ValueError(f"unexpected connector credential kind: {envelope.kind}")
    return json.dumps(envelope.values, ensure_ascii=False, separators=(",", ":")), True


def rewrap_provider_json(value: str, *, kind: str, enveloped: bool) -> str:
    if not enveloped:
        return value
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("provider credentials must serialize to a JSON object")
    return CredentialEnvelope(kind=kind, values=parsed).serialize()


__all__ = [
    "ConnectorCredentialStore",
    "CredentialEnvelope",
    "VersionedCredential",
    "VersionedTokenStore",
    "rewrap_provider_json",
    "unwrap_legacy_or_enveloped",
]
