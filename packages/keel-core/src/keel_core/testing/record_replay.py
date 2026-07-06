"""Provider record/replay: deterministic, offline provider streams.

A :class:`Cassette` is a JSON store mapping a stable hash of a
:class:`~keel_core.protocols.ProviderRequest` to the sequence of
:class:`~keel_core.protocols.ProviderChunk` it produced. :class:`ReplayProviderGateway`
implements the ``ProviderGateway`` Protocol by replaying that sequence, so tests
are byte-for-byte deterministic without a network call.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from keel_core.protocols import ProviderChunk, ProviderRequest


def request_key(request: ProviderRequest) -> str:
    """Return a stable short hash identifying a provider request."""
    blob = json.dumps(request.model_dump(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


class Cassette:
    """A JSON store of recorded provider chunk sequences, keyed by request."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, list[dict[str, Any]]] = {}
        if path.exists():
            self._data = json.loads(path.read_text("utf-8"))

    def get(self, key: str) -> list[ProviderChunk] | None:
        """Return the recorded chunks for ``key``, or ``None`` if absent."""
        raw = self._data.get(key)
        if raw is None:
            return None
        return [ProviderChunk.model_validate(chunk) for chunk in raw]

    def put(self, key: str, chunks: list[ProviderChunk]) -> None:
        """Record a chunk sequence under ``key``."""
        self._data[key] = [chunk.model_dump() for chunk in chunks]

    def save(self) -> None:
        """Persist the cassette to disk (stable ordering for clean diffs)."""
        self.path.write_text(
            json.dumps(self._data, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )


async def _aiter(chunks: list[ProviderChunk]) -> AsyncIterator[ProviderChunk]:
    for chunk in chunks:
        yield chunk


class ReplayProviderGateway:
    """A ``ProviderGateway`` that replays recorded chunks (no network)."""

    def __init__(self, cassette: Cassette) -> None:
        self._cassette = cassette

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        """Replay the recorded chunk sequence for ``request``."""
        chunks = self._cassette.get(request_key(request))
        if chunks is None:
            raise KeyError(f"no cassette entry for request {request_key(request)}")
        return _aiter(chunks)
