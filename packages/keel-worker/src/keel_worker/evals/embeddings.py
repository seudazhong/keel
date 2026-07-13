"""Deterministic embedding cassette + a fault-injection embedder.

Recorded embeddings are keyed by ``(model, dim, sha256(normalized_text))`` where
normalization only collapses whitespace (case is meaningful to an embedder). One
shared :class:`ReplayEmbedder` backs the production consolidation writes, the
recall search catch-up/query, and the semantic scorer, so a single cassette
covers every vector the run needs. :class:`FailingEmbedder` deterministically
drives the lexical-degraded recall path without touching the cassette.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path

from keel_core.embeddings import Embedder

_WS = re.compile(r"\s+")


class EmbeddingCassetteMiss(Exception):
    """Raised (and captured) when a text has no recorded embedding for (model, dim)."""

    def __init__(self, model: str, dim: int, text_hash: str) -> None:
        super().__init__(f"no recorded embedding for model={model} dim={dim} hash={text_hash}")
        self.model = model
        self.dim = dim
        self.text_hash = text_hash


def normalize_embedding_text(text: str) -> str:
    """Collapse internal whitespace runs and strip; case is preserved.

    An embedder treats case as semantic information, so we only remove the
    incidental whitespace variance that differs between a stored source excerpt
    and a query that quotes it.
    """
    return _WS.sub(" ", text).strip()


def _key(model: str, dim: int, text: str) -> str:
    digest = hashlib.sha256(normalize_embedding_text(text).encode("utf-8")).hexdigest()
    return f"{model}:{dim}:{digest}"


def _atomic_write_text(path: Path, data: str) -> None:
    """Write ``data`` to ``path`` atomically.

    A temp file in the same directory is fully written + fsynced, then
    :func:`os.replace` swaps it into place (an atomic rename on the same
    filesystem, including Windows). The existing file survives until the
    replace, so any failure before it leaves the old file intact; a temp file
    left by a mid-write failure is cleaned up.
    """
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=directory,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    tmp_path = Path(handle.name)
    try:
        with handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


class EmbeddingCassette:
    """A JSON-backed table of ``(model, dim, normalized-text-hash) -> vector``.

    Loading is strict: any pre-existing file must be valid JSON (raising
    otherwise is what we want — a corrupt cassette must fail loudly). Saves are
    atomic so a crash mid-write can never truncate the committed cassette.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, list[float]] = {}
        if path.exists():
            raw = json.loads(path.read_text("utf-8"))
            if not isinstance(raw, dict):
                raise ValueError(f"cassette root must be an object: {path}")
            for key, vector in raw.items():
                if not isinstance(key, str) or not isinstance(vector, list):
                    raise ValueError(f"invalid cassette entry for key {key!r}")
                self._data[key] = [float(x) for x in vector]

    def get(self, model: str, dim: int, text: str) -> list[float] | None:
        return self._data.get(_key(model, dim, text))

    def put(self, model: str, dim: int, text: str, vector: list[float]) -> None:
        if len(vector) != dim:
            raise ValueError(
                f"vector length {len(vector)} does not match dim {dim} for model {model!r}"
            )
        self._data[_key(model, dim, text)] = [float(x) for x in vector]

    def save(self) -> None:
        """Persist atomically (temp file in the same dir + :func:`os.replace`)."""
        blob = json.dumps(self._data, indent=2, sort_keys=True) + "\n"
        _atomic_write_text(self.path, blob)


class RecordingEmbedder:
    """Proxy an :class:`Embedder`, recording every produced vector into a cassette.

    ``.model`` / ``.dim`` are proxied from the inner embedder so a bound-recorder
    is a drop-in replacement for callers that check the pinning invariant.
    """

    def __init__(self, inner: Embedder) -> None:
        self._inner = inner
        self.model = inner.model
        self.dim = inner.dim
        self.cassette: EmbeddingCassette | None = None

    def bind(self, cassette: EmbeddingCassette) -> None:
        self.cassette = cassette

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        vectors = await self._inner.embed(list(texts))
        if self.cassette is not None:
            for text, vector in zip(texts, vectors, strict=True):
                self.cassette.put(self.model, self.dim, text, vector)
        return vectors


class ReplayEmbedder:
    """Serve embeddings from a cassette; a miss is captured and raised (never live).

    ``.miss`` retains the *first* miss for suite-level diagnostics; subsequent
    misses raise but do not overwrite the captured one.
    """

    def __init__(self, cassette: EmbeddingCassette, *, model: str, dim: int) -> None:
        self._cassette = cassette
        self.model = model
        self.dim = dim
        self.miss: EmbeddingCassetteMiss | None = None

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            vector = self._cassette.get(self.model, self.dim, text)
            if vector is None:
                digest = hashlib.sha256(normalize_embedding_text(text).encode("utf-8")).hexdigest()
                miss = EmbeddingCassetteMiss(self.model, self.dim, digest)
                if self.miss is None:
                    self.miss = miss
                raise miss
            if len(vector) != self.dim:
                raise ValueError(
                    f"cassette vector length {len(vector)} does not match "
                    f"expected dim {self.dim} for model {self.model!r}"
                )
            vectors.append(vector)
        return vectors


class FailingEmbedder:
    """A ``(model, dim)``-pinned embedder whose ``embed`` always raises.

    Deterministically drives the lexical-degraded recall path in eval cases
    that inject an embedding-backend outage. Never touches the cassette.
    """

    def __init__(self, model: str = "eval/failing", dim: int = 1024) -> None:
        self.model = model
        self.dim = dim

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        raise RuntimeError("FailingEmbedder: embedding backend unavailable (injected)")
