"""Embeddings + Reciprocal Rank Fusion for hybrid retrieval (WS-D, ADR-0007).

An :class:`Embedder` is pinned to a ``(model, dim)`` pair; archival rows record
which produced them so a search only compares like-with-like. :func:`rrf_fuse`
merges the lexical and semantic ranked lists into one order without tuning score
scales — the standard hybrid-search combiner.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import struct
from collections.abc import Iterable, Sequence
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Embedder(Protocol):
    """Turns text into vectors, pinned to one ``(model, dim)`` (ADR-0007)."""

    model: str
    dim: int

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Return one embedding per input text."""
        ...


def normalize_embedding_vector(value: object, *, dim: int) -> list[float] | None:
    """Return the finite, non-zero float32 vector pgvector will receive."""
    if not isinstance(value, Sequence) or isinstance(value, str | bytes) or len(value) != dim:
        return None
    vector: list[float] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int | float):
            return None
        try:
            number = float(item)
        except (OverflowError, TypeError, ValueError):
            return None
        if not math.isfinite(number):
            return None
        try:
            float32 = struct.unpack("!f", struct.pack("!f", number))[0]
        except (OverflowError, struct.error):
            return None
        if not math.isfinite(float32):
            return None
        vector.append(float32)
    return vector if any(vector) else None


class FakeEmbedder:
    """Deterministic, offline embedder for tests (hashed bag-of-words).

    Shared words produce similar vectors, so semantic retrieval is meaningful in
    tests without a network call.
    """

    def __init__(self, dim: int = 16, model: str = "fake/embed") -> None:
        self.model = model
        self.dim = dim

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [self._vector(text) for text in texts]

    def _vector(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        for token in text.lower().split():
            bucket = int(hashlib.sha1(token.encode("utf-8")).hexdigest(), 16) % self.dim
            vec[bucket] += 1.0
        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        return [x / norm for x in vec]


class LiteLLMEmbedder:
    """A ``(model, dim)``-pinned embedder over LiteLLM (any provider)."""

    def __init__(
        self,
        model: str,
        dim: int,
        *,
        send_dimensions: bool = False,
        timeout_seconds: float = 10.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        self.model = model
        self.dim = dim
        self.send_dimensions = send_dimensions
        self.timeout_seconds = timeout_seconds

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        from litellm import aembedding

        kwargs: dict[str, Any] = {"model": self.model, "input": list(texts)}
        if self.send_dimensions:  # OpenAI text-embedding-3 accepts it; Ollama rejects it
            kwargs["dimensions"] = self.dim
        async with asyncio.timeout(self.timeout_seconds):
            response = await aembedding(**kwargs)
        data: list[dict[str, Any]] = response.data
        vectors = [list(item["embedding"]) for item in data]
        for vec in vectors:
            if len(vec) != self.dim:
                raise ValueError(
                    f"embedding dim {len(vec)} != expected {self.dim} for model {self.model}"
                )
        return vectors


def rrf_fuse(ranked_lists: Iterable[Sequence[int]], *, k: int = 60) -> list[int]:
    """Fuse ranked id lists by Reciprocal Rank Fusion; return ids best-first.

    Each list contributes ``1 / (k + rank)`` to a document's score (rank is 0-based),
    so an item ranked highly by either arm floats up without scale tuning.
    """
    scores: dict[int, float] = {}
    for ranked in ranked_lists:
        for rank, doc_id in enumerate(ranked):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
    return sorted(scores, key=lambda doc_id: scores[doc_id], reverse=True)
