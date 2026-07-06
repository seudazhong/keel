"""Embeddings + Reciprocal Rank Fusion for hybrid retrieval (WS-D, ADR-0007).

An :class:`Embedder` is pinned to a ``(model, dim)`` pair; archival rows record
which produced them so a search only compares like-with-like. :func:`rrf_fuse`
merges the lexical and semantic ranked lists into one order without tuning score
scales — the standard hybrid-search combiner.
"""

from __future__ import annotations

import hashlib
import math
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

    def __init__(self, model: str, dim: int) -> None:
        self.model = model
        self.dim = dim

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        from litellm import aembedding

        response = await aembedding(model=self.model, input=list(texts), dimensions=self.dim)
        data: list[dict[str, Any]] = response.data
        return [list(item["embedding"]) for item in data]


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
