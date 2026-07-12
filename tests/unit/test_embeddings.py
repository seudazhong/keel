"""LiteLLMEmbedder: conditional dimensions kwarg + dimension assertion."""

from __future__ import annotations

import asyncio
from typing import Any

import litellm
import pytest

from keel_core.embeddings import LiteLLMEmbedder


def _fake_aembedding(captured: dict[str, Any], vec_len: int):
    async def aembedding(**kwargs: Any) -> Any:
        captured.update(kwargs)

        class _Resp:
            data = [{"embedding": [0.0] * vec_len}]

        return _Resp()

    return aembedding


async def test_omits_dimensions_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(litellm, "aembedding", _fake_aembedding(captured, 4))
    out = await LiteLLMEmbedder("ollama/bge-m3", 4).embed(["hi"])
    assert "dimensions" not in captured  # Ollama rejects it
    assert out == [[0.0] * 4]


async def test_sends_dimensions_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(litellm, "aembedding", _fake_aembedding(captured, 8))
    await LiteLLMEmbedder("openai/text-embedding-3-small", 8, send_dimensions=True).embed(["hi"])
    assert captured["dimensions"] == 8


async def test_dimension_mismatch_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(litellm, "aembedding", _fake_aembedding(captured, 3))  # wrong length
    with pytest.raises(ValueError, match="!= expected 1024"):
        await LiteLLMEmbedder("ollama/bge-m3", 1024).embed(["hi"])


async def test_embedding_call_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    async def slow_aembedding(**kwargs: Any) -> Any:
        await asyncio.sleep(1)
        raise AssertionError(f"unexpected completion: {kwargs}")

    monkeypatch.setattr(litellm, "aembedding", slow_aembedding)

    with pytest.raises(TimeoutError):
        await LiteLLMEmbedder(
            "ollama/bge-m3",
            1024,
            timeout_seconds=0.01,
        ).embed(["hi"])
