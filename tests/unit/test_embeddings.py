"""LiteLLMEmbedder: conditional dimensions kwarg + dimension assertion."""

from __future__ import annotations

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
