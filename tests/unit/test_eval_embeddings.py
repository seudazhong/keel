"""Embedding cassette record/replay + deterministic failing embedder."""

from __future__ import annotations

from pathlib import Path

import pytest

from keel_core.embeddings import FakeEmbedder
from keel_worker.evals.embeddings import (
    EmbeddingCassette,
    EmbeddingCassetteMiss,
    FailingEmbedder,
    RecordingEmbedder,
    ReplayEmbedder,
    normalize_embedding_text,
)


def test_normalize_preserves_case_collapses_ws() -> None:
    assert normalize_embedding_text("  Hello   World \n") == "Hello World"


async def test_record_then_replay(tmp_path: Path) -> None:
    path = tmp_path / "e.json"
    inner = FakeEmbedder(dim=16, model="fake/embed")
    recorder = RecordingEmbedder(inner)
    recorder.bind(EmbeddingCassette(path))
    vectors = await recorder.embed(["alpha beta", "gamma"])
    assert recorder.cassette is not None
    recorder.cassette.save()

    replay = ReplayEmbedder(EmbeddingCassette(path), model="fake/embed", dim=16)
    # whitespace-normalized cache hit
    replayed = await replay.embed(["alpha   beta", "gamma"])
    assert replayed == vectors
    assert replay.miss is None
    assert replay.model == "fake/embed"
    assert replay.dim == 16


async def test_replay_miss_sets_flag_and_raises(tmp_path: Path) -> None:
    replay = ReplayEmbedder(EmbeddingCassette(tmp_path / "empty.json"), model="fake/embed", dim=16)
    with pytest.raises(EmbeddingCassetteMiss):
        await replay.embed(["never recorded"])
    assert replay.miss is not None


async def test_failing_embedder_always_raises() -> None:
    failing = FailingEmbedder()
    assert failing.model == "eval/failing"
    assert failing.dim == 1024
    with pytest.raises(RuntimeError):
        await failing.embed(["anything"])


def test_embedding_save_is_atomic_and_preserves_old_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "e.json"
    first = EmbeddingCassette(path)
    first.put("fake/embed", 3, "alpha", [0.1, 0.2, 0.3])
    first.save()
    original = path.read_text("utf-8")

    # A failure during the atomic replace keeps the committed vectors intact and
    # leaves no temp file behind.
    second = EmbeddingCassette(path)
    second.put("fake/embed", 3, "beta", [0.4, 0.5, 0.6])

    def _boom(src: object, dst: object) -> None:
        raise RuntimeError("replace failed")

    monkeypatch.setattr("os.replace", _boom)
    with pytest.raises(RuntimeError):
        second.save()
    assert path.read_text("utf-8") == original  # old vectors survived the failure
    assert [p.name for p in tmp_path.iterdir()] == ["e.json"]  # temp file cleaned up

    monkeypatch.undo()
    second.save()  # a clean save atomically replaces the file, leaving no temp behind
    assert [p.name for p in tmp_path.iterdir()] == ["e.json"]
    reloaded = EmbeddingCassette(path)
    assert reloaded.get("fake/embed", 3, "beta") == [0.4, 0.5, 0.6]
    assert reloaded.get("fake/embed", 3, "alpha") == [0.1, 0.2, 0.3]


async def test_recording_embedder_proxies_model_and_dim() -> None:
    """RecordingEmbedder proxies .model and .dim from the inner embedder (no bind required)."""
    inner = FakeEmbedder(dim=8, model="fake/proxied")
    recorder = RecordingEmbedder(inner)
    assert recorder.model == "fake/proxied"
    assert recorder.dim == 8
    # unbound recorder still returns vectors (nothing to persist)
    vectors = await recorder.embed(["hello"])
    assert len(vectors) == 1 and len(vectors[0]) == 8


async def test_replay_batch_mixed_hit_miss_raises_on_first_miss(tmp_path: Path) -> None:
    """A batch with a recorded head + an unrecorded tail raises on the miss and captures it."""
    path = tmp_path / "e.json"
    cassette = EmbeddingCassette(path)
    cassette.put("fake/embed", 4, "known", [0.0, 0.1, 0.2, 0.3])
    cassette.save()

    replay = ReplayEmbedder(EmbeddingCassette(path), model="fake/embed", dim=4)
    with pytest.raises(EmbeddingCassetteMiss):
        await replay.embed(["known", "unknown"])
    assert replay.miss is not None
    assert replay.miss.model == "fake/embed"
    assert replay.miss.dim == 4
