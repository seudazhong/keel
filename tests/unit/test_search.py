"""Unit tests: RRF fusion and the deterministic fake embedder."""

from __future__ import annotations

import pytest

from keel_core.embeddings import FakeEmbedder, rrf_fuse


def test_rrf_prefers_items_ranked_high_by_either_arm() -> None:
    lexical = [1, 2, 3]
    semantic = [3, 4, 1]
    fused = rrf_fuse([lexical, semantic])
    # id 1 (rank 0 lexical, rank 2 semantic) and id 3 (rank 2 lexical, rank 0 semantic)
    # both score highly and lead; ids seen once trail.
    assert set(fused[:2]) == {1, 3}
    assert set(fused) == {1, 2, 3, 4}


def test_rrf_empty() -> None:
    assert rrf_fuse([]) == []
    assert rrf_fuse([[], []]) == []


async def test_fake_embedder_is_deterministic_and_shape_correct() -> None:
    embedder = FakeEmbedder(dim=16)
    a1 = (await embedder.embed(["hello world"]))[0]
    a2 = (await embedder.embed(["hello world"]))[0]
    assert a1 == a2  # deterministic
    assert len(a1) == 16


async def test_fake_embedder_shared_words_are_closer() -> None:
    embedder = FakeEmbedder(dim=64)
    vecs = await embedder.embed(["cat dog bird", "cat dog fish", "quantum photon electron"])

    def dot(x: list[float], y: list[float]) -> float:
        return sum(a * b for a, b in zip(x, y, strict=True))

    # The two animal phrases (share cat+dog) are more similar than either is to physics.
    assert dot(vecs[0], vecs[1]) > dot(vecs[0], vecs[2])


async def test_hybrid_search_sessions_passes_candidate_limit_to_rank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """hybrid_search_sessions must not cause a second 8x candidate expansion.

    Regression guard for the double-expansion bug: without candidate_limit being
    forwarded, rank_session_messages would compute max(k*8, 80) on the already-
    expanded message_limit, turning k=20 into 1280 candidates instead of 160.
    """
    from keel_core.recall import RecallStatus
    from keel_core.search import hybrid_search_sessions

    captured: dict[str, object] = {}

    async def _spy(
        engine: object,
        scope_id: object,
        query: object,
        *,
        k: int,
        embedder: object,
        candidate_limit: int | None = None,
        batch_size: int = 64,
        catchup_limit: int = 500,
    ) -> tuple[list[object], RecallStatus]:
        captured["k"] = k
        captured["candidate_limit"] = candidate_limit
        return [], RecallStatus(mode="lexical")

    monkeypatch.setattr("keel_core.search.rank_session_messages", _spy)

    await hybrid_search_sessions(None, "scope", "query", k=20)  # type: ignore[arg-type]

    expected = max(20 * 8, 80)  # 160
    assert captured["k"] == expected
    assert captured["candidate_limit"] == expected, (
        "candidate_limit must equal message_limit to prevent a second 8x expansion "
        f"(got {captured['candidate_limit']!r}, expected {expected})"
    )
