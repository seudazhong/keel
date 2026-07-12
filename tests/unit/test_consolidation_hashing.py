"""Unit tests for consolidation hashing + idempotency keys."""

from __future__ import annotations

from keel_core.consolidation.hashing import (
    archival_content_hash,
    consolidation_idempotency_key,
    normalize_whitespace,
)


def test_normalize_whitespace_collapses_and_strips() -> None:
    assert normalize_whitespace("  hello   world \n foo ") == "hello world foo"


def test_archival_content_hash_ignores_whitespace_and_case_differences() -> None:
    assert archival_content_hash("hello world") == archival_content_hash("  hello   world ")
    assert archival_content_hash("Hello World") == archival_content_hash("hello world")
    assert archival_content_hash("hello world") != archival_content_hash("hello mars")


def test_idempotency_key_is_stable_and_order_independent() -> None:
    a = consolidation_idempotency_key("web:local", "human", 0, "likes tea", [3, 1, 2])
    b = consolidation_idempotency_key("web:local", "human", 0, "likes tea", [2, 3, 1])
    assert a == b
    assert len(a) == 64


def test_idempotency_key_varies_with_every_input() -> None:
    base = consolidation_idempotency_key("web:local", "human", 0, "likes tea", [1])
    assert base != consolidation_idempotency_key("web:local", "human", 1, "likes tea", [1])
    assert base != consolidation_idempotency_key("web:local", "persona", 0, "likes tea", [1])
    assert base != consolidation_idempotency_key("web:local", "human", 0, "likes coffee", [1])
    assert base != consolidation_idempotency_key("other", "human", 0, "likes tea", [1])
