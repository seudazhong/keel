"""Deterministic hashing for consolidation dedupe + write idempotency.

``archival_content_hash`` keys archival dedupe on case-folded, whitespace-normalized
content so trivially reformatted passages collapse to one row. ``consolidation_idempotency_key``
makes a proposal write idempotent under whole-batch retry: the same (scope, block,
expected_version, value, cited events) always yields the same key, and the cited-event
list is order-independent.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence

_MARKDOWN_LIST_PREFIX = re.compile(r"(?m)^\s*(?:[-*+]|\d+[.)])\s+")
_TRAILING_SENTENCE_PUNCTUATION = re.compile(r"[.!?。！？]+$")


def normalize_whitespace(text: str) -> str:
    """Collapse every run of whitespace to a single space and strip the ends."""
    return " ".join(text.split())


def normalize_proposed_value(text: str) -> str:
    """Canonicalize formatting-only model drift without collapsing different facts."""
    without_list_markers = _MARKDOWN_LIST_PREFIX.sub("", text)
    normalized = normalize_whitespace(without_list_markers).casefold()
    return _TRAILING_SENTENCE_PUNCTUATION.sub("", normalized)


def archival_content_hash(content: str) -> str:
    """A stable SHA-256 of case-folded, whitespace-normalized content."""
    normalized = normalize_whitespace(content).casefold()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def consolidation_idempotency_key(
    scope_id: str,
    block: str,
    expected_version: int,
    proposed_value: str,
    source_event_ids: Sequence[int],
) -> str:
    """A stable SHA-256 identifying one normalized proposal write."""
    payload = json.dumps(
        {
            "scope_id": scope_id,
            "block": block,
            "expected_version": expected_version,
            "proposed_value": normalize_proposed_value(proposed_value),
            "source_event_ids": sorted({int(i) for i in source_event_ids}),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
