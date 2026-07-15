from __future__ import annotations

from collections.abc import Sequence

import pytest

from keel_core.knowledge.chunking import chunk_document, normalize_document_text
from keel_core.knowledge.models import (
    ChunkDraft,
    KnowledgeSourceType,
    KnowledgeValidationError,
    content_sha256,
)


def _assert_valid_chunks(content: str, chunks: Sequence[ChunkDraft]) -> None:
    assert chunks
    assert [chunk.ordinal for chunk in chunks] == list(range(len(chunks)))

    covered_until = 0
    previous_start = -1
    for chunk in chunks:
        assert chunk.text
        assert chunk.text.strip()
        assert chunk.text == content[chunk.char_start : chunk.char_end]
        assert chunk.content_hash == content_sha256(chunk.text)
        assert isinstance(chunk.heading_path, tuple)
        assert chunk.char_start >= previous_start
        assert chunk.char_start <= covered_until
        covered_until = max(covered_until, chunk.char_end)
        previous_start = chunk.char_start

    assert chunks[0].char_start == 0
    assert covered_until == len(content)


def test_normalize_document_text_applies_canonical_transformations() -> None:
    raw = "\ufeff \t\r\nFirst \t\r\n" + "\r\n" * 4 + "Second\t \r\n \t"

    assert normalize_document_text(raw, max_bytes=1024) == "First\n\n\nSecond"


def test_normalize_document_text_preserves_three_blank_lines() -> None:
    assert normalize_document_text("A\n\n\n\nB", max_bytes=1024) == "A\n\n\n\nB"


def test_normalize_document_text_enforces_utf8_bytes_after_normalizing() -> None:
    assert normalize_document_text("é \r\n", max_bytes=2) == "é"

    with pytest.raises(KnowledgeValidationError, match="content_too_large") as caught:
        normalize_document_text("private-é", max_bytes=1)
    assert "private" not in caught.value.public_message


@pytest.mark.parametrize("value", [None, b"text", 1, "\x00", "\ud800", " \t\r\n"])
def test_normalize_document_text_rejects_invalid_content(value: object) -> None:
    with pytest.raises(KnowledgeValidationError, match="content_invalid"):
        normalize_document_text(value, max_bytes=1024)  # type: ignore[arg-type]


@pytest.mark.parametrize("max_bytes", [True, False, 0, -1, 1.5, "10", None])
def test_normalize_document_text_requires_strict_positive_max_bytes(max_bytes: object) -> None:
    with pytest.raises(KnowledgeValidationError, match="invalid_knowledge_input"):
        normalize_document_text("text", max_bytes=max_bytes)  # type: ignore[arg-type]


def test_normalize_document_text_accepts_exact_multibyte_boundary() -> None:
    assert normalize_document_text("🙂", max_bytes=4) == "🙂"


@pytest.mark.parametrize(
    "content",
    [
        "\ufefftext",
        "text\r\n",
        " text",
        "text \nnext",
        "A\n\n\n\n\nB",
    ],
)
def test_chunk_document_requires_canonical_normalized_content(content: str) -> None:
    with pytest.raises(KnowledgeValidationError, match="content_invalid"):
        chunk_document(
            content,
            KnowledgeSourceType.text,
            target_chars=10,
            overlap_chars=0,
        )


def test_chunk_document_rejects_unsafe_content_without_echoing_it() -> None:
    with pytest.raises(KnowledgeValidationError, match="content_invalid") as caught:
        chunk_document(
            "secret\x00payload",
            KnowledgeSourceType.text,
            target_chars=10,
            overlap_chars=0,
        )
    assert "secret" not in caught.value.public_message


@pytest.mark.parametrize(
    ("target_chars", "overlap_chars"),
    [
        (True, 0),
        (1.5, 0),
        (0, 0),
        (-1, 0),
        (10, True),
        (10, 1.5),
        (10, -1),
        (10, 10),
        (10, 11),
    ],
)
def test_chunk_document_requires_strict_chunk_settings(
    target_chars: object,
    overlap_chars: object,
) -> None:
    with pytest.raises(ValueError):
        chunk_document(
            "text",
            KnowledgeSourceType.text,
            target_chars=target_chars,  # type: ignore[arg-type]
            overlap_chars=overlap_chars,  # type: ignore[arg-type]
        )


def test_chunk_document_requires_source_type_enum() -> None:
    with pytest.raises(ValueError, match="KnowledgeSourceType"):
        chunk_document(
            "text",
            "text",  # type: ignore[arg-type]
            target_chars=10,
            overlap_chars=0,
        )


def test_plain_text_packing_preserves_offsets_hashes_and_unicode() -> None:
    content = "Alpha.\n\n中文🙂 paragraph.\n\nOmega."
    chunks = chunk_document(
        content,
        KnowledgeSourceType.text,
        target_chars=22,
        overlap_chars=0,
    )

    _assert_valid_chunks(content, chunks)
    assert all(chunk.heading_path == () for chunk in chunks)
    assert "".join(chunk.text for chunk in chunks) == content


def test_plain_text_overlap_reuses_trailing_complete_units() -> None:
    content = "one.\n\ntwo.\n\nthree.\n\nfour."
    chunks = chunk_document(
        content,
        KnowledgeSourceType.text,
        target_chars=14,
        overlap_chars=6,
    )

    assert [chunk.text for chunk in chunks] == [
        "one.\n\ntwo.\n\n",
        "two.\n\nthree.\n\n",
        "four.",
    ]
    _assert_valid_chunks(content, chunks)


def test_oversized_plain_unit_hard_splits_with_character_overlap() -> None:
    content = "abcdefghij"
    chunks = chunk_document(
        content,
        KnowledgeSourceType.text,
        target_chars=4,
        overlap_chars=1,
    )

    assert [(chunk.char_start, chunk.char_end, chunk.text) for chunk in chunks] == [
        (0, 4, "abcd"),
        (3, 7, "defg"),
        (6, 10, "ghij"),
    ]
    _assert_valid_chunks(content, chunks)


def test_plain_text_target_boundary_and_zero_overlap() -> None:
    boundary = chunk_document(
        "甲乙🙂丙",
        KnowledgeSourceType.text,
        target_chars=4,
        overlap_chars=0,
    )
    split = chunk_document(
        "abcdefgh",
        KnowledgeSourceType.text,
        target_chars=4,
        overlap_chars=0,
    )

    assert [chunk.text for chunk in boundary] == ["甲乙🙂丙"]
    assert [chunk.text for chunk in split] == ["abcd", "efgh"]


def test_blank_only_separator_becomes_a_plain_text_boundary() -> None:
    content = normalize_document_text("One \t\r\n \t\r\nTwo", max_bytes=1024)
    chunks = chunk_document(
        content,
        KnowledgeSourceType.text,
        target_chars=5,
        overlap_chars=0,
    )

    assert content == "One\n\nTwo"
    assert [chunk.text for chunk in chunks] == ["One\n\n", "Two"]


def test_markdown_chunking_tracks_nested_heading_paths_and_offsets() -> None:
    content = normalize_document_text(
        "# Guide\r\n\r\nIntro.\r\n\r\n### Deep ###\r\n\r\nDetails.\r\n\r\n"
        "## Setup\r\n\r\nInstall Keel.",
        max_bytes=1024,
    )
    chunks = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=100,
        overlap_chars=10,
    )

    assert [chunk.heading_path for chunk in chunks] == [
        ("Guide",),
        ("Guide", "Deep"),
        ("Guide", "Setup"),
    ]
    _assert_valid_chunks(content, chunks)


def test_markdown_heading_inside_matching_fence_is_not_parsed() -> None:
    content = "# Guide\n\n```md\n# Not a heading\n```\n\nAfter."
    chunks = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=100,
        overlap_chars=10,
    )

    assert [chunk.heading_path for chunk in chunks] == [("Guide",)]
    assert chunks[0].text == content


def test_unclosed_or_mismatched_fence_consumes_the_remainder() -> None:
    content = "# Guide\n\n~~~md\n## Hidden\n```\n## Still hidden"
    chunks = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=100,
        overlap_chars=10,
    )

    assert [chunk.heading_path for chunk in chunks] == [("Guide",)]
    assert chunks[0].text == content


def test_fenced_code_is_indivisible_until_four_target_hard_limit() -> None:
    content = "~~~python\nabcdefghijklm\n~~~"
    chunks = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=10,
        overlap_chars=2,
    )

    assert len(content) > 10
    assert [chunk.text for chunk in chunks] == [content]


def test_fenced_code_above_hard_limit_uses_deterministic_hard_splits() -> None:
    content = "```\n" + "x" * 40 + "\n```"
    chunks = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=8,
        overlap_chars=2,
    )

    assert len(chunks) > 1
    assert all(len(chunk.text) <= 8 for chunk in chunks)
    assert all(
        current.char_start == previous.char_end - 2
        for previous, current in zip(chunks, chunks[1:], strict=False)
    )
    _assert_valid_chunks(content, chunks)


def test_markdown_contiguous_list_lines_stay_in_one_unit() -> None:
    content = "- one\n  continued\n  - nested\n- two\n\nAfter."
    chunks = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=36,
        overlap_chars=0,
    )

    assert [chunk.text for chunk in chunks] == [
        "- one\n  continued\n  - nested\n- two\n\n",
        "After.",
    ]
    _assert_valid_chunks(content, chunks)


def test_markdown_overlap_does_not_cross_heading_section_changes() -> None:
    content = "# One\n\nAlpha.\n\n# Two\n\nBeta."
    chunks = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=20,
        overlap_chars=8,
    )

    second_heading = content.index("# Two")
    second_section = next(chunk for chunk in chunks if chunk.heading_path == ("Two",))
    assert second_section.char_start == second_heading
    assert "# One" not in second_section.text
    _assert_valid_chunks(content, chunks)


def test_repeated_markdown_heading_path_still_starts_a_new_section() -> None:
    content = "# Same\n\nAlpha.\n\n# Same\n\nBeta."
    chunks = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=100,
        overlap_chars=20,
    )

    assert [chunk.heading_path for chunk in chunks] == [("Same",), ("Same",)]
    assert chunks[1].char_start == content.index("# Same", 1)
    _assert_valid_chunks(content, chunks)


def test_plain_text_does_not_interpret_markdown_headings() -> None:
    content = "# Not a heading\n\nBody."
    chunks = chunk_document(
        content,
        KnowledgeSourceType.text,
        target_chars=100,
        overlap_chars=10,
    )

    assert [chunk.heading_path for chunk in chunks] == [()]
    assert chunks[0].text == content


def test_chunking_is_deterministic_across_repeated_runs() -> None:
    content = "# Guide\n\nAlpha.\n\n- one\n- two\n\n```text\ncode\n```\n\nOmega."

    first = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=24,
        overlap_chars=6,
    )
    second = chunk_document(
        content,
        KnowledgeSourceType.markdown,
        target_chars=24,
        overlap_chars=6,
    )

    assert first == second
