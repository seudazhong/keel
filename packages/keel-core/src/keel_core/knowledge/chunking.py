"""Deterministic normalization and text/Markdown chunking.

Markdown fences are kept whole through four times ``target_chars``. A fence
larger than that derived hard limit uses the same character windows as any
other oversized unit.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .models import (
    ChunkDraft,
    KnowledgePublicCode,
    KnowledgeSourceType,
    KnowledgeValidationError,
    content_sha256,
    validate_document_max_bytes,
)

_UTF8_BOM = "\ufeff"
_FENCE_HARD_LIMIT_MULTIPLIER = 4
_ATX_HEADING_RE = re.compile(r"^[ \t]{0,3}(?P<marks>#{1,6})(?:[ \t]+(?P<title>.*)|[ \t]*)$")
_CLOSING_HEADING_MARKS_RE = re.compile(r"(?:^|[ \t]+)#+[ \t]*$")
_FENCE_OPEN_RE = re.compile(r"^[ \t]{0,3}(?P<marker>`{3,}|~{3,}).*$")
_FENCE_CLOSE_RE = re.compile(r"^[ \t]{0,3}(?P<marker>`+|~+)[ \t]*$")
_LIST_ITEM_RE = re.compile(r"^[ \t]*(?:[-+*](?:[ \t]+|$)|\d{1,9}[.)](?:[ \t]+|$))")


@dataclass(frozen=True, slots=True)
class _Line:
    start: int
    content_end: int
    end: int
    text: str


@dataclass(frozen=True, slots=True)
class _Unit:
    """A block whose ``end`` also owns its following blank separator."""

    start: int
    body_end: int
    end: int
    heading_path: tuple[str, ...]
    section: int
    is_fence: bool = False

    @property
    def body_length(self) -> int:
        return self.body_end - self.start


@dataclass(frozen=True, slots=True)
class _ChunkSpan:
    start: int
    end: int
    heading_path: tuple[str, ...]


def normalize_document_text(value: str, *, max_bytes: int) -> str:
    """Return canonical storage-safe document text within ``max_bytes``."""

    byte_limit = validate_document_max_bytes(max_bytes)
    if not isinstance(value, str) or "\x00" in value:
        raise _invalid_content()
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise _invalid_content() from exc

    normalized = value.removeprefix(_UTF8_BOM).replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip(" \t") for line in normalized.split("\n")]
    normalized = "\n".join(_collapse_blank_lines(lines)).strip()

    if not normalized:
        raise KnowledgeValidationError(
            KnowledgePublicCode.content_invalid,
            "Document content must not be blank.",
        )
    if len(normalized.encode("utf-8")) > byte_limit:
        raise KnowledgeValidationError(
            KnowledgePublicCode.content_too_large,
            "Document content exceeds the configured size limit.",
        )
    return normalized


def chunk_document(
    content: str,
    source_type: KnowledgeSourceType,
    *,
    target_chars: int,
    overlap_chars: int,
) -> list[ChunkDraft]:
    """Split normalized text into deterministic offset-preserving chunks."""

    target, overlap = _validate_chunk_settings(target_chars, overlap_chars)
    if not isinstance(source_type, KnowledgeSourceType):
        raise ValueError("source_type must be a KnowledgeSourceType")
    safe_content = _validate_normalized_content(content)

    if source_type is KnowledgeSourceType.text:
        units = _plain_text_units(safe_content)
    else:
        units = _markdown_units(safe_content)

    spans = _pack_units(units, target=target, overlap=overlap)
    return [
        ChunkDraft(
            ordinal=ordinal,
            text=safe_content[span.start : span.end],
            char_start=span.start,
            char_end=span.end,
            content_hash=content_sha256(safe_content[span.start : span.end]),
            heading_path=span.heading_path,
        )
        for ordinal, span in enumerate(spans)
    ]


def _invalid_content() -> KnowledgeValidationError:
    return KnowledgeValidationError(
        KnowledgePublicCode.content_invalid,
        "Document content is not storage-safe normalized UTF-8 text.",
    )


def _collapse_blank_lines(lines: list[str]) -> list[str]:
    collapsed: list[str] = []
    blank_count = 0
    for line in lines:
        if line == "":
            blank_count += 1
            continue
        if blank_count:
            collapsed.extend("" for _ in range(2 if blank_count > 3 else blank_count))
            blank_count = 0
        collapsed.append(line)
    if blank_count:
        collapsed.extend("" for _ in range(2 if blank_count > 3 else blank_count))
    return collapsed


def _validate_chunk_settings(target_chars: object, overlap_chars: object) -> tuple[int, int]:
    if type(target_chars) is not int or target_chars < 1:
        raise ValueError("target_chars must be a positive integer")
    if type(overlap_chars) is not int:
        raise ValueError("overlap_chars must be an integer")
    if overlap_chars < 0 or overlap_chars >= target_chars:
        raise ValueError("overlap_chars must be non-negative and less than target_chars")
    return target_chars, overlap_chars


def _validate_normalized_content(content: object) -> str:
    if not isinstance(content, str) or "\x00" in content:
        raise _invalid_content()
    try:
        encoded = content.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise _invalid_content() from exc

    if normalize_document_text(content, max_bytes=max(1, len(encoded))) != content:
        raise _invalid_content()
    return content


def _document_lines(content: str) -> list[_Line]:
    parts = content.split("\n")
    lines: list[_Line] = []
    start = 0
    for index, text in enumerate(parts):
        content_end = start + len(text)
        end = content_end + (1 if index < len(parts) - 1 else 0)
        lines.append(_Line(start=start, content_end=content_end, end=end, text=text))
        start = end
    return lines


def _is_blank(line: _Line) -> bool:
    return not line.text.strip()


def _make_unit(
    lines: list[_Line],
    start_index: int,
    block_end_index: int,
    heading_path: tuple[str, ...],
    section: int,
    *,
    is_fence: bool = False,
) -> tuple[_Unit, int]:
    next_index = block_end_index
    while next_index < len(lines) and _is_blank(lines[next_index]):
        next_index += 1

    last_index = next_index - 1 if next_index > block_end_index else block_end_index - 1
    return (
        _Unit(
            start=lines[start_index].start,
            body_end=lines[block_end_index - 1].content_end,
            end=lines[last_index].end,
            heading_path=heading_path,
            section=section,
            is_fence=is_fence,
        ),
        next_index,
    )


def _plain_text_units(content: str) -> list[_Unit]:
    lines = _document_lines(content)
    units: list[_Unit] = []
    index = 0
    while index < len(lines):
        block_end = index + 1
        while block_end < len(lines) and not _is_blank(lines[block_end]):
            block_end += 1
        unit, index = _make_unit(lines, index, block_end, (), 0)
        units.append(unit)
    return units


def _markdown_units(content: str) -> list[_Unit]:
    lines = _document_lines(content)
    units: list[_Unit] = []
    heading_stack: list[tuple[int, str]] = []
    section = 0
    index = 0

    while index < len(lines):
        opening_fence = _opening_fence(lines[index].text)
        if opening_fence is not None:
            block_end = _fence_end(lines, index, opening_fence)
            unit, index = _make_unit(
                lines,
                index,
                block_end,
                _heading_path(heading_stack),
                section,
                is_fence=True,
            )
            units.append(unit)
            continue

        heading = _atx_heading(lines[index].text)
        if heading is not None:
            level, title = heading
            while heading_stack and heading_stack[-1][0] >= level:
                heading_stack.pop()
            if title:
                heading_stack.append((level, title))
            section += 1
            unit, index = _make_unit(
                lines,
                index,
                index + 1,
                _heading_path(heading_stack),
                section,
            )
            units.append(unit)
            continue

        if _is_list_item(lines[index].text):
            block_end = index + 1
            while block_end < len(lines) and not _is_blank(lines[block_end]):
                if (
                    _opening_fence(lines[block_end].text) is not None
                    or _atx_heading(lines[block_end].text) is not None
                ):
                    break
                block_end += 1
        else:
            block_end = index + 1
            while block_end < len(lines) and not _is_blank(lines[block_end]):
                if (
                    _opening_fence(lines[block_end].text) is not None
                    or _atx_heading(lines[block_end].text) is not None
                    or _is_list_item(lines[block_end].text)
                ):
                    break
                block_end += 1

        unit, index = _make_unit(
            lines,
            index,
            block_end,
            _heading_path(heading_stack),
            section,
        )
        units.append(unit)

    return units


def _heading_path(stack: list[tuple[int, str]]) -> tuple[str, ...]:
    return tuple(title for _, title in stack)


def _atx_heading(line: str) -> tuple[int, str] | None:
    match = _ATX_HEADING_RE.fullmatch(line)
    if match is None:
        return None
    raw_title = match.group("title") or ""
    title = _CLOSING_HEADING_MARKS_RE.sub("", raw_title).strip()
    return len(match.group("marks")), title


def _opening_fence(line: str) -> tuple[str, int] | None:
    match = _FENCE_OPEN_RE.fullmatch(line)
    if match is None:
        return None
    marker = match.group("marker")
    return marker[0], len(marker)


def _fence_end(
    lines: list[_Line],
    start_index: int,
    opening_fence: tuple[str, int],
) -> int:
    marker_character, marker_length = opening_fence
    index = start_index + 1
    while index < len(lines):
        match = _FENCE_CLOSE_RE.fullmatch(lines[index].text)
        if match is not None:
            marker = match.group("marker")
            if marker[0] == marker_character and len(marker) >= marker_length:
                return index + 1
        index += 1
    return len(lines)


def _is_list_item(line: str) -> bool:
    return _LIST_ITEM_RE.match(line) is not None


def _pack_units(units: list[_Unit], *, target: int, overlap: int) -> list[_ChunkSpan]:
    spans: list[_ChunkSpan] = []
    current: list[_Unit] = []

    for unit in units:
        if current and current[-1].section != unit.section:
            spans.append(_span_for_units(current))
            current = []

        if _requires_hard_split(unit, target):
            if current:
                spans.append(_span_for_units(current))
                current = []
            spans.extend(_hard_split(unit, target=target, overlap=overlap))
            continue

        if unit.is_fence and unit.body_length > target:
            if current:
                spans.append(_span_for_units(current))
                current = []
            spans.append(_span_for_units([unit]))
            continue

        if not current:
            current = [unit]
            continue

        if unit.end - current[0].start <= target:
            current.append(unit)
            continue

        spans.append(_span_for_units(current))
        current = [*_overlap_units(current, unit, target=target, overlap=overlap), unit]

    if current:
        spans.append(_span_for_units(current))
    return spans


def _requires_hard_split(unit: _Unit, target: int) -> bool:
    if unit.is_fence:
        return unit.body_length > target * _FENCE_HARD_LIMIT_MULTIPLIER
    return unit.body_length > target


def _span_for_units(units: list[_Unit]) -> _ChunkSpan:
    return _ChunkSpan(
        start=units[0].start,
        end=units[-1].end,
        heading_path=units[0].heading_path,
    )


def _overlap_units(
    current: list[_Unit],
    next_unit: _Unit,
    *,
    target: int,
    overlap: int,
) -> list[_Unit]:
    if overlap == 0:
        return []

    selected_start = len(current)
    for index in range(len(current) - 1, -1, -1):
        overlap_length = current[-1].end - current[index].start
        combined_length = next_unit.end - current[index].start
        if overlap_length > overlap or combined_length > target:
            break
        selected_start = index
    return current[selected_start:]


def _hard_split(unit: _Unit, *, target: int, overlap: int) -> list[_ChunkSpan]:
    spans: list[_ChunkSpan] = []
    step = target - overlap
    start = unit.start
    while start < unit.body_end:
        body_end = min(start + target, unit.body_end)
        end = unit.end if body_end == unit.body_end else body_end
        spans.append(
            _ChunkSpan(
                start=start,
                end=end,
                heading_path=unit.heading_path,
            )
        )
        if body_end == unit.body_end:
            break
        start += step
    return spans


__all__ = ["chunk_document", "normalize_document_text"]
