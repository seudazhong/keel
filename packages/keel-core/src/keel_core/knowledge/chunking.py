"""Deterministic normalization and text/Markdown chunking.

Markdown fences are kept whole through four times ``target_chars``. A fence
larger than that derived hard limit uses the same character windows as any
other oversized unit.
"""

from __future__ import annotations

import io
import re
from collections.abc import Iterable, Iterator
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
_ATX_HEADING_RE = re.compile(r"^ {0,3}(?P<marks>#{1,6})(?:[ \t]+(?P<title>.*)|[ \t]*)$")
_CLOSING_HEADING_MARKS_RE = re.compile(r"(?:^|[ \t]+)#+[ \t]*$")
_FENCE_OPEN_RE = re.compile(r"^ {0,3}(?P<marker>`{3,}|~{3,}).*$")
_FENCE_CLOSE_RE = re.compile(r"^ {0,3}(?P<marker>`+|~+)[ \t]*$")
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


class _LineCursor:
    def __init__(self, lines: Iterator[_Line]) -> None:
        self._lines = lines
        self._next: _Line | None = None
        self._finished = False

    def peek(self) -> _Line | None:
        if self._next is None and not self._finished:
            try:
                self._next = next(self._lines)
            except StopIteration:
                self._finished = True
        return self._next

    def pop(self) -> _Line | None:
        line = self.peek()
        self._next = None
        return line


def normalize_document_text(value: str, *, max_bytes: int) -> str:
    """Return canonical storage-safe document text within ``max_bytes``."""

    byte_limit = validate_document_max_bytes(max_bytes)
    if not isinstance(value, str) or "\x00" in value:
        raise _invalid_content()
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise _invalid_content() from exc

    normalized_line_endings = value.replace("\r\n", "\n").replace("\r", "\n")
    output = io.StringIO()
    pending_blank_lines = 0
    has_content = False
    for line in _line_texts(normalized_line_endings):
        line = line.rstrip(" \t")
        if line == "":
            if has_content:
                pending_blank_lines += 1
            continue
        if has_content:
            retained_blank_lines = 2 if pending_blank_lines > 3 else pending_blank_lines
            output.write("\n" * (retained_blank_lines + 1))
        output.write(line)
        pending_blank_lines = 0
        has_content = True

    normalized = _strip_document_boundaries(output.getvalue())

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

    drafts: list[ChunkDraft] = []
    spans = _pack_units(units, content=safe_content, target=target, overlap=overlap)
    for ordinal, span in enumerate(spans):
        text = safe_content[span.start : span.end]
        drafts.append(
            ChunkDraft(
                ordinal=ordinal,
                text=text,
                char_start=span.start,
                char_end=span.end,
                content_hash=content_sha256(text),
                heading_path=span.heading_path,
            )
        )
    return drafts


def _invalid_content() -> KnowledgeValidationError:
    return KnowledgeValidationError(
        KnowledgePublicCode.content_invalid,
        "Document content is not storage-safe normalized UTF-8 text.",
    )


def _line_texts(content: str) -> Iterator[str]:
    start = 0
    while True:
        newline = content.find("\n", start)
        if newline == -1:
            yield content[start:]
            return
        yield content[start:newline]
        start = newline + 1


def _strip_document_boundaries(value: str) -> str:
    start = 0
    while start < len(value) and (value[start] == _UTF8_BOM or value[start].isspace()):
        start += 1
    return value[start:].rstrip()


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

    if normalize_document_text(content, max_bytes=max(1, len(content) * 4)) != content:
        raise _invalid_content()
    return content


def _document_lines(content: str) -> Iterator[_Line]:
    start = 0
    while True:
        newline = content.find("\n", start)
        if newline == -1:
            yield _Line(
                start=start,
                content_end=len(content),
                end=len(content),
                text=content[start:],
            )
            return
        yield _Line(
            start=start,
            content_end=newline,
            end=newline + 1,
            text=content[start:newline],
        )
        start = newline + 1


def _is_blank(line: _Line) -> bool:
    return not line.text.strip()


def _make_unit(
    cursor: _LineCursor,
    first_line: _Line,
    last_line: _Line,
    heading_path: tuple[str, ...],
    section: int,
    *,
    is_fence: bool = False,
) -> _Unit:
    end = last_line.end
    following = cursor.peek()
    while following is not None and _is_blank(following):
        cursor.pop()
        end = following.end
        following = cursor.peek()

    return _Unit(
        start=first_line.start,
        body_end=last_line.content_end,
        end=end,
        heading_path=heading_path,
        section=section,
        is_fence=is_fence,
    )


def _plain_text_units(content: str) -> Iterator[_Unit]:
    cursor = _LineCursor(_document_lines(content))
    first_line = cursor.pop()
    while first_line is not None:
        last_line = first_line
        following = cursor.peek()
        while following is not None and not _is_blank(following):
            cursor.pop()
            last_line = following
            following = cursor.peek()
        yield _make_unit(cursor, first_line, last_line, (), 0)
        first_line = cursor.pop()


def _markdown_units(content: str) -> Iterator[_Unit]:
    cursor = _LineCursor(_document_lines(content))
    heading_stack: list[tuple[int, str]] = []
    section = 0
    first_line = cursor.pop()

    while first_line is not None:
        opening_fence = _opening_fence(first_line.text)
        if opening_fence is not None:
            last_line = _fence_end(cursor, first_line, opening_fence)
            yield _make_unit(
                cursor,
                first_line,
                last_line,
                _heading_path(heading_stack),
                section,
                is_fence=True,
            )
            first_line = cursor.pop()
            continue

        heading = _atx_heading(first_line.text)
        if heading is not None:
            level, title = heading
            while heading_stack and heading_stack[-1][0] >= level:
                heading_stack.pop()
            if title:
                heading_stack.append((level, title))
            section += 1
            yield _make_unit(
                cursor,
                first_line,
                first_line,
                _heading_path(heading_stack),
                section,
            )
            first_line = cursor.pop()
            continue

        is_list_block = _is_list_item(first_line.text)
        last_line = first_line
        following = cursor.peek()
        while following is not None and not _is_blank(following):
            if (
                _opening_fence(following.text) is not None
                or _atx_heading(following.text) is not None
                or (not is_list_block and _is_list_item(following.text))
            ):
                break
            cursor.pop()
            last_line = following
            following = cursor.peek()

        yield _make_unit(
            cursor,
            first_line,
            last_line,
            _heading_path(heading_stack),
            section,
        )
        first_line = cursor.pop()


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
    cursor: _LineCursor,
    opening_line: _Line,
    opening_fence: tuple[str, int],
) -> _Line:
    marker_character, marker_length = opening_fence
    last_line = opening_line
    line = cursor.pop()
    while line is not None:
        last_line = line
        match = _FENCE_CLOSE_RE.fullmatch(line.text)
        if match is not None:
            marker = match.group("marker")
            if marker[0] == marker_character and len(marker) >= marker_length:
                return line
        line = cursor.pop()
    return last_line


def _is_list_item(line: str) -> bool:
    return _LIST_ITEM_RE.match(line) is not None


def _pack_units(
    units: Iterable[_Unit],
    *,
    content: str,
    target: int,
    overlap: int,
) -> Iterator[_ChunkSpan]:
    current: list[_Unit] = []

    for unit in units:
        if current and current[-1].section != unit.section:
            yield _span_for_units(current)
            current = []

        if _requires_hard_split(unit, target):
            if current:
                yield _span_for_units(current)
                current = []
            yield from _hard_split(
                content,
                unit,
                target=target,
                overlap=overlap,
            )
            continue

        if unit.is_fence and unit.body_length > target:
            if current:
                yield _span_for_units(current)
                current = []
            yield _span_for_units([unit])
            continue

        if not current:
            current = [unit]
            continue

        if unit.end - current[0].start <= target:
            current.append(unit)
            continue

        yield _span_for_units(current)
        current = [*_overlap_units(current, unit, target=target, overlap=overlap), unit]

    if current:
        yield _span_for_units(current)


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


def _hard_split(
    content: str,
    unit: _Unit,
    *,
    target: int,
    overlap: int,
) -> Iterator[_ChunkSpan]:
    step = target - overlap
    start = unit.start
    pending: _ChunkSpan | None = None
    while start < unit.body_end:
        body_end = min(start + target, unit.body_end)
        end = unit.end if body_end == unit.body_end else body_end

        if content[start:body_end].strip():
            span_start = unit.start if pending is None else start
            if pending is not None:
                yield pending
            pending = _ChunkSpan(
                start=span_start,
                end=end,
                heading_path=unit.heading_path,
            )
        elif pending is not None:
            pending = _ChunkSpan(
                start=pending.start,
                end=max(pending.end, end),
                heading_path=pending.heading_path,
            )

        if body_end == unit.body_end:
            break
        start += step

    if pending is not None:
        yield pending


__all__ = ["chunk_document", "normalize_document_text"]
