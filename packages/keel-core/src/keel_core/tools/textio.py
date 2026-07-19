"""Bytes-aware, strict-UTF-8 text helpers for the file ``write``/``edit`` tools.

The model-facing ``write``/``edit`` tools must round-trip a text file's *exact* bytes: a file
stored with Windows ``CRLF`` endings must stay ``CRLF`` after an edit, and an untouched region
must be preserved byte-for-byte. Python's ``Path.read_text``/``write_text`` default to
universal-newline translation (``CRLF`` -> ``LF`` on read, ``LF`` -> ``os.linesep`` on write),
which silently rewrites every line ending, and ``errors="replace"`` would mask a corrupt file
with U+FFFD.

These helpers fail closed instead: invalid UTF-8, a NUL byte (binary), a bare ``CR``, or a file
that mixes ``CRLF`` and ``LF`` all raise :class:`TextPolicyError` rather than guessing. A caller
edits on a *logical* ``LF`` view (:func:`to_logical_newlines`) and re-emits the file's original
newline style (:func:`encode_text`), so untouched bytes and the file's endings are preserved.
"""

from __future__ import annotations

__all__ = [
    "TextPolicyError",
    "decode_text",
    "detect_newline",
    "encode_text",
    "is_binary",
    "to_logical_newlines",
]


class TextPolicyError(ValueError):
    """A text operation failed closed (binary content, invalid UTF-8, or mixed newlines)."""


def is_binary(data: bytes) -> bool:
    """A file is treated as binary (and never read/written as text) if it contains a NUL byte."""
    return b"\x00" in data


def decode_text(data: bytes) -> str:
    """Strictly decode ``data`` as UTF-8, failing closed on binary or invalid bytes.

    Unlike ``bytes.decode(..., errors="replace")`` this never substitutes U+FFFD; a file that is
    not valid UTF-8 (or that contains a NUL byte) raises :class:`TextPolicyError`.
    """
    if is_binary(data):
        raise TextPolicyError("refusing to treat binary content as text")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TextPolicyError("content is not valid UTF-8") from exc


def detect_newline(data: bytes) -> str:
    r"""Return the file's newline style (``"\r\n"`` or ``"\n"``), failing closed on mixed/bare CR.

    A file with no newline at all defaults to ``"\n"`` (there is nothing to preserve). A file that
    mixes ``CRLF`` and lone ``LF``, or that contains a bare ``CR`` (old-Mac endings), raises
    :class:`TextPolicyError` so the tool never silently normalizes ambiguous content.
    """
    crlf = data.count(b"\r\n")
    bare_cr = data.count(b"\r") - crlf
    bare_lf = data.count(b"\n") - crlf
    if bare_cr:
        raise TextPolicyError("bare carriage-return line endings are not supported")
    if crlf and bare_lf:
        raise TextPolicyError("mixed CRLF and LF line endings are not supported")
    return "\r\n" if crlf else "\n"


def to_logical_newlines(text: str) -> str:
    """Collapse ``CRLF`` to a logical ``LF`` view for matching/replacement (no other change)."""
    return text.replace("\r\n", "\n")


def encode_text(logical_text: str, newline: str) -> bytes:
    r"""Re-emit logical-``LF`` text in ``newline`` style and UTF-8 encode it, failing closed.

    ``logical_text`` is expected to use ``LF``; any embedded ``CRLF`` is normalized first so the
    output never contains ``\r\r\n``. A NUL byte or an un-encodable lone surrogate fails closed
    with :class:`TextPolicyError` rather than corrupting the file.
    """
    body = to_logical_newlines(logical_text)
    if newline == "\r\n":
        body = body.replace("\n", "\r\n")
    if "\x00" in body:
        raise TextPolicyError("refusing to write binary content as text")
    try:
        return body.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise TextPolicyError("content is not encodable as UTF-8") from exc
