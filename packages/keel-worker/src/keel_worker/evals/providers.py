"""Case/turn provider cassettes with volatile-ID-canonical fingerprints.

A :class:`CaseCassette` stores, per case id, an ordered list of provider turns;
each turn keeps the canonical request fingerprint plus its recorded chunk
sequence. Replay matches by ``(case_id, invocation_index)`` and asserts the
fingerprint so a drifted prompt fails loudly instead of returning a stale
turn. Fingerprints canonicalize the DB-generated proposal UUID / archival
serial that appear in tool-result messages (their exact value differs each
run) while preserving the ``created``/``merged``/``inserted``/``already
proposed`` state word so state drift still trips a mismatch.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

from keel_core.protocols import ProviderChunk, ProviderRequest
from keel_worker.evals.models import JudgeResult as JudgeResult  # explicit re-export

_PROPOSAL_ID = re.compile(r"\bproposal [0-9a-f]{32} (created|already proposed)\b")
_ARCHIVAL_ID = re.compile(r"\barchival \d+ (inserted|merged)\b")


class CassetteMiss(Exception):
    """Raised (and captured on the gateway) when a case turn is absent or drifted."""

    def __init__(
        self,
        case_id: str,
        index: int,
        kind: str,
        *,
        expected_fingerprint: str | None,
        actual_fingerprint: str,
    ) -> None:
        super().__init__(
            f"cassette {kind} for case {case_id!r} turn {index} "
            f"(expected={expected_fingerprint}, actual={actual_fingerprint})"
        )
        self.case_id = case_id
        self.index = index
        self.kind = kind
        self.expected_fingerprint = expected_fingerprint
        self.actual_fingerprint = actual_fingerprint


def canonicalize_tool_content(content: str) -> str:
    """Replace volatile DB ids in tool-result payloads with a fixed placeholder.

    Only the numeric/UUID token is masked; the trailing state word
    (``created`` / ``already proposed`` / ``inserted`` / ``merged``) is
    preserved so state drift still produces a different fingerprint.
    """
    content = _PROPOSAL_ID.sub(r"proposal <ID> \1", content)
    content = _ARCHIVAL_ID.sub(r"archival <ID> \1", content)
    return content


def _canonical_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for message in messages:
        if message.get("role") == "tool" and isinstance(message.get("content"), str):
            copy = dict(message)
            copy["content"] = canonicalize_tool_content(message["content"])
            out.append(copy)
        else:
            out.append(message)
    return out


def canonical_request_fingerprint(request: ProviderRequest) -> str:
    """Return a stable sha256 hex fingerprint that ignores volatile DB ids."""
    payload = {
        "model": request.model,
        "messages": _canonical_messages(request.messages),
        "tools": request.tools,
    }
    blob = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass
class CaseEntry:
    fingerprint: str
    chunks: list[ProviderChunk]


def _atomic_write_text(path: Path, data: str) -> None:
    """Write ``data`` to ``path`` atomically.

    A temp file in the same directory is fully written + fsynced, then
    :func:`os.replace` swaps it into place (an atomic rename on the same
    filesystem, including Windows). The existing file survives until the
    replace, so any failure before it leaves the old file intact; a temp file
    left by a mid-write failure is cleaned up.
    """
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=directory,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    tmp_path = Path(handle.name)
    try:
        with handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


class CaseCassette:
    """Ordered per-case turns keyed by ``(case_id, index)`` with a request fingerprint."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, list[dict[str, Any]]] = {}
        if path.exists():
            self._data = json.loads(path.read_text("utf-8"))

    def entries(self, case_id: str) -> list[dict[str, Any]]:
        return self._data.get(case_id, [])

    def get(self, case_id: str, index: int) -> CaseEntry | None:
        turns = self._data.get(case_id)
        if turns is None or index >= len(turns):
            return None
        raw = turns[index]
        return CaseEntry(
            fingerprint=str(raw["fingerprint"]),
            chunks=[ProviderChunk.model_validate(c) for c in raw["chunks"]],
        )

    def put(
        self,
        case_id: str,
        index: int,
        fingerprint: str,
        chunks: list[ProviderChunk],
    ) -> None:
        turns = self._data.setdefault(case_id, [])
        record = {
            "fingerprint": fingerprint,
            "chunks": [c.model_dump() for c in chunks],
        }
        if index < len(turns):
            turns[index] = record
        elif index == len(turns):
            turns.append(record)
        else:  # pragma: no cover - indices are assigned densely
            raise ValueError(f"non-contiguous cassette index {index} for {case_id!r}")

    def save(self) -> None:
        """Persist atomically (temp file in the same dir + :func:`os.replace`).

        A crash mid-write can never truncate the committed cassette; the previous
        file stays intact until the atomic replace and any failure leaves it
        untouched.
        """
        blob = (
            json.dumps(self._data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        )
        _atomic_write_text(self.path, blob)


async def _aiter(chunks: list[ProviderChunk]) -> AsyncIterator[ProviderChunk]:
    for chunk in chunks:
        yield chunk


class ReplayCaseProviderGateway:
    """Replay a case's recorded turns; never falls back to a live provider."""

    def __init__(self, cassette: CaseCassette, case_id: str) -> None:
        self._cassette = cassette
        self._case_id = case_id
        self._index = 0
        self.miss: CassetteMiss | None = None

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        fingerprint = canonical_request_fingerprint(request)
        entry = self._cassette.get(self._case_id, self._index)
        if entry is None:
            self._fail(self._index, "cassette_miss", None, fingerprint)
        if entry.fingerprint != fingerprint:
            self._fail(
                self._index, "fingerprint_mismatch", entry.fingerprint, fingerprint
            )
        self._index += 1  # advance only on a matched turn (retry-safe)
        return _aiter(entry.chunks)

    def _fail(
        self, index: int, kind: str, expected: str | None, actual: str
    ) -> NoReturn:
        miss = CassetteMiss(
            self._case_id,
            index,
            kind,
            expected_fingerprint=expected,
            actual_fingerprint=actual,
        )
        if self.miss is None:
            self.miss = miss  # keep the first, most-informative miss
        raise miss


class RecordingCaseProviderGateway:
    """Wrap a live gateway; record each completed turn under the case's next index."""

    def __init__(self, inner: Any, cassette: CaseCassette, case_id: str) -> None:
        self._inner = inner
        self.cassette = cassette
        self._case_id = case_id
        self._index = 0
        self.errored = False

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        return self._record_stream(request)

    async def _record_stream(
        self, request: ProviderRequest
    ) -> AsyncIterator[ProviderChunk]:
        fingerprint = canonical_request_fingerprint(request)
        index = self._index
        buffer: list[ProviderChunk] = []
        try:
            async for chunk in self._inner.stream(request):
                buffer.append(chunk)
                yield chunk
        except Exception:
            self.errored = True  # partial turn: do not record, do not advance
            raise
        self.cassette.put(self._case_id, index, fingerprint, buffer)
        self._index += 1


def parse_judge_response(text: str) -> JudgeResult:
    """Parse a judge JSON verdict; any failure is fail-open (advisory only)."""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        return JudgeResult(error=f"no json object in judge output: {text[:80]!r}")
    try:
        data = json.loads(text[start : end + 1])
        return JudgeResult(
            score=float(data.get("score", 0.0)),
            passed=bool(data.get("passed", True)),
            rationale=str(data.get("rationale", "")),
        )
    except (ValueError, TypeError) as exc:
        return JudgeResult(error=f"judge parse error: {exc}")


class LiteLLMMemoryJudge:
    """An optional LLM judge; a failure never fails the eval (fail-open)."""

    def __init__(self, model: str, *, gateway: Any | None = None) -> None:
        self._model = model
        if gateway is None:
            from keel_core.providers import LiteLLMGateway

            gateway = LiteLLMGateway()
        self._gateway = gateway

    async def judge(self, prompt: str) -> JudgeResult:
        request = ProviderRequest(
            model=self._model, messages=[{"role": "user", "content": prompt}]
        )
        try:
            text = ""
            async for chunk in self._gateway.stream(request):
                text += chunk.delta
            return parse_judge_response(text)
        except Exception as exc:  # noqa: BLE001 - judge is advisory + fail-open
            return JudgeResult(error=f"judge invocation failed: {exc}")
