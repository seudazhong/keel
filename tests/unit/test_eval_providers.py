"""Fingerprint canonicalization + case-cassette replay/record + miss capture."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from keel_core.protocols import ProviderChunk, ProviderRequest
from keel_core.testing.record_replay import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_worker.evals.providers import (
    CaseCassette,
    CassetteMiss,
    RecordingCaseProviderGateway,
    ReplayCaseProviderGateway,
    canonical_request_fingerprint,
    canonicalize_tool_content,
)


def test_canonicalize_masks_volatile_ids_but_keeps_state() -> None:
    assert (
        canonicalize_tool_content("proposal a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4 created")
        == "proposal <ID> created"
    )
    assert (
        canonicalize_tool_content("archival 917 inserted") == "archival <ID> inserted"
    )
    # state word is preserved (created vs already proposed must still differ)
    assert (
        canonicalize_tool_content(
            "proposal ffffffffffffffffffffffffffffffff already proposed"
        )
        == "proposal <ID> already proposed"
    )


def _request(tool_content: str) -> ProviderRequest:
    return ProviderRequest(
        model="eval/scripted",
        messages=[
            {"role": "user", "text": "hi"},
            {"role": "tool", "tool_call_id": "call_1", "content": tool_content},
        ],
    )


def test_fingerprint_stable_across_volatile_ids() -> None:
    a = canonical_request_fingerprint(
        _request("proposal aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa created")
    )
    b = canonical_request_fingerprint(
        _request("proposal bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb created")
    )
    assert a == b


def test_fingerprint_differs_on_state_drift() -> None:
    created = canonical_request_fingerprint(
        _request("proposal aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa created")
    )
    already = canonical_request_fingerprint(
        _request("proposal aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa already proposed")
    )
    assert created != already


def _turn() -> list[ProviderChunk]:
    return [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]


async def _drain(
    gateway: ReplayCaseProviderGateway | RecordingCaseProviderGateway,
    request: ProviderRequest,
) -> list[ProviderChunk]:
    return [chunk async for chunk in gateway.stream(request)]


async def test_record_then_replay_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "p.json"
    scripted = ScriptedProviderGateway([_turn()])
    recorder = RecordingCaseProviderGateway(scripted, CaseCassette(path), "con-x")
    request = _request("proposal aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa created")
    await _drain(recorder, request)
    recorder.cassette.save()

    replay = ReplayCaseProviderGateway(CaseCassette(path), "con-x")
    # a different volatile id still matches (canonicalized)
    chunks = await _drain(
        replay,
        _request("proposal bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb created"),
    )
    assert chunks[0].delta == "done"
    assert replay.miss is None


async def test_replay_miss_sets_flag_and_raises(tmp_path: Path) -> None:
    replay = ReplayCaseProviderGateway(CaseCassette(tmp_path / "empty.json"), "con-x")
    with pytest.raises(CassetteMiss):
        await _drain(
            replay,
            _request("proposal aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa created"),
        )
    assert replay.miss is not None
    assert replay.miss.kind == "cassette_miss"


async def test_replay_fingerprint_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "p.json"
    recorder = RecordingCaseProviderGateway(
        ScriptedProviderGateway([_turn()]), CaseCassette(path), "con-x"
    )
    await _drain(
        recorder, _request("proposal aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa created")
    )
    recorder.cassette.save()
    replay = ReplayCaseProviderGateway(CaseCassette(path), "con-x")
    with pytest.raises(CassetteMiss):
        await _drain(
            replay,
            _request("proposal aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa already proposed"),
        )
    assert replay.miss is not None
    assert replay.miss.kind == "fingerprint_mismatch"


def test_save_is_atomic_and_preserves_old_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "p.json"
    first = CaseCassette(path)
    first.put("con-x", 0, "fp1", _turn())
    first.save()
    original = path.read_text("utf-8")

    # A failure during the atomic replace must leave the committed file untouched
    # and must not leave a temp file behind (old data survives a partial write).
    second = CaseCassette(path)
    second.put("con-x", 0, "fp2", _turn())

    def _boom(src: object, dst: object) -> None:
        raise RuntimeError("replace failed")

    monkeypatch.setattr("os.replace", _boom)
    with pytest.raises(RuntimeError):
        second.save()
    assert path.read_text("utf-8") == original  # old file survived the failure
    assert [p.name for p in tmp_path.iterdir()] == ["p.json"]  # temp file cleaned up

    monkeypatch.undo()
    second.save()  # a clean save atomically replaces the content, leaving no temp file
    assert [p.name for p in tmp_path.iterdir()] == ["p.json"]
    assert json.loads(path.read_text("utf-8"))["con-x"][0]["fingerprint"] == "fp2"
