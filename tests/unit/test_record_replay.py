"""Tests for the provider record/replay harness (deterministic, offline)."""

from __future__ import annotations

from pathlib import Path

import pytest

from keel_core.protocols import ProviderChunk, ProviderGateway, ProviderRequest
from keel_core.testing.record_replay import Cassette, ReplayProviderGateway, request_key
from keel_core.types import StopReason


def test_request_key_is_stable() -> None:
    req_a = ProviderRequest(model="m", messages=[{"role": "user", "content": "hi"}])
    req_b = ProviderRequest(model="m", messages=[{"role": "user", "content": "hi"}])
    assert request_key(req_a) == request_key(req_b)


async def test_replay_is_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "cassette.json"
    request = ProviderRequest(model="test/model", messages=[{"role": "user", "content": "hi"}])
    recorded = [
        ProviderChunk(delta="he"),
        ProviderChunk(delta="llo"),
        ProviderChunk(stop_reason=StopReason.completed),
    ]

    writer = Cassette(path)
    writer.put(request_key(request), recorded)
    writer.save()

    # A fresh gateway loads from disk and replays identically (twice).
    gateway: ProviderGateway = ReplayProviderGateway(Cassette(path))
    for _ in range(2):
        out = [chunk async for chunk in gateway.stream(request)]
        assert [c.delta for c in out] == ["he", "llo", ""]
        assert out[-1].stop_reason is StopReason.completed


def test_replay_missing_entry_raises(tmp_path: Path) -> None:
    gateway = ReplayProviderGateway(Cassette(tmp_path / "empty.json"))
    with pytest.raises(KeyError):
        gateway.stream(ProviderRequest(model="x", messages=[]))
