"""End-to-end runner with injected deps: record a consolidation case, then replay it."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.testing.record_replay import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_worker.evals.embeddings import EmbeddingCassette, RecordingEmbedder, ReplayEmbedder
from keel_worker.evals.loader import event_id_for
from keel_worker.evals.providers import (
    CaseCassette,
    RecordingCaseProviderGateway,
    ReplayCaseProviderGateway,
)
from keel_worker.evals.runner import RunDeps, run_evals

pytestmark = pytest.mark.integration


def _dataset(tmp_path: Path) -> Path:
    case = {
        "version": 1,
        "suite": "consolidation",
        "id": "con-en-preference",
        "model": "eval/scripted",
        "messages": [
            {"role": "user", "text": "Call me Sam and always reply in English."},
            {"role": "assistant", "text": "Understood."},
        ],
        "expected": {
            "required_core_claims": ["prefers to be called Sam"],
            "min_proposals": 1,
            "max_proposals": 1,
        },
    }
    path = tmp_path / "v1.jsonl"
    path.write_text(json.dumps(case) + "\n", encoding="utf-8")
    return path


def _turns() -> list[list[ProviderChunk]]:
    return [
        [
            ProviderChunk(
                tool_call=ToolCall(
                    id="call_1",
                    name="memory_propose_rewrite",
                    arguments={
                        "block": "human",
                        "proposed_value": (
                            "The user prefers to be called Sam and writes in English."
                        ),
                        "reason": "stated preference",
                        "confidence": 0.95,
                        "source_event_ids": [event_id_for("con-en-preference", 0)],
                    },
                ),
                finish_reason=FinishReason.tool_use,
            )
        ],
        [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
    ]


async def test_record_then_replay(migrated_db: AsyncEngine, tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    provider_path = tmp_path / "prov.json"
    embed_path = tmp_path / "emb.json"

    prov_cassette = CaseCassette(provider_path)
    embed_cassette = EmbeddingCassette(embed_path)
    recorder = RecordingEmbedder(FakeEmbedder())
    recorder.bind(embed_cassette)

    def _save() -> None:
        prov_cassette.save()
        embed_cassette.save()

    rec_deps = RunDeps(
        make_provider=lambda cid: RecordingCaseProviderGateway(
            ScriptedProviderGateway(_turns()), prov_cassette, cid
        ),
        embedder=recorder,
        save=_save,
    )
    rec_report = await run_evals(
        dataset_path=dataset,
        provider_cassette_path=provider_path,
        embedding_cassette_path=embed_path,
        mode="live",
        record=True,
        suites={"consolidation"},
        enforce=False,
        out_dir=tmp_path / "rec",
        deps=rec_deps,
    )
    assert rec_report.exit_code == 0
    assert rec_report.mode == "record"  # a live run with record=True is labelled "record"
    assert provider_path.exists() and embed_path.exists()

    replay_cassette = CaseCassette(provider_path)
    replay_deps = RunDeps(
        make_provider=lambda cid: ReplayCaseProviderGateway(replay_cassette, cid),
        embedder=ReplayEmbedder(EmbeddingCassette(embed_path), model="fake/embed", dim=16),
        save=lambda: None,
    )
    replay_report = await run_evals(
        dataset_path=dataset,
        provider_cassette_path=provider_path,
        embedding_cassette_path=embed_path,
        mode="replay",
        suites={"consolidation"},
        enforce=True,
        out_dir=tmp_path / "rep",
        deps=replay_deps,
    )
    rec_con = next(s for s in rec_report.suites if s.suite == "consolidation")
    replay_con = next(s for s in replay_report.suites if s.suite == "consolidation")
    assert replay_con.cases[0].status != "error"  # replay matched the cassette (no miss/fallback)
    assert replay_con.cases[0].status == rec_con.cases[0].status  # record and replay are identical
