"""Golden acceptance: replay/enforce passes; drift exits 2; a bad gate exits 1."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.testing.record_replay import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_worker.evals.loader import event_id_for
from keel_worker.evals.runner import RunDeps, run_evals

pytestmark = pytest.mark.integration

DATASET = Path("evals/datasets/memory/v1.jsonl")
PROVIDER_CASSETTE = Path("evals/cassettes/memory/v1-provider.json")
EMBEDDING_CASSETTE = Path("evals/cassettes/memory/v1-embeddings.json")


@pytest.fixture(autouse=True)
def _eval_db(monkeypatch: pytest.MonkeyPatch, migrated_db: AsyncEngine) -> None:
    import os

    monkeypatch.setenv("KEEL_EVAL_DATABASE_URL", os.environ["KEEL_TEST_DATABASE_URL"])
    # Embedding model routed through the OpenAI-compatible proxy endpoint; must match
    # the model name used when recording cassettes so cassette key lookup succeeds.
    monkeypatch.setenv("KEEL_EMBEDDING_MODEL", "openai/bge-m3")
    monkeypatch.setenv("KEEL_EMBEDDING_DIM", "1024")


async def test_replay_enforce_all_gates_pass(tmp_path: Path) -> None:
    report = await run_evals(
        dataset_path=DATASET,
        provider_cassette_path=PROVIDER_CASSETTE,
        embedding_cassette_path=EMBEDDING_CASSETTE,
        mode="replay",
        suites={"consolidation", "recall", "safety"},
        enforce=True,
        out_dir=tmp_path / "run",
    )
    assert report.exit_code == 0, [g for g in report.gates if not g.passed]
    assert all(gate.passed for gate in report.gates)
    idempotent = next(
        case
        for suite in report.suites
        for case in suite.cases
        if case.case_id == "con-idempotent-replay"
    )
    assert idempotent.status == "pass", idempotent.failures
    assert (tmp_path / "run" / "report.json").exists()


async def test_cassette_drift_exits_2_without_fallback(tmp_path: Path) -> None:
    corrupt = json.loads(await asyncio.to_thread(PROVIDER_CASSETTE.read_text, "utf-8"))
    corrupt["con-en-preference"][0]["fingerprint"] = "deadbeef"  # force a mismatch
    corrupt_path = tmp_path / "provider.json"
    await asyncio.to_thread(corrupt_path.write_text, json.dumps(corrupt), "utf-8")
    report = await run_evals(
        dataset_path=DATASET,
        provider_cassette_path=corrupt_path,
        embedding_cassette_path=EMBEDDING_CASSETTE,
        mode="replay",
        suites={"consolidation"},
        enforce=True,
        out_dir=tmp_path / "run",
    )
    assert report.exit_code == 2  # infra error dominates; never falls back to live
    errored = [c for s in report.suites for c in s.cases if c.status == "error"]
    assert any(c.reason == "fingerprint_mismatch" for c in errored)


async def test_gate_failure_exits_1_with_junit_failure(tmp_path: Path) -> None:
    case = {
        "version": 1,
        "suite": "consolidation",
        "id": "con-en-preference",
        "model": "eval/scripted",
        "messages": [
            {"role": "user", "text": "Please call me Alex."},
            {"role": "assistant", "text": "Sure."},
        ],
        "expected": {
            "required_core_claims": ["prefers to be called Alex"],
            "min_proposals": 1,
            "max_proposals": 1,
        },
    }
    dataset = tmp_path / "one.jsonl"
    await asyncio.to_thread(dataset.write_text, json.dumps(case) + "\n", "utf-8")
    wrong = [
        [
            ProviderChunk(
                tool_call=ToolCall(
                    id="call_1",
                    name="memory_propose_rewrite",
                    arguments={
                        "block": "human",
                        "proposed_value": "The user enjoys pineapple pizza.",
                        "reason": "unrelated",
                        "confidence": 0.9,
                        "source_event_ids": [event_id_for("con-en-preference", 0)],
                    },
                ),
                finish_reason=FinishReason.tool_use,
            )
        ],
        [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
    ]
    deps = RunDeps(
        make_provider=lambda cid: ScriptedProviderGateway(wrong),
        embedder=FakeEmbedder(),
        save=lambda: None,
    )
    report = await run_evals(
        dataset_path=dataset,
        provider_cassette_path=tmp_path / "unused-provider.json",
        embedding_cassette_path=tmp_path / "unused-embed.json",
        mode="replay",
        suites={"consolidation"},
        enforce=True,
        out_dir=tmp_path / "run",
        deps=deps,
    )
    assert report.exit_code == 1  # a gate failed, but nothing errored
    junit_text = await asyncio.to_thread((tmp_path / "run" / "junit.xml").read_text, "utf-8")
    assert "<failure" in junit_text
