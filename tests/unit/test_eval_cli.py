"""CLI arg parsing, enforce defaults, and replay path validation."""

from __future__ import annotations

from pathlib import Path

from keel_worker.evals.cli import (
    build_parser,
    resolve_enforce,
    resolve_suites,
    run_cli,
    validate_paths,
    validate_record,
)


def test_defaults() -> None:
    args = build_parser().parse_args([])
    assert args.mode == "replay"
    assert args.suite == "all"
    assert args.record is False
    assert args.enforce is None  # unset → resolved from mode
    assert args.dataset == Path("evals/datasets/memory/v1.jsonl")
    assert args.provider_cassette == Path("evals/cassettes/memory/v1-provider.json")
    assert args.embedding_cassette == Path("evals/cassettes/memory/v1-embeddings.json")
    assert args.output == Path(".keel/evals")
    assert args.model is None  # no override
    assert args.judge is False and args.judge_model is None  # judge opt-in
    assert args.langfuse is False  # Langfuse opt-in


def test_resolve_enforce_defaults_by_mode() -> None:
    assert resolve_enforce("replay", None) is True
    assert resolve_enforce("live", None) is False


def test_record_requires_live() -> None:
    assert validate_record("replay", True)  # non-empty → error
    assert validate_record("live", True) == []
    assert validate_record("live", False) == []


def test_resolve_enforce_explicit_override() -> None:
    assert resolve_enforce("replay", False) is False
    assert resolve_enforce("live", True) is True


def test_resolve_suites() -> None:
    assert resolve_suites("all") == {"consolidation", "recall", "safety"}
    assert resolve_suites("recall") == {"recall"}


def test_validate_paths_replay_requires_cassettes(tmp_path: Path) -> None:
    dataset = tmp_path / "v1.jsonl"
    dataset.write_text("{}\n", encoding="utf-8")
    missing_provider = tmp_path / "prov.json"
    missing_embed = tmp_path / "emb.json"
    errors = validate_paths("replay", dataset, missing_provider, missing_embed)
    assert any("provider cassette" in e for e in errors)
    assert any("embedding cassette" in e for e in errors)


def test_validate_paths_live_ignores_cassettes(tmp_path: Path) -> None:
    dataset = tmp_path / "v1.jsonl"
    dataset.write_text("{}\n", encoding="utf-8")
    errors = validate_paths("live", dataset, tmp_path / "x.json", tmp_path / "y.json")
    assert errors == []


def test_validate_paths_missing_dataset(tmp_path: Path) -> None:
    errors = validate_paths("live", tmp_path / "nope.jsonl", tmp_path / "x", tmp_path / "y")
    assert any("dataset" in e for e in errors)


async def test_run_cli_returns_infra_exit_for_invalid_paths(
    tmp_path: Path,
    capsys,
) -> None:
    args = build_parser().parse_args(
        [
            "--mode",
            "replay",
            "--dataset",
            str(tmp_path / "missing.jsonl"),
            "--provider-cassette",
            str(tmp_path / "missing-provider.json"),
            "--embedding-cassette",
            str(tmp_path / "missing-embeddings.json"),
        ]
    )

    assert await run_cli(args) == 2
    assert "error:" in capsys.readouterr().err
