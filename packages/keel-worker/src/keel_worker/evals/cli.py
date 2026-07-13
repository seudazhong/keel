"""Command-line entry for the memory eval suite.

Modes:
  replay          — read only the checked-in cassettes; enforce gates by default (CI).
  live            — call live services without recording; gates not enforced.
  live --record   — a live run that (re)generates both cassettes atomically; not enforced.

``--record`` requires ``--mode live`` (recording is a live run that additionally persists
the cassettes). Database safety is enforced by ``create_eval_engine`` +
``assert_current_database`` (``runner.run_evals``): ``KEEL_EVAL_DATABASE_URL`` must be set
and must not point at a live ``keel`` database. This CLI never runs against production.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from keel_worker.evals.reporting import to_terminal
from keel_worker.evals.runner import ALL_SUITES, default_enforce, run_evals

DEFAULT_DATASET = Path("evals/datasets/memory/v1.jsonl")
DEFAULT_PROVIDER_CASSETTE = Path("evals/cassettes/memory/v1-provider.json")
DEFAULT_EMBEDDING_CASSETTE = Path("evals/cassettes/memory/v1-embeddings.json")
DEFAULT_OUT = Path(".keel/evals")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="run_memory_evals", description="Keel memory eval suite")
    parser.add_argument("--mode", choices=("replay", "live"), default="replay")
    parser.add_argument(
        "--record",
        action="store_true",
        help="Regenerate both cassettes from a live run (requires --mode live).",
    )
    parser.add_argument("--suite", choices=("all", *ALL_SUITES), default="all")
    parser.add_argument(
        "--enforce",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Force gate enforcement on/off; default depends on --mode.",
    )
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--provider-cassette", type=Path, default=DEFAULT_PROVIDER_CASSETTE)
    parser.add_argument("--embedding-cassette", type=Path, default=DEFAULT_EMBEDDING_CASSETTE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--model", default=None, help="Override the provider model for every case.")
    parser.add_argument(
        "--judge",
        action="store_true",
        help="Attach an advisory LLM-judge verdict per case (never gates the run).",
    )
    parser.add_argument(
        "--judge-model",
        default=None,
        help="Model for --judge (defaults to Settings.default_model).",
    )
    parser.add_argument(
        "--langfuse",
        action="store_true",
        help="Also publish the run to Langfuse (best-effort; requires Settings keys).",
    )
    return parser


def resolve_enforce(mode: str, enforce_flag: bool | None) -> bool:
    return default_enforce(mode) if enforce_flag is None else enforce_flag  # type: ignore[arg-type]


def resolve_suites(suite: str) -> set[str]:
    return set(ALL_SUITES) if suite == "all" else {suite}


def validate_record(mode: str, record: bool) -> list[str]:
    if record and mode != "live":
        return ["--record requires --mode live"]
    return []


def validate_paths(
    mode: str, dataset: Path, provider_cassette: Path, embedding_cassette: Path
) -> list[str]:
    errors: list[str] = []
    if not dataset.exists():
        errors.append(f"dataset not found: {dataset}")
    if mode == "replay":
        if not provider_cassette.exists():
            errors.append(f"provider cassette not found (required for replay): {provider_cassette}")
        if not embedding_cassette.exists():
            errors.append(
                f"embedding cassette not found (required for replay): {embedding_cassette}"
            )
    return errors


async def run_cli(args: argparse.Namespace) -> int:
    errors = validate_record(args.mode, args.record) + validate_paths(
        args.mode, args.dataset, args.provider_cassette, args.embedding_cassette
    )
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 2
    report = await run_evals(
        dataset_path=args.dataset,
        provider_cassette_path=args.provider_cassette,
        embedding_cassette_path=args.embedding_cassette,
        mode=args.mode,
        suites=resolve_suites(args.suite),
        enforce=resolve_enforce(args.mode, args.enforce),
        out_dir=args.output,
        record=args.record,
        model=args.model,
        judge=args.judge,
        judge_model=args.judge_model,
        langfuse=args.langfuse,
    )
    print(to_terminal(report))
    return report.exit_code


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return asyncio.run(run_cli(args))


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
