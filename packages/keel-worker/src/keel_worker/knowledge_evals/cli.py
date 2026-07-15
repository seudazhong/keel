"""Command-line entry for replayable Knowledge Base evals."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from .runner import run_evals, to_terminal

DEFAULT_DATASET = Path("evals/datasets/knowledge/v1.jsonl")
DEFAULT_EMBEDDING_CASSETTE = Path("evals/cassettes/knowledge/v1-embeddings.json")
DEFAULT_OUT = Path(".keel/evals/knowledge")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_knowledge_evals",
        description="Keel Knowledge retrieval eval suite",
    )
    parser.add_argument("--mode", choices=("replay", "live"), default="replay")
    parser.add_argument(
        "--record",
        action="store_true",
        help="Regenerate the embedding cassette from a live run.",
    )
    parser.add_argument(
        "--enforce",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Force quality-gate enforcement; replay enforces by default.",
    )
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument(
        "--embedding-cassette",
        type=Path,
        default=DEFAULT_EMBEDDING_CASSETTE,
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--model", default=None)
    parser.add_argument("--dim", type=int, default=None)
    return parser


def validate_args(args: argparse.Namespace) -> list[str]:
    errors: list[str] = []
    if args.record and args.mode != "live":
        errors.append("--record requires --mode live")
    if not args.dataset.exists():
        errors.append(f"dataset not found: {args.dataset}")
    if args.mode == "replay" and not args.embedding_cassette.exists():
        errors.append(f"embedding cassette not found: {args.embedding_cassette}")
    if args.dim is not None and args.dim < 1:
        errors.append("--dim must be a positive integer")
    return errors


async def run_cli(args: argparse.Namespace) -> int:
    errors = validate_args(args)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 2
    enforce = args.mode == "replay" if args.enforce is None else bool(args.enforce)
    try:
        report = await run_evals(
            dataset_path=args.dataset,
            embedding_cassette_path=args.embedding_cassette,
            mode=args.mode,
            enforce=enforce,
            out_dir=args.output,
            record=args.record,
            model=args.model,
            dim=args.dim,
        )
    except Exception as exc:  # noqa: BLE001 - CLI returns bounded infrastructure failure
        print(f"error: {type(exc).__name__}", file=sys.stderr)
        return 2
    print(to_terminal(report))
    return report.exit_code


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(run_cli(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["build_parser", "main", "run_cli", "validate_args"]
