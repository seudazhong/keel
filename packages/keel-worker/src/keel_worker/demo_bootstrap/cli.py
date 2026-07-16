"""Command-line entry point for the demo data bootstrap.

    uv run python scripts/seed_demo_data.py --dry-run
    uv run python scripts/seed_demo_data.py --yes

Refuses to run outside a recognized dev/demo environment (see
``keel_core.demo_guard``) and refuses to mutate anything without ``--yes`` or
``--dry-run``. Safe to re-run: seeded resources are reused, never duplicated,
and nothing pre-existing is ever modified or deleted.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from keel_core.config import get_settings, load_env_file
from keel_core.demo_guard import DemoGuardError, assert_demo_environment

from .runner import (
    DEFAULT_SCOPE_ID,
    DemoBootstrapError,
    describe_plan,
    render_result,
    run_bootstrap,
)

EXIT_OK = 0
EXIT_GUARD_REFUSED = 2
EXIT_CONFIRMATION_REQUIRED = 3
EXIT_BOOTSTRAP_FAILED = 4


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="seed_demo_data",
        description=(
            "Seed idempotent, non-destructive demo data (a Knowledge Base with "
            "searchable documents, plus a welcome session) into the configured "
            "local dev/demo stack."
        ),
    )
    parser.add_argument(
        "--scope",
        default=DEFAULT_SCOPE_ID,
        help="Scope id to seed (default: %(default)s, matching the server/worker default).",
    )
    parser.add_argument(
        "--mode",
        choices=("auto", "fake", "live"),
        default="auto",
        help=(
            "Embedding mode. 'auto' (default) probes the configured embedder and falls "
            "back to a deterministic offline embedder when it is unavailable, so lexical "
            "search always works even with no embedding provider configured. 'fake' "
            "always uses the deterministic offline embedder (no network call). 'live' "
            "requires the configured embedder to respond, or the run fails."
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Explicitly confirm the mutation (required unless --dry-run).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan and exit; make no changes.",
    )
    parser.add_argument("--json", action="store_true", help="Print the result as JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if sys.platform == "win32":  # psycopg async needs a selector loop on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    load_env_file()
    settings = get_settings()

    try:
        assert_demo_environment(settings)
    except DemoGuardError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_GUARD_REFUSED

    if args.dry_run:
        print(describe_plan(scope_id=args.scope, mode=args.mode, settings=settings))
        return EXIT_OK

    if not args.yes:
        print(describe_plan(scope_id=args.scope, mode=args.mode, settings=settings))
        print(
            "\nRefusing to mutate without explicit confirmation. Re-run with --yes to "
            "apply, or --dry-run to preview only.",
            file=sys.stderr,
        )
        return EXIT_CONFIRMATION_REQUIRED

    try:
        result = asyncio.run(
            run_bootstrap(settings=settings, scope_id=args.scope, mode=args.mode)
        )
    except DemoGuardError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_GUARD_REFUSED
    except DemoBootstrapError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_BOOTSTRAP_FAILED

    if args.json:
        print(json.dumps(result.to_dict(), indent=2, default=str))
    else:
        print(render_result(result))
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
