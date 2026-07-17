"""Operator CLI for durable identity erasure (M3.6, WS-L).

The **minimum production-usable** path for erasing a user (data subject) or an organization
until identity erasure is folded into the durable lifecycle API. It runs through the
:class:`~keel_core.identity.purge.IdentityPurgeRepository`, which connects on the dedicated
least-privilege maintenance login (``KEEL_MAINTENANCE_DATABASE_URL`` -> a member of only the
``keel_maintenance_exec`` executor role) and invokes the ``keel_erase_user`` /
``keel_erase_organization`` SECURITY DEFINER functions.

Design:

* **Fail closed.** A missing maintenance URL (or, in cloud mode, a copy of the runtime URL)
  and an over-privileged connection are refused before anything is touched. The connection
  URL / credentials are never printed.
* **Preflight / dry-run.** Every run first executes the erasure inside a rolled-back
  transaction to preview the real deletes (or reveal a blocked owner) without persisting.
  ``--dry-run`` stops there.
* **Explicit confirmation.** A real erasure requires ``--yes`` (non-interactive) or typing the
  target id back at an interactive prompt; without a TTY and without ``--yes`` it fails closed.
* **Structured result.** ``--json`` emits a machine-readable result; the blocked-owner case is
  reported explicitly with the blocking organization ids.

Run as ``python -m keel_core.identity.erase_cli user <user_id>`` /
``python -m keel_core.identity.erase_cli organization <org_id>``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import Any

from keel_core.config import Settings, get_settings, load_env_file
from keel_core.errors import MaintenanceDatabaseNotConfigured
from keel_core.identity.purge import (
    IdentityPurgeRepository,
    MaintenancePrincipalError,
    OrganizationErasureResult,
    UserErasureBlockedError,
    UserErasureResult,
    create_identity_purge_repository,
)

# Exit codes (stable for scripting).
EXIT_OK = 0
EXIT_USAGE = 2
EXIT_BLOCKED = 3
EXIT_CONFIG = 4
EXIT_ABORTED = 5

RepoFactory = Callable[[Settings], Awaitable[IdentityPurgeRepository]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="keel-erase-identity",
        description=(
            "Erase a user (data subject) or organization through the least-privilege "
            "maintenance executor. Destructive and irreversible; always preflights first."
        ),
    )
    sub = parser.add_subparsers(dest="target_kind", required=True)
    for kind, idname in (("user", "user_id"), ("organization", "org_id")):
        p = sub.add_parser(kind, help=f"erase one {kind}")
        p.add_argument(idname, help=f"the {kind} id to erase")
        p.add_argument(
            "--yes",
            action="store_true",
            help="confirm non-interactively (required when stdin is not a TTY).",
        )
        p.add_argument(
            "--dry-run",
            action="store_true",
            help="preflight only: preview the erasure (rolled back) and exit.",
        )
        p.add_argument(
            "--json",
            dest="as_json",
            action="store_true",
            help="emit the structured result as JSON on stdout.",
        )
    return parser


def _target_id(args: argparse.Namespace) -> str:
    return str(args.user_id if args.target_kind == "user" else args.org_id)


def _emit(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True))
    else:
        kind = payload.get("target_kind")
        tid = payload.get("target_id")
        status = payload.get("status")
        print(f"{status}: {kind} {tid}")
        for key, value in sorted(payload.items()):
            if key in {"target_kind", "target_id", "status"}:
                continue
            print(f"  {key}: {value}")


def _confirm(
    target_kind: str, target_id: str, *, yes: bool, input_fn: Callable[[str], str]
) -> bool:
    """Return True when the operator has confirmed the destructive erasure."""
    if yes:
        return True
    if not sys.stdin.isatty():
        print(
            "error: refusing to erase without --yes when stdin is not a TTY",
            file=sys.stderr,
        )
        return False
    prompt = f"Type the {target_kind} id '{target_id}' to permanently erase it: "
    return input_fn(prompt).strip() == target_id


async def _run_erasure(
    repo: IdentityPurgeRepository, target_kind: str, target_id: str, *, dry_run: bool
) -> dict[str, Any]:
    """Execute (or preview) the erasure and return a structured result dict."""
    result: UserErasureResult | OrganizationErasureResult
    if target_kind == "user":
        result = await repo.erase_user(target_id, dry_run=dry_run)
    else:
        result = await repo.erase_organization(target_id, dry_run=dry_run)
    payload = asdict(result)
    payload["total"] = result.total
    return payload


async def run_cli(
    args: argparse.Namespace,
    *,
    settings: Settings | None = None,
    repo_factory: RepoFactory | None = None,
    input_fn: Callable[[str], str] = input,
) -> int:
    settings = settings or get_settings()
    factory = repo_factory or (lambda s: create_identity_purge_repository(s, verify=False))
    target_kind = args.target_kind
    target_id = _target_id(args)
    as_json = bool(args.as_json)

    try:
        repo = await factory(settings)
    except MaintenanceDatabaseNotConfigured as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    try:
        try:
            principal = await repo.verify_principal()
        except MaintenancePrincipalError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return EXIT_CONFIG
        print(f"verified maintenance principal: {principal}", file=sys.stderr)

        # Preflight: preview the real erasure inside a rolled-back transaction.
        try:
            preview = await _run_erasure(repo, target_kind, target_id, dry_run=True)
        except UserErasureBlockedError as exc:
            _emit(
                {
                    "target_kind": target_kind,
                    "target_id": target_id,
                    "status": "blocked",
                    "blocking_org_ids": list(exc.blocking_org_ids),
                    "detail": str(exc),
                },
                as_json=as_json,
            )
            return EXIT_BLOCKED
        preview.update({"target_kind": target_kind, "target_id": target_id, "status": "preview"})
        _emit(preview, as_json=as_json)

        if args.dry_run:
            return EXIT_OK

        if not _confirm(target_kind, target_id, yes=args.yes, input_fn=input_fn):
            print("aborted: erasure not confirmed", file=sys.stderr)
            return EXIT_ABORTED

        try:
            result = await _run_erasure(repo, target_kind, target_id, dry_run=False)
        except UserErasureBlockedError as exc:
            _emit(
                {
                    "target_kind": target_kind,
                    "target_id": target_id,
                    "status": "blocked",
                    "blocking_org_ids": list(exc.blocking_org_ids),
                    "detail": str(exc),
                },
                as_json=as_json,
            )
            return EXIT_BLOCKED
        result.update({"target_kind": target_kind, "target_id": target_id, "status": "erased"})
        _emit(result, as_json=as_json)
        return EXIT_OK
    finally:
        await repo.aclose()


def main(argv: list[str] | None = None) -> int:
    load_env_file()
    args = build_parser().parse_args(argv)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(run_cli(args))


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
