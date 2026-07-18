"""Operator CLI to provision the least-privilege runtime DB login (M3A, WS-DB).

Creates (or idempotently repairs) a dedicated ``LOGIN`` role that is a member of only the
non-owner, non-bypass ``keel_runtime`` group, so a deployment can point ``KEEL_DATABASE_URL``
at it and make ``FORCE ROW LEVEL SECURITY`` a hard boundary (the schema owner / a superuser
would bypass RLS). It runs through :func:`keel_core.runtime_db.provision_runtime_login` on the
dedicated owner/migrator connection (``KEEL_MIGRATION_DATABASE_URL`` -> a privileged principal).

Design (mirrors ``keel_core.identity.erase_cli``):

* **Fail closed.** The owner/migrator URL is resolved via
  :meth:`Settings.require_migration_database_url` (in cloud mode a missing
  ``KEEL_MIGRATION_DATABASE_URL`` is refused rather than falling back to the runtime
  ``KEEL_DATABASE_URL``). A missing ``keel_runtime`` group also fails closed.
* **Secret hygiene.** The login password is NEVER a CLI argument: it comes from an environment
  variable (``--password-env``, default ``KEEL_RUNTIME_DB_PASSWORD``) or ``--password-stdin``.
  It is quoted server-side and never printed, and no connection URL is logged.
* **Optional end-to-end verify.** ``--verify`` connects AS the freshly provisioned login and
  asserts it is least-privilege (not superuser / bypass / owner) so a misprovisioned role is
  caught immediately.

Run as ``python -m keel_core.provision_runtime_cli`` (see ``docs/OPERATIONS.md``).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from keel_core.config import Settings, get_settings, load_env_file
from keel_core.errors import MigrationDatabaseNotConfigured, RuntimePrincipalError
from keel_core.runtime_db import (
    DEFAULT_RUNTIME_LOGIN,
    RUNTIME_GROUP_ROLE,
    provision_runtime_login,
    verify_runtime_principal,
)

# Exit codes (stable for scripting).
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_CONFIG = 4

_DEFAULT_PASSWORD_ENV = "KEEL_RUNTIME_DB_PASSWORD"  # noqa: S105 - env var NAME, not a secret


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="keel-provision-runtime-login",
        description=(
            "Idempotently create/repair the least-privilege runtime DB login (a member of only "
            f"{RUNTIME_GROUP_ROLE}) that the server/worker connect as. Runs on the owner/migrator "
            "connection (KEEL_MIGRATION_DATABASE_URL)."
        ),
    )
    parser.add_argument(
        "--login-name",
        default=DEFAULT_RUNTIME_LOGIN,
        help=f"runtime login role to create/repair (default: {DEFAULT_RUNTIME_LOGIN}).",
    )
    parser.add_argument(
        "--group-role",
        default=RUNTIME_GROUP_ROLE,
        help=f"non-owner runtime group to grant (default: {RUNTIME_GROUP_ROLE}).",
    )
    secret = parser.add_mutually_exclusive_group()
    secret.add_argument(
        "--password-env",
        default=_DEFAULT_PASSWORD_ENV,
        metavar="VAR",
        help=(
            "environment variable holding the login password "
            f"(default: {_DEFAULT_PASSWORD_ENV}). The password is never passed as an argument."
        ),
    )
    secret.add_argument(
        "--password-stdin",
        action="store_true",
        help="read the login password from the first line of stdin instead of the environment.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="after provisioning, connect AS the login and assert it is least-privilege.",
    )
    return parser


def _resolve_password(args: argparse.Namespace) -> str:
    """Resolve the login password from stdin or the environment (never a CLI argument)."""
    if args.password_stdin:
        # First line only; strip the trailing newline but preserve any interior characters.
        raw = sys.stdin.readline()
        password = raw.rstrip("\r\n")
        source = "stdin"
    else:
        password = os.environ.get(args.password_env, "")
        source = f"${args.password_env}"
    if not password:
        raise ValueError(f"runtime login password is empty (from {source})")
    return password


async def _verify_login(admin_url: str, login: str, password: str) -> str:
    """Connect AS the provisioned login and assert it is least-privilege; return its name."""
    url = make_url(admin_url).set(username=login, password=password)
    engine = create_async_engine(url, pool_pre_ping=True, future=True, hide_parameters=True)
    try:
        async with engine.connect() as conn:
            return await verify_runtime_principal(conn)
    finally:
        await engine.dispose()


async def run_cli(args: argparse.Namespace, *, settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    try:
        password = _resolve_password(args)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    try:
        admin_url = settings.require_migration_database_url()
    except MigrationDatabaseNotConfigured as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    engine = create_async_engine(admin_url, pool_pre_ping=True, future=True, hide_parameters=True)
    try:
        login = await provision_runtime_login(
            engine,
            password=password,
            login_name=args.login_name,
            group_role=args.group_role,
        )
    except (RuntimePrincipalError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    finally:
        await engine.dispose()

    print(f"provisioned runtime login: {login} (member of {args.group_role})", file=sys.stderr)

    if args.verify:
        try:
            principal = await _verify_login(admin_url, login, password)
        except RuntimePrincipalError as exc:
            print(f"error: verification failed: {exc}", file=sys.stderr)
            return EXIT_CONFIG
        except Exception as exc:  # noqa: BLE001 - report connect/verify failure, never crash
            print(f"error: could not verify login: {exc.__class__.__name__}", file=sys.stderr)
            return EXIT_ERROR
        print(f"verified least-privilege runtime principal: {principal}", file=sys.stderr)

    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    load_env_file()
    args = build_parser().parse_args(argv)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(run_cli(args))


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
