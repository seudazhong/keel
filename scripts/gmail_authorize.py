"""Authorize the Gmail read connector once (host-side), storing the refresh token.

    python scripts/gmail_authorize.py --scope web:local

Runs the OAuth *installed app* flow: opens a browser for the signed-in Google account
to consent to read-only Gmail access, then stores the resulting credentials
(``Credentials.to_json()``) via the scope-bound, envelope-encrypted connector token
store (connector ``"gmail"``). The worker later loads this token to read the inbox.

Prerequisites:
- ``.secrets/gmail_client.json`` — an OAuth *Desktop app* client (``installed`` format).
- ``KEEL_SECRET_KEY`` set (same value the worker uses) so the token can be decrypted.
- ``KEEL_DATABASE_URL`` reachable from the host (compose publishes Postgres on :5432).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from keel_core.config import get_settings, load_env_file
from keel_core.db import make_async_engine
from keel_core.gmail import GMAIL_CONNECTOR_ID, GMAIL_SCOPES
from keel_core.secrets import cipher_from_settings
from keel_core.tokens import PostgresTokenStore


async def _store(scope_id: str, creds_json: str) -> None:
    settings = get_settings()
    engine = make_async_engine(settings)
    store = PostgresTokenStore(engine, scope_id, cipher_from_settings(settings))
    try:
        await store.put(GMAIL_CONNECTOR_ID, creds_json)
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Authorize the Gmail read connector.")
    parser.add_argument("--scope", default="web:local")
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="print the consent URL instead of opening a browser (drive it yourself)",
    )
    args = parser.parse_args()

    load_env_file()  # KEEL_SECRET_KEY / KEEL_DATABASE_URL from .env
    settings = get_settings()

    client_path = Path(settings.gmail_client_secrets_path)
    if not client_path.is_file():
        raise SystemExit(
            f"missing OAuth client at {client_path} — create a Desktop app client and "
            "save it there (installed format)"
        )
    if not settings.secret_key:
        raise SystemExit("KEEL_SECRET_KEY is not set — token encryption is unavailable")

    from google_auth_oauthlib.flow import InstalledAppFlow

    flow = InstalledAppFlow.from_client_secrets_file(str(client_path), list(GMAIL_SCOPES))
    # offline + forced consent guarantees a refresh_token even on re-authorization.
    creds = flow.run_local_server(
        port=0,
        open_browser=not args.no_browser,
        access_type="offline",
        prompt="consent",
    )

    if sys.platform == "win32":  # psycopg async needs a selector loop on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(_store(args.scope, creds.to_json()))
    print(f"stored gmail token for scope {args.scope} (connector {GMAIL_CONNECTOR_ID})")


if __name__ == "__main__":
    main()
