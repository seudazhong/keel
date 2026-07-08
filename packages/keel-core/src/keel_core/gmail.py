"""Real read-only Gmail connector (WS-G): the digest's ``inbox_list`` backed by Gmail.

Loads an OAuth refresh token from the scope-bound connector token store, reads the
inbox read-only, and formats messages exactly like the fake ``_inbox_text()`` so the
loop and confused-deputy guard behave identically — only the :data:`ActionFn` changes.
The ``ConnectorTool(outbound=False)`` seam still taints the output (G17). Outbound send
is never granted here: the OAuth scope is ``gmail.readonly``, so a compromised run can
read but never send through this connector.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Protocol

from keel_core.connectors import ActionFn
from keel_core.errors import KeelError
from keel_core.protocols import ToolContext

GMAIL_SCOPES: tuple[str, ...] = ("https://www.googleapis.com/auth/gmail.readonly",)
GMAIL_CONNECTOR_ID = "gmail"


class GmailError(KeelError):
    """The Gmail connector is unauthorized or an API call failed."""


class TokenStore(Protocol):
    """The slice of the connector token store this module needs (scope-bound)."""

    async def get(self, connector_id: str) -> str | None: ...

    async def put(self, connector_id: str, secret: str) -> None: ...


def format_inbox(messages: list[dict[str, str]]) -> str:
    """Render messages like the fake inbox: ``[i] from — subject: snippet`` per line."""
    return "\n".join(
        f"[{i}] {m.get('from', '')} — {m.get('subject', '')}: {m.get('snippet', '')}"
        for i, m in enumerate(messages)
    )


def _fetch_inbox_sync(creds_json: str, max_messages: int) -> tuple[str, str]:
    """Blocking Gmail read. Returns ``(formatted_text, latest_creds_json)``.

    Google's client libraries are synchronous; call this via ``asyncio.to_thread``.
    The returned creds json reflects any access-token rotation from a refresh so the
    caller can persist it back into the token store.
    """
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    creds = Credentials.from_authorized_user_info(  # type: ignore[no-untyped-call]
        json.loads(creds_json), list(GMAIL_SCOPES)
    )
    if not creds.valid:
        if creds.refresh_token:
            creds.refresh(Request())
        else:
            raise GmailError("gmail credentials are invalid and have no refresh token")

    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    try:
        listing = (
            service.users()
            .messages()
            .list(userId="me", labelIds=["INBOX"], maxResults=max_messages)
            .execute()
        )
        messages: list[dict[str, str]] = []
        for ref in listing.get("messages", []):
            msg = (
                service.users()
                .messages()
                .get(
                    userId="me",
                    id=ref["id"],
                    format="metadata",
                    metadataHeaders=["From", "Subject"],
                )
                .execute()
            )
            headers = {
                h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])
            }
            messages.append(
                {
                    "from": headers.get("from", ""),
                    "subject": headers.get("subject", ""),
                    "snippet": msg.get("snippet", ""),
                }
            )
    finally:
        service.close()
    return format_inbox(messages), creds.to_json()


def make_gmail_inbox_action(store: TokenStore, max_messages: int = 5) -> ActionFn:
    """Build an inbound ``inbox_list`` ActionFn backed by the real Gmail API.

    Loads the refresh token from ``store`` (connector ``"gmail"``), reads the inbox in
    a worker thread, and persists any rotated credentials. Raises :class:`GmailError`
    when the connector has not been authorized (run ``scripts/gmail_authorize.py``).
    """

    async def gmail_inbox(args: dict[str, Any], ctx: ToolContext) -> str:
        creds_json = await store.get(GMAIL_CONNECTOR_ID)
        if creds_json is None:
            raise GmailError(
                "gmail connector is not authorized for this scope — run scripts/gmail_authorize.py"
            )
        rendered, latest = await asyncio.to_thread(_fetch_inbox_sync, creds_json, max_messages)
        if latest and latest != creds_json:
            await store.put(GMAIL_CONNECTOR_ID, latest)
        return rendered

    return gmail_inbox
