"""Real Gmail connector (WS-G): the digest's ``inbox_list`` + ``email_send`` over Gmail.

Loads an OAuth refresh token from the scope-bound connector token store and reads the
inbox (``gmail.readonly``) / sends mail (``gmail.send``), formatting reads exactly like
the fake ``_inbox_text()`` so the loop and confused-deputy guard behave identically —
only the :data:`ActionFn` changes. Reads taint their output (``outbound=False``, G17);
sends stay ``outbound=True`` so a run that ingested tainted content still needs an
approval before mail leaves (the confused-deputy guard is the headline defense).
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any, Protocol

from keel_core.connector_credentials import rewrap_provider_json, unwrap_legacy_or_enveloped
from keel_core.connectors import ActionFn, ConnectorActionUserError
from keel_core.errors import KeelError
from keel_core.protocols import ToolContext

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials

GMAIL_SCOPES: tuple[str, ...] = (
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
)
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


def _load_credentials(creds_json: str) -> Credentials:
    """Reconstruct OAuth credentials, refreshing an expired access token if possible."""
    from google.auth.exceptions import RefreshError
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials

    creds: Credentials = Credentials.from_authorized_user_info(  # type: ignore[no-untyped-call]
        json.loads(creds_json), list(GMAIL_SCOPES)
    )
    if not creds.valid:
        if creds.refresh_token:
            try:
                creds.refresh(Request())  # type: ignore[no-untyped-call]
            except RefreshError as exc:
                raise ConnectorActionUserError(
                    "Gmail authorization expired or was revoked. Reconnect Gmail in Connectors."
                ) from exc
        else:
            raise GmailError("gmail credentials are invalid and have no refresh token")
    return creds


def _fetch_inbox_sync(creds_json: str, max_messages: int) -> tuple[str, str]:
    """Blocking Gmail read. Returns ``(formatted_text, latest_creds_json)``.

    Google's client libraries are synchronous; call this via ``asyncio.to_thread``.
    The returned creds json reflects any access-token rotation from a refresh so the
    caller can persist it back into the token store.
    """
    from googleapiclient.discovery import build

    creds = _load_credentials(creds_json)
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
    return format_inbox(messages), creds.to_json()  # type: ignore[no-untyped-call]


def make_gmail_inbox_action(store: TokenStore, max_messages: int = 5) -> ActionFn:
    """Build an inbound ``inbox_list`` ActionFn backed by the real Gmail API.

    Loads the refresh token from ``store`` (connector ``"gmail"``), reads the inbox in
    a worker thread, and persists any rotated credentials. Raises :class:`GmailError`
    when the connector has not been authorized (run ``scripts/gmail_authorize.py``).
    """

    async def gmail_inbox(args: dict[str, Any], ctx: ToolContext) -> str:
        stored = await store.get(GMAIL_CONNECTOR_ID)
        if stored is None:
            raise GmailError(
                "gmail connector is not authorized for this scope — run scripts/gmail_authorize.py"
            )
        try:
            creds_json, enveloped = unwrap_legacy_or_enveloped(stored, expected_kind="oauth")
        except ValueError as exc:
            raise GmailError("gmail credentials have an invalid envelope") from exc
        rendered, latest = await asyncio.to_thread(_fetch_inbox_sync, creds_json, max_messages)
        if latest and latest != creds_json:
            await store.put(
                GMAIL_CONNECTOR_ID,
                rewrap_provider_json(latest, kind="oauth", enveloped=enveloped),
            )
        return rendered

    return gmail_inbox


def _send_sync(creds_json: str, to: str, subject: str, body: str) -> tuple[str, str]:
    """Blocking Gmail send. Returns ``(message_id, latest_creds_json)``.

    Builds an RFC 822 message and submits it base64url-encoded to
    ``users().messages().send``; call via ``asyncio.to_thread``.
    """
    import base64
    from email.message import EmailMessage

    from googleapiclient.discovery import build

    creds = _load_credentials(creds_json)
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    try:
        message = EmailMessage()
        message["To"] = to
        message["Subject"] = subject
        message.set_content(body)
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")
        result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
        message_id = str(result.get("id", ""))
    finally:
        service.close()
    return message_id, creds.to_json()  # type: ignore[no-untyped-call]


def make_gmail_send_action(store: TokenStore) -> ActionFn:
    """Build an outbound ``email_send`` ActionFn backed by the real Gmail API.

    Loads the refresh token from ``store`` (connector ``"gmail"``), sends the message in
    a worker thread, and persists any rotated credentials. The ``ConnectorTool`` remains
    ``outbound=True``, so this only ever runs after the confused-deputy guard's approval
    when the run has ingested tainted content. Raises :class:`GmailError` when the
    connector is unauthorized or ``to`` is missing.
    """

    async def gmail_send(args: dict[str, Any], ctx: ToolContext) -> str:
        stored = await store.get(GMAIL_CONNECTOR_ID)
        if stored is None:
            raise GmailError(
                "gmail connector is not authorized for this scope — run scripts/gmail_authorize.py"
            )
        try:
            creds_json, enveloped = unwrap_legacy_or_enveloped(stored, expected_kind="oauth")
        except ValueError as exc:
            raise GmailError("gmail credentials have an invalid envelope") from exc
        to = str(args.get("to", "")).strip()
        if not to:
            raise GmailError("email_send requires a 'to' address")
        subject = str(args.get("subject", ""))
        body = str(args.get("body", ""))
        message_id, latest = await asyncio.to_thread(_send_sync, creds_json, to, subject, body)
        if latest and latest != creds_json:
            await store.put(
                GMAIL_CONNECTOR_ID,
                rewrap_provider_json(latest, kind="oauth", enveloped=enveloped),
            )
        return f"sent (id={message_id})"

    return gmail_send
