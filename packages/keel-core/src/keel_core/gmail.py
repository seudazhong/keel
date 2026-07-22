"""Real Gmail connector (WS-G): the digest's ``inbox_list`` + ``email_send`` over Gmail.

Loads an OAuth refresh token from the scope-bound connector token store and reads the
inbox (``gmail.readonly``) / sends mail (``gmail.send``), formatting reads exactly like
the fake ``_inbox_text()`` so the loop and confused-deputy guard behave identically —
only the :data:`ActionFn` changes. Reads taint their output (``outbound=False``, G17);
sends stay ``outbound=True`` so a run that ingested tainted content still needs an
approval before mail leaves (the confused-deputy guard is the headline defense).

R1B (C4/C5): ``email_send`` embeds a deterministic RFC 5322 ``Message-ID`` derived from
``(scope_id, idempotency_key)`` (:func:`gmail_message_id`) before submitting — Gmail's
raw-MIME ``users.messages.send`` preserves a caller-supplied ``Message-ID`` as-is, so the
same logical send always produces the same header regardless of how many times (or by
which worker) it is attempted. A network failure *after* the request may have reached
Gmail (a timeout/connection reset while waiting for the response) raises
:class:`~keel_core.connectors.ProviderAmbiguousError` instead of returning/re-raising an
ordinary failure, so :class:`~keel_core.connectors.ConnectorTool` marks the Effect
``unknown`` and blocks retry until :class:`GmailSendReconciler` proves whether the
message actually sent (a Gmail search by ``rfc822msgid:``) — never inferred from a bare
retry succeeding. A validation error (missing ``to``) or an authorization failure — both
provably *before* any request could have reached Gmail — stay ordinary, retryable
failures.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import TYPE_CHECKING, Any, Protocol

from keel_core.connector_contracts import (
    ConnectorReconciliationOutcome,
    ConnectorReconciliationRequest,
    ConnectorReconciliationResult,
)
from keel_core.connector_credentials import rewrap_provider_json, unwrap_legacy_or_enveloped
from keel_core.connectors import ActionFn, ConnectorActionUserError, ProviderAmbiguousError
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


def gmail_message_id(scope_id: str, idempotency_key: str) -> str:
    """A deterministic RFC 5322 ``Message-ID`` for one logical ``email_send`` Effect.

    Gmail's raw-MIME ``users.messages.send`` preserves a caller-supplied ``Message-ID``
    header verbatim, so embedding this before every send attempt (including a retry after
    an ordinary failure) gives a stable identity a reconciler can search for later —
    without ever needing to persist Gmail's own server-assigned message id up front."""
    digest = hashlib.sha256(f"{scope_id}\x1fgmail-send\x1f{idempotency_key}".encode()).hexdigest()
    return f"<keel-{digest}@keel.effects>"


def _load_ambiguous_transport_errors() -> tuple[type[BaseException], ...]:
    """Transport failures where the request may have already reached Gmail before the
    response was lost — as opposed to a DNS failure or a connection that was never
    established (ordinary, provably pre-send). Kept as an explicit, narrow tuple rather
    than a broad ``OSError`` catch (which would also swallow definitely-pre-send failures
    like DNS resolution) so ambiguity is never over-claimed for the common ordinary-
    failure case."""
    import http.client
    import ssl

    return (
        TimeoutError,
        ConnectionResetError,
        BrokenPipeError,
        http.client.RemoteDisconnected,
        ssl.SSLError,
    )


def _send_sync(
    creds_json: str, to: str, subject: str, body: str, message_id: str
) -> tuple[str, str]:
    """Blocking Gmail send. Returns ``(message_id, latest_creds_json)``.

    Builds an RFC 822 message (with the deterministic ``message_id`` as its
    ``Message-ID`` header, R1B) and submits it base64url-encoded to
    ``users().messages().send``; call via ``asyncio.to_thread``. Any exception
    (including a transport failure after submission) propagates to the caller, which
    classifies it (:func:`make_gmail_send_action`) — kept as a single, easily-testable
    seam rather than duplicating classification inside the blocking call.
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
        message["Message-ID"] = message_id
        message.set_content(body)
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")
        result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
        returned_id = str(result.get("id", ""))
    finally:
        service.close()
    return returned_id, creds.to_json()  # type: ignore[no-untyped-call]


def make_gmail_send_action(store: TokenStore) -> ActionFn:
    """Build an outbound ``email_send`` ActionFn backed by the real Gmail API.

    Loads the refresh token from ``store`` (connector ``"gmail"``), sends the message in
    a worker thread, and persists any rotated credentials. The ``ConnectorTool`` remains
    ``outbound=True``, so this only ever runs after the confused-deputy guard's approval
    when the run has ingested tainted content. Raises :class:`GmailError` when the
    connector is unauthorized or ``to`` is missing (ordinary, pre-send failures) and
    :class:`~keel_core.connectors.ProviderAmbiguousError` when the request may have
    reached Gmail before the response was lost (R1B, C4) — classified here so a
    ``googleapiclient.errors.HttpError`` (Gmail *did* respond, even with an error status)
    always stays an ordinary failure, never ambiguous.
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
        # ConnectorTool always resolves the exact idempotency key and carries it on
        # ``ctx.idempotency_key`` (even when the model omitted it), so the Message-ID
        # stays stable across every attempt of the same logical Effect.
        idempotency_key = (
            ctx.idempotency_key or str(args.get("idempotency_key", "")) or (ctx.tool_call_id or "")
        )
        message_id = gmail_message_id(ctx.scope_id, idempotency_key)
        try:
            returned_id, latest = await asyncio.to_thread(
                _send_sync, creds_json, to, subject, body, message_id
            )
        except _load_ambiguous_transport_errors() as exc:
            raise ProviderAmbiguousError(
                f"gmail email_send transport failure after submission: {exc.__class__.__name__}"
            ) from exc
        if latest and latest != creds_json:
            await store.put(
                GMAIL_CONNECTOR_ID,
                rewrap_provider_json(latest, kind="oauth", enveloped=enveloped),
            )
        return json.dumps({"id": returned_id, "message_id": message_id})

    return gmail_send


def _search_by_message_id_sync(creds_json: str, message_id: str) -> str | None:
    """Blocking Gmail search for a message by its exact ``Message-ID`` header.

    Returns Gmail's own message id when found, else ``None`` (the send never reached
    Gmail, or has not yet been indexed — see :class:`GmailSendReconciler`). Call via
    ``asyncio.to_thread``.
    """
    from googleapiclient.discovery import build

    creds = _load_credentials(creds_json)
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    try:
        query_message_id = message_id.removeprefix("<").removesuffix(">")
        listing = (
            service.users()
            .messages()
            .list(userId="me", q=f"rfc822msgid:{query_message_id}")
            .execute()
        )
        messages = listing.get("messages", [])
        return str(messages[0]["id"]) if messages else None
    finally:
        service.close()


class GmailSendReconciler:
    """Prove whether an ``unknown`` ``email_send`` Effect actually reached Gmail.

    Recomputes the deterministic ``Message-ID`` from the Effect's own
    ``(scope_id, idempotency_key)`` (never trusts a caller-supplied value) and searches
    Gmail for it — a real provider-side proof, never an inference from a bare retry."""

    def __init__(self, store: TokenStore) -> None:
        self._store = store

    async def reconcile(
        self, request: ConnectorReconciliationRequest
    ) -> ConnectorReconciliationResult:
        stored = await self._store.get(GMAIL_CONNECTOR_ID)
        if stored is None:
            return ConnectorReconciliationResult(ConnectorReconciliationOutcome.incapable)
        try:
            creds_json, _enveloped = unwrap_legacy_or_enveloped(stored, expected_kind="oauth")
        except ValueError:
            return ConnectorReconciliationResult(ConnectorReconciliationOutcome.incapable)
        message_id = gmail_message_id(request.scope_id, request.idempotency_key)
        found = await asyncio.to_thread(_search_by_message_id_sync, creds_json, message_id)
        if found is not None:
            return ConnectorReconciliationResult(
                ConnectorReconciliationOutcome.confirmed,
                provider_ref=found,
                result=json.dumps({"id": found, "message_id": message_id}),
            )
        # Gmail search can prove existence, but a not-found result cannot prove that an
        # older in-flight send will never land. Gmail send is not provider-idempotent, so
        # absence must never unlock an automatic retry (C4).
        return ConnectorReconciliationResult(ConnectorReconciliationOutcome.incapable)


def make_gmail_reconciler(store: TokenStore) -> GmailSendReconciler:
    return GmailSendReconciler(store)
