"""GitHub webhook verification + event parsing (M3.7, WS-P).

Pure, network-free helpers the webhook endpoint uses to authenticate and normalize an inbound
GitHub delivery *before* any tenant context exists:

* :func:`verify_signature` — constant-time ``X-Hub-Signature-256`` HMAC over the **raw** body.
* :func:`delivery_id` / :func:`event_name` — extract the durable idempotency key + event.
* :data:`ALLOWED_EVENTS` — the event allowlist (anything else is skipped, not processed).
* :func:`parse_event` — validate + normalize the JSON payload into a typed
  :class:`WebhookEvent`, surfacing the installation id used for the installation<->org binding.

The endpoint remains responsible for replay protection (via the durable delivery ledger) and
for binding the delivery's installation id to an org before doing any work.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field
from typing import Any

_SIGNATURE_PREFIX = "sha256="

# The events this phase understands. Everything else is acknowledged then skipped.
ALLOWED_EVENTS = frozenset(
    {
        "ping",
        "installation",
        "installation_repositories",
        "push",
        "repository",
        "pull_request",
    }
)


class WebhookVerificationError(ValueError):
    """A webhook failed signature / structural verification (fail closed)."""


def verify_signature(secret: str, body: bytes, signature_header: str | None) -> bool:
    """Constant-time verify an ``X-Hub-Signature-256: sha256=<hex>`` header over ``body``."""
    if not secret or not signature_header:
        return False
    header = signature_header.strip()
    if not header.startswith(_SIGNATURE_PREFIX):
        return False
    provided = header[len(_SIGNATURE_PREFIX) :]
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, provided)


def delivery_id(header: str | None) -> str | None:
    """The ``X-GitHub-Delivery`` id (durable idempotency key), or ``None`` if absent/blank."""
    if header is None:
        return None
    value = header.strip()
    return value or None


def event_name(header: str | None) -> str | None:
    """The ``X-GitHub-Event`` name, or ``None`` if absent/blank."""
    if header is None:
        return None
    value = header.strip()
    return value or None


@dataclass(frozen=True)
class WebhookEvent:
    """A normalized, allowlisted webhook event."""

    delivery_id: str
    event: str
    action: str | None
    installation_id: int | None
    repository_ids: tuple[int, ...] = ()
    ref: str | None = None
    before: str | None = None
    after: str | None = None
    default_branch: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


def _installation_id(payload: dict[str, Any]) -> int | None:
    installation = payload.get("installation")
    if isinstance(installation, dict):
        value = installation.get("id")
        if isinstance(value, int):
            return value
    return None


def _repo_ids(payload: dict[str, Any]) -> tuple[int, ...]:
    ids: list[int] = []
    repo = payload.get("repository")
    if isinstance(repo, dict) and isinstance(repo.get("id"), int):
        ids.append(repo["id"])
    for key in ("repositories", "repositories_added", "repositories_removed"):
        items = payload.get(key)
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and isinstance(item.get("id"), int):
                    ids.append(item["id"])
    # Preserve order but de-duplicate.
    seen: set[int] = set()
    unique: list[int] = []
    for value in ids:
        if value not in seen:
            seen.add(value)
            unique.append(value)
    return tuple(unique)


def parse_event(*, event: str, delivery: str, payload: dict[str, Any]) -> WebhookEvent:
    """Validate + normalize an allowlisted event (raises for a malformed/oversize payload)."""
    if event not in ALLOWED_EVENTS:
        raise WebhookVerificationError("event is not on the allowlist")
    if not isinstance(payload, dict):
        raise WebhookVerificationError("payload must be a JSON object")
    action = payload.get("action")
    if action is not None and not isinstance(action, str):
        raise WebhookVerificationError("action must be a string")
    ref = payload.get("ref") if isinstance(payload.get("ref"), str) else None
    before = payload.get("before") if isinstance(payload.get("before"), str) else None
    after = payload.get("after") if isinstance(payload.get("after"), str) else None
    default_branch: str | None = None
    repo = payload.get("repository")
    if isinstance(repo, dict) and isinstance(repo.get("default_branch"), str):
        default_branch = repo["default_branch"]
    return WebhookEvent(
        delivery_id=delivery,
        event=event,
        action=action,
        installation_id=_installation_id(payload),
        repository_ids=_repo_ids(payload),
        ref=ref,
        before=before,
        after=after,
        default_branch=default_branch,
        payload=payload,
    )


__all__ = [
    "ALLOWED_EVENTS",
    "WebhookEvent",
    "WebhookVerificationError",
    "delivery_id",
    "event_name",
    "parse_event",
    "verify_signature",
]
