"""The generic durable Effect ledger (R1B, invariants C4/C5).

An **Effect** is the durable record of one external, at-most-once provider mutation
(send an email, create/update a calendar event, post a comment, ...). It is the single
seam every outbound connector action goes through instead of the narrower
``connector_outbox`` claim/finalize/release triple (:mod:`keel_core.outbox`), so:

* a possible provider success followed by response loss can **never** collapse into an
  ordinary ``failed`` result or have its claim silently deleted (C4);
* an approval/action-hash binds to the *exact* effect it authorized (C5);
* a duplicate logical send always observes the *same* Effect and can never issue a
  second provider mutation.

State machine (the only legal transitions — see :data:`_TRANSITIONS`)::

    reserved --------> executing --------> confirmed              (terminal, success)
                            |
                            +-----------> failed -------> executing   (ordinary retry)
                            |
                            +-----------> unknown ----+--> reconciled_confirmed (terminal)
                                                       |
                                                       +--> reconciled_absent --> executing
                                                                                  (exactly one
                                                                                   controlled retry)

``unknown`` is a trap state: nothing may leave it except the reconciliation worker
(:mod:`keel_worker` proving the provider-side mutation exists or definitely does not), and
an ordinary retry attempt is refused (:func:`is_retryable`) while an effect sits in
``unknown`` — the headline of C4.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from keel_core.errors import KeelError

# Argument keys that must never be persisted verbatim in the immutable canonical-args
# column (mirrors keel_core.connector_credentials / migration 0016's secret-key guard).
_SECRET_ARG_KEYS = frozenset(
    {
        "secret",
        "token",
        "password",
        "client_secret",
        "private_key",
        "api_key",
        "access_token",
        "refresh_token",
        "authorization",
        "credential",
        "credentials",
    }
)


class EffectStatus(StrEnum):
    """The durable lifecycle of one external mutation attempt (C4/C5)."""

    reserved = "reserved"
    executing = "executing"
    confirmed = "confirmed"
    unknown = "unknown"
    reconciled_confirmed = "reconciled_confirmed"
    reconciled_absent = "reconciled_absent"
    failed = "failed"


# The complete legal-transition graph. Anything not listed here is illegal and must be
# rejected fail-closed by every store implementation (never silently coerced).
_TRANSITIONS: dict[EffectStatus, frozenset[EffectStatus]] = {
    EffectStatus.reserved: frozenset({EffectStatus.executing}),
    EffectStatus.executing: frozenset(
        {EffectStatus.confirmed, EffectStatus.unknown, EffectStatus.failed}
    ),
    EffectStatus.confirmed: frozenset(),
    EffectStatus.unknown: frozenset(
        {EffectStatus.reconciled_confirmed, EffectStatus.reconciled_absent}
    ),
    EffectStatus.reconciled_confirmed: frozenset(),
    EffectStatus.reconciled_absent: frozenset(
        {EffectStatus.executing, EffectStatus.reconciled_confirmed}
    ),
    EffectStatus.failed: frozenset({EffectStatus.executing, EffectStatus.reconciled_confirmed}),
}

# Statuses from which a fresh execution attempt (a retry) may legally begin. ``unknown``
# is deliberately absent — retry is blocked until reconciliation proves absence (C4).
RETRYABLE_STATUSES: frozenset[EffectStatus] = frozenset(
    {EffectStatus.reserved, EffectStatus.failed, EffectStatus.reconciled_absent}
)

# Terminal statuses: no further transition is ever legal.
TERMINAL_STATUSES: frozenset[EffectStatus] = frozenset(
    {EffectStatus.confirmed, EffectStatus.reconciled_confirmed}
)


class EffectError(KeelError):
    """Base class for Effect ledger errors."""


class EffectTransitionError(EffectError):
    """An illegal status transition was attempted (fail closed, never coerced)."""

    def __init__(self, current: EffectStatus, target: EffectStatus) -> None:
        self.current = current
        self.target = target
        super().__init__(f"illegal effect transition: {current.value} -> {target.value}")


class EffectConflictError(EffectError):
    """A duplicate logical send reused an idempotency key for a different exact action.

    The Effect's identity is ``(scope_id, provider, action_name, idempotency_key)``; its
    ``action_hash`` is bound immutably at reservation (C5). A caller that resolves the
    same identity with a *different* ``action_hash`` is either a bug or an attempted
    cross-action replay — reject rather than silently reusing the first action's Effect.
    """


class EffectNotFoundError(EffectError):
    """No Effect exists for the given id."""


def validate_transition(current: EffectStatus, target: EffectStatus) -> None:
    """Raise :class:`EffectTransitionError` unless ``current -> target`` is legal."""
    if target not in _TRANSITIONS.get(current, frozenset()):
        raise EffectTransitionError(current, target)


def is_retryable(status: EffectStatus) -> bool:
    """Whether a fresh execution attempt may legally begin from ``status``."""
    return status in RETRYABLE_STATUSES


def is_terminal(status: EffectStatus) -> bool:
    """Whether ``status`` accepts no further transition."""
    return status in TERMINAL_STATUSES


_DIGEST_PREFIX = "sha256:"


def _contains_secret_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if isinstance(key, str) and key.strip().lower() in _SECRET_ARG_KEYS:
                return True
            if _contains_secret_key(nested):
                return True
        return False
    if isinstance(value, (list, tuple)):
        return any(_contains_secret_key(item) for item in value)
    return False


def canonical_args_or_digest(args: Mapping[str, Any]) -> str:
    """The immutable canonical-args column value for a new Effect.

    Canonical JSON (sorted keys) when ``args`` carries no secret-shaped key anywhere in
    its (possibly nested) structure, so an operator/reconciliation read can see exactly
    what was requested. When a secret-shaped key is present the raw value is *never*
    persisted — a stable ``sha256:<hex>`` digest is stored instead (still enough to prove
    two attempts requested the identical payload without ever writing the secret out).
    """
    canonical = json.dumps(args, sort_keys=True, separators=(",", ":"), default=str)
    if _contains_secret_key(args):
        return _DIGEST_PREFIX + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return canonical


def is_digest(value: str) -> bool:
    """Whether a canonical-args column value is a safe digest rather than raw JSON."""
    return value.startswith(_DIGEST_PREFIX)


_ID_RE = re.compile(r'"(?:id|message_id|event_id)"\s*:\s*"([^"]+)"')
_KV_RE = re.compile(r"\bid=([^\s)]+)")


def default_provider_ref(output: str) -> str:
    """Best-effort, generic ``provider_ref`` extraction from a connector action's output.

    Providers are free to structure their action output (Google Calendar already returns
    normalized JSON with an ``id`` field); this generic default recognizes a JSON ``id``/
    ``message_id``/``event_id`` key or a ``key=value`` style ``id=...`` token (Gmail's
    ``"sent (id=<id>)"``) and otherwise returns an empty string rather than guessing.
    """
    match = _ID_RE.search(output)
    if match:
        return match.group(1)
    match = _KV_RE.search(output)
    if match:
        return match.group(1)
    return ""


@dataclass(frozen=True, slots=True)
class EffectRecord:
    """An immutable snapshot of one durable Effect row."""

    id: str
    scope_id: str
    org_id: str
    agent_id: str
    actor_id: str
    run_id: str
    tool_name: str
    provider: str
    resource_id: str
    action_name: str
    action_hash: str
    idempotency_key: str
    canonical_args: str
    status: EffectStatus
    attempt: int
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: datetime | None
    provider_ref: str
    result: str
    error: str
    reconciliation_attempts: int
    next_reconciliation_at: datetime | None
    reconciled_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def args_is_digest(self) -> bool:
        return is_digest(self.canonical_args)


__all__ = [
    "EffectConflictError",
    "EffectError",
    "EffectNotFoundError",
    "EffectRecord",
    "EffectStatus",
    "EffectTransitionError",
    "RETRYABLE_STATUSES",
    "TERMINAL_STATUSES",
    "canonical_args_or_digest",
    "default_provider_ref",
    "is_digest",
    "is_retryable",
    "is_terminal",
    "validate_transition",
]
