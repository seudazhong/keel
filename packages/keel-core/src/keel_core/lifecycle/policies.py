"""Typed retention policies and classes (M3.5, WS-K).

Retention is expressed as a small set of *classes* (transient .. permanent), each with a
default TTL. Every persisted resource kind is assigned a default :class:`RetentionPolicy`
in :data:`DEFAULT_RETENTION`; a durable per-scope override (``retention_policies`` table,
see :mod:`keel_core.lifecycle.store`) layers on top. The retention scheduler uses
:func:`retention_expires_at` / :func:`is_expired` to decide when an expiring resource
(ephemeral OAuth state, webhook dedup rows, resolved approvals/jobs) is due for cleanup.

User content (sessions, events, memory, archival, knowledge, connector tokens) is
``permanent`` by default: it is retained until an explicit erasure request removes it,
never on a timer. Only derived or bounded operational data carries a finite TTL.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

_DAY = 86_400


class RetentionClass(StrEnum):
    """Named retention tiers with a default horizon (see :data:`CLASS_DEFAULT_TTL`)."""

    transient = "transient"  # minutes–hour: single-use CSRF state, in-flight claims
    short = "short"  # ~1 day: replay dedup windows
    standard = "standard"  # ~30 days: resolved approvals, finished jobs
    long = "long"  # ~1 year: audit trail (erasure ledger)
    permanent = "permanent"  # kept until explicit erasure (user content)


CLASS_DEFAULT_TTL: Mapping[RetentionClass, int | None] = {
    RetentionClass.transient: _DAY // 24,  # 1 hour
    RetentionClass.short: _DAY,  # 1 day
    RetentionClass.standard: 30 * _DAY,  # 30 days
    RetentionClass.long: 365 * _DAY,  # 1 year
    RetentionClass.permanent: None,  # never expires on a timer
}


@dataclass(frozen=True)
class RetentionPolicy:
    """A resource class's retention decision: its tier and effective TTL in seconds.

    ``ttl_seconds is None`` means *permanent* — the resource is only removed by an explicit
    erasure, never by the retention sweeper.
    """

    resource_class: str
    retention_class: RetentionClass
    ttl_seconds: int | None

    @property
    def is_permanent(self) -> bool:
        return self.ttl_seconds is None


def _policy(resource_class: str, retention_class: RetentionClass) -> RetentionPolicy:
    """A policy whose TTL is the class default (see :data:`CLASS_DEFAULT_TTL`)."""
    return RetentionPolicy(resource_class, retention_class, CLASS_DEFAULT_TTL[retention_class])


# defaults, and override table all speak the same vocabulary.
SESSION = "session"
EVENT = "event"
MESSAGE_EMBEDDING = "message_embedding"
ARCHIVAL = "archival"
MEMORY_BLOCK = "memory_block"
MEMORY_PROPOSAL = "memory_proposal"
CONSOLIDATION_CURSOR = "consolidation_cursor"
KNOWLEDGE = "knowledge"
CONNECTOR_TOKEN = "connector_token"
CONNECTOR_STATE = "connector_state"
CONNECTOR_DELIVERY = "connector_delivery"
CONNECTOR_OUTBOX = "connector_outbox"
OAUTH_STATE = "oauth_state"
WEBHOOK_DELIVERY = "webhook_delivery"
SCHEDULE = "schedule"
APPROVAL = "approval"
JOB = "job"
RUN = "run"
CODING_ARTIFACT = "coding_artifact"
TOOL_SPILL = "tool_spill"
ERASURE_LEDGER = "erasure_ledger"
# Durable identity (M3.6): users/orgs/memberships/Agents/grants/OIDC links. User content —
# permanent, removed only by an explicit organization- or user-erasure (never on a timer).
IDENTITY = "identity"


DEFAULT_RETENTION: Mapping[str, RetentionPolicy] = {
    SESSION: _policy(SESSION, RetentionClass.permanent),
    EVENT: _policy(EVENT, RetentionClass.permanent),
    MESSAGE_EMBEDDING: _policy(MESSAGE_EMBEDDING, RetentionClass.permanent),
    ARCHIVAL: _policy(ARCHIVAL, RetentionClass.permanent),
    MEMORY_BLOCK: _policy(MEMORY_BLOCK, RetentionClass.permanent),
    MEMORY_PROPOSAL: _policy(MEMORY_PROPOSAL, RetentionClass.long),
    CONSOLIDATION_CURSOR: _policy(CONSOLIDATION_CURSOR, RetentionClass.permanent),
    KNOWLEDGE: _policy(KNOWLEDGE, RetentionClass.permanent),
    CONNECTOR_TOKEN: _policy(CONNECTOR_TOKEN, RetentionClass.permanent),
    CONNECTOR_STATE: _policy(CONNECTOR_STATE, RetentionClass.permanent),
    CONNECTOR_DELIVERY: _policy(CONNECTOR_DELIVERY, RetentionClass.short),
    CONNECTOR_OUTBOX: _policy(CONNECTOR_OUTBOX, RetentionClass.short),
    OAUTH_STATE: _policy(OAUTH_STATE, RetentionClass.transient),
    WEBHOOK_DELIVERY: _policy(WEBHOOK_DELIVERY, RetentionClass.short),
    SCHEDULE: _policy(SCHEDULE, RetentionClass.permanent),
    APPROVAL: _policy(APPROVAL, RetentionClass.standard),
    JOB: _policy(JOB, RetentionClass.standard),
    RUN: _policy(RUN, RetentionClass.standard),
    CODING_ARTIFACT: _policy(CODING_ARTIFACT, RetentionClass.standard),
    TOOL_SPILL: _policy(TOOL_SPILL, RetentionClass.short),
    ERASURE_LEDGER: _policy(ERASURE_LEDGER, RetentionClass.long),
    IDENTITY: _policy(IDENTITY, RetentionClass.permanent),
}
"""The typed retention defaults for every persisted resource class."""


def resolve_policy(
    resource_class: str,
    *,
    overrides: Mapping[str, RetentionPolicy] | None = None,
) -> RetentionPolicy:
    """Resolve the effective policy for a resource class (override wins over default).

    Raises :class:`KeyError` for an unknown resource class so a typo can't silently pick a
    permissive default.
    """
    if overrides is not None and resource_class in overrides:
        return overrides[resource_class]
    return DEFAULT_RETENTION[resource_class]


def retention_expires_at(created_at: datetime, policy: RetentionPolicy) -> datetime | None:
    """The instant a resource created at ``created_at`` becomes eligible for cleanup.

    ``None`` for a permanent policy (never expires on a timer).
    """
    if policy.ttl_seconds is None:
        return None
    return created_at + timedelta(seconds=policy.ttl_seconds)


def is_expired(created_at: datetime, policy: RetentionPolicy, now: datetime) -> bool:
    """Whether a resource created at ``created_at`` has passed its retention horizon."""
    expires = retention_expires_at(created_at, policy)
    return expires is not None and expires <= now
