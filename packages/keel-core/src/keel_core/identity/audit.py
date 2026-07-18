"""Audit records for identity mutations (M3.6, WS-L).

Membership, Agent, and grant changes emit a structured, non-sensitive audit record. The
records deliberately carry only *metadata* (ids, roles, capabilities, names) — never a
credential, raw JWT, API key, or an Agent's persona/instruction text — so the audit trail
is safe to ship to a log pipeline.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

logger = logging.getLogger("keel.identity.audit")

# Keys whose values must never appear in an audit record (defense-in-depth against a
# caller passing sensitive detail through ``details``).
_FORBIDDEN_DETAIL_KEYS = frozenset(
    {"persona", "password", "token", "jwt", "api_key", "secret", "instruction", "prompt"}
)


class AuditAction(StrEnum):
    org_created = "org.created"
    org_archived = "org.archived"
    member_added = "member.added"
    member_role_changed = "member.role_changed"
    member_removed = "member.removed"
    agent_created = "agent.created"
    agent_updated = "agent.updated"
    agent_archived = "agent.archived"
    grant_created = "grant.created"
    grant_revoked = "grant.revoked"
    user_provisioned = "user.provisioned"
    user_erased = "user.erased"
    im_route_claimed = "im.route_claimed"
    im_route_released = "im.route_released"


@dataclass(frozen=True)
class AuditEvent:
    """One immutable, non-sensitive audit record."""

    action: AuditAction
    actor_user_id: str | None
    org_id: str | None
    target_id: str | None
    details: Mapping[str, str] = field(default_factory=dict)
    at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        forbidden = _FORBIDDEN_DETAIL_KEYS & set(self.details)
        if forbidden:
            raise ValueError(f"audit details must not contain sensitive keys: {sorted(forbidden)}")


@runtime_checkable
class AuditSink(Protocol):
    """Where audit records are delivered."""

    def record(self, event: AuditEvent) -> None: ...


class LoggingAuditSink:
    """Emit audit records to the structured logger (default sink)."""

    def record(self, event: AuditEvent) -> None:
        logger.info(
            "identity.audit action=%s actor=%s org=%s target=%s details=%s",
            event.action.value,
            event.actor_user_id,
            event.org_id,
            event.target_id,
            dict(event.details),
        )


class InMemoryAuditSink:
    """Collect audit records in a list (tests)."""

    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    def record(self, event: AuditEvent) -> None:
        self.events.append(event)


__all__ = [
    "AuditAction",
    "AuditEvent",
    "AuditSink",
    "InMemoryAuditSink",
    "LoggingAuditSink",
]
