"""Retention scheduling primitives (M3.5, WS-K).

Given a set of retention *candidates* (a resource's class + creation time), decide which
have passed their retention horizon and are due for cleanup. This is the small, pure core
a retention sweeper/scheduler is built on: the sweeper enumerates expiring rows (ephemeral
OAuth state, webhook dedup, resolved approvals/jobs), asks :func:`select_expired` which are
due, and erases them. Permanent-class resources never appear as due.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime

from keel_core.lifecycle.policies import (
    RetentionPolicy,
    is_expired,
    resolve_policy,
    retention_expires_at,
)


@dataclass(frozen=True)
class RetentionCandidate:
    """A resource that may be due for retention cleanup."""

    resource_class: str
    resource_id: str
    created_at: datetime


def select_expired(
    candidates: Iterable[RetentionCandidate],
    now: datetime,
    *,
    overrides: Mapping[str, RetentionPolicy] | None = None,
) -> list[RetentionCandidate]:
    """Return the candidates whose retention horizon has passed at ``now``."""
    due: list[RetentionCandidate] = []
    for candidate in candidates:
        policy = resolve_policy(candidate.resource_class, overrides=overrides)
        if is_expired(candidate.created_at, policy, now):
            due.append(candidate)
    return due


def next_expiry(
    candidates: Iterable[RetentionCandidate],
    *,
    overrides: Mapping[str, RetentionPolicy] | None = None,
) -> datetime | None:
    """The earliest upcoming expiry across candidates (None if all permanent/empty)."""
    horizons: list[datetime] = []
    for candidate in candidates:
        policy = resolve_policy(candidate.resource_class, overrides=overrides)
        expires = retention_expires_at(candidate.created_at, policy)
        if expires is not None:
            horizons.append(expires)
    return min(horizons) if horizons else None


__all__ = ["RetentionCandidate", "next_expiry", "select_expired"]
