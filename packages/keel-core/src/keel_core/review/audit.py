"""Structured audit for the read-only review lifecycle (WS-R).

Mirrors :mod:`keel_core.identity.audit` / :mod:`keel_core.projects.audit`: a small enum of
actions and a logging sink that refuses to emit sensitive detail keys (tokens, prompts,
secrets). Audit records request/start/complete/fail so a review is observable end to end.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

logger = logging.getLogger("keel.review.audit")

_FORBIDDEN_DETAIL_KEYS = frozenset(
    {"token", "jwt", "api_key", "secret", "instruction", "prompt", "snippet", "diff"}
)


class ReviewAuditAction(StrEnum):
    review_requested = "review.requested"
    review_started = "review.started"
    review_completed = "review.completed"
    review_failed = "review.failed"


@dataclass(frozen=True)
class ReviewAuditEvent:
    action: ReviewAuditAction
    actor: str | None
    org_id: str | None
    review_id: str | None
    details: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        forbidden = _FORBIDDEN_DETAIL_KEYS & set(self.details)
        if forbidden:
            raise ValueError(f"review audit details must not include: {sorted(forbidden)}")


class ReviewAuditSink(Protocol):
    def record(self, event: ReviewAuditEvent) -> None: ...


class LoggingReviewAuditSink:
    """Best-effort structured audit to the application log."""

    def record(self, event: ReviewAuditEvent) -> None:
        logger.info(
            "review.audit action=%s actor=%s org=%s review=%s details=%s",
            event.action.value,
            event.actor,
            event.org_id,
            event.review_id,
            dict(event.details),
        )


__all__ = [
    "LoggingReviewAuditSink",
    "ReviewAuditAction",
    "ReviewAuditEvent",
    "ReviewAuditSink",
]
