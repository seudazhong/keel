"""Value types for durable erasure requests, steps, and results (M3.5, WS-K)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum


class ErasureTargetKind(StrEnum):
    """The granularity of an erasure request."""

    scope = "scope"  # everything owned by a scope
    session = "session"  # one session's events + derived rows (scope-global data kept)
    project = "project"  # one coding project's on-disk artifacts


class ErasureStatus(StrEnum):
    """Lifecycle of an erasure request."""

    pending = "pending"
    running = "running"
    completed = "completed"  # every step done/skipped, no external gaps
    partial = "partial"  # data stores erased but an external step could not be verified
    failed = "failed"  # an internal step exhausted its retries


class StepStatus(StrEnum):
    """Outcome of one erasure step."""

    pending = "pending"
    done = "done"  # completed successfully
    skipped = "skipped"  # not applicable to this target (recorded, not an error)
    unsupported = "unsupported"  # external deletion not implemented — forces ``partial``
    failed = "failed"  # external deletion attempted and failed — forces ``partial``


# Step statuses that leave an external gap: the request can never report ``completed``.
INCOMPLETE_STEP_STATUSES = frozenset({StepStatus.unsupported, StepStatus.failed})


@dataclass(frozen=True)
class ErasureTarget:
    """What to erase: a scope, a session within a scope, or a coding project."""

    scope_id: str
    kind: ErasureTargetKind = ErasureTargetKind.scope
    resource_id: str | None = None

    def __post_init__(self) -> None:
        if not self.scope_id:
            raise ValueError("scope_id is required")
        if self.kind is ErasureTargetKind.scope and self.resource_id is not None:
            raise ValueError("scope erasure must not carry a resource_id")
        if self.kind is not ErasureTargetKind.scope and not self.resource_id:
            raise ValueError(f"{self.kind} erasure requires a resource_id")


@dataclass(frozen=True)
class ErasureRequest:
    """A durable erasure request row."""

    id: str
    scope_id: str
    target_kind: ErasureTargetKind
    target_id: str | None
    idempotency_key: str
    status: ErasureStatus = ErasureStatus.pending
    requested_by: str | None = None
    reason: str | None = None
    external_incomplete: bool = False
    attempts: int = 0
    last_error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def target(self) -> ErasureTarget:
        return ErasureTarget(self.scope_id, self.target_kind, self.target_id)


@dataclass(frozen=True)
class ErasureStep:
    """A recorded step in an erasure request's ledger."""

    request_id: str
    scope_id: str
    step: str
    status: StepStatus
    rows_affected: int = 0
    detail: str | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class ErasureResult:
    """The observable outcome of executing an erasure request."""

    request_id: str
    status: ErasureStatus
    external_incomplete: bool
    steps: tuple[ErasureStep, ...] = field(default_factory=tuple)

    @property
    def rows_affected(self) -> int:
        return sum(step.rows_affected for step in self.steps)
