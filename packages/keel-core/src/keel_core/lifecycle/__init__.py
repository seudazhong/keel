"""Data lifecycle: typed retention policies + durable, idempotent erasure (M3.5, WS-K).

This package implements retention and erasure on top of the event-sourced core:

* :mod:`policies` — typed retention classes/policies, defaults, and expiry math.
* :mod:`datamap` — the authoritative, documented map of every persisted store, its
  retention class, and how scope/session/project erasure treats it.
* :mod:`models` — value types for erasure requests, steps, and results.
* :mod:`store` — durable erasure request/step ledger + session tombstones + retention
  overrides (in-memory + Postgres).
* :mod:`tombstones` — event tombstone semantics and the projection-rebuild hook that
  guarantees erased content can never be resurrected.
* :mod:`redis` — bounded Redis event-stream / pubsub key cleanup.
* :mod:`coding` — bounded local coding-artifact + tool-spill file cleanup.
* :mod:`purge` — the scoped purge repository sequencing every store's ``purge_scope``.
* :mod:`coordinator` — the restart-safe, idempotent, resumable erasure state machine.
* :mod:`jobs` — durable-job handler wiring for the worker.
"""

from __future__ import annotations

from keel_core.lifecycle.coordinator import (
    ErasureCoordinator,
    ExternalDeletionStep,
    ExternalStepOutcome,
    UnsupportedExternalStep,
)
from keel_core.lifecycle.datamap import DATA_MAP, DataMapEntry, ErasureTreatment
from keel_core.lifecycle.models import (
    ErasureRequest,
    ErasureResult,
    ErasureStatus,
    ErasureStep,
    ErasureTarget,
    ErasureTargetKind,
    StepStatus,
)
from keel_core.lifecycle.policies import (
    DEFAULT_RETENTION,
    RetentionClass,
    RetentionPolicy,
    is_expired,
    resolve_policy,
    retention_expires_at,
)
from keel_core.lifecycle.purge import ScopePurgeRepository
from keel_core.lifecycle.redis import RedisLifecycleCleaner
from keel_core.lifecycle.retention import (
    RetentionCandidate,
    next_expiry,
    select_expired,
)
from keel_core.lifecycle.store import (
    ErasureStore,
    InMemoryErasureStore,
    PostgresErasureStore,
)
from keel_core.lifecycle.tombstones import (
    SessionTombstoneSet,
    load_tombstone_hook,
    make_tombstone_hook,
)

__all__ = [
    "DATA_MAP",
    "DEFAULT_RETENTION",
    "DataMapEntry",
    "ErasureCoordinator",
    "ErasureRequest",
    "ErasureResult",
    "ErasureStatus",
    "ErasureStep",
    "ErasureStore",
    "ErasureTarget",
    "ErasureTargetKind",
    "ErasureTreatment",
    "ExternalDeletionStep",
    "ExternalStepOutcome",
    "InMemoryErasureStore",
    "PostgresErasureStore",
    "RedisLifecycleCleaner",
    "RetentionCandidate",
    "RetentionClass",
    "RetentionPolicy",
    "ScopePurgeRepository",
    "SessionTombstoneSet",
    "StepStatus",
    "UnsupportedExternalStep",
    "is_expired",
    "load_tombstone_hook",
    "make_tombstone_hook",
    "next_expiry",
    "resolve_policy",
    "retention_expires_at",
    "select_expired",
]
