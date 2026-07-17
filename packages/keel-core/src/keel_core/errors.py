"""Contract exceptions.

Security-relevant errors (permission, cross-scope) fail closed (P5) and are
audited by their enforcement component in M1.
"""

from __future__ import annotations

from .types import ScopeId


class KeelError(Exception):
    """Base class for all Keel domain errors."""


class PermissionDenied(KeelError):
    """A tool/resource action was denied by the permission engine (fail closed)."""


class MaintenanceDatabaseNotConfigured(KeelError):
    """Identity erasure was requested without a configured maintenance connection.

    Identity erasure runs through a dedicated least-privilege maintenance login (a member of
    only the ``keel_maintenance_exec`` executor role). When ``KEEL_MAINTENANCE_DATABASE_URL``
    is unset — or, in cloud mode, is a copy of the runtime URL — erasure fails closed with
    this error rather than falling back to the runtime connection. Never carries the URL.
    """


class DuplicateEventError(KeelError):
    """A durably-unique event append lost the race to a concurrent/duplicate writer.

    Raised when appending an event whose uniqueness marker (an admission or steering
    ``dedup_key``) is already present — the DB partial-unique index (or the in-memory
    double) rejects the second insert. The caller treats this as an idempotent no-op:
    the winning writer already persisted the durable turn, so a retry/concurrent admitter
    or a reclaiming steering watcher must **observe** rather than append a duplicate
    message (M3.6 concurrent-admission + steering-idempotency invariants).
    """

    def __init__(self, dedup_key: str) -> None:
        self.dedup_key = dedup_key
        super().__init__(f"duplicate durable event for dedup_key={dedup_key!r}")


class CrossScopeError(KeelError):
    """A cross-scope access was attempted and denied (ADR-0009 / DESIGN-REVIEW G16).

    Raised by the ScopeGuard seam when an actor scope tries to reach another
    scope's data. Always audited.
    """

    def __init__(self, actor_scope: ScopeId, resource_scope: ScopeId) -> None:
        self.actor_scope = actor_scope
        self.resource_scope = resource_scope
        super().__init__(
            f"cross-scope access denied: actor={actor_scope!r} resource={resource_scope!r}"
        )
