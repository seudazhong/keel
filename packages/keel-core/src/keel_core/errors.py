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
