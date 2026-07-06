"""Per-scope data-isolation guard — application layer (spike S5, ADR-0009 / G16).

Every scoped data access routes through :meth:`DefaultScopeGuard.enforce`. A
cross-scope attempt raises :class:`~keel_core.errors.CrossScopeError` and is
audited. This is the application-layer half of the isolation invariant; Postgres
RLS is the defense-in-depth half (see tests/integration/test_spike_s5_rls.py).
"""

from __future__ import annotations

from collections.abc import Callable

from keel_core.errors import CrossScopeError
from keel_core.types import ScopeId

# (reason, actor_scope, resource_scope)
AuditHook = Callable[[str, ScopeId, ScopeId], None]


class DefaultScopeGuard:
    """Deny-by-default cross-scope access, with an audit hook.

    The ``system`` scope is a trusted super-scope (platform internals) permitted
    to cross; every other actor may only reach its own scope.
    """

    def __init__(self, audit: AuditHook | None = None, system_scope: ScopeId = "system") -> None:
        self._audit = audit
        self._system_scope = system_scope

    def enforce(self, actor_scope: ScopeId, resource_scope: ScopeId) -> None:
        """Deny (and audit) if ``actor_scope`` may not access ``resource_scope``."""
        if actor_scope == resource_scope:
            return
        if actor_scope == self._system_scope:
            return
        if self._audit is not None:
            self._audit("cross_scope_denied", actor_scope, resource_scope)
        raise CrossScopeError(actor_scope, resource_scope)
