"""Spike S5 acceptance (application layer): per-scope isolation via ScopeGuard."""

from __future__ import annotations

import pytest

from keel_core.errors import CrossScopeError
from keel_core.protocols import ScopeGuard
from keel_core.scope import DefaultScopeGuard


def test_same_scope_allowed() -> None:
    DefaultScopeGuard().enforce("u:1", "u:1")  # does not raise


def test_cross_scope_denied_and_audited() -> None:
    audited: list[tuple[str, str, str]] = []
    guard = DefaultScopeGuard(
        audit=lambda reason, actor, resource: audited.append((reason, actor, resource))
    )
    with pytest.raises(CrossScopeError) as excinfo:
        guard.enforce("group:1", "u:1")
    assert excinfo.value.actor_scope == "group:1"
    assert excinfo.value.resource_scope == "u:1"
    assert audited == [("cross_scope_denied", "group:1", "u:1")]


def test_system_scope_may_cross() -> None:
    DefaultScopeGuard().enforce("system", "u:1")  # trusted super-scope


def test_conforms_to_scope_guard_protocol() -> None:
    guard: ScopeGuard = DefaultScopeGuard()
    guard.enforce("a", "a")
