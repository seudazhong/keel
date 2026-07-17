"""Canonical data-plane scope derivation + validation (M3.6, org/Agent isolation)."""

from __future__ import annotations

import pytest

from keel_core.scoping import (
    LOCAL_PREVIEW_SCOPE,
    ScopeValidationError,
    derive_agent_scope,
    is_agent_scope,
    is_local_preview_scope,
    validate_scope_id,
    workspace_namespace,
)


def test_derive_agent_scope_is_canonical_and_distinct_per_org() -> None:
    assert derive_agent_scope("org_a", "agt_1") == "agent:org_a/agt_1"
    # Two orgs never collide even with the same agent id / session ids downstream.
    assert derive_agent_scope("org_a", "agt_1") != derive_agent_scope("org_b", "agt_1")
    assert is_agent_scope(derive_agent_scope("org_a", "agt_1"))
    assert not is_local_preview_scope(derive_agent_scope("org_a", "agt_1"))


def test_derive_agent_scope_never_reuses_local_preview() -> None:
    # The local-preview scope is distinct from every derived per-Agent scope.
    assert LOCAL_PREVIEW_SCOPE == "web:local"
    assert derive_agent_scope("local", "web") != LOCAL_PREVIEW_SCOPE
    assert is_local_preview_scope(LOCAL_PREVIEW_SCOPE)


@pytest.mark.parametrize(
    "org,agent",
    [
        ("", "agt"),
        ("org", ""),
        ("org/evil", "agt"),
        ("org", "../etc"),
        ("..", "agt"),
        ("org id", "agt"),
        ("org", "a\tb"),
        ("org:x", "agt"),
    ],
)
def test_derive_agent_scope_rejects_malformed_segments(org: str, agent: str) -> None:
    with pytest.raises(ScopeValidationError):
        derive_agent_scope(org, agent)


def test_validate_scope_id_accepts_local_and_derived_only() -> None:
    assert validate_scope_id(LOCAL_PREVIEW_SCOPE) == LOCAL_PREVIEW_SCOPE
    scope = derive_agent_scope("org_a", "agt_1")
    assert validate_scope_id(scope) == scope
    for bad in ["", "web", "agent:", "agent:org", "agent:org/", "agent:/agt", "random:scope"]:
        with pytest.raises(ScopeValidationError):
            validate_scope_id(bad)


def test_workspace_namespace_is_opaque_stable_and_distinct() -> None:
    a = workspace_namespace(derive_agent_scope("org_a", "agt_1"))
    b = workspace_namespace(derive_agent_scope("org_b", "agt_1"))
    assert a.startswith("ws_") and b.startswith("ws_")
    assert a != b  # distinct scopes -> distinct workspaces
    assert a == workspace_namespace(derive_agent_scope("org_a", "agt_1"))  # stable
    # No path separators / traversal in the namespace.
    assert "/" not in a and ".." not in a
    with pytest.raises(ScopeValidationError):
        workspace_namespace("agent:bad/../x")
