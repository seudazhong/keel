"""Unit tests for the immutable Agent configuration snapshot value object (R1B).

Covers the properties the rest of the admission/fingerprint/worker-reconstruction machinery
depends on: canonical JSON stability, hash validation (tamper/corruption detection), additive
schema-version tolerance, and the tool-restriction "never expand authority" invariant.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.agent_config_snapshot import (
    AgentConfigSnapshot,
    AgentConfigSnapshotError,
    MemoryPolicySnapshot,
    ResourceGrantSnapshot,
)
from keel_core.connector_contracts import (
    ConnectorAction,
    ConnectorActionManifest,
    ConnectorActionSemantics,
)
from keel_core.interactive import InteractiveCapabilities, build_interactive_tool_names
from keel_core.protocols import ToolContext


def test_canonical_json_is_stable_regardless_of_construction_order() -> None:
    a = AgentConfigSnapshot(agent_id="a1", agent_name="Scout", model="gpt-5", tools=("b", "a"))
    b = AgentConfigSnapshot(agent_id="a1", model="gpt-5", agent_name="Scout", tools=("a", "b"))
    assert a.canonical_json() == b.canonical_json()
    assert a.content_hash() == b.content_hash()


def test_tools_and_grants_are_normalized_sorted_and_deduplicated() -> None:
    snapshot = AgentConfigSnapshot(
        tools=("write", "read", "read"),
        resource_grants=(
            ResourceGrantSnapshot("kb", "1", "read"),
            ResourceGrantSnapshot("kb", "1", "read"),
            ResourceGrantSnapshot("kb", "0", "read"),
        ),
    )
    assert snapshot.tools == ("read", "write")
    assert snapshot.resource_grants == (
        ResourceGrantSnapshot("kb", "0", "read"),
        ResourceGrantSnapshot("kb", "1", "read"),
    )


def test_a_different_field_changes_the_hash() -> None:
    base = AgentConfigSnapshot(agent_id="a1", persona="be nice", model="gpt-5")
    changed_persona = AgentConfigSnapshot(agent_id="a1", persona="be mean", model="gpt-5")
    changed_model = AgentConfigSnapshot(agent_id="a1", persona="be nice", model="gpt-4")
    assert base.content_hash() != changed_persona.content_hash()
    assert base.content_hash() != changed_model.content_hash()


def test_round_trip_through_canonical_json() -> None:
    snapshot = AgentConfigSnapshot(
        agent_id="a1",
        agent_version=4,
        agent_name="Scout",
        persona="Be terse.",
        model="gpt-5",
        max_iterations=12,
        token_budget=1000,
        permission_profile="default",
        tools=("read", "write"),
        memory_policy=MemoryPolicySnapshot(archival_enabled=True, memory_block_max_chars=4000),
        resource_grants=(ResourceGrantSnapshot("knowledge_base", "kb-1", "read"),),
    )
    restored = AgentConfigSnapshot.from_canonical_json(
        snapshot.canonical_json(), expected_hash=snapshot.content_hash()
    )
    assert restored == snapshot


def test_from_canonical_json_defaults_absent_fields_for_schema_evolution() -> None:
    """An older/minimal payload (as if from an earlier additive schema version) decodes with
    safe defaults rather than raising — additive fields must never break existing rows."""
    restored = AgentConfigSnapshot.from_canonical_json('{"agent_id": "a1"}')
    assert restored.agent_id == "a1"
    assert restored.agent_version == 0
    assert restored.persona == ""
    assert restored.max_iterations == 40
    assert restored.token_budget is None
    assert restored.memory_policy == MemoryPolicySnapshot()
    assert restored.resource_grants == ()


def test_from_canonical_json_rejects_a_hash_mismatch() -> None:
    """Tamper/corruption detection: a stored hash that no longer matches the content fails
    closed rather than silently trusting unverifiable persisted data."""
    snapshot = AgentConfigSnapshot(agent_id="a1", persona="original")
    tampered_json = snapshot.canonical_json().replace("original", "tampered!")
    with pytest.raises(AgentConfigSnapshotError):
        AgentConfigSnapshot.from_canonical_json(
            tampered_json, expected_hash=snapshot.content_hash()
        )


def test_from_canonical_json_rejects_malformed_payloads() -> None:
    with pytest.raises(AgentConfigSnapshotError):
        AgentConfigSnapshot.from_canonical_json("not json")
    with pytest.raises(AgentConfigSnapshotError):
        AgentConfigSnapshot.from_canonical_json("[1, 2, 3]")


def test_restrict_tools_never_expands_authority() -> None:
    """The intersection of admitted vs. currently-available tools: a tool the snapshot admitted
    but that no longer exists is dropped; a tool that exists now but was never admitted is never
    added (authority can only shrink after admission, never expand)."""
    snapshot = AgentConfigSnapshot(tools=("read", "write", "removed_connector"))
    # "removed_connector" no longer exists; a brand-new "shell" tool exists now but was never
    # admitted for this run.
    restricted = snapshot.restrict_tools(("read", "write", "shell"))
    assert restricted == ("read", "write")


async def _connector_read(_args: dict[str, Any], _ctx: ToolContext) -> str:
    return "ok"


def test_admission_tool_names_include_durable_memory_and_connector_tools() -> None:
    connector = ConnectorAction(
        ConnectorActionManifest(
            name="mail_read",
            description="Read mail.",
            input_schema={"type": "object"},
            semantics=ConnectorActionSemantics.read,
        ),
        _connector_read,
    )
    names = build_interactive_tool_names(
        engine=cast(AsyncEngine, object()),
        scope_id="agent:org/agent",
        embedder=None,
        caps=InteractiveCapabilities(),
        connector_actions=(connector,),
    )
    assert {
        "read",
        "write",
        "memory_append",
        "memory_replace",
        "memory_rethink",
        "session_search",
        "mail_read",
    } <= set(names)
    snapshot = AgentConfigSnapshot(tools=names)
    assert "session_search" in snapshot.restrict_tools((*names, "new_unadmitted_tool"))


def test_memory_policy_round_trips() -> None:
    policy = MemoryPolicySnapshot(
        core_memory_enabled=False, archival_enabled=True, memory_block_max_chars=1234
    )
    assert MemoryPolicySnapshot.from_dict(policy.to_dict()) == policy
    assert MemoryPolicySnapshot.from_dict(None) == MemoryPolicySnapshot()
