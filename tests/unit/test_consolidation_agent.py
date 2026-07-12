"""Unit tests for the consolidation agent glue (identity, prompt, permissions)."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import create_async_engine

from keel_core.config import Settings
from keel_core.consolidation.agent import (
    CONSOLIDATION_SYSTEM_INSTRUCTION,
    MEMORY_CONSOLIDATOR_AGENT_ID,
    build_consolidation_agent,
    consolidation_permissions,
    consolidation_registry,
    consolidation_schedule_id,
    consolidation_session_id,
    consolidation_system_context,
    format_consolidation_prompt,
)
from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.reader import ConsolidationMessage
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ToolContext
from keel_core.types import PermissionDecision, ScopeKind, TrustLevel

_DUMMY_URL = "postgresql+asyncpg://localhost:5432/keel"


def test_agent_identity_and_toolset() -> None:
    agent = build_consolidation_agent("web:local", "gpt-4o-mini", token_budget=4000)
    assert agent.id == MEMORY_CONSOLIDATOR_AGENT_ID
    assert agent.model == "gpt-4o-mini"
    assert agent.scope.kind is ScopeKind.system
    assert agent.scope.trust is TrustLevel.trusted
    assert agent.max_iterations == 8
    assert agent.token_budget == 4000
    assert agent.toolset == ["memory_propose_rewrite", "archival_consolidate_insert"]


def test_permissions_allow_only_the_two_tools() -> None:
    ctx = ToolContext(scope_id="web:local", session_id="s")
    perms = consolidation_permissions()
    assert perms.evaluate("memory_propose_rewrite", {}, ctx) is PermissionDecision.allow
    assert perms.evaluate("archival_consolidate_insert", {}, ctx) is PermissionDecision.allow
    assert perms.evaluate("memory_append", {}, ctx) is PermissionDecision.deny


def test_registry_advertises_exactly_two_tools() -> None:
    engine = create_async_engine(_DUMMY_URL)
    run_context = ConsolidationRunContext(
        allowed_event_ids=frozenset(), allowed_user_event_ids=frozenset()
    )
    registry = consolidation_registry(engine, FakeEmbedder(), run_context, Settings())
    names = {schema["function"]["name"] for schema in registry.schemas()}
    assert names == {"memory_propose_rewrite", "archival_consolidate_insert"}


async def test_system_context_returns_instruction() -> None:
    assert await consolidation_system_context() == CONSOLIDATION_SYSTEM_INSTRUCTION


def test_prompt_includes_versions_and_events() -> None:
    prompt = format_consolidation_prompt(
        {"persona": "helpful", "human": ""},
        {"persona": 3},
        [
            ConsolidationMessage(
                event_id=7, session_id="chat:a", role="user", content="I love tea"
            ),
            ConsolidationMessage(
                event_id=8, session_id="chat:a", role="assistant", content="Noted"
            ),
        ],
    )
    assert "persona (version 3)" in prompt
    assert "human (version 0)" in prompt
    assert "7 [user] I love tea" in prompt
    assert "8 [assistant] Noted" in prompt


def test_session_id_matches_exclusion_prefix() -> None:
    assert consolidation_session_id("web:local", "abc") == "consolidation:web:local:abc"
    assert consolidation_session_id("web:local", "abc").startswith("consolidation:")


def test_schedule_id_is_stable() -> None:
    assert consolidation_schedule_id("web:local") == "memory-consolidation:web:local"
