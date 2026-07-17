"""Shared interactive-run wiring: toolset, capabilities, permissions, and Agent build (M3.6).

Extracted so the **server** (local-preview in-process runtime) and the **worker**
(durable, worker-owned execution) build the *same* interactive toolset, permission policy,
memory/Knowledge capabilities, and :class:`AgentSpec` from **one** definition — there is
exactly one agent loop and one toolset/capability contract, differing only in where
execution runs. Read-only tools are allowed; mutating tools (write/edit/shell) require an
approval (fail-closed ``ask`` default), matching the server's web policy. Every file/shell
tool is built over a fail-closed :class:`ExecutionEnvironment`.

The durable worker reuses :func:`build_interactive_registry` /
:func:`build_interactive_agent` to reach **capability parity** with the server web runtime
(tools + memory + Knowledge), and :func:`build_interactive_agent` binds the *persisted*
Agent profile (id/name/persona/scope) so the worker-owned run runs as the selected Agent.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.agents import AgentSpec, Scope
from keel_core.embeddings import Embedder
from keel_core.knowledge import KnowledgeSearcher, KnowledgeSearchTool
from keel_core.memory import MemoryAppendTool, MemoryReplaceTool, MemoryRethinkTool
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import Tool
from keel_core.search import ArchivalInsertTool, ArchivalSearchTool, SessionSearchTool
from keel_core.tools import (
    EditTool,
    ExecutionEnvironment,
    GlobTool,
    GrepTool,
    LsTool,
    ReadTool,
    ShellTool,
    WriteTool,
)
from keel_core.types import PermissionDecision, ScopeId, ScopeKind, TrustLevel

READ_ONLY_TOOLS: tuple[str, ...] = ("read", "ls", "glob", "grep")
MUTATING_TOOLS: tuple[str, ...] = ("write", "edit", "shell")

# The stable local-preview compatibility identity for non-cloud single-operator use. A run
# admitted without a real authenticated user/org (open-mode preview) binds this explicit,
# never-blank org + Agent rather than the ambient data-plane scope — it is *only* reachable
# in non-cloud mode; a cloud request with no authorized org/Agent fails closed.
LOCAL_PREVIEW_ORG_ID = "local"
LOCAL_PREVIEW_AGENT_ID = "web"
LOCAL_PREVIEW_AGENT_NAME = "Keel Web"


@dataclass(frozen=True)
class InteractiveCapabilities:
    """Caps controlling the shared memory/Knowledge tool builders (server + worker parity)."""

    memory_block_max_chars: int = 2000
    session_embedding_batch_size: int = 64
    session_embedding_catchup_limit: int = 500
    knowledge_search_query_max_chars: int = 2_000
    knowledge_search_k_max: int = 10
    knowledge_tool_output_max_chars: int = 8_000


def build_interactive_tools(environment: ExecutionEnvironment) -> list[Tool]:
    """The full interactive file/shell toolset over a fail-closed ExecutionEnvironment."""
    return [
        ReadTool(environment),
        WriteTool(environment),
        EditTool(environment),
        LsTool(environment),
        GlobTool(environment),
        GrepTool(environment),
        ShellTool(environment),
    ]


def build_interactive_memory_tools(
    engine: AsyncEngine,
    embedder: Embedder | None,
    caps: InteractiveCapabilities,
) -> list[Tool]:
    """Core-memory editing, hybrid recall, and optional archival memory (durable engine)."""
    tools: list[Tool] = [
        MemoryAppendTool(engine, max_chars=caps.memory_block_max_chars),
        MemoryReplaceTool(engine, max_chars=caps.memory_block_max_chars),
        MemoryRethinkTool(engine, max_chars=caps.memory_block_max_chars),
        SessionSearchTool(
            engine,
            embedder,
            batch_size=caps.session_embedding_batch_size,
            catchup_limit=caps.session_embedding_catchup_limit,
        ),
    ]
    if embedder is not None:
        tools += [ArchivalInsertTool(engine, embedder), ArchivalSearchTool(engine, embedder)]
    return tools


def build_interactive_knowledge_tools(
    engine: AsyncEngine,
    scope_id: ScopeId,
    embedder: Embedder | None,
    caps: InteractiveCapabilities,
) -> list[Tool]:
    """The scope-bound Knowledge-base search tool (only when an embedder is configured)."""
    if embedder is None:
        return []
    return [
        KnowledgeSearchTool(
            KnowledgeSearcher(
                engine,
                scope_id,
                embedder,
                query_max_chars=caps.knowledge_search_query_max_chars,
                k_max=caps.knowledge_search_k_max,
            ),
            output_max_chars=caps.knowledge_tool_output_max_chars,
        )
    ]


def build_interactive_registry(
    environment: ExecutionEnvironment,
    *,
    engine: AsyncEngine | None,
    scope_id: ScopeId,
    embedder: Embedder | None,
    caps: InteractiveCapabilities | None = None,
) -> tuple[list[Tool], tuple[str, ...]]:
    """Build the shared interactive tool list + the extra (memory/Knowledge) tool names.

    Returns ``(tools, extra_names)`` where ``tools`` is the full registry (file/shell +
    memory + Knowledge) and ``extra_names`` are the memory/Knowledge tool names to allow
    read-only in :func:`interactive_permissions` and add to the Agent's ``toolset``. When no
    durable ``engine`` is configured (in-memory preview) only the file/shell tools are built,
    matching the server's behaviour so the two surfaces stay in lock-step.
    """
    caps = caps or InteractiveCapabilities()
    tools = build_interactive_tools(environment)
    extra: list[Tool] = []
    if engine is not None:
        extra += build_interactive_memory_tools(engine, embedder, caps)
        extra += build_interactive_knowledge_tools(engine, scope_id, embedder, caps)
    tools += extra
    return tools, tuple(tool.name for tool in extra)


def interactive_permissions(
    read_only_allow: tuple[str, ...] = (),
) -> RuleBasedPermissionEngine:
    """Read-only + own-scope tools allowed; mutating tools require an approval (ask)."""
    rules = [Rule(name, PermissionDecision.allow) for name in READ_ONLY_TOOLS]
    rules += [Rule(name, PermissionDecision.allow) for name in read_only_allow]
    rules += [Rule(name, PermissionDecision.ask) for name in MUTATING_TOOLS]
    return RuleBasedPermissionEngine(rules, default=PermissionDecision.ask)


def build_interactive_agent(
    *,
    scope_id: ScopeId,
    model: str,
    agent_id: str = LOCAL_PREVIEW_AGENT_ID,
    name: str = LOCAL_PREVIEW_AGENT_NAME,
    persona: str = "",
    extra_tool_names: tuple[str, ...] = (),
    max_iterations: int = 40,
    token_budget: int | None = None,
) -> AgentSpec:
    """Build the interactive :class:`AgentSpec` (shared by server preview + durable worker).

    ``agent_id`` / ``name`` / ``persona`` come from the *persisted* selected Agent profile so
    a durable worker-owned run executes as the selected Agent (parity with the server web
    runtime, which uses the local-preview profile). The scope is trusted (the interactive web
    surface), and the toolset is the file/shell tools plus any memory/Knowledge tool names.
    """
    scope = Scope(id=scope_id, kind=ScopeKind.personal, trust=TrustLevel.trusted)
    return AgentSpec(
        id=agent_id,
        name=name,
        model=model,
        scope=scope,
        persona=persona,
        toolset=list(READ_ONLY_TOOLS + MUTATING_TOOLS) + list(extra_tool_names),
        max_iterations=max_iterations,
        token_budget=token_budget,
    )


__all__ = [
    "LOCAL_PREVIEW_AGENT_ID",
    "LOCAL_PREVIEW_AGENT_NAME",
    "LOCAL_PREVIEW_ORG_ID",
    "MUTATING_TOOLS",
    "READ_ONLY_TOOLS",
    "InteractiveCapabilities",
    "build_interactive_agent",
    "build_interactive_knowledge_tools",
    "build_interactive_memory_tools",
    "build_interactive_registry",
    "build_interactive_tools",
    "interactive_permissions",
]
