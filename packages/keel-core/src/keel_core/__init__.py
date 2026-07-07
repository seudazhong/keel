"""Keel agent runtime core.

M0 ships the frozen **contracts** (Protocols, event vocabulary, REST DTOs,
scope model) and shared infrastructure; behaviour lands in M1
(see docs/IMPLEMENTATION-PLAN.md).
"""

from __future__ import annotations

__version__ = "0.0.0"

from .agents import AgentSpec, Scope
from .connectors import ConfusedDeputyEngine, Connector, ConnectorTool, taint_from_events
from .embeddings import Embedder, FakeEmbedder, LiteLLMEmbedder, rrf_fuse
from .errors import CrossScopeError, KeelError, PermissionDenied
from .events import Event, EventType
from .extensibility import (
    CatalogEntry,
    ImportGuard,
    ImportVerdict,
    MCPClient,
    MCPTool,
    MCPToolSpec,
    Skill,
    SkillCatalog,
    ToolSearchTool,
    scan_for_injection,
    tool_search,
)
from .loop import RunBudget, RunResult, ToolRegistry, admit, run
from .memory import PostgresMemoryStore
from .permissions import Rule, RuleBasedPermissionEngine
from .projections import project_messages
from .protocols import (
    EventStore,
    PermissionEngine,
    PromptAssembler,
    PromptBundle,
    ProviderChunk,
    ProviderGateway,
    ProviderRequest,
    ScopeGuard,
    Tool,
    ToolCall,
    ToolContext,
    ToolResult,
    Usage,
)
from .providers import LiteLLMGateway
from .search import (
    ArchivalSearchTool,
    ArchivalStore,
    SearchHit,
    SessionSearchTool,
    session_search,
)
from .secrets import EnvelopeCipher, SecretsError, cipher_from_settings
from .state import InMemoryEventStore, PostgresEventStore
from .tokens import InMemoryTokenStore, PostgresTokenStore
from .tools import ExecRequest, execute
from .tracing import LangfuseTracer, NoopTracer, Tracer, make_tracer
from .types import (
    AgentId,
    ContentTaint,
    FinishReason,
    PermissionDecision,
    Role,
    RunId,
    ScopeId,
    ScopeKind,
    SessionId,
    StopReason,
    TrustLevel,
)

__all__ = [
    "__version__",
    # types
    "AgentId",
    "ContentTaint",
    "FinishReason",
    "PermissionDecision",
    "Role",
    "RunId",
    "ScopeId",
    "ScopeKind",
    "SessionId",
    "StopReason",
    "TrustLevel",
    # errors
    "KeelError",
    "PermissionDenied",
    "CrossScopeError",
    # events
    "Event",
    "EventType",
    # agents
    "AgentSpec",
    "Scope",
    # protocols + models
    "Tool",
    "ToolCall",
    "ToolContext",
    "ToolResult",
    "EventStore",
    "ProviderGateway",
    "ProviderRequest",
    "ProviderChunk",
    "Usage",
    "PermissionEngine",
    "PromptAssembler",
    "PromptBundle",
    "ScopeGuard",
    # runtime (loop + state)
    "run",
    "admit",
    "ToolRegistry",
    "RunBudget",
    "RunResult",
    "InMemoryEventStore",
    "PostgresEventStore",
    "PostgresMemoryStore",
    "project_messages",
    "LiteLLMGateway",
    # connectors + secrets (WS-G)
    "Connector",
    "ConnectorTool",
    "ConfusedDeputyEngine",
    "taint_from_events",
    "EnvelopeCipher",
    "SecretsError",
    "cipher_from_settings",
    "InMemoryTokenStore",
    "PostgresTokenStore",
    # search + archival (WS-D)
    "Embedder",
    "FakeEmbedder",
    "LiteLLMEmbedder",
    "rrf_fuse",
    "ArchivalStore",
    "SearchHit",
    "session_search",
    "ArchivalSearchTool",
    "SessionSearchTool",
    # extensibility (WS-G, I8)
    "ImportGuard",
    "ImportVerdict",
    "scan_for_injection",
    "Skill",
    "SkillCatalog",
    "MCPClient",
    "MCPTool",
    "MCPToolSpec",
    "CatalogEntry",
    "tool_search",
    "ToolSearchTool",
    # observability (WS-H)
    "Tracer",
    "NoopTracer",
    "LangfuseTracer",
    "make_tracer",
    # permissions + tool execution
    "Rule",
    "RuleBasedPermissionEngine",
    "ExecRequest",
    "execute",
]
