"""Keel agent runtime core.

M0 ships the frozen **contracts** (Protocols, event vocabulary, REST DTOs,
scope model) and shared infrastructure; behaviour lands in M1
(see docs/IMPLEMENTATION-PLAN.md).
"""

from __future__ import annotations

__version__ = "0.0.0"

from .agents import AgentSpec, Scope
from .errors import CrossScopeError, KeelError, PermissionDenied
from .events import Event, EventType
from .loop import RunBudget, RunResult, ToolRegistry, admit, run
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
)
from .providers import LiteLLMGateway
from .state import InMemoryEventStore
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
    "LiteLLMGateway",
]
