"""Agent & scope models (ADR-0009: an agent is a scoped, persisted entity).

A group agent and a personal agent are the *same* abstraction with a different
scope: persona + memory + toolset + connectors + permission boundary + provider
+ trust level.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from .types import AgentId, ScopeId, ScopeKind, TrustLevel


class Scope(BaseModel):
    """An isolation boundary. Every persisted row carries its ``scope_id``."""

    id: ScopeId
    kind: ScopeKind
    trust: TrustLevel = TrustLevel.untrusted


class AgentSpec(BaseModel):
    """Declarative definition of an agent (no behaviour)."""

    id: AgentId
    name: str
    scope: Scope
    persona: str = ""
    model: str
    toolset: list[str] = Field(default_factory=list)
    # ADR-0009: connectors are granted to this agent scope, never ambient.
    connectors: list[str] = Field(default_factory=list)
    permission_profile: str = "default"
    max_iterations: int = 40
    token_budget: int | None = None
