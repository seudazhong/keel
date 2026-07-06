"""Shared contract types: identifiers and enumerations.

Frozen in M0 (contract-first). These are stable vocabulary the whole system
codes against; behaviour lands in M1.
"""

from __future__ import annotations

from enum import StrEnum

# Opaque identifiers (kept as str aliases for v0; may become NewType later).
type ScopeId = str
type SessionId = str
type RunId = str
type AgentId = str


class Role(StrEnum):
    """Message author role."""

    system = "system"
    user = "user"
    assistant = "assistant"
    tool = "tool"


class StopReason(StrEnum):
    """Named termination reasons (bounded-loop invariant)."""

    completed = "completed"
    max_iterations = "max_iterations"
    budget_exhausted = "budget_exhausted"
    interrupted = "interrupted"
    halted = "halted"
    error = "error"


class FinishReason(StrEnum):
    """Turn-level provider finish signal (distinct from the run-level StopReason)."""

    end_turn = "end_turn"
    tool_use = "tool_use"
    length = "length"
    error = "error"


class PermissionDecision(StrEnum):
    """Permission-engine verdict (deny > ask > allow; default ask)."""

    allow = "allow"
    ask = "ask"
    deny = "deny"


class TrustLevel(StrEnum):
    """Trust of a *scope* (see ScopeKind) — distinct from content taint."""

    trusted = "trusted"
    untrusted = "untrusted"


class ScopeKind(StrEnum):
    """Kind of agent scope (ADR-0009: an agent is a scoped entity)."""

    group = "group"
    personal = "personal"
    system = "system"


class ContentTaint(StrEnum):
    """Trust of *content* — separate from scope trust (DESIGN-REVIEW G17).

    A trusted personal agent may still ingest tainted content (an email body,
    a fetched web page); outbound/cross-connector actions gate on taint.
    """

    clean = "clean"
    tainted = "tainted"
