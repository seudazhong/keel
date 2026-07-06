"""Context engineering — byte-stable prompt assembly (spike S1).

Proves the invariant: a byte-stable prompt **prefix** yields a stable
``prompt_cache_key`` across turns/agents, and volatile content (history, memory)
lives only in the **suffix** so it never perturbs the cache key.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence

from keel_core.agents import AgentSpec
from keel_core.events import Event
from keel_core.protocols import PromptBundle

_SYSTEM_HEADER = "You are Keel, a self-hostable AI agent."


class StablePromptAssembler:
    """Assemble a layered prompt with a byte-stable, cache-friendly prefix."""

    def __init__(self, system_header: str = _SYSTEM_HEADER) -> None:
        self._system_header = system_header

    def _build_prefix(self, agent: AgentSpec) -> str:
        # Depends only on stable inputs (identity, persona, sorted toolset).
        # No history, no memory — those are volatile and belong in the suffix.
        layers = [
            self._system_header,
            f"# Agent\n{agent.name}",
            f"# Persona\n{agent.persona}",
            "# Tools\n" + "\n".join(sorted(agent.toolset)),
        ]
        return "\n\n".join(layers)

    def assemble(self, agent: AgentSpec, history: Sequence[Event]) -> PromptBundle:
        """Build the prompt bundle for the next model call."""
        prefix = self._build_prefix(agent)
        cache_key = hashlib.sha256(prefix.encode("utf-8")).hexdigest()
        suffix = json.dumps(
            [{"seq": event.seq, "type": str(event.type)} for event in history],
            separators=(",", ":"),
        )
        return PromptBundle(prefix=prefix, suffix=suffix, cache_key=cache_key)
