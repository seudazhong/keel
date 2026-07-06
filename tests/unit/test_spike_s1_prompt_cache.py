"""Spike S1 acceptance: byte-stable prompt prefix -> stable cache key."""

from __future__ import annotations

from datetime import UTC, datetime

from keel_core.agents import AgentSpec, Scope
from keel_core.context import StablePromptAssembler
from keel_core.events import Event, EventType
from keel_core.types import ScopeKind


def _agent(persona: str = "helpful", tools: tuple[str, ...] = ("a", "b")) -> AgentSpec:
    return AgentSpec(
        id="a1",
        name="A",
        model="test/model",
        persona=persona,
        toolset=list(tools),
        scope=Scope(id="s", kind=ScopeKind.personal),
    )


def _event(seq: int, text: str = "") -> Event:
    return Event(
        type=EventType.message_token,
        seq=seq,
        session_id="s",
        scope_id="sc",
        ts=datetime.now(UTC),
        payload={"text": text},
    )


def test_prefix_and_key_stable_across_turns() -> None:
    asm = StablePromptAssembler()
    empty = asm.assemble(_agent(), history=[])
    with_history = asm.assemble(_agent(), history=[_event(1), _event(2)])
    assert empty.prefix == with_history.prefix
    assert empty.cache_key == with_history.cache_key


def test_toolset_order_does_not_change_key() -> None:
    asm = StablePromptAssembler()
    k1 = asm.assemble(_agent(tools=("a", "b")), []).cache_key
    k2 = asm.assemble(_agent(tools=("b", "a")), []).cache_key
    assert k1 == k2


def test_memory_never_in_prefix() -> None:
    asm = StablePromptAssembler()
    secret = "SENSITIVE_MEMORY_BLOCK"
    bundle = asm.assemble(_agent(), history=[_event(1, text=secret)])
    assert secret not in bundle.prefix


def test_persona_change_changes_key() -> None:
    asm = StablePromptAssembler()
    assert asm.assemble(_agent(persona="x"), []).cache_key != (
        asm.assemble(_agent(persona="y"), []).cache_key
    )
