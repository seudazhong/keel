"""Core-memory injection into the loop (Approach A: re-read each turn)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from keel_core.agents import AgentSpec, Scope
from keel_core.loop import admit, run
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ProviderChunk, ProviderRequest
from keel_core.state import InMemoryEventStore
from keel_core.types import FinishReason, PermissionDecision, ScopeKind

_TEST_ALLOW_ALL = RuleBasedPermissionEngine([Rule("*", PermissionDecision.allow)])


class _CapturingProvider:
    """Records each ProviderRequest and replies once per turn."""

    def __init__(self, replies: list[str]) -> None:
        self._replies = replies
        self.requests: list[ProviderRequest] = []

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        self.requests.append(request)
        reply = self._replies[len(self.requests) - 1]
        return self._gen(reply)

    async def _gen(self, reply: str) -> AsyncIterator[ProviderChunk]:
        yield ProviderChunk(delta=reply, finish_reason=FinishReason.end_turn)


def _agent() -> AgentSpec:
    return AgentSpec(id="a", name="n", model="m", scope=Scope(id="u", kind=ScopeKind.personal))


async def test_system_context_injected_as_first_message() -> None:
    store = InMemoryEventStore()
    await admit(store, "s", "u", "hi")
    provider = _CapturingProvider(["ok"])

    async def ctx() -> str:
        return "<core_memory><human>X</human></core_memory>"

    await run(
        agent=_agent(),
        session_id="s",
        store=store,
        provider=provider,
        permissions=_TEST_ALLOW_ALL,
        system_context=ctx,
    )
    assert provider.requests[0].messages[0] == {
        "role": "system",
        "content": "<core_memory><human>X</human></core_memory>",
    }


async def test_system_context_reread_between_runs() -> None:
    store = InMemoryEventStore()
    holder = {"v": "before"}

    async def ctx() -> str:
        return holder["v"]

    await admit(store, "s", "u", "hi")
    p1 = _CapturingProvider(["r1"])
    await run(
        agent=_agent(),
        session_id="s",
        store=store,
        provider=p1,
        permissions=_TEST_ALLOW_ALL,
        system_context=ctx,
    )
    assert p1.requests[0].messages[0]["content"] == "before"

    holder["v"] = "after"  # a self-edit lands between runs
    await admit(store, "s", "u", "again")
    p2 = _CapturingProvider(["r2"])
    await run(
        agent=_agent(),
        session_id="s",
        store=store,
        provider=p2,
        permissions=_TEST_ALLOW_ALL,
        system_context=ctx,
    )
    assert p2.requests[0].messages[0]["content"] == "after"  # re-read, not cached


async def test_no_system_context_leaves_messages_unchanged() -> None:
    store = InMemoryEventStore()
    await admit(store, "s", "u", "hi")
    provider = _CapturingProvider(["ok"])
    await run(
        agent=_agent(),
        session_id="s",
        store=store,
        provider=provider,
        permissions=_TEST_ALLOW_ALL,
    )
    assert all(m["role"] != "system" for m in provider.requests[0].messages)
