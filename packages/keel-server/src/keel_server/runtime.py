"""Server-side agent runtime: run orchestration, event fan-out, approvals (WS-E).

Wires the keel-core loop into the FastAPI server. A run executes as an asyncio
task whose events are appended to a **durable** store (authoritative ``seq``) and
fanned out to a **Redis stream**; the SSE endpoint tails Redis. Tool approvals are
resolved out-of-band over HTTP via an async handshake: the loop emits
``approval.requested`` and awaits a future that ``POST /v1/approvals/{id}`` resolves.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core import (
    AgentSpec,
    EventStore,
    InMemoryEventStore,
    LiteLLMGateway,
    PostgresEventStore,
    ProviderGateway,
    Rule,
    RuleBasedPermissionEngine,
    Scope,
    ScopeKind,
    ToolRegistry,
    TrustLevel,
    admit,
    run,
)
from keel_core.eventbus import RedisEventStore
from keel_core.events import Event, EventType
from keel_core.protocols import Tool as ToolProto
from keel_core.protocols import ToolCall, ToolContext
from keel_core.tools import (
    EditTool,
    GlobTool,
    GrepTool,
    LsTool,
    ReadTool,
    ShellTool,
    WriteTool,
)
from keel_core.tools.executor import ApproveFn
from keel_core.types import PermissionDecision, RunId, ScopeId, SessionId

logger = logging.getLogger("keel.server.runtime")

_READ_ONLY = ("read", "ls", "glob", "grep")
_MUTATING = ("write", "edit", "shell")


def _now() -> datetime:
    return datetime.now(UTC)


def _build_tools(workspace: Path) -> list[ToolProto]:
    tools: list[ToolProto] = [
        ReadTool(workspace),
        WriteTool(workspace),
        EditTool(workspace),
        LsTool(workspace),
        GlobTool(workspace),
        GrepTool(workspace),
        ShellTool(workspace),
    ]
    return tools


def _web_permissions() -> RuleBasedPermissionEngine:
    """Read-only tools allowed; mutating tools require an approval over HTTP."""
    rules = [Rule(name, PermissionDecision.allow) for name in _READ_ONLY]
    rules += [Rule(name, PermissionDecision.ask) for name in _MUTATING]
    return RuleBasedPermissionEngine(rules, default=PermissionDecision.ask)


class CompositeEventStore:
    """Append to a durable store (authoritative ``seq``) then fan out to Redis.

    The durable store assigns ``seq`` on append; the same, now-numbered event is
    republished to the Redis stream so SSE/WS surfaces can tail it live. Reads
    always come from the durable store.
    """

    def __init__(self, durable: EventStore, fanout: RedisEventStore) -> None:
        self._durable = durable
        self._fanout = fanout

    async def append(self, event: Event) -> None:
        # Streaming-only partial deltas relay to Redis but never touch the durable
        # log (the whole message.token is emitted at turn end).
        if event.type is EventType.message_token and event.payload.get("partial"):
            await self._fanout.append(event)
            return
        await self._durable.append(event)  # assigns seq
        await self._fanout.append(event)  # same event, now carrying seq

    def read(self, session_id: SessionId, after: int | None = None) -> AsyncIterator[Event]:
        return self._durable.read(session_id, after)


class ApprovalRegistry:
    """Track pending tool approvals as futures resolved out-of-band over HTTP."""

    def __init__(self, *, timeout: float = 300.0) -> None:
        self._pending: dict[str, asyncio.Future[bool]] = {}
        self._timeout = timeout

    def register(self) -> tuple[str, asyncio.Future[bool]]:
        approval_id = uuid.uuid4().hex
        fut: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pending[approval_id] = fut
        return approval_id, fut

    def discard(self, approval_id: str) -> None:
        self._pending.pop(approval_id, None)

    def resolve(self, approval_id: str, approved: bool) -> bool:
        """Resolve a pending approval; return False if it is unknown/already done."""
        fut = self._pending.get(approval_id)
        if fut is None or fut.done():
            return False
        fut.set_result(approved)
        return True

    @property
    def timeout(self) -> float:
        return self._timeout


class AgentRuntime:
    """Orchestrates web-triggered agent runs with Redis fan-out and HTTP approvals."""

    def __init__(
        self,
        *,
        redis_client: redis.Redis,
        model: str,
        workspace: Path,
        engine: AsyncEngine | None = None,
        scope_id: ScopeId = "web:local",
        provider: ProviderGateway | None = None,
    ) -> None:
        self._engine = engine
        self._fanout = RedisEventStore(redis_client)
        self._memory = InMemoryEventStore()  # shared durable fallback when no engine
        self._scope = Scope(id=scope_id, kind=ScopeKind.personal, trust=TrustLevel.trusted)
        self._agent = AgentSpec(
            id="web",
            name="Keel Web",
            model=model,
            scope=self._scope,
            toolset=list(_READ_ONLY + _MUTATING),
        )
        self._provider = provider or LiteLLMGateway()
        self._registry = ToolRegistry(_build_tools(workspace))
        self._permissions = _web_permissions()
        self._approvals = ApprovalRegistry()
        self._runs: dict[RunId, asyncio.Task[None]] = {}

    @property
    def scope_id(self) -> ScopeId:
        return self._scope.id

    def _durable(self) -> EventStore:
        if self._engine is not None:
            return PostgresEventStore(self._engine, self._scope.id)
        return self._memory

    def _store(self) -> CompositeEventStore:
        return CompositeEventStore(self._durable(), self._fanout)

    async def admit_and_run(self, session_id: SessionId, content: str) -> RunId:
        """Durably admit input (I2), then launch the run as a background task."""
        store = self._store()
        await admit(store, session_id, self._scope.id, content)
        run_id = uuid.uuid4().hex
        task = asyncio.create_task(self._run(store, session_id, run_id))
        self._runs[run_id] = task
        task.add_done_callback(lambda _t: self._runs.pop(run_id, None))
        return run_id

    async def _run(self, store: CompositeEventStore, session_id: SessionId, run_id: RunId) -> None:
        approve = self._approver(store, session_id, run_id)
        try:
            await run(
                agent=self._agent,
                session_id=session_id,
                store=store,
                provider=self._provider,
                registry=self._registry,
                permissions=self._permissions,
                approve=approve,
                run_id=run_id,
                stream_deltas=True,  # relay token-by-token over SSE
            )
        except Exception:  # noqa: BLE001 - a run task must not take the server down
            logger.exception("run %s failed", run_id)
            await self._emit(
                store, EventType.error, session_id, run_id, {"message": "internal run error"}
            )
            await self._emit(store, EventType.run_ended, session_id, run_id, {"reason": "error"})

    def _approver(
        self, store: CompositeEventStore, session_id: SessionId, run_id: RunId
    ) -> ApproveFn:
        async def _approve(call: ToolCall, ctx: ToolContext) -> bool:
            approval_id, fut = self._approvals.register()
            await self._emit(
                store,
                EventType.approval_requested,
                session_id,
                run_id,
                {
                    "approval_id": approval_id,
                    "tool": call.name,
                    "args": call.arguments,
                    "call_id": call.id,
                },
            )
            try:
                approved = await asyncio.wait_for(fut, timeout=self._approvals.timeout)
            except (TimeoutError, asyncio.CancelledError):
                approved = False  # fail closed
            finally:
                self._approvals.discard(approval_id)
            await self._emit(
                store,
                EventType.approval_resolved,
                session_id,
                run_id,
                {"approval_id": approval_id, "approved": approved},
            )
            return approved

        return _approve

    def resolve_approval(self, approval_id: str, approved: bool) -> bool:
        """Resolve a pending approval (called by the HTTP endpoint)."""
        return self._approvals.resolve(approval_id, approved)

    def tail(self, session_id: SessionId, after: int | None = None) -> AsyncIterator[Event]:
        """Live event stream for a session (replay from ``after`` then follow)."""
        return self._fanout.tail(session_id, after)

    async def _emit(
        self,
        store: CompositeEventStore,
        event_type: EventType,
        session_id: SessionId,
        run_id: RunId | None,
        payload: dict[str, object],
    ) -> None:
        await store.append(
            Event(
                type=event_type,
                seq=0,
                session_id=session_id,
                scope_id=self._scope.id,
                run_id=run_id,
                ts=_now(),
                payload=payload,
            )
        )
