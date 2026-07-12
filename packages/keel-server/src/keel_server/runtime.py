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
    ArchivalInsertTool,
    EventStore,
    InMemoryEventStore,
    LiteLLMGateway,
    MemoryAppendTool,
    MemoryReplaceTool,
    MemoryRethinkTool,
    PostgresEventStore,
    ProviderGateway,
    Rule,
    RuleBasedPermissionEngine,
    Scope,
    ScopeKind,
    ToolRegistry,
    TrustLevel,
    admit,
    format_core_memory,
    make_tracer,
    run,
)
from keel_core.embeddings import Embedder, LiteLLMEmbedder
from keel_core.eventbus import RedisEventStore
from keel_core.events import Event, EventType
from keel_core.memory import PostgresMemoryStore
from keel_core.protocols import Tool as ToolProto
from keel_core.protocols import ToolCall, ToolContext
from keel_core.recall import MessageEmbeddingIndexer
from keel_core.search import ArchivalSearchTool, SessionSearchTool
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


def _build_memory_tools(
    engine: AsyncEngine,
    embedder: Embedder | None,
    *,
    cap: int,
    batch_size: int,
    catchup_limit: int,
) -> list[ToolProto]:
    """Core-memory editing, hybrid recall, and optional archival memory."""
    tools: list[ToolProto] = [
        MemoryAppendTool(engine, max_chars=cap),
        MemoryReplaceTool(engine, max_chars=cap),
        MemoryRethinkTool(engine, max_chars=cap),
        SessionSearchTool(
            engine,
            embedder,
            batch_size=batch_size,
            catchup_limit=catchup_limit,
        ),
    ]
    if embedder is not None:
        tools += [ArchivalInsertTool(engine, embedder), ArchivalSearchTool(engine, embedder)]
    return tools


def _web_permissions(memory_allow: tuple[str, ...] = ()) -> RuleBasedPermissionEngine:
    """Read-only + own-scope memory tools allowed; mutating tools require an approval."""
    rules = [Rule(name, PermissionDecision.allow) for name in _READ_ONLY]
    rules += [Rule(name, PermissionDecision.allow) for name in memory_allow]
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
        embedder: Embedder | None = None,
        embedding_model: str = "ollama/bge-m3",
        embedding_dim: int = 1024,
        embedding_send_dimensions: bool = False,
        memory_block_max_chars: int = 2000,
        session_embedding_batch_size: int = 64,
        session_embedding_catchup_limit: int = 500,
        session_indexer: MessageEmbeddingIndexer | None = None,
    ) -> None:
        self._engine = engine
        self._fanout = RedisEventStore(redis_client)
        self._memory = InMemoryEventStore()  # shared durable fallback when no engine
        self._scope = Scope(id=scope_id, kind=ScopeKind.personal, trust=TrustLevel.trusted)
        if embedder is None and engine is not None and embedding_model:
            embedder = LiteLLMEmbedder(
                embedding_model, embedding_dim, send_dimensions=embedding_send_dimensions
            )
        self._embedder = embedder
        self._session_embedding_batch_size = session_embedding_batch_size
        self._session_embedding_catchup_limit = session_embedding_catchup_limit
        if session_indexer is None and engine is not None and embedder is not None:
            session_indexer = MessageEmbeddingIndexer(
                engine,
                self._scope.id,
                embedder,
                batch_size=session_embedding_batch_size,
            )
        self._session_indexer = session_indexer
        memory_tools: list[ToolProto] = (
            _build_memory_tools(
                engine,
                embedder,
                cap=memory_block_max_chars,
                batch_size=session_embedding_batch_size,
                catchup_limit=session_embedding_catchup_limit,
            )
            if engine is not None
            else []
        )
        memory_names = tuple(tool.name for tool in memory_tools)
        self._agent = AgentSpec(
            id="web",
            name="Keel Web",
            model=model,
            scope=self._scope,
            toolset=list(_READ_ONLY + _MUTATING) + list(memory_names),
        )
        self._provider = provider or LiteLLMGateway()
        self._registry = ToolRegistry(_build_tools(workspace) + memory_tools)
        self._permissions = _web_permissions(memory_names)
        self._approvals = ApprovalRegistry()
        self._tracer = make_tracer()  # Langfuse if configured, else no-op
        self._runs: dict[RunId, asyncio.Task[None]] = {}
        self._interrupted: set[RunId] = set()
        self._index_tasks: set[asyncio.Task[None]] = set()
        self._closing = False

    @property
    def scope_id(self) -> ScopeId:
        return self._scope.id

    @property
    def model(self) -> str:
        return self._agent.model

    @property
    def embedder(self) -> Embedder | None:
        return self._embedder

    @property
    def session_embedding_batch_size(self) -> int:
        return self._session_embedding_batch_size

    @property
    def session_embedding_catchup_limit(self) -> int:
        return self._session_embedding_catchup_limit

    def set_model(self, model: str) -> None:
        """Switch the model used by subsequent runs (applies immediately)."""
        self._agent = self._agent.model_copy(update={"model": model})

    def _durable(self) -> EventStore:
        if self._engine is not None:
            return PostgresEventStore(self._engine, self._scope.id)
        return self._memory

    def _store(self) -> CompositeEventStore:
        return CompositeEventStore(self._durable(), self._fanout)

    async def _core_memory_context(self) -> str:
        if self._engine is None:
            return ""
        blocks = await PostgresMemoryStore(self._engine, self._scope.id).blocks()
        return format_core_memory(blocks)

    async def admit_and_run(self, session_id: SessionId, content: str) -> RunId:
        """Durably admit input (I2), then launch the run as a background task."""
        store = self._store()
        await admit(store, session_id, self._scope.id, content)
        run_id = uuid.uuid4().hex
        task = asyncio.create_task(self._run(store, session_id, run_id))
        self._runs[run_id] = task
        task.add_done_callback(lambda _t: self._forget(run_id))
        return run_id

    def _forget(self, run_id: RunId) -> None:
        self._runs.pop(run_id, None)
        self._interrupted.discard(run_id)

    def interrupt_run(self, run_id: RunId) -> bool:
        """Request an in-flight run to stop at its next iteration. False if unknown."""
        if run_id not in self._runs:
            return False
        self._interrupted.add(run_id)
        return True

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
                interrupt=lambda: run_id in self._interrupted,
                stream_deltas=True,  # relay token-by-token over SSE
                on_event=self._tracer.record,  # export the run to Langfuse (if configured)
                system_context=self._core_memory_context,
            )
        except Exception:  # noqa: BLE001 - a run task must not take the server down
            logger.exception("run %s failed", run_id)
            await self._emit(
                store, EventType.error, session_id, run_id, {"message": "internal run error"}
            )
            await self._emit(store, EventType.run_ended, session_id, run_id, {"reason": "error"})
        finally:
            self._tracer.flush()
            self._schedule_session_index(session_id)

    def _schedule_session_index(self, session_id: SessionId) -> None:
        if self._session_indexer is None or self._closing:
            return
        task = asyncio.create_task(self._index_session(session_id))
        self._index_tasks.add(task)
        task.add_done_callback(self._index_tasks.discard)

    async def _index_session(self, session_id: SessionId) -> None:
        assert self._session_indexer is not None
        try:
            indexed = await self._session_indexer.index_session(session_id)
            logger.info(
                "session embedding index complete scope=%s session=%s model=%s dim=%s indexed=%d",
                self._scope.id,
                session_id,
                self._embedder.model if self._embedder is not None else "none",
                self._embedder.dim if self._embedder is not None else 0,
                indexed,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - background indexing is explicitly best-effort
            logger.exception(
                "session embedding index failed scope=%s session=%s",
                self._scope.id,
                session_id,
            )

    async def aclose(self) -> None:
        """Cancel pending background index tasks and block until all have terminated."""
        self._closing = True
        tasks = list(self._index_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._index_tasks.clear()

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
