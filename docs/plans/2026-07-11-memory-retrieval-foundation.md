# Memory + Semantic-Retrieval Foundation — Implementation Plan

> **Execution:** Work through the checklist task-by-task and run the narrowest applicable validation before advancing.

**Goal:** Give the web assistant a working memory triad — self-edited in-context **core memory**, semantic **archival memory**, and lexical **session recall** — by wiring the (mostly-built) storage layer into the loop and runtime with a real Ollama `bge-m3` embedder.

**Architecture:** Approach A — core-memory blocks live in Postgres (`memory_blocks`) and are re-read fresh each turn, injected as a `<core_memory>` system message via a memory-agnostic `system_context` callable on `loop.run`. Self-edit tools (`memory_append/replace/rethink`) write blocks; archival tools (`archival_insert` + existing `archival_search`) use `ArchivalStore` + a `LiteLLMEmbedder("ollama/bge-m3", 1024)`. All new tools are own-scope and side-effect-free → permission `allow`.

**Tech Stack:** Python (keel-core / keel-server), Postgres + pgvector, LiteLLM, Ollama (`bge-m3`), Docker Compose, pytest.

**Design:** `docs/designs/2026-07-09-memory-retrieval-foundation-design.md` (+ Chinese deep-dive `docs/designs/2026-07-09-core-memory-notes-zh.md`).

## Global Constraints

- Python invoked as `.\.venv\Scripts\python.exe` (bare `python` lacks workspace deps); prefix commands needing Chinese output with `$env:PYTHONUTF8=1`.
- Integration tests: set `$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"` and select with `-m integration` (the `migrated_db` fixture TRUNCATEs the isolated `keel_test` DB — never point it at live `keel`).
- After every code change: `.\.venv\Scripts\python.exe -m ruff format <files>` + `ruff check <files>` + `.\.venv\Scripts\python.exe -m mypy packages` must be clean.
- Provider messages use the OpenAI/LiteLLM shape `{"role": ..., "content": ...}` (NOT `text`).
- The archival `embedding` column is an unconstrained pgvector `vector`, so `FakeEmbedder(dim=16)` works in tests; the `(model, dim)` pin keeps searches same-model.
- Stage commits with `git -c core.safecrlf=false add <files>`; end every commit message with:
  `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`
- New tools follow the existing tool protocol: class attrs `name: str`, `description: str`, `writes: bool`; methods `input_schema() -> dict`, `async run(args: dict, ctx: ToolContext) -> ToolResult`. `ToolResult(ok=False, output="…")` signals a tool-level error (there is no `error` field). `ToolContext` carries `.scope_id`.

---

## File structure

| File | Change | Responsibility |
|---|---|---|
| `packages/keel-core/src/keel_core/embeddings.py` | modify `LiteLLMEmbedder` | conditional `dimensions` + length assert |
| `packages/keel-core/src/keel_core/memory.py` | add `format_core_memory`, `blocks()`, 3 tools | core-memory formatter, list-blocks, self-edit tools |
| `packages/keel-core/src/keel_core/loop.py` | thread `system_context` | injection seam |
| `packages/keel-core/src/keel_core/search.py` | add `ArchivalInsertTool` | archival write tool |
| `packages/keel-core/src/keel_core/config.py` | +4 settings | embedder + block-cap config |
| `packages/keel-core/src/keel_core/__init__.py` | exports | public API |
| `packages/keel-server/src/keel_server/runtime.py` | wire tools + permissions + `system_context` | integration hub |
| `packages/keel-server/src/keel_server/app.py` | pass settings to runtime | wiring |
| `docker-compose.yml` | `ollama` + `ollama-pull` services + env | infra |
| `tests/unit/test_embeddings.py` | new | embedder tests |
| `tests/unit/test_memory.py` | new | formatter test |
| `tests/unit/test_loop_memory.py` | new | injection + re-read tests |
| `tests/unit/test_runtime_memory.py` | new | tool-registration + permission tests |
| `tests/unit/test_config.py` | extend | settings defaults |
| `tests/integration/test_memory_postgres.py` | extend | `blocks()` + 3 tools |
| `tests/integration/test_search_postgres.py` | extend | `ArchivalInsertTool` round-trip |

---

## Task 1: `LiteLLMEmbedder` — conditional `dimensions` + length assert

**Files:**
- Modify: `packages/keel-core/src/keel_core/embeddings.py` (the `LiteLLMEmbedder` class)
- Test: `tests/unit/test_embeddings.py` (create)

**Interfaces:**
- Produces: `LiteLLMEmbedder(model: str, dim: int, *, send_dimensions: bool = False)` with `async embed(texts) -> list[list[float]]` that omits `dimensions` unless `send_dimensions`, and raises `ValueError` on a wrong-length vector.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_embeddings.py`:
```python
"""LiteLLMEmbedder: conditional dimensions kwarg + dimension assertion."""

from __future__ import annotations

from typing import Any

import litellm
import pytest

from keel_core.embeddings import LiteLLMEmbedder


def _fake_aembedding(captured: dict[str, Any], vec_len: int):
    async def aembedding(**kwargs: Any) -> Any:
        captured.update(kwargs)

        class _Resp:
            data = [{"embedding": [0.0] * vec_len}]

        return _Resp()

    return aembedding


async def test_omits_dimensions_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(litellm, "aembedding", _fake_aembedding(captured, 4))
    out = await LiteLLMEmbedder("ollama/bge-m3", 4).embed(["hi"])
    assert "dimensions" not in captured  # Ollama rejects it
    assert out == [[0.0] * 4]


async def test_sends_dimensions_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(litellm, "aembedding", _fake_aembedding(captured, 8))
    await LiteLLMEmbedder("openai/text-embedding-3-small", 8, send_dimensions=True).embed(["hi"])
    assert captured["dimensions"] == 8


async def test_dimension_mismatch_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(litellm, "aembedding", _fake_aembedding(captured, 3))  # wrong length
    with pytest.raises(ValueError, match="!= expected 1024"):
        await LiteLLMEmbedder("ollama/bge-m3", 1024).embed(["hi"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_embeddings.py -q`
Expected: FAIL (`TypeError: unexpected keyword argument 'send_dimensions'` / no assertion).

- [ ] **Step 3: Implement**

In `embeddings.py`, replace the `LiteLLMEmbedder` class body with:
```python
class LiteLLMEmbedder:
    """A ``(model, dim)``-pinned embedder over LiteLLM (any provider)."""

    def __init__(self, model: str, dim: int, *, send_dimensions: bool = False) -> None:
        self.model = model
        self.dim = dim
        self.send_dimensions = send_dimensions

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        from litellm import aembedding

        kwargs: dict[str, Any] = {"model": self.model, "input": list(texts)}
        if self.send_dimensions:  # OpenAI text-embedding-3 accepts it; Ollama rejects it
            kwargs["dimensions"] = self.dim
        response = await aembedding(**kwargs)
        data: list[dict[str, Any]] = response.data
        vectors = [list(item["embedding"]) for item in data]
        for vec in vectors:
            if len(vec) != self.dim:
                raise ValueError(
                    f"embedding dim {len(vec)} != expected {self.dim} for model {self.model}"
                )
        return vectors
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_embeddings.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Lint + commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format packages/keel-core/src/keel_core/embeddings.py tests/unit/test_embeddings.py
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/embeddings.py tests/unit/test_embeddings.py
.\.venv\Scripts\python.exe -m mypy packages
git -c core.safecrlf=false add packages/keel-core/src/keel_core/embeddings.py tests/unit/test_embeddings.py
git commit -m "feat(embeddings): conditional dimensions kwarg + dim assertion

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 2: `format_core_memory`

**Files:**
- Modify: `packages/keel-core/src/keel_core/memory.py` (add function)
- Test: `tests/unit/test_memory.py` (create)

**Interfaces:**
- Produces: `format_core_memory(blocks: dict[str, str], *, defaults: tuple[str, ...] = ("persona", "human")) -> str` — renders every default block (even if absent) then any extra blocks, wrapped in `<core_memory>…</core_memory>`.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_memory.py`:
```python
"""Core-memory formatting (pure)."""

from __future__ import annotations

from keel_core.memory import format_core_memory


def test_renders_defaults_and_extras() -> None:
    out = format_core_memory({"human": "name is X", "notes": "likes tea"})
    assert out.startswith("<core_memory>")
    assert out.rstrip().endswith("</core_memory>")
    assert "<persona></persona>" in out  # empty default still shown
    assert "<human>name is X</human>" in out
    assert "<notes>likes tea</notes>" in out  # extras rendered after defaults
    assert out.index("<persona>") < out.index("<notes>")  # defaults first
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_memory.py -q`
Expected: FAIL (`ImportError: cannot import name 'format_core_memory'`).

- [ ] **Step 3: Implement**

Add to `memory.py` (after the imports, before the class):
```python
def format_core_memory(
    blocks: dict[str, str], *, defaults: tuple[str, ...] = ("persona", "human")
) -> str:
    """Render core-memory blocks as an always-visible system message.

    Default blocks render even when absent (empty tags) so the model knows they
    exist; any extra blocks follow, in insertion order.
    """
    keys = list(defaults) + [key for key in blocks if key not in defaults]
    lines = [
        "<core_memory>",
        "Editable long-term memory, always visible. Keep it accurate with the "
        "memory_* tools; it persists across all your sessions.",
    ]
    lines += [f"<{key}>{blocks.get(key, '')}</{key}>" for key in keys]
    lines.append("</core_memory>")
    return "\n".join(lines)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_memory.py -q`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format packages/keel-core/src/keel_core/memory.py tests/unit/test_memory.py
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/memory.py tests/unit/test_memory.py
git -c core.safecrlf=false add packages/keel-core/src/keel_core/memory.py tests/unit/test_memory.py
git commit -m "feat(memory): format_core_memory renderer

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 3: `PostgresMemoryStore.blocks()`

**Files:**
- Modify: `packages/keel-core/src/keel_core/memory.py` (add method to `PostgresMemoryStore`)
- Test: `tests/integration/test_memory_postgres.py` (extend)

**Interfaces:**
- Consumes: `PostgresMemoryStore(engine, scope_id)` with `set(key, value)` (existing).
- Produces: `async PostgresMemoryStore.blocks() -> dict[str, str]` — all of the scope's blocks.

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/test_memory_postgres.py`:
```python
async def test_blocks_returns_all_scope_blocks(migrated_db: AsyncEngine) -> None:
    store = PostgresMemoryStore(migrated_db, "u:blk1")
    await store.set("persona", "concise")
    await store.set("human", "name X")
    assert await store.blocks() == {"persona": "concise", "human": "name X"}
    assert await PostgresMemoryStore(migrated_db, "u:blk2").blocks() == {}  # scope-isolated
```
(Ensure the file imports `PostgresMemoryStore` and `AsyncEngine`, and has `pytestmark = pytest.mark.integration` — it already does.)

- [ ] **Step 2: Run test to verify it fails**

Run: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; .\.venv\Scripts\python.exe -m pytest -m integration tests/integration/test_memory_postgres.py::test_blocks_returns_all_scope_blocks -q`
Expected: FAIL (`AttributeError: 'PostgresMemoryStore' object has no attribute 'blocks'`).

- [ ] **Step 3: Implement**

Add to `PostgresMemoryStore` (after `history`):
```python
    async def blocks(self) -> dict[str, str]:
        """Return all of the scope's memory blocks as ``{key: value}``."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text("SELECT key, value FROM memory_blocks WHERE scope_id = :scope"),
                    {"scope": self._scope_id},
                )
            ).all()
        return {str(row.key): str(row.value) for row in rows}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; .\.venv\Scripts\python.exe -m pytest -m integration tests/integration/test_memory_postgres.py::test_blocks_returns_all_scope_blocks -q`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format packages/keel-core/src/keel_core/memory.py tests/integration/test_memory_postgres.py
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/memory.py tests/integration/test_memory_postgres.py
.\.venv\Scripts\python.exe -m mypy packages
git -c core.safecrlf=false add packages/keel-core/src/keel_core/memory.py tests/integration/test_memory_postgres.py
git commit -m "feat(memory): PostgresMemoryStore.blocks() lists a scope's blocks

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 4: Core-memory self-edit tools

**Files:**
- Modify: `packages/keel-core/src/keel_core/memory.py` (add 3 tool classes)
- Test: `tests/integration/test_memory_postgres.py` (extend)

**Interfaces:**
- Consumes: `PostgresMemoryStore` (`get`, `set`); `ToolContext.scope_id`; `ToolResult`.
- Produces: `MemoryAppendTool(engine, *, max_chars=2000)`, `MemoryReplaceTool(engine, *, max_chars=2000)`, `MemoryRethinkTool(engine, *, max_chars=2000)` — each has `name`, `description`, `writes=True`, `input_schema()`, `async run(args, ctx) -> ToolResult`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/integration/test_memory_postgres.py` (add imports `from keel_core.memory import MemoryAppendTool, MemoryReplaceTool, MemoryRethinkTool` and `from keel_core.protocols import ToolContext`):
```python
def _ctx(scope: str) -> ToolContext:
    return ToolContext(scope_id=scope, session_id="s")


async def test_memory_append_creates_and_grows(migrated_db: AsyncEngine) -> None:
    tool = MemoryAppendTool(migrated_db, max_chars=100)
    assert (await tool.run({"block": "human", "content": "name is X"}, _ctx("u:ma"))).ok
    assert (await tool.run({"block": "human", "content": "likes tea"}, _ctx("u:ma"))).ok
    assert await PostgresMemoryStore(migrated_db, "u:ma").get("human") == "name is X\nlikes tea"


async def test_memory_append_over_cap_errors(migrated_db: AsyncEngine) -> None:
    result = await MemoryAppendTool(migrated_db, max_chars=5).run(
        {"block": "human", "content": "way too long"}, _ctx("u:mc")
    )
    assert not result.ok and "exceed" in result.output


async def test_memory_replace_missing_old_errors(migrated_db: AsyncEngine) -> None:
    result = await MemoryReplaceTool(migrated_db).run(
        {"block": "human", "old": "nope", "new": "x"}, _ctx("u:mr")
    )
    assert not result.ok and "not found" in result.output


async def test_memory_rethink_overwrites(migrated_db: AsyncEngine) -> None:
    store = PostgresMemoryStore(migrated_db, "u:mk")
    await store.set("persona", "old value")
    assert (await MemoryRethinkTool(migrated_db).run(
        {"block": "persona", "content": "new value"}, _ctx("u:mk")
    )).ok
    assert await store.get("persona") == "new value"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; .\.venv\Scripts\python.exe -m pytest -m integration tests/integration/test_memory_postgres.py -q -k "append or replace or rethink"`
Expected: FAIL (`ImportError`).

- [ ] **Step 3: Implement**

Add to `memory.py` (top-of-file imports need `from typing import Any` and `from keel_core.protocols import ToolContext, ToolResult`), then append these classes:
```python
class _MemoryTool:
    writes = True

    def __init__(self, engine: AsyncEngine, *, max_chars: int = 2000) -> None:
        self._engine = engine
        self._max_chars = max_chars

    def _store(self, ctx: ToolContext) -> PostgresMemoryStore:
        return PostgresMemoryStore(self._engine, ctx.scope_id)


class MemoryAppendTool(_MemoryTool):
    name = "memory_append"
    description = "Append a line to one of your core memory blocks (creates it if absent)."

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"block": {"type": "string"}, "content": {"type": "string"}},
            "required": ["block", "content"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        store = self._store(ctx)
        block, content = str(args.get("block", "")), str(args.get("content", ""))
        current = await store.get(block) or ""
        updated = f"{current}\n{content}".strip() if current else content
        if len(updated) > self._max_chars:
            return ToolResult(
                ok=False,
                output=f"block '{block}' would exceed {self._max_chars} chars; "
                "use memory_rethink to summarize.",
            )
        version = await store.set(block, updated)
        return ToolResult(ok=True, output=f"appended to '{block}' (v{version})")


class MemoryReplaceTool(_MemoryTool):
    name = "memory_replace"
    description = "Replace the first occurrence of old with new in a core memory block."

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "block": {"type": "string"},
                "old": {"type": "string"},
                "new": {"type": "string"},
            },
            "required": ["block", "old", "new"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        store = self._store(ctx)
        block, old, new = str(args.get("block", "")), str(args.get("old", "")), str(args.get("new", ""))
        current = await store.get(block)
        if current is None or old not in current:
            return ToolResult(ok=False, output=f"'{old}' not found in block '{block}'")
        updated = current.replace(old, new, 1)
        if len(updated) > self._max_chars:
            return ToolResult(ok=False, output=f"block '{block}' would exceed {self._max_chars} chars")
        version = await store.set(block, updated)
        return ToolResult(ok=True, output=f"replaced in '{block}' (v{version})")


class MemoryRethinkTool(_MemoryTool):
    name = "memory_rethink"
    description = "Overwrite a core memory block entirely (for compaction or correction)."

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"block": {"type": "string"}, "content": {"type": "string"}},
            "required": ["block", "content"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        block, content = str(args.get("block", "")), str(args.get("content", ""))
        if len(content) > self._max_chars:
            return ToolResult(ok=False, output=f"content exceeds {self._max_chars} chars")
        version = await self._store(ctx).set(block, content)
        return ToolResult(ok=True, output=f"rewrote '{block}' (v{version})")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; .\.venv\Scripts\python.exe -m pytest -m integration tests/integration/test_memory_postgres.py -q`
Expected: PASS (all, including the earlier `blocks()` test).

- [ ] **Step 5: Lint + commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format packages/keel-core/src/keel_core/memory.py tests/integration/test_memory_postgres.py
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/memory.py tests/integration/test_memory_postgres.py
.\.venv\Scripts\python.exe -m mypy packages
git -c core.safecrlf=false add packages/keel-core/src/keel_core/memory.py tests/integration/test_memory_postgres.py
git commit -m "feat(memory): memory_append/replace/rethink self-edit tools

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 5: Inject core memory into the loop (`system_context`)

**Files:**
- Modify: `packages/keel-core/src/keel_core/loop.py` (`_build_request` line ~217, `_agent_loop` line ~386 + its calls at ~432/545/691, `run` ~492, `resume` ~609)
- Test: `tests/unit/test_loop_memory.py` (create)

**Interfaces:**
- Produces: `run(..., system_context: SystemContextFn | None = None)` and `resume(..., system_context=None)`, where `SystemContextFn = Callable[[], Awaitable[str]]`. When provided, its returned text is inserted as `messages[0] = {"role": "system", "content": text}` **each turn** (skipped when empty).

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_loop_memory.py`:
```python
"""Core-memory injection into the loop (Approach A: re-read each turn)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from keel_core.agents import AgentSpec, Scope
from keel_core.loop import admit, run
from keel_core.protocols import ProviderChunk, ProviderRequest
from keel_core.state import InMemoryEventStore
from keel_core.types import FinishReason, ScopeKind


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

    await run(agent=_agent(), session_id="s", store=store, provider=provider, system_context=ctx)
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
    await run(agent=_agent(), session_id="s", store=store, provider=p1, system_context=ctx)
    assert p1.requests[0].messages[0]["content"] == "before"

    holder["v"] = "after"  # a self-edit lands between runs
    await admit(store, "s", "u", "again")
    p2 = _CapturingProvider(["r2"])
    await run(agent=_agent(), session_id="s", store=store, provider=p2, system_context=ctx)
    assert p2.requests[0].messages[0]["content"] == "after"  # re-read, not cached


async def test_no_system_context_leaves_messages_unchanged() -> None:
    store = InMemoryEventStore()
    await admit(store, "s", "u", "hi")
    provider = _CapturingProvider(["ok"])
    await run(agent=_agent(), session_id="s", store=store, provider=provider)
    assert all(m["role"] != "system" for m in provider.requests[0].messages)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_loop_memory.py -q`
Expected: FAIL (`run() got an unexpected keyword argument 'system_context'`).

- [ ] **Step 3: Implement (four edits in `loop.py`)**

(a) Add the type alias near the top (after the existing imports / type aliases):
```python
SystemContextFn = Callable[[], Awaitable[str]]
```

(b) `_build_request` — add the param and the injection:
```python
async def _build_request(
    agent: AgentSpec,
    store: EventStore,
    session_id: SessionId,
    registry: ToolRegistry,
    system_context: SystemContextFn | None = None,
) -> ProviderRequest:
    events = [event async for event in store.read(session_id)]
    messages = project_messages(events)
    if system_context is not None:
        text = await system_context()
        if text:
            messages.insert(0, {"role": "system", "content": text})
    return ProviderRequest(model=agent.model, messages=messages, tools=registry.schemas())
```

(c) `_agent_loop` — add `system_context: SystemContextFn | None = None` to its keyword params (after `start_iteration: int = 0`) and pass it in its `_build_request` call:
```python
        request = await _build_request(agent, store, session_id, registry, system_context)
```

(d) `run` and `resume` — add `system_context: SystemContextFn | None = None` to each signature, and add `system_context=system_context,` to each `_agent_loop(...)` call (the calls at ~545 and ~691).

- [ ] **Step 4: Run tests to verify they pass**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_loop_memory.py tests/unit/test_loop.py tests/unit/test_loop_suspend_resume.py -q`
Expected: PASS (new tests + no regression in existing loop tests).

- [ ] **Step 5: Lint + commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format packages/keel-core/src/keel_core/loop.py tests/unit/test_loop_memory.py
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/loop.py tests/unit/test_loop_memory.py
.\.venv\Scripts\python.exe -m mypy packages
git -c core.safecrlf=false add packages/keel-core/src/keel_core/loop.py tests/unit/test_loop_memory.py
git commit -m "feat(loop): system_context seam injects core memory each turn

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 6: `ArchivalInsertTool`

**Files:**
- Modify: `packages/keel-core/src/keel_core/search.py` (add class)
- Test: `tests/integration/test_search_postgres.py` (extend)

**Interfaces:**
- Consumes: `ArchivalStore(engine, scope_id, embedder)` (`add`, `search`); `Embedder`; `ToolContext.scope_id`.
- Produces: `ArchivalInsertTool(engine, embedder)` — `name="archival_insert"`, `writes=True`, `input_schema()`, `async run(args, ctx) -> ToolResult`.

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/test_search_postgres.py`:
```python
async def test_archival_insert_then_search(migrated_db: AsyncEngine) -> None:
    from keel_core.embeddings import FakeEmbedder
    from keel_core.protocols import ToolContext
    from keel_core.search import ArchivalInsertTool, ArchivalSearchTool

    embedder = FakeEmbedder(dim=16)
    ctx = ToolContext(scope_id="u:arch", session_id="s")
    inserted = await ArchivalInsertTool(migrated_db, embedder).run(
        {"content": "the capital of France is Paris"}, ctx
    )
    assert inserted.ok
    found = await ArchivalSearchTool(migrated_db, embedder).run(
        {"query": "France capital", "k": 3}, ctx
    )
    assert found.ok and "Paris" in found.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; .\.venv\Scripts\python.exe -m pytest -m integration tests/integration/test_search_postgres.py::test_archival_insert_then_search -q`
Expected: FAIL (`ImportError: cannot import name 'ArchivalInsertTool'`).

- [ ] **Step 3: Implement**

Add to `search.py` (after `ArchivalSearchTool`):
```python
class ArchivalInsertTool:
    """``archival_insert`` (P3): save a passage to scope-bound archival memory."""

    name = "archival_insert"
    description = "Save a passage to your archival memory for later semantic recall."
    writes = True

    def __init__(self, engine: AsyncEngine, embedder: Embedder) -> None:
        self._engine = engine
        self._embedder = embedder

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"content": {"type": "string"}},
            "required": ["content"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        store = ArchivalStore(self._engine, ctx.scope_id, self._embedder)
        row_id = await store.add(str(args.get("content", "")))
        return ToolResult(ok=True, output=f"saved to archival memory (id={row_id})")
```
> Note: `writes` only affects the executor's **parallel-conflict scheduling** (`_conflicts`: a write serializes against conflicting calls in the same batch) — it does **not** affect permission. Permission is set entirely by the name-based `allow` rule added in Task 8, so `archival_insert` runs without an approval despite `writes=True`. We mark this DB-writing insert `writes=True` (like the memory tools) so concurrent writes serialize safely.

- [ ] **Step 4: Run test to verify it passes**

Run: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; .\.venv\Scripts\python.exe -m pytest -m integration tests/integration/test_search_postgres.py -q`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format packages/keel-core/src/keel_core/search.py tests/integration/test_search_postgres.py
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/search.py tests/integration/test_search_postgres.py
.\.venv\Scripts\python.exe -m mypy packages
git -c core.safecrlf=false add packages/keel-core/src/keel_core/search.py tests/integration/test_search_postgres.py
git commit -m "feat(search): archival_insert tool

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 7: Settings + package exports

**Files:**
- Modify: `packages/keel-core/src/keel_core/config.py` (+4 settings)
- Modify: `packages/keel-core/src/keel_core/__init__.py` (exports)
- Test: `tests/unit/test_config.py` (extend)

**Interfaces:**
- Produces: `Settings.embedding_model/embedding_dim/embedding_send_dimensions/memory_block_max_chars`; package exports `format_core_memory`, `MemoryAppendTool`, `MemoryReplaceTool`, `MemoryRethinkTool`, `ArchivalInsertTool`.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_config.py`:
```python
def test_memory_embedding_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.embedding_model == "ollama/bge-m3"
    assert settings.embedding_dim == 1024
    assert settings.embedding_send_dimensions is False
    assert settings.memory_block_max_chars == 2000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_config.py::test_memory_embedding_defaults -q`
Expected: FAIL (`AttributeError: 'Settings' object has no attribute 'embedding_model'`).

- [ ] **Step 3: Implement**

In `config.py`, add after `default_model`/`fallback_models`:
```python
    # Embeddings for archival memory / RAG (ADR-0007). Empty embedding_model -> archival off
    # (core memory still works, it is embedding-free).
    embedding_model: str = "ollama/bge-m3"
    embedding_dim: int = 1024
    embedding_send_dimensions: bool = False  # OpenAI text-embedding-3 needs it; Ollama rejects it
    memory_block_max_chars: int = 2000
```
In `__init__.py`, add to the `keel_core.memory`/`keel_core.search` import groups and `__all__`:
```python
from .memory import (
    MemoryAppendTool,
    MemoryReplaceTool,
    MemoryRethinkTool,
    PostgresMemoryStore,
    format_core_memory,
)
from .search import ArchivalInsertTool  # add to the existing search import group
```
(Add `"MemoryAppendTool"`, `"MemoryReplaceTool"`, `"MemoryRethinkTool"`, `"format_core_memory"`, `"ArchivalInsertTool"` to `__all__`. Keep `PostgresMemoryStore` if already exported.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_config.py -q; .\.venv\Scripts\python.exe -c "import keel_core; keel_core.MemoryAppendTool; keel_core.format_core_memory; keel_core.ArchivalInsertTool"`
Expected: PASS + no import error.

- [ ] **Step 5: Lint + commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format packages/keel-core/src/keel_core/config.py packages/keel-core/src/keel_core/__init__.py tests/unit/test_config.py
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/config.py packages/keel-core/src/keel_core/__init__.py tests/unit/test_config.py
.\.venv\Scripts\python.exe -m mypy packages
git -c core.safecrlf=false add packages/keel-core/src/keel_core/config.py packages/keel-core/src/keel_core/__init__.py tests/unit/test_config.py
git commit -m "feat(config): embedding + memory settings; export memory/archival tools

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 8: Runtime wiring (tools + permissions + `system_context`)

**Files:**
- Modify: `packages/keel-server/src/keel_server/runtime.py`
- Modify: `packages/keel-server/src/keel_server/app.py` (pass settings)
- Test: `tests/unit/test_runtime_memory.py` (create)

**Interfaces:**
- Consumes: `_build_tools`, `_READ_ONLY`, `_MUTATING`, `LiteLLMEmbedder`, `PostgresMemoryStore`, `format_core_memory`, `MemoryAppendTool/ReplaceTool/RethinkTool`, `ArchivalInsertTool`, `ArchivalSearchTool`, `SessionSearchTool`, `run(system_context=…)`.
- Produces: `_build_memory_tools(engine, embedder, *, cap) -> list[ToolProto]`; `_web_permissions(memory_allow: tuple[str, ...] = ()) -> RuleBasedPermissionEngine`; `AgentRuntime(..., embedder=None, embedding_model="ollama/bge-m3", embedding_dim=1024, embedding_send_dimensions=False, memory_block_max_chars=2000)` registering the memory triad and injecting core memory.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_runtime_memory.py`:
```python
"""Runtime wiring: memory/archival tool registration + permissions."""

from __future__ import annotations

from keel_core.embeddings import FakeEmbedder
from keel_core.permissions import RuleBasedPermissionEngine
from keel_core.protocols import ToolContext
from keel_core.types import PermissionDecision
from sqlalchemy.ext.asyncio import create_async_engine

from keel_server.runtime import _build_memory_tools, _web_permissions

_ENGINE = create_async_engine("postgresql+psycopg://keel:keel@localhost:5432/keel")  # not connected


def test_build_memory_tools_includes_archival_with_embedder() -> None:
    names = {t.name for t in _build_memory_tools(_ENGINE, FakeEmbedder(dim=16), cap=2000)}
    assert names == {
        "memory_append",
        "memory_replace",
        "memory_rethink",
        "session_search",
        "archival_insert",
        "archival_search",
    }


def test_build_memory_tools_omits_archival_without_embedder() -> None:
    names = {t.name for t in _build_memory_tools(_ENGINE, None, cap=2000)}
    assert "archival_insert" not in names and "archival_search" not in names
    assert {"memory_append", "session_search"} <= names  # memory + lexical recall still there


def test_web_permissions_allow_memory_tools() -> None:
    perms: RuleBasedPermissionEngine = _web_permissions(("memory_append", "archival_insert"))
    ctx = ToolContext(scope_id="u", session_id="s")
    assert perms.evaluate("memory_append", {}, ctx) is PermissionDecision.allow
    assert perms.evaluate("archival_insert", {}, ctx) is PermissionDecision.allow
    assert perms.evaluate("write", {}, ctx) is PermissionDecision.ask  # mutating still gated
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_runtime_memory.py -q`
Expected: FAIL (`ImportError: cannot import name '_build_memory_tools'`).

- [ ] **Step 3: Implement (`runtime.py`)**

Add imports at the top:
```python
from keel_core import (
    ArchivalInsertTool,
    MemoryAppendTool,
    MemoryReplaceTool,
    MemoryRethinkTool,
    format_core_memory,
)
from keel_core.embeddings import Embedder, LiteLLMEmbedder
from keel_core.memory import PostgresMemoryStore
from keel_core.search import ArchivalSearchTool, SessionSearchTool
```
Add the tool-name groups next to `_READ_ONLY`/`_MUTATING`:
```python
_MEMORY = ("memory_append", "memory_replace", "memory_rethink")
_RECALL = ("session_search",)
_ARCHIVAL = ("archival_insert", "archival_search")
```
Add the builder (after `_build_tools`):
```python
def _build_memory_tools(
    engine: AsyncEngine, embedder: Embedder | None, *, cap: int
) -> list[ToolProto]:
    """Core-memory self-edit + lexical recall (always) + archival (only with an embedder)."""
    tools: list[ToolProto] = [
        MemoryAppendTool(engine, max_chars=cap),
        MemoryReplaceTool(engine, max_chars=cap),
        MemoryRethinkTool(engine, max_chars=cap),
        SessionSearchTool(engine),
    ]
    if embedder is not None:
        tools += [ArchivalInsertTool(engine, embedder), ArchivalSearchTool(engine, embedder)]
    return tools
```
Change `_web_permissions` to accept the memory allow-list:
```python
def _web_permissions(memory_allow: tuple[str, ...] = ()) -> RuleBasedPermissionEngine:
    """Read-only + own-scope memory tools allowed; mutating tools require an approval."""
    rules = [Rule(name, PermissionDecision.allow) for name in _READ_ONLY]
    rules += [Rule(name, PermissionDecision.allow) for name in memory_allow]
    rules += [Rule(name, PermissionDecision.ask) for name in _MUTATING]
    return RuleBasedPermissionEngine(rules, default=PermissionDecision.ask)
```
In `AgentRuntime.__init__`, add params and build the tools/embedder/permissions/context. Replace the `_agent`/`_registry`/`_permissions` assignment block (lines ~156-165):
```python
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
    ) -> None:
        self._engine = engine
        self._fanout = RedisEventStore(redis_client)
        self._memory = InMemoryEventStore()
        self._scope = Scope(id=scope_id, kind=ScopeKind.personal, trust=TrustLevel.trusted)
        if embedder is None and engine is not None and embedding_model:
            embedder = LiteLLMEmbedder(
                embedding_model, embedding_dim, send_dimensions=embedding_send_dimensions
            )
        self._embedder = embedder
        memory_tools: list[ToolProto] = (
            _build_memory_tools(engine, embedder, cap=memory_block_max_chars)
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
        self._tracer = make_tracer()
        self._runs: dict[RunId, asyncio.Task[None]] = {}
        self._interrupted: set[RunId] = set()
```
Add the context method (near `_store`):
```python
    async def _core_memory_context(self) -> str:
        if self._engine is None:
            return ""
        blocks = await PostgresMemoryStore(self._engine, self._scope.id).blocks()
        return format_core_memory(blocks)
```
In `_run`, pass the context to `run(...)` (add one kwarg to the existing call):
```python
                system_context=self._core_memory_context,
```

- [ ] **Step 4: Implement (`app.py`) — pass settings**

In `app.py` `_lifespan`, extend the `AgentRuntime(...)` construction:
```python
    app.state.runtime = AgentRuntime(
        redis_client=redis_client,
        engine=engine,
        model=settings.default_model,
        workspace=Path.cwd(),
        embedding_model=settings.embedding_model,
        embedding_dim=settings.embedding_dim,
        embedding_send_dimensions=settings.embedding_send_dimensions,
        memory_block_max_chars=settings.memory_block_max_chars,
    )
```

- [ ] **Step 5: Run tests + regression**

Run: `.\.venv\Scripts\python.exe -m pytest tests/unit/test_runtime_memory.py tests/unit/test_server.py tests/unit/test_runtime_interrupt.py -q`
Expected: PASS (new tests + no runtime/server regression).

- [ ] **Step 6: Lint + typecheck + commit**

```powershell
.\.venv\Scripts\python.exe -m ruff format packages/keel-server/src/keel_server/runtime.py packages/keel-server/src/keel_server/app.py tests/unit/test_runtime_memory.py
.\.venv\Scripts\python.exe -m ruff check packages/keel-server tests/unit/test_runtime_memory.py
.\.venv\Scripts\python.exe -m mypy packages
git -c core.safecrlf=false add packages/keel-server/src/keel_server/runtime.py packages/keel-server/src/keel_server/app.py tests/unit/test_runtime_memory.py
git commit -m "feat(runtime): wire core+archival+recall tools and core-memory injection

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 9: Ollama service + full live-verification

**Files:**
- Modify: `docker-compose.yml` (add `ollama` + `ollama-pull`; add env + depends_on to `keel-server` and `keel-worker`; add the `ollama` named volume)

**Interfaces:** none (infra + end-to-end verification).

- [ ] **Step 1: Add the Ollama services**

In `docker-compose.yml`, add under `services:`:
```yaml
  ollama:
    image: ollama/ollama:latest
    volumes: ["ollama:/root/.ollama"]
    healthcheck:
      test: ["CMD-SHELL", "ollama list >/dev/null 2>&1 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 20
      start_period: 10s
    profiles: ["dev", "full"]

  ollama-pull:
    image: ollama/ollama:latest
    environment: { OLLAMA_HOST: "http://ollama:11434" }
    entrypoint: ["/bin/sh", "-c", "ollama pull bge-m3"]
    depends_on:
      ollama:
        condition: service_healthy
    restart: "no"
    profiles: ["dev", "full"]
```
Add to the top-level `volumes:` block: `  ollama:`.
Add to **both** `keel-server` and `keel-worker` `environment:` maps:
```yaml
      OLLAMA_API_BASE: "http://ollama:11434"
```
Add to **both** `keel-server` and `keel-worker` `depends_on:` maps:
```yaml
      ollama-pull:
        condition: service_completed_successfully
```

- [ ] **Step 2: Bring up Ollama + pull the model + verify the embedder**

```powershell
cd C:\src\keel
docker compose --profile dev up -d ollama
# wait for healthy, then pull:
docker compose --profile dev up ollama-pull
docker compose exec -T ollama ollama list          # expect bge-m3 listed
# verify a 1024-dim embedding:
docker compose exec -T ollama sh -c "curl -s http://localhost:11434/api/embeddings -d '{\"model\":\"bge-m3\",\"prompt\":\"hello\"}'" | python -c "import sys,json; print(len(json.load(sys.stdin)['embedding']))"
```
Expected: `bge-m3` in the list; the last command prints `1024`.

- [ ] **Step 3: Rebuild + recreate server/worker on the new image + config**

```powershell
docker compose build keel-server keel-worker
docker compose up -d --force-recreate keel-server keel-worker
docker compose ps   # server + worker healthy; ollama healthy; ollama-pull exited 0
```

- [ ] **Step 4: Live-verify archival memory (semantic)**

Send a chat that saves a passage, then one that recalls it (via the running assistant on `http://localhost:8000`). Using the session API:
```powershell
# 1) save a fact
$b1 = '{"content":"Please remember: the launch code phrase is BLUE-HERON-42."}'
Invoke-RestMethod -Uri 'http://localhost:8000/v1/sessions/mem-live/messages' -Method POST -ContentType 'application/json' -Body $b1
Start-Sleep 12
# 2) new session: ask to recall via archival search
$b2 = '{"content":"Search your archival memory: what is the launch code phrase?"}'
Invoke-RestMethod -Uri 'http://localhost:8000/v1/sessions/mem-live-2/messages' -Method POST -ContentType 'application/json' -Body $b2
Start-Sleep 12
docker compose logs keel-server --since 40s | Select-String -Pattern 'archival_insert|archival_search|BLUE-HERON'
```
Expected: logs show the agent called `archival_insert` then `archival_search`, and the reply contains `BLUE-HERON-42`.

- [ ] **Step 5: Live-verify core memory (cross-session)**

```powershell
$c1 = '{"content":"记住我叫大中，我在做 Keel 项目。"}'
Invoke-RestMethod -Uri 'http://localhost:8000/v1/sessions/cm-live/messages' -Method POST -ContentType 'application/json' -Body $c1
Start-Sleep 12
docker compose exec -T postgres psql -U keel -d keel -c "SELECT key, value FROM memory_blocks WHERE scope_id='web:local';"
# NEW session — must recall from injected core memory:
$c2 = '{"content":"我叫什么名字？我在做什么项目？"}'
Invoke-RestMethod -Uri 'http://localhost:8000/v1/sessions/cm-live-2/messages' -Method POST -ContentType 'application/json' -Body $c2
Start-Sleep 12
docker compose logs keel-server --since 30s | Select-String -Pattern '大中|Keel|memory_append'
```
Expected: `memory_blocks` shows a `human` (and/or `persona`) block mentioning 大中/Keel; the **new** session's reply names 大中 + Keel (proving injection + cross-session).

- [ ] **Step 6: Live-verify graceful degradation**

```powershell
docker compose stop ollama
$d = '{"content":"Save this to archival memory: test note."}'
Invoke-RestMethod -Uri 'http://localhost:8000/v1/sessions/deg-live/messages' -Method POST -ContentType 'application/json' -Body $d
Start-Sleep 10
docker compose logs keel-server --since 20s | Select-String -Pattern 'error|archival'   # archival errors, no crash
docker compose start ollama
```
Expected: the chat completes (assistant reports the archival save failed); the server does not crash; core-memory chat still works.

- [ ] **Step 7: Commit**

```powershell
git -c core.safecrlf=false add docker-compose.yml
git commit -m "feat(infra): Ollama bge-m3 embedder service for archival memory

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Final verification (after all tasks)

```powershell
cd C:\src\keel; $env:PYTHONUTF8=1; $env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests -q            # full backend suite green
.\.venv\Scripts\python.exe -m ruff check packages tests
.\.venv\Scripts\python.exe -m mypy packages
```
Expected: all tests pass; ruff + mypy clean.

---

## Self-review notes (author checklist — completed)

- **Spec coverage:** §4 core memory → Tasks 2/3/4/5/8; §5 archival + embedder → Tasks 1/6/8/9; §6 Ollama → Task 9; §7 settings → Task 7; §8 runtime wiring → Task 8; §10 testing → each task's tests + Task 9 live checks. All spec sections map to tasks.
- **Placeholder scan:** every code step contains full code; no TBD/"similar to"/vague error handling.
- **Type consistency:** `system_context: SystemContextFn` (`Callable[[], Awaitable[str]]`) is used identically in `_build_request`/`_agent_loop`/`run`/`resume`; `_build_memory_tools(engine, embedder, *, cap)` and `_web_permissions(memory_allow)` signatures match their call sites in `AgentRuntime.__init__`; tool `name` strings (`memory_append`/`memory_replace`/`memory_rethink`/`archival_insert`/`archival_search`/`session_search`) are identical across Tasks 4/6/7/8; `format_core_memory` and the 4 settings names match the spec §11 summary.
