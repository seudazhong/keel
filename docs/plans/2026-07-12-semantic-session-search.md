# Semantic Session Search Implementation Plan

> **Execution:** Work through the checklist task-by-task and run the narrowest applicable validation before advancing.

**Goal:** Upgrade historical-session search to a scope-bound lexical + semantic RRF pipeline, with background message indexing, search-time catch-up, and identical ranking for the agent `session_search` tool and Sessions API.

**Architecture:** `events` remains the append-only source of truth. A new `message_embeddings` projection stores complete user/assistant messages pinned to `(model,dim)`; `MessageEmbeddingIndexer` fills it after web runs and opportunistically before searches. A single `rank_session_messages()` pipeline fuses trigram/FTS and pgvector ranks; wrapper functions map it to message-level tool hits or session-level API hits.

**Tech Stack:** Python, FastAPI, SQLAlchemy async, Postgres 16 + pgvector + pg_trgm, LiteLLM/Ollama `bge-m3`, pytest, Alembic.

**Design:** `docs/designs/2026-07-11-semantic-session-search-design.md`

## Global Constraints

- Use Windows PowerShell and Windows paths. Invoke Python as `.\.venv\Scripts\python.exe`; bare `python` does not have workspace dependencies.
- Integration tests MUST use `$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"` and `-m integration`. The `migrated_db` fixture truncates tables; never point it at live `keel`.
- Only complete, non-empty `message.token` events with role `user` or `assistant` are embedded. Exclude partial, system, tool-call/result, and empty content.
- Every scoped SQL transaction MUST set `app.scope_id` and explicitly filter `scope_id`; `message_embeddings` MUST have RLS.
- Keep `(event_id, model, dim)` idempotency and filter semantic KNN by the current `(model,dim)`.
- Semantic failure MUST be explicit: API header `X-Keel-Search-Mode: lexical-degraded`, tool prefix `[semantic unavailable; lexical results only]`, and an error log. Do not silently present lexical fallback as full hybrid success.
- Existing calls with no embedder remain backward-compatible lexical-only; the Sessions API list body shape remains unchanged.
- Run `ruff format`, `ruff check`, and `mypy packages` after code changes. Run the smallest targeted tests first, then the full backend suite at the end.
- Stage with `git -c core.safecrlf=false add`. Every commit ends with:
  `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`
  and `Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49`.

---

## File Structure

| File | Responsibility |
|---|---|
| `migrations/versions/0007_message_embeddings.py` | Projection table, indexes, FK cascade, RLS |
| `packages/keel-core/src/keel_core/recall.py` | Projection indexing, catch-up, message-level hybrid ranking |
| `packages/keel-core/src/keel_core/search.py` | Backward-compatible wrappers, session grouping, tool behavior |
| `packages/keel-core/src/keel_core/config.py` | Batch/catch-up settings |
| `packages/keel-core/src/keel_core/__init__.py` | Public exports |
| `packages/keel-server/src/keel_server/runtime.py` | Post-run indexing tasks, lifecycle, tool wiring, embedder/config properties |
| `packages/keel-server/src/keel_server/app.py` | Runtime settings + shutdown ordering |
| `packages/keel-server/src/keel_server/api/v1.py` | Hybrid Sessions endpoint + mode header |
| `tests/integration/test_recall_postgres.py` | Schema, indexer, backfill, RRF, scope/model/cascade tests |
| `tests/integration/test_search_postgres.py` | Public wrapper/tool integration |
| `tests/integration/test_sessions_api.py` | API header + unchanged body tests |
| `tests/unit/test_runtime_recall.py` | Runtime background-task lifecycle |
| `tests/unit/test_config.py` | New defaults |
| `tests/integration/conftest.py` | Truncate projection table |

---

### Task 1: Add the `message_embeddings` projection schema

**Files:**
- Create: `migrations/versions/0007_message_embeddings.py`
- Create: `tests/integration/test_recall_postgres.py`
- Modify: `tests/integration/conftest.py:80-86`

**Interfaces:**
- Consumes: existing `events(id, session_id, scope_id, seq, payload)` and pgvector extension.
- Produces: `message_embeddings` keyed by `(event_id, model, dim)`, with `ON DELETE CASCADE`, RLS, and scope/session/model indexes.

- [ ] **Step 1: Write the failing schema test**

Create `tests/integration/test_recall_postgres.py`:

```python
"""Integration: semantic session-recall projection and ranking."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


async def test_message_embeddings_schema_and_rls(migrated_db: AsyncEngine) -> None:
    async with migrated_db.connect() as conn:
        table_count = await conn.scalar(
            text(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'message_embeddings'"
            )
        )
        rls = await conn.scalar(
            text(
                "SELECT relrowsecurity FROM pg_class "
                "WHERE relname = 'message_embeddings'"
            )
        )
        columns = {
            str(row.column_name)
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'public' "
                        "AND table_name = 'message_embeddings'"
                    )
                )
            )
        }

    assert table_count == 1
    assert rls is True
    assert columns == {
        "event_id",
        "scope_id",
        "session_id",
        "seq",
        "role",
        "content",
        "model",
        "dim",
        "embedding",
        "created_at",
    }
```

- [ ] **Step 2: Run it and confirm RED**

```powershell
cd C:\src\keel
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_recall_postgres.py::test_message_embeddings_schema_and_rls -q
```

Expected: FAIL because `message_embeddings` does not exist.

- [ ] **Step 3: Create the migration**

Create `migrations/versions/0007_message_embeddings.py`:

```python
"""semantic session recall: message embedding projection

Revision ID: 0007_message_embeddings
Revises: 0006_session_search_idx
Create Date: 2026-07-12
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0007_message_embeddings"
down_revision: str | None = "0006_session_search_idx"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE message_embeddings (
            event_id bigint NOT NULL REFERENCES events(id) ON DELETE CASCADE,
            scope_id text NOT NULL,
            session_id text NOT NULL,
            seq bigint NOT NULL,
            role text NOT NULL CHECK (role IN ('user', 'assistant')),
            content text NOT NULL,
            model text NOT NULL,
            dim int NOT NULL,
            embedding vector NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (event_id, model, dim)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_message_embeddings_scope_session "
        "ON message_embeddings (scope_id, session_id)"
    )
    op.execute(
        "CREATE INDEX ix_message_embeddings_scope_model_dim "
        "ON message_embeddings (scope_id, model, dim)"
    )
    op.execute("ALTER TABLE message_embeddings ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON message_embeddings "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS message_embeddings CASCADE")
```

- [ ] **Step 4: Add the projection to integration cleanup**

Change the truncate SQL in `tests/integration/conftest.py` to:

```python
        await conn.execute(
            text(
                "TRUNCATE message_embeddings, events, sessions, memory_blocks, "
                "memory_block_versions, connector_tokens, archival, schedules, approvals"
            )
        )
```

- [ ] **Step 5: Run GREEN + quality gates**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_recall_postgres.py -q
.\.venv\Scripts\python.exe -m ruff format migrations\versions\0007_message_embeddings.py tests\integration\test_recall_postgres.py tests\integration\conftest.py
.\.venv\Scripts\python.exe -m ruff check migrations\versions\0007_message_embeddings.py tests\integration\test_recall_postgres.py tests\integration\conftest.py
.\.venv\Scripts\python.exe -m mypy packages
```

Expected: schema test passes; ruff/mypy clean.

- [ ] **Step 6: Commit**

```powershell
git -c core.safecrlf=false add migrations\versions\0007_message_embeddings.py tests\integration\test_recall_postgres.py tests\integration\conftest.py
git commit -m "feat(recall): add message_embeddings projection schema`n`nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`nCopilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

### Task 2: Implement session message indexing

**Files:**
- Create: `packages/keel-core/src/keel_core/recall.py`
- Modify: `tests/integration/test_recall_postgres.py`

**Interfaces:**
- Produces:
  - `BackfillResult(indexed: int, remaining: bool)`
  - `MessageEmbeddingIndexer(engine, scope_id, embedder, *, batch_size=64)`
  - `async MessageEmbeddingIndexer.index_session(session_id) -> int`
- Guarantees: only complete non-empty user/assistant messages; scope-bound; batched embedding; conflict-safe inserts.

- [ ] **Step 1: Add event-seeding helpers and a failing index test**

Append to `tests/integration/test_recall_postgres.py`:

```python
import json
import uuid

from keel_core.embeddings import FakeEmbedder
from keel_core.recall import MessageEmbeddingIndexer


async def _seed_event(
    engine: AsyncEngine,
    scope: str,
    session_id: str,
    seq: int,
    *,
    role: str,
    content: str,
    partial: bool = False,
    event_type: str = "message.token",
) -> int:
    payload = {"role": role, "text": content}
    if partial:
        payload["partial"] = True
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope},
        )
        await conn.execute(
            text(
                "INSERT INTO sessions (id, scope_id, next_seq) "
                "VALUES (:sid, :scope, :next_seq) "
                "ON CONFLICT (id) DO UPDATE "
                "SET next_seq = GREATEST(sessions.next_seq, :next_seq)"
            ),
            {"sid": session_id, "scope": scope, "next_seq": seq + 1},
        )
        event_id = await conn.scalar(
            text(
                "INSERT INTO events "
                "(session_id, scope_id, seq, type, version, ts, payload) "
                "VALUES (:sid, :scope, :seq, :type, 1, now(), CAST(:payload AS jsonb)) "
                "RETURNING id"
            ),
            {
                "sid": session_id,
                "scope": scope,
                "seq": seq,
                "type": event_type,
                "payload": json.dumps(payload),
            },
        )
    assert event_id is not None
    return int(event_id)


async def _projection_rows(engine: AsyncEngine, scope: str) -> list[object]:
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope},
        )
        return list(
            (
                await conn.execute(
                    text(
                        "SELECT event_id, session_id, seq, role, content, model, dim "
                        "FROM message_embeddings "
                        "WHERE scope_id = :scope ORDER BY seq"
                    ),
                    {"scope": scope},
                )
            ).all()
        )


async def test_index_session_filters_and_persists_complete_messages(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await _seed_event(migrated_db, scope, session_id, 1, role="user", content="hello")
    await _seed_event(migrated_db, scope, session_id, 2, role="assistant", content="hi")
    await _seed_event(
        migrated_db, scope, session_id, 3, role="assistant", content="partial", partial=True
    )
    await _seed_event(migrated_db, scope, session_id, 4, role="system", content="hidden")
    await _seed_event(migrated_db, scope, session_id, 5, role="user", content="   ")
    await _seed_event(
        migrated_db,
        scope,
        session_id,
        6,
        role="tool",
        content="tool output",
        event_type="tool.result",
    )

    indexed = await MessageEmbeddingIndexer(
        migrated_db, scope, FakeEmbedder(dim=16, model="fake/recall")
    ).index_session(session_id)

    rows = await _projection_rows(migrated_db, scope)
    assert indexed == 2
    assert [(r.role, r.content) for r in rows] == [("user", "hello"), ("assistant", "hi")]
    assert all(r.model == "fake/recall" and r.dim == 16 for r in rows)
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_recall_postgres.py::test_index_session_filters_and_persists_complete_messages -q
```

Expected: FAIL because `keel_core.recall` does not exist.

- [ ] **Step 3: Create `recall.py` with the indexer**

Create `packages/keel-core/src/keel_core/recall.py`:

```python
"""Semantic recall projection: message indexing + hybrid message ranking."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import Embedder, rrf_fuse
from keel_core.types import ScopeId, SessionId

logger = logging.getLogger("keel.core.recall")

RecallMode = Literal["hybrid", "lexical", "lexical-degraded"]
_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")
_INSERT_EMBEDDING = text(
    "INSERT INTO message_embeddings "
    "(event_id, scope_id, session_id, seq, role, content, model, dim, embedding) "
    "VALUES (:event_id, :scope_id, :session_id, :seq, :role, :content, "
    ":model, :dim, CAST(:embedding AS vector)) "
    "ON CONFLICT (event_id, model, dim) DO NOTHING "
    "RETURNING event_id"
)


@dataclass(frozen=True)
class BackfillResult:
    indexed: int
    remaining: bool


@dataclass(frozen=True)
class RankedMessage:
    event_id: int
    session_id: str
    seq: int
    role: str
    content: str


@dataclass(frozen=True)
class RecallStatus:
    mode: RecallMode
    indexed: int = 0
    remaining: bool = False
    error: str | None = None


@dataclass(frozen=True)
class _MessageRow:
    event_id: int
    session_id: str
    seq: int
    role: str
    content: str


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in vector) + "]"


class MessageEmbeddingIndexer:
    """Build the rebuildable semantic projection for one scope."""

    def __init__(
        self,
        engine: AsyncEngine,
        scope_id: ScopeId,
        embedder: Embedder,
        *,
        batch_size: int = 64,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._engine = engine
        self._scope_id = scope_id
        self._embedder = embedder
        self._batch_size = batch_size

    async def index_session(self, session_id: SessionId) -> int:
        rows = await self._missing_rows(session_id=session_id)
        return await self._index_rows(rows)

    async def _missing_rows(
        self,
        *,
        session_id: SessionId | None = None,
        limit: int | None = None,
    ) -> list[_MessageRow]:
        sql = (
            "SELECT e.id AS event_id, e.session_id, e.seq, "
            "e.payload->>'role' AS role, e.payload->>'text' AS content "
            "FROM events e "
            "LEFT JOIN message_embeddings m "
            "ON m.event_id = e.id AND m.model = :model AND m.dim = :dim "
            "WHERE e.scope_id = :scope "
            "AND e.type = 'message.token' "
            "AND e.payload->>'role' IN ('user', 'assistant') "
            "AND COALESCE((e.payload->>'partial')::boolean, false) = false "
            "AND NULLIF(BTRIM(e.payload->>'text'), '') IS NOT NULL "
            "AND m.event_id IS NULL "
        )
        params: dict[str, object] = {
            "scope": self._scope_id,
            "model": self._embedder.model,
            "dim": self._embedder.dim,
        }
        if session_id is not None:
            sql += "AND e.session_id = :session_id "
            params["session_id"] = session_id
        sql += "ORDER BY e.id "
        if limit is not None:
            sql += "LIMIT :limit"
            params["limit"] = limit

        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (await conn.execute(text(sql), params)).all()
        return [
            _MessageRow(
                event_id=int(row.event_id),
                session_id=str(row.session_id),
                seq=int(row.seq),
                role=str(row.role),
                content=str(row.content),
            )
            for row in rows
        ]

    async def _index_rows(self, rows: list[_MessageRow]) -> int:
        inserted = 0
        for start in range(0, len(rows), self._batch_size):
            batch = rows[start : start + self._batch_size]
            vectors = await self._embedder.embed([row.content for row in batch])
            params = [
                {
                    "event_id": row.event_id,
                    "scope_id": self._scope_id,
                    "session_id": row.session_id,
                    "seq": row.seq,
                    "role": row.role,
                    "content": row.content,
                    "model": self._embedder.model,
                    "dim": self._embedder.dim,
                    "embedding": _vector_literal(vector),
                }
                for row, vector in zip(batch, vectors, strict=True)
            ]
            async with self._engine.begin() as conn:
                await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
                for item in params:
                    result = await conn.execute(_INSERT_EMBEDDING, item)
                    if result.scalar_one_or_none() is not None:
                        inserted += 1
        return inserted
```

- [ ] **Step 4: Run GREEN**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_recall_postgres.py -q
.\.venv\Scripts\python.exe -m ruff format packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
.\.venv\Scripts\python.exe -m mypy packages
```

Expected: schema + indexing tests pass.

- [ ] **Step 5: Commit**

```powershell
git -c core.safecrlf=false add packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
git commit -m "feat(recall): index complete session messages`n`nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`nCopilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

### Task 3: Add bounded catch-up and projection guarantees

**Files:**
- Modify: `packages/keel-core/src/keel_core/recall.py`
- Modify: `tests/integration/test_recall_postgres.py`

**Interfaces:**
- Produces: `async MessageEmbeddingIndexer.backfill_scope(*, limit=500) -> BackfillResult`.
- Guarantees: bounded catch-up, repeat/concurrent idempotency, multi-model coexistence, FK cascade.

- [ ] **Step 1: Write failing tests**

Append to `tests/integration/test_recall_postgres.py`:

```python
import asyncio


async def test_backfill_scope_is_bounded_and_resumable(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    for seq in range(1, 4):
        await _seed_event(
            migrated_db,
            scope,
            session_id,
            seq,
            role="user",
            content=f"message {seq}",
        )
    indexer = MessageEmbeddingIndexer(migrated_db, scope, FakeEmbedder(dim=8))

    first = await indexer.backfill_scope(limit=2)
    second = await indexer.backfill_scope(limit=2)

    assert first.indexed == 2 and first.remaining is True
    assert second.indexed == 1 and second.remaining is False
    assert len(await _projection_rows(migrated_db, scope)) == 3


async def test_index_session_is_repeat_and_concurrency_safe(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await _seed_event(migrated_db, scope, session_id, 1, role="user", content="one")
    indexer = MessageEmbeddingIndexer(migrated_db, scope, FakeEmbedder(dim=8))

    await asyncio.gather(indexer.index_session(session_id), indexer.index_session(session_id))
    assert len(await _projection_rows(migrated_db, scope)) == 1
    assert await indexer.index_session(session_id) == 0


async def test_projection_is_model_pinned_and_event_cascades(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    event_id = await _seed_event(
        migrated_db, scope, session_id, 1, role="user", content="one"
    )
    await MessageEmbeddingIndexer(
        migrated_db, scope, FakeEmbedder(dim=8, model="fake/a")
    ).index_session(session_id)
    await MessageEmbeddingIndexer(
        migrated_db, scope, FakeEmbedder(dim=16, model="fake/b")
    ).index_session(session_id)

    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope},
        )
        pin_rows = (
            await conn.execute(
                text(
                    "SELECT model, dim FROM message_embeddings "
                    "WHERE event_id = :event_id"
                ),
                {"event_id": event_id},
            )
        ).all()
        await conn.execute(
            text("DELETE FROM events WHERE id = :event_id AND scope_id = :scope"),
            {"event_id": event_id, "scope": scope},
        )
        remaining = await conn.scalar(
            text("SELECT count(*) FROM message_embeddings WHERE event_id = :event_id"),
            {"event_id": event_id},
        )

    assert {(str(row.model), int(row.dim)) for row in pin_rows} == {
        ("fake/a", 8),
        ("fake/b", 16),
    }
    assert remaining == 0
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_recall_postgres.py -q -k "backfill or concurrency or pinned"
```

Expected: FAIL because `backfill_scope` does not exist.

- [ ] **Step 3: Implement `backfill_scope`**

Add to `MessageEmbeddingIndexer` immediately after `index_session`:

```python
    async def backfill_scope(self, *, limit: int = 500) -> BackfillResult:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        rows = await self._missing_rows(limit=limit + 1)
        selected = rows[:limit]
        indexed = await self._index_rows(selected)
        return BackfillResult(indexed=indexed, remaining=len(rows) > limit)
```

- [ ] **Step 4: Run GREEN + all recall integration tests**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_recall_postgres.py -q
.\.venv\Scripts\python.exe -m ruff format packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
.\.venv\Scripts\python.exe -m mypy packages
```

- [ ] **Step 5: Commit**

```powershell
git -c core.safecrlf=false add packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
git commit -m "feat(recall): bounded idempotent embedding catch-up`n`nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`nCopilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

### Task 4: Implement lexical + semantic RRF message ranking

**Files:**
- Modify: `packages/keel-core/src/keel_core/recall.py`
- Modify: `tests/integration/test_recall_postgres.py`

**Interfaces:**
- Produces:
  - `rank_session_messages(engine, scope_id, query, *, k, embedder, batch_size=64, catchup_limit=500) -> tuple[list[RankedMessage], RecallStatus]`
  - modes `hybrid`, `lexical`, `lexical-degraded`.

- [ ] **Step 1: Add deterministic semantic and degradation tests**

Append to `tests/integration/test_recall_postgres.py`:

```python
from collections.abc import Sequence

from keel_core.recall import rank_session_messages


class _MeaningEmbedder:
    model = "fake/meaning"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for value in texts:
            lowered = value.lower()
            if "feline" in lowered or "cat nap" in lowered:
                vectors.append([1.0, 0.0])
            else:
                vectors.append([0.0, 1.0])
        return vectors


class _FailingEmbedder:
    model = "fake/failing"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        raise RuntimeError(f"embedding unavailable for {len(texts)} texts")


async def test_rank_session_messages_finds_semantic_only_match(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    target = f"s:{uuid.uuid4().hex}"
    other = f"s:{uuid.uuid4().hex}"
    await _seed_event(
        migrated_db, scope, target, 1, role="user", content="The feline sleeps on the sofa"
    )
    await _seed_event(
        migrated_db, scope, other, 1, role="user", content="Quarterly finance report"
    )

    hits, status = await rank_session_messages(
        migrated_db,
        scope,
        "cat nap",
        k=5,
        embedder=_MeaningEmbedder(),
        batch_size=2,
        catchup_limit=20,
    )

    assert status.mode == "hybrid"
    assert hits and hits[0].session_id == target
    assert "feline" in hits[0].content


async def test_rank_session_messages_explicitly_degrades_to_lexical(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await _seed_event(
        migrated_db, scope, session_id, 1, role="user", content="quarterly invoices"
    )

    hits, status = await rank_session_messages(
        migrated_db,
        scope,
        "invoices",
        k=5,
        embedder=_FailingEmbedder(),
    )

    assert status.mode == "lexical-degraded"
    assert status.error is not None and "RuntimeError" in status.error
    assert hits and hits[0].session_id == session_id


async def test_rank_session_messages_without_embedder_is_lexical(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await _seed_event(
        migrated_db, scope, session_id, 1, role="assistant", content="Tokyo flight"
    )

    hits, status = await rank_session_messages(
        migrated_db, scope, "Tokyo", k=5, embedder=None
    )

    assert status.mode == "lexical"
    assert hits and hits[0].session_id == session_id
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_recall_postgres.py -q -k "rank_session_messages"
```

Expected: FAIL because `rank_session_messages` does not exist.

- [ ] **Step 3: Add ranking helpers and implementation**

Append to `packages/keel-core/src/keel_core/recall.py`:

```python
async def _lexical_event_ids(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    limit: int,
) -> list[int]:
    sql = text(
        "SELECT e.id, GREATEST("
        "similarity(e.payload->>'text', :query), "
        "ts_rank(to_tsvector('simple', e.payload->>'text'), "
        "plainto_tsquery('simple', :query))) AS score "
        "FROM events e "
        "WHERE e.scope_id = :scope "
        "AND e.type = 'message.token' "
        "AND e.payload->>'role' IN ('user', 'assistant') "
        "AND COALESCE((e.payload->>'partial')::boolean, false) = false "
        "AND NULLIF(BTRIM(e.payload->>'text'), '') IS NOT NULL "
        "AND (similarity(e.payload->>'text', :query) > 0.1 "
        "OR to_tsvector('simple', e.payload->>'text') "
        "@@ plainto_tsquery('simple', :query)) "
        "ORDER BY score DESC, e.id DESC LIMIT :limit"
    )
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = await conn.execute(
            sql,
            {"scope": scope_id, "query": query, "limit": limit},
        )
        return [int(row.id) for row in rows]


async def _semantic_event_ids(
    engine: AsyncEngine,
    scope_id: ScopeId,
    embedder: Embedder,
    query: str,
    *,
    limit: int,
) -> list[int]:
    query_vector = (await embedder.embed([query]))[0]
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = await conn.execute(
            text(
                "SELECT event_id FROM message_embeddings "
                "WHERE scope_id = :scope AND model = :model AND dim = :dim "
                "ORDER BY embedding <=> CAST(:query_vector AS vector) "
                "LIMIT :limit"
            ),
            {
                "scope": scope_id,
                "model": embedder.model,
                "dim": embedder.dim,
                "query_vector": _vector_literal(query_vector),
                "limit": limit,
            },
        )
        return [int(row.event_id) for row in rows]


async def _messages_by_id(
    engine: AsyncEngine,
    scope_id: ScopeId,
    event_ids: list[int],
) -> dict[int, RankedMessage]:
    if not event_ids:
        return {}
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = await conn.execute(
            text(
                "SELECT id, session_id, seq, payload->>'role' AS role, "
                "payload->>'text' AS content "
                "FROM events WHERE scope_id = :scope AND id = ANY(:event_ids)"
            ),
            {"scope": scope_id, "event_ids": event_ids},
        )
        return {
            int(row.id): RankedMessage(
                event_id=int(row.id),
                session_id=str(row.session_id),
                seq=int(row.seq),
                role=str(row.role),
                content=str(row.content),
            )
            for row in rows
        }


async def rank_session_messages(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int,
    embedder: Embedder | None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> tuple[list[RankedMessage], RecallStatus]:
    """Rank complete session messages by lexical + semantic RRF."""
    if not query.strip():
        mode: RecallMode = "hybrid" if embedder is not None else "lexical"
        return [], RecallStatus(mode=mode)

    candidate_limit = max(k * 8, 80)
    lexical_ids = await _lexical_event_ids(
        engine, scope_id, query, limit=candidate_limit
    )
    semantic_ids: list[int] = []
    indexed = 0
    remaining = False
    catchup_error: str | None = None

    if embedder is None:
        mode = "lexical"
    else:
        indexer = MessageEmbeddingIndexer(
            engine, scope_id, embedder, batch_size=batch_size
        )
        try:
            backfill = await indexer.backfill_scope(limit=catchup_limit)
            indexed = backfill.indexed
            remaining = backfill.remaining
            if indexed:
                logger.info(
                    "session embedding catch-up complete scope=%s model=%s "
                    "dim=%s indexed=%d remaining=%s",
                    scope_id,
                    embedder.model,
                    embedder.dim,
                    indexed,
                    remaining,
                )
            if remaining:
                logger.info(
                    "session embedding catch-up reached limit scope=%s model=%s dim=%s",
                    scope_id,
                    embedder.model,
                    embedder.dim,
                )
        except Exception as exc:  # noqa: BLE001 - visible best-effort catch-up
            catchup_error = f"{exc.__class__.__name__}: {exc}"
            logger.exception(
                "session embedding catch-up failed scope=%s model=%s dim=%s",
                scope_id,
                embedder.model,
                embedder.dim,
            )
        try:
            semantic_ids = await _semantic_event_ids(
                engine,
                scope_id,
                embedder,
                query,
                limit=candidate_limit,
            )
            mode = "hybrid"
        except Exception as exc:  # noqa: BLE001 - explicit lexical degradation
            error = f"{exc.__class__.__name__}: {exc}"
            logger.exception(
                "semantic session search failed; using lexical only "
                "scope=%s model=%s dim=%s",
                scope_id,
                embedder.model,
                embedder.dim,
            )
            fused = rrf_fuse([lexical_ids])[:k]
            rows = await _messages_by_id(engine, scope_id, fused)
            return (
                [rows[event_id] for event_id in fused if event_id in rows],
                RecallStatus(
                    mode="lexical-degraded",
                    indexed=indexed,
                    remaining=remaining,
                    error=error,
                ),
            )

    ranked_lists = [lexical_ids, semantic_ids] if embedder is not None else [lexical_ids]
    fused = rrf_fuse(ranked_lists)[:k]
    rows = await _messages_by_id(engine, scope_id, fused)
    return (
        [rows[event_id] for event_id in fused if event_id in rows],
        RecallStatus(
            mode=mode,
            indexed=indexed,
            remaining=remaining,
            error=catchup_error,
        ),
    )
```

- [ ] **Step 4: Run GREEN + quality gates**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_recall_postgres.py -q
.\.venv\Scripts\python.exe -m ruff format packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
.\.venv\Scripts\python.exe -m mypy packages
```

- [ ] **Step 5: Commit**

```powershell
git -c core.safecrlf=false add packages\keel-core\src\keel_core\recall.py tests\integration\test_recall_postgres.py
git commit -m "feat(recall): hybrid RRF ranking for session messages`n`nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`nCopilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

### Task 5: Upgrade public search wrappers and `SessionSearchTool`

**Files:**
- Modify: `packages/keel-core/src/keel_core/search.py`
- Modify: `tests/integration/test_search_postgres.py`

**Interfaces:**
- Produces:
  - `hybrid_session_search(...) -> tuple[list[SearchHit], RecallStatus]`
  - backward-compatible `session_search(...) -> list[SearchHit]`
  - `hybrid_search_sessions(...) -> tuple[list[SessionSearchHit], RecallStatus]`
  - backward-compatible `search_sessions(...) -> list[SessionSearchHit]`
  - `SessionSearchTool(engine, embedder=None, *, batch_size=64, catchup_limit=500)`.

- [ ] **Step 1: Add failing wrapper/tool tests**

Append to `tests/integration/test_search_postgres.py`:

```python
from collections.abc import Sequence

from keel_core.protocols import ToolContext
from keel_core.search import (
    SessionSearchTool,
    hybrid_search_sessions,
    hybrid_session_search,
)


class _RecallEmbedder:
    model = "fake/search-meaning"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [
            [1.0, 0.0]
            if ("feline" in text.lower() or "cat nap" in text.lower())
            else [0.0, 1.0]
            for text in texts
        ]


class _BrokenRecallEmbedder:
    model = "fake/search-broken"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        raise RuntimeError(f"offline for {len(texts)} texts")


async def test_message_and_session_wrappers_share_semantic_ranking(
    migrated_db: AsyncEngine,
) -> None:
    from keel_core.state import PostgresEventStore
    from keel_core.loop import admit

    scope = f"u:{uuid.uuid4().hex}"
    target = f"s:{uuid.uuid4().hex}"
    other = f"s:{uuid.uuid4().hex}"
    await admit(
        PostgresEventStore(migrated_db, scope),
        target,
        scope,
        "The feline sleeps on the sofa",
    )
    await admit(
        PostgresEventStore(migrated_db, scope),
        other,
        scope,
        "Quarterly finance report",
    )

    message_hits, message_status = await hybrid_session_search(
        migrated_db, scope, "cat nap", k=5, embedder=_RecallEmbedder()
    )
    session_hits, session_status = await hybrid_search_sessions(
        migrated_db, scope, "cat nap", k=5, embedder=_RecallEmbedder()
    )

    assert message_status.mode == session_status.mode == "hybrid"
    assert message_hits and message_hits[0].source == f"session:{target}"
    assert session_hits and session_hits[0].id == target
    assert "feline" in session_hits[0].snippet


async def test_session_search_tool_marks_degraded_results(
    migrated_db: AsyncEngine,
) -> None:
    from keel_core.state import PostgresEventStore
    from keel_core.loop import admit

    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await admit(
        PostgresEventStore(migrated_db, scope),
        session_id,
        scope,
        "quarterly invoices",
    )

    result = await SessionSearchTool(
        migrated_db, _BrokenRecallEmbedder()
    ).run(
        {"query": "invoices", "k": 5},
        ToolContext(scope_id=scope, session_id="current"),
    )

    assert result.ok
    assert result.output.startswith("[semantic unavailable; lexical results only]")
    assert "invoices" in result.output
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_search_postgres.py -q -k "wrappers or degraded"
```

Expected: FAIL because hybrid wrapper functions and the new tool constructor do not exist.

- [ ] **Step 3: Replace lexical wrappers with hybrid-compatible wrappers**

In `search.py`, add:

```python
from keel_core.recall import RecallStatus, rank_session_messages
```

Replace the existing `session_search` and `search_sessions` functions with:

```python
async def hybrid_session_search(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int = 5,
    embedder: Embedder | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> tuple[list[SearchHit], RecallStatus]:
    ranked, status = await rank_session_messages(
        engine,
        scope_id,
        query,
        k=k,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    return (
        [
            SearchHit(content=message.content, source=f"session:{message.session_id}")
            for message in ranked
        ],
        status,
    )


async def session_search(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int = 5,
    embedder: Embedder | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> list[SearchHit]:
    hits, _ = await hybrid_session_search(
        engine,
        scope_id,
        query,
        k=k,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    return hits


async def hybrid_search_sessions(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int = 20,
    embedder: Embedder | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> tuple[list[SessionSearchHit], RecallStatus]:
    message_limit = max(k * 8, 80)
    ranked, status = await rank_session_messages(
        engine,
        scope_id,
        query,
        k=message_limit,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    best_messages = []
    seen_sessions: set[str] = set()
    for message in ranked:
        if message.session_id not in seen_sessions:
            seen_sessions.add(message.session_id)
            best_messages.append(message)
        if len(best_messages) >= k:
            break
    if not best_messages:
        return [], status

    session_ids = [message.session_id for message in best_messages]
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = (
            await conn.execute(
                text(
                    "SELECT s.id, s.title, s.updated_at, "
                    "(SELECT count(*) FROM events e "
                    "WHERE e.session_id = s.id AND e.scope_id = s.scope_id "
                    "AND e.type = 'message.token') AS messages "
                    "FROM sessions s "
                    "WHERE s.scope_id = :scope AND s.id = ANY(:session_ids)"
                ),
                {"scope": scope_id, "session_ids": session_ids},
            )
        ).all()
    summaries = {str(row.id): row for row in rows}
    hits = [
        SessionSearchHit(
            id=message.session_id,
            title=summaries[message.session_id].title,
            snippet=message.content,
            messages=int(summaries[message.session_id].messages),
            updated_at=summaries[message.session_id].updated_at,
        )
        for message in best_messages
        if message.session_id in summaries
    ]
    return hits, status


async def search_sessions(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int = 20,
    embedder: Embedder | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> list[SessionSearchHit]:
    hits, _ = await hybrid_search_sessions(
        engine,
        scope_id,
        query,
        k=k,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    return hits
```

- [ ] **Step 4: Upgrade `SessionSearchTool`**

Update the class docstring and description:

```python
class SessionSearchTool:
    """``session_search``: scope-bound hybrid recall over past messages."""

    name = "session_search"
    description = "Search this scope's past messages by lexical and semantic relevance."
    writes = False
```

Then replace its constructor and `run` method with:

```python
    def __init__(
        self,
        engine: AsyncEngine,
        embedder: Embedder | None = None,
        *,
        batch_size: int = 64,
        catchup_limit: int = 500,
    ) -> None:
        self._engine = engine
        self._embedder = embedder
        self._batch_size = batch_size
        self._catchup_limit = catchup_limit

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"query": {"type": "string"}, "k": {"type": "integer"}},
            "required": ["query"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        hits, status = await hybrid_session_search(
            self._engine,
            ctx.scope_id,
            str(args.get("query", "")),
            k=int(args.get("k", 5)),
            embedder=self._embedder,
            batch_size=self._batch_size,
            catchup_limit=self._catchup_limit,
        )
        output = "\n".join(hit.content for hit in hits)
        if status.mode == "lexical-degraded":
            output = "[semantic unavailable; lexical results only]\n" + output
        return ToolResult(ok=True, output=output)
```

- [ ] **Step 5: Run wrapper/tool + regression tests**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_search_postgres.py tests\integration\test_sessions_api.py -q
.\.venv\Scripts\python.exe -m ruff format packages\keel-core\src\keel_core\search.py tests\integration\test_search_postgres.py
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\search.py tests\integration\test_search_postgres.py
.\.venv\Scripts\python.exe -m mypy packages
```

- [ ] **Step 6: Commit**

```powershell
git -c core.safecrlf=false add packages\keel-core\src\keel_core\search.py tests\integration\test_search_postgres.py
git commit -m "feat(search): hybrid semantic session wrappers and tool`n`nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`nCopilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

### Task 6: Add recall configuration and exports

**Files:**
- Modify: `packages/keel-core/src/keel_core/config.py`
- Modify: `packages/keel-core/src/keel_core/__init__.py`
- Modify: `tests/unit/test_config.py`

**Interfaces:**
- Produces settings:
  - `session_embedding_batch_size: int = 64`
  - `session_embedding_catchup_limit: int = 500`
- Exports recall indexer/status/ranking and hybrid wrappers.

- [ ] **Step 1: Write the failing settings assertions**

Extend `test_memory_embedding_defaults()` in `tests/unit/test_config.py`:

```python
    assert settings.session_embedding_batch_size == 64
    assert settings.session_embedding_catchup_limit == 500
```

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_config.py::test_memory_embedding_defaults -q
```

Expected: FAIL with missing settings.

- [ ] **Step 3: Add settings**

Add directly after `memory_block_max_chars` in `Settings`:

```python
    session_embedding_batch_size: int = 64
    session_embedding_catchup_limit: int = 500
```

- [ ] **Step 4: Export public recall interfaces**

Add to `packages/keel-core/src/keel_core/__init__.py` imports:

```python
from .recall import (
    BackfillResult,
    MessageEmbeddingIndexer,
    RankedMessage,
    RecallStatus,
    rank_session_messages,
)
```

Add `hybrid_search_sessions`, `hybrid_session_search`, and `search_sessions` to the
existing `.search` import group.

Add these names once to `__all__`:

```python
    "BackfillResult",
    "MessageEmbeddingIndexer",
    "RankedMessage",
    "RecallStatus",
    "rank_session_messages",
    "hybrid_session_search",
    "hybrid_search_sessions",
    "search_sessions",
```

- [ ] **Step 5: Run GREEN + import smoke**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_config.py -q
.\.venv\Scripts\python.exe -c "import keel_core; keel_core.MessageEmbeddingIndexer; keel_core.hybrid_search_sessions"
.\.venv\Scripts\python.exe -m ruff format packages\keel-core\src\keel_core\config.py packages\keel-core\src\keel_core\__init__.py tests\unit\test_config.py
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\config.py packages\keel-core\src\keel_core\__init__.py tests\unit\test_config.py
.\.venv\Scripts\python.exe -m mypy packages
```

- [ ] **Step 6: Commit**

```powershell
git -c core.safecrlf=false add packages\keel-core\src\keel_core\config.py packages\keel-core\src\keel_core\__init__.py tests\unit\test_config.py
git commit -m "feat(config): semantic session indexing settings and exports`n`nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`nCopilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

### Task 7: Wire background indexing into `AgentRuntime`

**Files:**
- Modify: `packages/keel-server/src/keel_server/runtime.py`
- Modify: `packages/keel-server/src/keel_server/app.py`
- Modify: `tests/unit/test_runtime_memory.py`
- Create: `tests/unit/test_runtime_recall.py`

**Interfaces:**
- Produces:
  - runtime properties `embedder`, `session_embedding_batch_size`, `session_embedding_catchup_limit`
  - separate `_index_tasks`
  - post-run `_schedule_session_index`
  - `async AgentRuntime.aclose()`
- `SessionSearchTool` receives the runtime embedder/config.

- [ ] **Step 1: Update the memory-tool builder test first**

Change calls in `tests/unit/test_runtime_memory.py` to:

```python
    names = {
        t.name
        for t in _build_memory_tools(
            _ENGINE,
            FakeEmbedder(dim=16),
            cap=2000,
            batch_size=64,
            catchup_limit=500,
        )
    }
```

and:

```python
    names = {
        t.name
        for t in _build_memory_tools(
            _ENGINE,
            None,
            cap=2000,
            batch_size=64,
            catchup_limit=500,
        )
    }
```

- [ ] **Step 2: Write failing runtime lifecycle tests**

Create `tests/unit/test_runtime_recall.py`:

```python
"""AgentRuntime background session-indexing lifecycle."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from keel_core.embeddings import FakeEmbedder
from keel_server.runtime import AgentRuntime


class _RecordingIndexer:
    def __init__(self) -> None:
        self.called = asyncio.Event()
        self.release = asyncio.Event()
        self.sessions: list[str] = []

    async def index_session(self, session_id: str) -> int:
        self.sessions.append(session_id)
        self.called.set()
        await self.release.wait()
        return 1


class _FailingIndexer:
    async def index_session(self, session_id: str) -> int:
        raise RuntimeError(f"cannot index {session_id}")


class _BlockingIndexer:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def index_session(self, session_id: str) -> int:
        self.started.set()
        await asyncio.Event().wait()
        return 0


async def test_run_completion_schedules_separate_index_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    indexer = _RecordingIndexer()
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        embedder=FakeEmbedder(dim=8),
        session_indexer=indexer,
    )
    monkeypatch.setattr("keel_server.runtime.run", AsyncMock())

    await runtime._run(MagicMock(), "session-1", "run-1")
    await asyncio.wait_for(indexer.called.wait(), timeout=1)

    assert indexer.sessions == ["session-1"]
    assert len(runtime._index_tasks) == 1
    assert runtime._runs == {}
    indexer.release.set()
    await runtime.aclose()


async def test_index_failure_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        embedder=FakeEmbedder(dim=8),
        session_indexer=_FailingIndexer(),
    )
    monkeypatch.setattr("keel_server.runtime.run", AsyncMock())
    caplog.set_level(logging.ERROR, logger="keel.server.runtime")

    await runtime._run(MagicMock(), "broken-session", "run-1")
    await asyncio.sleep(0)

    assert "session embedding index failed" in caplog.text
    assert "broken-session" in caplog.text
    await runtime.aclose()


async def test_aclose_cancels_pending_index_tasks() -> None:
    indexer = _BlockingIndexer()
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        embedder=FakeEmbedder(dim=8),
        session_indexer=indexer,
    )

    runtime._schedule_session_index("session-1")
    await asyncio.wait_for(indexer.started.wait(), timeout=1)
    assert runtime._index_tasks

    await runtime.aclose()
    assert not runtime._index_tasks
```

- [ ] **Step 3: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_runtime_memory.py tests\unit\test_runtime_recall.py -q
```

Expected: FAIL because the builder signature, `session_indexer`, indexing lifecycle, and `aclose` are absent.

- [ ] **Step 4: Upgrade imports and `_build_memory_tools`**

In `runtime.py`, import:

```python
from keel_core.recall import MessageEmbeddingIndexer
```

Replace `_build_memory_tools` with:

```python
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
```

- [ ] **Step 5: Extend `AgentRuntime.__init__` and properties**

Add parameters:

```python
        session_embedding_batch_size: int = 64,
        session_embedding_catchup_limit: int = 500,
        session_indexer: MessageEmbeddingIndexer | None = None,
```

After `self._embedder = embedder`, add:

```python
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
```

Change `_build_memory_tools(...)` call to:

```python
            _build_memory_tools(
                engine,
                embedder,
                cap=memory_block_max_chars,
                batch_size=session_embedding_batch_size,
                catchup_limit=session_embedding_catchup_limit,
            )
```

After `self._interrupted`, add:

```python
        self._index_tasks: set[asyncio.Task[None]] = set()
```

Add properties after `model`:

```python
    @property
    def embedder(self) -> Embedder | None:
        return self._embedder

    @property
    def session_embedding_batch_size(self) -> int:
        return self._session_embedding_batch_size

    @property
    def session_embedding_catchup_limit(self) -> int:
        return self._session_embedding_catchup_limit
```

- [ ] **Step 6: Add indexing-task lifecycle**

Add before `_approver`:

```python
    def _schedule_session_index(self, session_id: SessionId) -> None:
        if self._session_indexer is None:
            return
        task = asyncio.create_task(self._index_session(session_id))
        self._index_tasks.add(task)
        task.add_done_callback(self._index_tasks.discard)

    async def _index_session(self, session_id: SessionId) -> None:
        assert self._session_indexer is not None
        try:
            indexed = await self._session_indexer.index_session(session_id)
            logger.info(
                "session embedding index complete scope=%s session=%s "
                "model=%s dim=%s indexed=%d",
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
        tasks = list(self._index_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._index_tasks.clear()
```

In `_run` finally, after `self._tracer.flush()`, add:

```python
            self._schedule_session_index(session_id)
```

- [ ] **Step 7: Pass settings and close runtime before the engine**

In `app.py`, extend the `AgentRuntime(...)` call:

```python
        session_embedding_batch_size=settings.session_embedding_batch_size,
        session_embedding_catchup_limit=settings.session_embedding_catchup_limit,
```

At the start of lifespan `finally`, before closing Redis/arq/engine:

```python
        await app.state.runtime.aclose()
```

- [ ] **Step 8: Run GREEN + regressions**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_runtime_memory.py tests\unit\test_runtime_recall.py tests\unit\test_runtime_interrupt.py tests\unit\test_server.py -q
.\.venv\Scripts\python.exe -m ruff format packages\keel-server\src\keel_server\runtime.py packages\keel-server\src\keel_server\app.py tests\unit\test_runtime_memory.py tests\unit\test_runtime_recall.py
.\.venv\Scripts\python.exe -m ruff check packages\keel-server tests\unit\test_runtime_memory.py tests\unit\test_runtime_recall.py
.\.venv\Scripts\python.exe -m mypy packages
```

- [ ] **Step 9: Commit**

```powershell
git -c core.safecrlf=false add packages\keel-server\src\keel_server\runtime.py packages\keel-server\src\keel_server\app.py tests\unit\test_runtime_memory.py tests\unit\test_runtime_recall.py
git commit -m "feat(runtime): background semantic session indexing`n`nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`nCopilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

### Task 8: Upgrade the Sessions API and expose search mode

**Files:**
- Modify: `packages/keel-server/src/keel_server/api/v1.py`
- Modify: `tests/integration/test_sessions_api.py`

**Interfaces:**
- `GET /v1/sessions/search` body remains a list.
- Response header is `hybrid`, `lexical`, or `lexical-degraded`.
- Runtime-less test apps remain lexical-only.

- [ ] **Step 1: Extend the fixture and write failing mode tests**

Add imports to `tests/integration/test_sessions_api.py`:

```python
from collections.abc import Sequence

from keel_core.embeddings import Embedder
```

Add test doubles:

```python
class _ApiMeaningEmbedder:
    model = "fake/api-meaning"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [
            [1.0, 0.0]
            if ("feline" in value.lower() or "cat nap" in value.lower())
            else [0.0, 1.0]
            for value in texts
        ]


class _ApiFailingEmbedder:
    model = "fake/api-failing"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        raise RuntimeError(f"offline for {len(texts)} texts")


class _SearchRuntime:
    def __init__(self, embedder: Embedder | None) -> None:
        self.embedder = embedder
        self.session_embedding_batch_size = 64
        self.session_embedding_catchup_limit = 500
```

Change the fixture yield type and value to include the FastAPI app:

```python
) -> AsyncIterator[tuple[httpx.AsyncClient, str, AsyncEngine, FastAPI]]:
```

and:

```python
        yield client, scope, migrated_db, app
```

Update existing fixture consumers to unpack the fourth item.

Append:

```python
async def test_search_endpoint_hybrid_mode_and_unchanged_body(
    sessions_client: tuple[httpx.AsyncClient, str, AsyncEngine, FastAPI],
) -> None:
    client, scope, engine, app = sessions_client
    target = f"s:{uuid.uuid4().hex}"
    other = f"s:{uuid.uuid4().hex}"
    await admit(
        PostgresEventStore(engine, scope),
        target,
        scope,
        "The feline sleeps on the sofa",
    )
    await admit(
        PostgresEventStore(engine, scope),
        other,
        scope,
        "Quarterly finance report",
    )
    app.state.runtime = _SearchRuntime(_ApiMeaningEmbedder())

    response = await client.get("/v1/sessions/search", params={"q": "cat nap"})
    rows = response.json()

    assert response.headers["X-Keel-Search-Mode"] == "hybrid"
    assert isinstance(rows, list)
    assert rows and rows[0]["id"] == target
    assert set(rows[0]) == {"id", "title", "snippet", "messages", "updated_at"}


async def test_search_endpoint_explicit_lexical_degradation(
    sessions_client: tuple[httpx.AsyncClient, str, AsyncEngine, FastAPI],
) -> None:
    client, scope, engine, app = sessions_client
    session_id = f"s:{uuid.uuid4().hex}"
    await admit(
        PostgresEventStore(engine, scope),
        session_id,
        scope,
        "quarterly invoices",
    )
    app.state.runtime = _SearchRuntime(_ApiFailingEmbedder())

    response = await client.get("/v1/sessions/search", params={"q": "invoices"})

    assert response.headers["X-Keel-Search-Mode"] == "lexical-degraded"
    assert any(row["id"] == session_id for row in response.json())
```

Also extend the existing lexical endpoint test:

```python
    response = await client.get("/v1/sessions/search", params={"q": "OKR"})
    assert response.headers["X-Keel-Search-Mode"] == "lexical"
    rows = response.json()
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_sessions_api.py -q
```

Expected: FAIL because the endpoint has no hybrid call or mode header.

- [ ] **Step 3: Upgrade the endpoint**

In `v1.py`, change imports:

```python
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from keel_core.search import hybrid_search_sessions
```

Replace `search_sessions_endpoint` with:

```python
@router.get("/sessions/search", summary="Search the scope's sessions (hybrid recall)")
async def search_sessions_endpoint(
    request: Request,
    response: Response,
    q: str = Query(""),
) -> list[dict[str, object]]:
    """Rank sessions by lexical + semantic RRF, with explicit degradation mode."""
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    runtime = getattr(request.app.state, "runtime", None)
    embedder = getattr(runtime, "embedder", None)
    batch_size = int(getattr(runtime, "session_embedding_batch_size", 64))
    catchup_limit = int(getattr(runtime, "session_embedding_catchup_limit", 500))
    if engine is None or not q.strip():
        response.headers["X-Keel-Search-Mode"] = (
            "hybrid" if embedder is not None else "lexical"
        )
        return []

    hits, recall_status = await hybrid_search_sessions(
        engine,
        scope,
        q,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    response.headers["X-Keel-Search-Mode"] = recall_status.mode
    return [
        {
            "id": hit.id,
            "title": hit.title,
            "snippet": hit.snippet,
            "messages": hit.messages,
            "updated_at": hit.updated_at.isoformat() if hit.updated_at else None,
        }
        for hit in hits
    ]
```

- [ ] **Step 4: Run GREEN + web/API regression**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest -m integration tests\integration\test_sessions_api.py tests\integration\test_search_postgres.py tests\integration\test_recall_postgres.py -q
.\.venv\Scripts\python.exe -m ruff format packages\keel-server\src\keel_server\api\v1.py tests\integration\test_sessions_api.py
.\.venv\Scripts\python.exe -m ruff check packages\keel-server\src\keel_server\api\v1.py tests\integration\test_sessions_api.py
.\.venv\Scripts\python.exe -m mypy packages
```

- [ ] **Step 5: Commit**

```powershell
git -c core.safecrlf=false add packages\keel-server\src\keel_server\api\v1.py tests\integration\test_sessions_api.py
git commit -m "feat(api): hybrid semantic Sessions search mode`n`nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`nCopilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

### Task 9: Full regression and live verification

**Files:**
- No planned source changes. If verification reveals a real bug, fix it with a focused failing test and a separate commit.

**Interfaces:**
- Verifies projection migration, run-end indexing, search catch-up, both search surfaces, and explicit degradation against real Ollama `bge-m3`.

- [ ] **Step 1: Run all automated verification**

```powershell
cd C:\src\keel
$env:PYTHONUTF8 = 1
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests -q
.\.venv\Scripts\python.exe -m ruff check packages tests migrations
.\.venv\Scripts\python.exe -m mypy packages
```

Expected: full suite passes; ruff/mypy clean.

- [ ] **Step 2: Apply migration and rebuild the server**

```powershell
docker compose run --rm keel-migrate
docker compose build keel-server
docker compose up -d --force-recreate keel-server
Start-Sleep -Seconds 12
docker compose ps keel-server ollama
```

Expected: migration exits 0; server and Ollama healthy.

- [ ] **Step 3: Clear the current projection to test catch-up**

```powershell
docker compose exec -T postgres psql -U keel -d keel -c "TRUNCATE message_embeddings;"
$response = Invoke-WebRequest -Uri "http://localhost:8000/v1/sessions/search?q=Keel" -Method GET
$response.Headers["X-Keel-Search-Mode"]
docker compose exec -T postgres psql -U keel -d keel -c "SELECT count(*) FROM message_embeddings WHERE scope_id='web:local';"
```

Expected: header `hybrid`; count becomes greater than zero.

- [ ] **Step 4: Verify run-end background indexing**

```powershell
$before = docker compose exec -T postgres psql -U keel -d keel -Atc "SELECT count(*) FROM message_embeddings WHERE scope_id='web:local';"
$body = '{"content":"语义召回测试：北极星项目的发布窗口在周四夜间。"}'
Invoke-RestMethod -Uri "http://localhost:8000/v1/sessions/semantic-live/messages" -Method POST -ContentType "application/json" -Body $body
Start-Sleep -Seconds 20
$after = docker compose exec -T postgres psql -U keel -d keel -Atc "SELECT count(*) FROM message_embeddings WHERE scope_id='web:local';"
Write-Output "before=$before after=$after"
```

Expected: `after` is at least two greater than `before` when both user and assistant complete messages exist.

- [ ] **Step 5: Verify semantic paraphrase through the API**

```powershell
$response = Invoke-WebRequest -Uri "http://localhost:8000/v1/sessions/search?q=哪个项目安排在星期四晚上上线" -Method GET
$response.Headers["X-Keel-Search-Mode"]
$response.Content
```

Expected: header `hybrid`; response contains session `semantic-live` and a relevant snippet.

- [ ] **Step 6: Verify the agent tool**

```powershell
$body = '{"content":"请使用 session_search 搜索过去对话：哪个项目安排在星期四晚上发布？"}'
Invoke-RestMethod -Uri "http://localhost:8000/v1/sessions/semantic-tool-live/messages" -Method POST -ContentType "application/json" -Body $body
Start-Sleep -Seconds 20
docker compose logs keel-server --since 40s | Select-String -Pattern "session_search|北极星|周四"
```

Expected: tool executes and the answer identifies the North Star project / Thursday-night window.

- [ ] **Step 7: Verify explicit degradation and recovery**

```powershell
docker compose stop ollama
$degraded = Invoke-WebRequest -Uri "http://localhost:8000/v1/sessions/search?q=Keel" -Method GET
$degraded.Headers["X-Keel-Search-Mode"]
$degraded.Content
docker compose start ollama
Start-Sleep -Seconds 10
$recovered = Invoke-WebRequest -Uri "http://localhost:8000/v1/sessions/search?q=哪个项目安排在星期四晚上上线" -Method GET
$recovered.Headers["X-Keel-Search-Mode"]
```

Expected: stopped mode is `lexical-degraded` with lexical results and no server crash; restarted mode returns `hybrid`.

- [ ] **Step 8: Confirm final repository state**

```powershell
git status --short
git --no-pager log --oneline -10
```

Expected: working tree clean; all feature commits present.

---

## Self-Review Checklist

- **Spec coverage:** Task 1 schema/RLS/cascade; Tasks 2–3 indexing/catch-up/idempotency/model pins; Task 4 shared RRF/degradation; Task 5 tool + both wrapper levels; Task 6 settings/exports; Task 7 background lifecycle; Task 8 API header/body compatibility; Task 9 live verification.
- **Placeholder scan:** all code-changing steps contain concrete code and exact commands; no unfinished or cross-task shorthand instructions.
- **Type consistency:** `MessageEmbeddingIndexer`, `BackfillResult`, `RankedMessage`, `RecallStatus`, `rank_session_messages`, hybrid wrappers, runtime properties, settings, and API usage use identical names and defaults throughout.
- **Scope:** no frontend, RAG, consolidation, IM/digest indexing, reranker, ANN, or re-embed cleanup was added.
