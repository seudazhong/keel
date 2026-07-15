# RAG / Knowledge Base Vertical Slice Implementation Plan

> **Execution:** Work through the checklist task-by-task and run the narrowest applicable validation before advancing.

**Goal:** Deliver a scope-bound text/Markdown Knowledge Base with durable ingest/delete jobs, deterministic chunking, hybrid retrieval, stable citations, always-tainted `kb_search`, REST/UI management, and replayable quality evals.

**Architecture:** Postgres owns KB/document/version/chunk lifecycle and the idempotency request ledger; `desired_version_id` and `active_version_id` isolate in-progress updates from the searchable version. The existing durable-job substrate gains additive cancellation modes and terminal lifecycle hooks so domain cleanup always precedes job cancellation/failure, while Redis/arq remains an at-least-once delivery layer. Server/runtime surfaces reuse one scope-bound store and pinned embedder, and the React page consumes structured citations rather than parsing model text.

**Tech Stack:** Python 3.12, Pydantic 2, frozen dataclasses and `StrEnum`, SQLAlchemy 2 async + psycopg3, Alembic/Postgres RLS, pgvector, `pg_trgm`, arq/Redis, FastAPI, React 19, TanStack Query, Vitest/MSW, pytest, Ruff, mypy strict. No new dependency.

## Global Constraints

- The approved contract is `docs/designs/2026-07-14-rag-knowledge-base-design.md`; implementation must not weaken its lifecycle, deletion, scope, taint, citation, or idempotency guarantees.
- Current visibility is exactly one explicit agent scope (`web:local` in production wiring); every Postgres transaction sets `app.scope_id` and every query also filters `scope_id`.
- Composite foreign keys must reject both cross-scope and same-scope cross-KB document/version/chunk references.
- Text/Markdown only; normalized UTF-8 raw content is stored in Postgres and is limited to 1,048,576 bytes. Reject NUL, unpaired surrogates, blank normalized content, and unsafe error echoing.
- A KB snapshots the configured server `(embedding_model, embedding_dim)` and clients cannot choose it. Worker mismatch fails ingest permanently; server mismatch degrades search to lexical.
- `index_fingerprint` is not globally unique. Reuse only a current pending/indexing/active desired or active version; historical failed/cancelled/superseded/purged matches create N+1.
- Search reads only active KBs, active documents, and chunks belonging to `active_version_id`.
- All `kb_search` results use `ContentTaint.tainted`; there is no trusted override.
- Delete is synchronous hide plus asynchronous purge. `knowledge.delete` is non-cancellable and effectively retry-until-success.
- No raw content, chunk text, query text, snippets, payloads, provider messages, or secrets in logs/spans.
- No PDF/OCR/connectors/MinIO/cross-scope grants/reranker/global search/generic job create or retry API.
- Reuse existing helpers and patterns before adding new ones: `Embedder`, `rrf_fuse`, JobStore validation/finalization, `_SET_SCOPE`, FastAPI RBAC dependencies, TanStack Query, MSW, and embedding cassettes.
- Existing jobs default to `CancelMode.immediate`; the migration and code change must preserve all pre-RAG behavior.
- Sub-agents, when useful, use their configured default models; do not override model selections. Reviews are targeted engineering gates, not a mandatory workflow ceremony.
- Fresh worktree setup: run `python -m uv sync --frozen`; run `npm ci` under `web` only before web validation.
- Integration commands always set:

  ```powershell
  $env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
  $env:KEEL_EVAL_DATABASE_URL = $env:KEEL_TEST_DATABASE_URL
  $env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
  ```

- Use the narrowest tests during each task. The final task runs all non-integration tests, all integration tests, Ruff, format check, mypy, web tests/lint/build, migration-head check, replay evals, and an isolated live smoke.
- Every implementation commit includes:

  ```text
  Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
  Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49
  ```

---

## Architecture and File Map

### Create

| File | Responsibility |
|---|---|
| `migrations/versions/0010_knowledge_base.py` | Add `jobs.cancel_mode`; create KB, document, version, chunk, and idempotency tables, indexes, composite FKs, and RLS. |
| `packages/keel-core/src/keel_core/knowledge/__init__.py` | Public Knowledge exports. |
| `packages/keel-core/src/keel_core/knowledge/models.py` | Strict IDs, enums, records, request commands, citations, search status, and public domain errors. |
| `packages/keel-core/src/keel_core/knowledge/chunking.py` | UTF-8 normalization and deterministic text/Markdown chunking. |
| `packages/keel-core/src/keel_core/knowledge/store.py` | Store protocol plus in-memory and Postgres lifecycle/idempotency implementations. |
| `packages/keel-core/src/keel_core/knowledge/search.py` | Active-only lexical/semantic retrieval, degradation, local-int RRF mapping, and citations. |
| `packages/keel-core/src/keel_core/knowledge/tools.py` | Always-tainted bounded `kb_search` tool. |
| `packages/keel-core/src/keel_core/knowledge/jobs.py` | Strict payloads plus worker-agnostic ingest/delete handlers and terminal lifecycle hooks. |
| `packages/keel-core/src/keel_core/knowledge/service.py` | Idempotent create/update/reindex/delete orchestration and deterministic durable-job creation. |
| `packages/keel-server/src/keel_server/api/knowledge.py` | Scoped `/v1/knowledge-bases` router, DTOs, RBAC, header validation, and error mapping. |
| `packages/keel-worker/src/keel_worker/knowledge.py` | Wrap core handlers/hooks in `JobDefinition` entries without reversing the core dependency. |
| `web/src/features/knowledge/types.ts` | API types for KBs, documents, versions, jobs, search status, hits, and citations. |
| `web/src/features/knowledge/useKnowledge.ts` | Query/mutation hooks with generated `Idempotency-Key` headers and job polling. |
| `web/src/features/knowledge/KnowledgePage.tsx` | Minimal three-pane Knowledge management/search page. |
| `web/src/features/knowledge/KnowledgePage.test.tsx` | Page behavior tests with MSW. |
| `packages/keel-worker/src/keel_worker/knowledge_evals/models.py` | Strict independent Knowledge eval dataset/report models. |
| `packages/keel-worker/src/keel_worker/knowledge_evals/runner.py` | Replay/live/record execution and quality gates. |
| `packages/keel-worker/src/keel_worker/knowledge_evals/cli.py` | Safe CLI with exit codes 0/1/2. |
| `scripts/run_knowledge_evals.py` | Thin CLI entry point. |
| `evals/datasets/knowledge/v1.jsonl` | Versioned retrieval/citation/deletion/taint cases. |
| `evals/cassettes/knowledge/v1-embeddings.json` | Checked-in deterministic embedding replay data. |

### Test files

| File | Coverage |
|---|---|
| `tests/integration/test_knowledge_schema.py` | Migration, checks, RLS, composite FKs, active-name uniqueness. |
| `tests/unit/test_knowledge_models.py` | IDs, commands, bounds, fingerprints, citations, status models. |
| `tests/unit/test_knowledge_chunking.py` | Normalization, headings, code fences, overlap, offsets, determinism. |
| `tests/unit/test_knowledge_store.py` | In-memory lifecycle/idempotency/activation/cancel/failure/delete semantics. |
| `tests/integration/test_knowledge_postgres.py` | Postgres lifecycle, request ledger, races, purge, active-only state. |
| `tests/integration/test_knowledge_search.py` | Lexical/hybrid/degraded/CJK/model pin/citations. |
| `tests/unit/test_knowledge_tools.py` | `kb_search` schema, bounds, output, citations, taint. |
| `tests/unit/test_knowledge_jobs.py` | Payloads, handlers, hooks, retries, zombie guards. |
| `tests/integration/test_knowledge_worker.py` | Real Postgres + Redis/arq ingest/delete acceptance. |
| `tests/integration/test_knowledge_api.py` | RBAC, `Idempotency-Key`, CRUD/reindex/search/delete, job recovery. |
| `tests/unit/test_knowledge_eval_models.py` | Dataset validation. |
| `tests/unit/test_knowledge_eval_runner.py` | Metrics, gates, replay misses, exit codes. |

### Modify

| File | Change |
|---|---|
| `packages/keel-core/src/keel_core/jobs.py` | `CancelMode`, immutable `JobTerminalIntent`, cooperative/disabled cancel behavior, reserve/finalize protocol, exhaustion recovery. |
| `packages/keel-worker/src/keel_worker/jobs.py` | `JobContext.max_attempts`, `JobDefinition` hooks, intent-before-hook and fresh-clock finalization orchestration. |
| `packages/keel-core/src/keel_core/protocols.py` | Add generic `Citation` and `ToolResult.citations`. |
| `packages/keel-core/src/keel_core/events.py` | Add citations to strict `ToolResultPayload`. |
| `packages/keel-core/src/keel_core/loop.py` | Persist citations in normal and resumed `tool.result` events. |
| `packages/keel-core/src/keel_core/config.py` | Add validated Knowledge limits/batch settings. |
| `packages/keel-core/src/keel_core/api.py` | Expose `cancel_mode` on `JobResponse`; export Knowledge DTOs only if shared by server. |
| `packages/keel-core/src/keel_core/__init__.py` | Add public Knowledge and citation exports. |
| `packages/keel-worker/src/keel_worker/main.py` | Build Knowledge store and register real ingest/delete job definitions. |
| `packages/keel-server/src/keel_server/app.py` | Build Knowledge store/service, include router, and pass store into runtime. |
| `packages/keel-server/src/keel_server/runtime.py` | Register `kb_search` when engine/embedder exist. |
| `packages/keel-server/src/keel_server/api/v1.py` | Preserve jobs API while mapping disabled cancellation to 409. |
| `tests/integration/conftest.py` | Truncate Knowledge tables in FK-safe order. |
| `web/src/lib/api.ts` | Accept per-request headers for idempotent mutations. |
| `web/src/router.tsx` | Add `/knowledge`. |
| `web/src/components/Sidebar.tsx` | Enable Knowledge link. |
| `web/src/test/handlers.ts` | Add stateful Knowledge API mocks. |
| `docs/STATUS.md` | Mark RAG/KB complete only after final verification. |

## Authoritative Interfaces

```python
# keel_core.jobs
class CancelMode(StrEnum):
    immediate = "immediate"
    cooperative = "cooperative"
    disabled = "disabled"


class JobTerminalIntent(StrEnum):
    failed = "failed"
    cancelled = "cancelled"


class JobStore(Protocol):
    async def enqueue_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        cancel_mode: CancelMode = CancelMode.immediate,
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]: ...

    async def reserve_terminal(
        self,
        lease: JobLease,
        intent: JobTerminalIntent,
        *,
        now: datetime,
        error: JobError | None = None,
    ) -> JobRecord: ...

    async def finalize_terminal(self, lease: JobLease, now: datetime) -> JobRecord: ...
    async def reserve_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...
    async def finalize_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...
```

```python
# keel_worker.jobs
JobCancelledHook = Callable[[JobRecord], Awaitable[None]]
JobFailedHook = Callable[[JobRecord, JobError], Awaitable[None]]


@dataclass(frozen=True)
class JobDefinition:
    kind: str
    handler: JobHandler
    max_attempts: int = 3
    lease_seconds: int = 300
    on_cancelled: JobCancelledHook | None = None
    on_failed: JobFailedHook | None = None
```

```python
# keel_core.knowledge.models
class KnowledgeSourceType(StrEnum):
    text = "text"
    markdown = "markdown"


class KnowledgeVersionStatus(StrEnum):
    pending = "pending"
    indexing = "indexing"
    active = "active"
    superseded = "superseded"
    failed = "failed"
    cancelled = "cancelled"
    deleted = "deleted"
    purged = "purged"


def knowledge_source_type_from_mime_type(mime_type: object) -> KnowledgeSourceType: ...


@dataclass(frozen=True)
class KnowledgeDocumentVersionRecord:
    id: str
    scope_id: str
    kb_id: str
    document_id: str
    version: int
    content: str | None
    content_sha256: str
    index_fingerprint: str
    mime_type: str
    chunking_version: str
    target_chars: int
    overlap_chars: int
    ingest_job_id: str | None
    status: KnowledgeVersionStatus
    error_kind: str | None
    error_message: str | None
    created_at: datetime
    activated_at: datetime | None
    deleted_at: datetime | None
    purged_at: datetime | None


class KnowledgeSearchMode(StrEnum):
    hybrid = "hybrid"
    lexical = "lexical"
    lexical_degraded = "lexical-degraded"


@dataclass(frozen=True)
class ChunkDraft:
    ordinal: int
    text: str
    char_start: int
    char_end: int
    content_hash: str
    heading_path: tuple[str, ...]


class KnowledgeCitation(BaseModel):
    id: str
    kb_id: str
    document_id: str
    document_version_id: str
    chunk_id: str
    title: str
    source_uri: str | None
    ordinal: int
    char_start: int
    char_end: int
    label: str


class KnowledgeHit(BaseModel):
    snippet: str
    rank: int
    citation: KnowledgeCitation
    heading_path: list[str] = Field(default_factory=list)


class KnowledgeSearchStatus(BaseModel):
    mode: KnowledgeSearchMode
    semantic_error: str | None = None
```

```python
# keel_core.knowledge.chunking
def normalize_document_text(value: str, *, max_bytes: int) -> str: ...


def chunk_document(
    content: str,
    source_type: KnowledgeSourceType,
    *,
    target_chars: int,
    overlap_chars: int,
) -> list[ChunkDraft]: ...
```

```python
# keel_core.knowledge.search
class KnowledgeSearcher:
    async def search(
        self, kb_id: str, query: str, *, k: int = 5
    ) -> tuple[list[KnowledgeHit], KnowledgeSearchStatus]: ...
```

```python
# keel_core.knowledge.jobs
class KnowledgeIngestPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kb_id: str
    document_id: str
    document_version_id: str


class KnowledgeDeletePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kb_id: str
    document_id: str | None = None


class KnowledgeJobContext(Protocol):
    job_id: str
    scope_id: str
    attempt: int
    max_attempts: int

    async def progress(
        self,
        current: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None: ...

    async def checkpoint(self) -> None: ...


class KnowledgeJobHandlers:
    def __init__(
        self,
        store: KnowledgeStore,
        embedder: Embedder,
        settings: Settings,
    ) -> None: ...

    async def ingest(
        self, context: KnowledgeJobContext, payload: dict[str, Any]
    ) -> JobResult: ...

    async def delete(
        self, context: KnowledgeJobContext, payload: dict[str, Any]
    ) -> JobResult: ...

    async def ingest_cancelled(self, record: JobRecord) -> None: ...
    async def ingest_failed(self, record: JobRecord, error: JobError) -> None: ...
    async def delete_failed(self, record: JobRecord, error: JobError) -> None: ...
```

```python
# keel_worker.knowledge
def knowledge_job_definitions(
    store: KnowledgeStore,
    embedder: Embedder,
    settings: Settings,
) -> tuple[JobDefinition, JobDefinition]: ...
```

---

### Task 1: Extend Durable Jobs and Create the Knowledge Schema

**Files:**
- Create: `migrations/versions/0010_knowledge_base.py`
- Modify: `packages/keel-core/src/keel_core/jobs.py`
- Modify: `packages/keel-worker/src/keel_worker/jobs.py`
- Modify: `packages/keel-core/src/keel_core/api.py`
- Modify: `packages/keel-server/src/keel_server/api/v1.py`
- Modify: `tests/integration/conftest.py`
- Test: `tests/unit/test_jobs.py`
- Test: `tests/unit/test_worker_jobs.py`
- Test: `tests/integration/test_jobs_postgres.py`
- Test: `tests/integration/test_jobs_api.py`
- Test: `tests/integration/test_knowledge_schema.py`

**Interfaces:**
- Produces `CancelMode`, `JobTerminalIntent`, `JobRecord.cancel_mode/terminal_intent`,
  `enqueue_once(..., cancel_mode=...)`, reserve/finalize methods,
  `JobDefinition.on_cancelled/on_failed`, and all SQL tables required by later tasks.

- [ ] **Step 1: Write failing unit tests for cancel modes and terminal hooks**

```python
async def test_cooperative_queued_cancel_waits_for_worker_cleanup() -> None:
    store = InMemoryJobStore("web:local")
    row, _ = await store.enqueue_once(
        kind="test.cleanup",
        payload={},
        target_session_id=None,
        idempotency_key="cleanup",
        max_attempts=3,
        cancel_mode=CancelMode.cooperative,
        now=_NOW,
    )
    cancelled = await store.request_cancel(row.id, _NOW)
    assert cancelled is not None
    assert cancelled.status is JobStatus.queued
    assert cancelled.cancel_requested_at == _NOW


async def test_failed_hook_runs_before_terminal_transition() -> None:
    observed: list[JobStatus] = []

    async def on_failed(row: JobRecord, error: JobError) -> None:
        observed.append(row.status)
        assert error.kind == "permanent"

    registry = JobRegistry()
    registry.register(
        JobDefinition(
            kind="test.fail",
            handler=_permanent_handler,
            on_failed=on_failed,
        )
    )
    assert await run_job(_ctx(store, registry), "web:local", job_id) == "failed"
    assert observed == [JobStatus.running]
```

- [ ] **Step 2: Run the new unit tests and confirm RED**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_jobs.py tests/unit/test_worker_jobs.py -q
```

Expected: failures for missing `CancelMode`, `cancel_mode`, hooks, and exhaustion cancellation.

- [ ] **Step 3: Implement the additive jobs contracts and orchestration**

Implement exactly:

```python
class CancelMode(StrEnum):
    immediate = "immediate"
    cooperative = "cooperative"
    disabled = "disabled"
```

Queued cooperative cancellation sets `cancel_requested_at` and makes `next_attempt_at <= now`;
disabled cancellation raises `JobValidationError("job_not_cancellable", ...)`. Before every
hook-bearing cancelled/failed transition, atomically reserve immutable terminal intent, call only
that intent's hook, obtain a fresh clock value, and finalize only while the lease remains valid.
Hook failure leaves the job running with intent. Expired intent rows are not reclaimed; dispatcher
replays the same hook and finalizes exactly once. Exhaustion reserves failed/cancelled under row
lock based on whether cancellation was persisted before exact expiry.

- [ ] **Step 4: Write the complete 0010 migration**

Create every table/check/index/FK/RLS policy from design §§6 and 10.0, including:

```sql
ALTER TABLE jobs
ADD COLUMN cancel_mode text NOT NULL DEFAULT 'immediate'
CHECK (cancel_mode IN ('immediate', 'cooperative', 'disabled'));

ALTER TABLE jobs
ADD COLUMN terminal_intent text
    CHECK (terminal_intent IN ('failed', 'cancelled')),
ADD COLUMN terminal_intent_at timestamptz;
```

The downgrade drops Knowledge tables in FK order, then drops `jobs.cancel_mode`.

- [ ] **Step 5: Run schema and jobs integration tests**

Run:

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests/integration/test_jobs_postgres.py tests/integration/test_jobs_api.py tests/integration/test_knowledge_schema.py -q
```

Expected: migration head is `0010_knowledge_base`; existing rows default to immediate; RLS and same-scope cross-KB FKs fail closed.

- [ ] **Step 6: Run targeted quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/jobs.py packages/keel-worker/src/keel_worker/jobs.py packages/keel-server/src/keel_server/api/v1.py tests/unit/test_jobs.py tests/unit/test_worker_jobs.py tests/integration/test_jobs_postgres.py tests/integration/test_jobs_api.py tests/integration/test_knowledge_schema.py migrations/versions/0010_knowledge_base.py
.\.venv\Scripts\python.exe -m mypy packages/keel-core/src/keel_core/jobs.py packages/keel-worker/src/keel_worker/jobs.py packages/keel-server/src/keel_server/api/v1.py
```

- [ ] **Step 7: Commit**

```powershell
git add migrations/versions/0010_knowledge_base.py packages/keel-core/src/keel_core/jobs.py packages/keel-worker/src/keel_worker/jobs.py packages/keel-core/src/keel_core/api.py packages/keel-server/src/keel_server/api/v1.py tests/integration/conftest.py tests/unit/test_jobs.py tests/unit/test_worker_jobs.py tests/integration/test_jobs_postgres.py tests/integration/test_jobs_api.py tests/integration/test_knowledge_schema.py
git commit -m "feat(jobs): add knowledge lifecycle hooks" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 2: Add Knowledge Models, Settings, and In-Memory Lifecycle

**Files:**
- Create: `packages/keel-core/src/keel_core/knowledge/__init__.py`
- Create: `packages/keel-core/src/keel_core/knowledge/models.py`
- Create: `packages/keel-core/src/keel_core/knowledge/store.py`
- Modify: `packages/keel-core/src/keel_core/config.py`
- Modify: `packages/keel-core/src/keel_core/__init__.py`
- Test: `tests/unit/test_knowledge_models.py`
- Test: `tests/unit/test_knowledge_store.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces strict records/errors/commands, `KnowledgeStore`, `InMemoryKnowledgeStore`,
  ID/fingerprint helpers, and validated settings consumed by every later task. Document versions
  durably carry `mime_type`, `chunking_version`, `target_chars`, and `overlap_chars`; source type
  is strictly recoverable from the version MIME type.

- [ ] **Step 1: Write RED model/settings tests**

```python
def test_index_fingerprint_changes_with_every_index_input() -> None:
    base = index_fingerprint(
        "a" * 64,
        KnowledgeSourceType.markdown,
        "keel-char-v1",
        "fake/embed",
        16,
        1600,
        200,
    )
    assert base != index_fingerprint(
        "b" * 64,
        KnowledgeSourceType.markdown,
        "keel-char-v1",
        "fake/embed",
        16,
        1600,
        200,
    )
    assert base != index_fingerprint(
        "a" * 64,
        KnowledgeSourceType.text,
        "keel-char-v1",
        "fake/embed",
        16,
        1600,
        200,
    )


def test_knowledge_settings_reject_overlap_at_or_above_target() -> None:
    with pytest.raises(ValidationError):
        Settings(knowledge_chunk_target_chars=100, knowledge_chunk_overlap_chars=100)
```

- [ ] **Step 2: Define strict domain models and errors**

Use `StrEnum` and frozen dataclasses for persistence records; Pydantic
`ConfigDict(extra="forbid")` for API/job commands. `KnowledgeDocumentVersionRecord` persists every
non-KB input to `index_fingerprint`, including strict positive `target_chars`, non-negative
`overlap_chars`, and `overlap_chars < target_chars`. Implement storage-safe `kb_`, `doc_`, `kbv_`,
`kbc_`, and `kbi_` IDs and bounded public errors such as `KnowledgeNotFound`,
`KnowledgeConflict`, `KnowledgeValidationError`, and `KnowledgeEmbeddingMismatch`.

- [ ] **Step 3: Add the settings from design §18**

Add positive fields for document/title/description/query/k/chunk/batch/tool-output limits and a model validator enforcing:

```python
if self.knowledge_chunk_overlap_chars >= self.knowledge_chunk_target_chars:
    raise ValueError("knowledge_chunk_overlap_chars must be less than target")
```

- [ ] **Step 4: Implement the in-memory lifecycle**

Cover base creation/deletion, monotonic document versions, current-only fingerprint reuse, desired/active switching, stale activation, failure/cancel precedence, tombstone/purge, chunk replacement, and an idempotency ledger that returns the same IDs for the same fingerprint and raises conflict for key reuse with different input.

- [ ] **Step 5: Run tests and quality gates**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_knowledge_models.py tests/unit/test_knowledge_store.py tests/unit/test_config.py -q
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/knowledge packages/keel-core/src/keel_core/config.py tests/unit/test_knowledge_models.py tests/unit/test_knowledge_store.py tests/unit/test_config.py
.\.venv\Scripts\python.exe -m mypy packages/keel-core/src/keel_core/knowledge packages/keel-core/src/keel_core/config.py
```

- [ ] **Step 6: Commit**

```powershell
git add packages/keel-core/src/keel_core/knowledge packages/keel-core/src/keel_core/config.py packages/keel-core/src/keel_core/__init__.py tests/unit/test_knowledge_models.py tests/unit/test_knowledge_store.py tests/unit/test_config.py
git commit -m "feat(knowledge): add lifecycle contracts" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 3: Implement Deterministic Text and Markdown Chunking

**Files:**
- Create: `packages/keel-core/src/keel_core/knowledge/chunking.py`
- Test: `tests/unit/test_knowledge_chunking.py`

**Interfaces:**
- Consumes `KnowledgeSourceType` and `ChunkDraft`.
- Produces `normalize_document_text()` and `chunk_document()`.

- [ ] **Step 1: Write RED normalization/chunk tests**

```python
def test_markdown_chunking_preserves_heading_path_and_offsets() -> None:
    text = "# Guide\r\n\r\nIntro.\r\n\r\n## Setup\r\n\r\nInstall Keel."
    normalized = normalize_document_text(text, max_bytes=1024)
    chunks = chunk_document(
        normalized,
        KnowledgeSourceType.markdown,
        target_chars=20,
        overlap_chars=5,
    )
    assert chunks[-1].heading_path == ("Guide", "Setup")
    for chunk in chunks:
        assert normalized[chunk.char_start : chunk.char_end] == chunk.text


def test_chunking_is_byte_stable() -> None:
    first = chunk_document(TEXT, KnowledgeSourceType.text, target_chars=40, overlap_chars=8)
    second = chunk_document(TEXT, KnowledgeSourceType.text, target_chars=40, overlap_chars=8)
    assert first == second
```

The shared chunk invariant must keep ordinal/order/overlap/hash/exact-substring checks, assert every
non-whitespace index is covered, and allow a gap only when `gap.strip()` is empty. Add regressions
for `A + 8_000 spaces + B`, overlap-stepped non-whitespace islands, a trailing blank separator, and a
near-1 MiB whitespace run with bounded peak memory. Assert no empty, whitespace-only, or duplicate
chunks and `len(chunk.text) <= target_chars` for emitted hard splits.

- [ ] **Step 2: Implement normalization**

Normalize BOM/CRLF/trailing whitespace/blank-line runs/document ends, then enforce blank/NUL/surrogate/UTF-8 byte limits. Offsets always refer to the returned normalized string.

- [ ] **Step 3: Implement unit parsing and packing**

Markdown recognizes ATX headings, fenced code, paragraphs, and list blocks. Plain text splits blank-line paragraphs. Packing keeps whole units when possible, overlaps complete trailing units, and hard-splits only an oversized single unit.

Hard splitting emits unchanged contiguous substrings of normalized text with original offsets. Skip
whitespace-only candidate windows instead of merging them: every non-whitespace character must be
covered by at least one chunk, while uncovered gaps are permitted only when they contain Unicode
whitespace. Every emitted hard-split span stays within `target_chars`; trailing separator whitespace
may be omitted rather than exceeding that bound. The existing unsplit-fence exception remains
limited to `4 * target_chars`. Chunk consumers, evals, and citations must not assume chunks form a
contiguous full-text partition or reconstruct the document by concatenating them.

- [ ] **Step 4: Run tests and quality gates**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_knowledge_chunking.py -q
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/knowledge/chunking.py tests/unit/test_knowledge_chunking.py
.\.venv\Scripts\python.exe -m mypy packages/keel-core/src/keel_core/knowledge/chunking.py
```

- [ ] **Step 5: Commit**

```powershell
git add packages/keel-core/src/keel_core/knowledge/chunking.py tests/unit/test_knowledge_chunking.py
git commit -m "feat(knowledge): add deterministic chunking" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 4: Implement the Postgres Knowledge Store and Request Ledger

**Files:**
- Modify: `packages/keel-core/src/keel_core/knowledge/store.py`
- Test: `tests/integration/test_knowledge_postgres.py`

**Interfaces:**
- Produces `PostgresKnowledgeStore(engine, scope_id)` with the same behavior as the in-memory store.

- [ ] **Step 1: Write RED integration tests**

Cover: create-base model snapshot; concurrent idempotency; same-key/different-body conflict; crash-retry job attachment; N+1 allocation under document lock; old active surviving failure; stale desired activation; failed reindex/content revert; cancellation/failure/delete precedence; purge tombstones; batch write refusal unless version is indexing.

- [ ] **Step 2: Implement scope-safe SQL helpers**

Every method uses:

```python
async with self._engine.begin() as conn:
    await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
```

Every `SELECT`, `UPDATE`, and `DELETE` includes `scope_id = :scope`; global-ID foreign-scope
collisions return not found. Version row serialization includes `target_chars` and
`overlap_chars`. Use transaction advisory locks for idempotency keys and `SELECT ... FOR UPDATE`
for document version allocation/activation/chunk writes/purge.

- [ ] **Step 3: Implement request-ledger crash recovery**

Canonical fingerprint input is `{method, operation, normalized path ids, validated body}` serialized with sorted compact JSON and SHA-256. Store only the digest. The resource/ledger transaction commits before job creation; `attach_idempotent_job()` fills both ledger and version job IDs after `enqueue_once`.

- [ ] **Step 4: Run integration tests and quality gates**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests/integration/test_knowledge_postgres.py -q
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/knowledge/store.py tests/integration/test_knowledge_postgres.py
.\.venv\Scripts\python.exe -m mypy packages/keel-core/src/keel_core/knowledge/store.py
```

- [ ] **Step 5: Commit**

```powershell
git add packages/keel-core/src/keel_core/knowledge/store.py tests/integration/test_knowledge_postgres.py
git commit -m "feat(knowledge): add postgres lifecycle store" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 5: Add Hybrid Retrieval and Stable Citations

**Files:**
- Create: `packages/keel-core/src/keel_core/knowledge/search.py`
- Test: `tests/integration/test_knowledge_search.py`

**Interfaces:**
- Produces `KnowledgeSearcher.search()` and immutable version/chunk citations.

- [ ] **Step 1: Write RED search tests**

```python
async def test_search_returns_only_active_version_with_citation() -> None:
    hits, status = await searcher.search(kb_id, "installation", k=5)
    assert status.mode is KnowledgeSearchMode.hybrid
    assert [hit.citation.document_version_id for hit in hits] == [active_version_id]
    assert hits[0].citation.label == "Guide.md#chunk-1"


async def test_embedding_failure_keeps_lexical_results() -> None:
    hits, status = await failing_searcher.search(kb_id, "安装", k=5)
    assert hits
    assert status.mode is KnowledgeSearchMode.lexical_degraded
    assert status.semantic_error == "embedding_unavailable"
```

- [ ] **Step 2: Implement lexical and semantic arms**

Use trigram plus `plainto_tsquery('simple', ...)`, exact cosine KNN, strict active joins, and KB model/dim filters. Reject invalid query/k before SQL. If no embedder is configured use lexical; if embedder errors or pin mismatch use lexical-degraded with a bounded error kind.

- [ ] **Step 3: Implement local integer RRF and citations**

Build one insertion-ordered `dict[str, int]` across candidate chunk IDs, pass integer lists to existing `rrf_fuse()`, map back, and emit `cite_1..N` citations containing immutable version/chunk IDs, ordinal, offsets, title, source URI, and label.

- [ ] **Step 4: Run tests and quality gates**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests/integration/test_knowledge_search.py tests/unit/test_embeddings.py -q
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/knowledge/search.py tests/integration/test_knowledge_search.py
.\.venv\Scripts\python.exe -m mypy packages/keel-core/src/keel_core/knowledge/search.py
```

- [ ] **Step 5: Commit**

```powershell
git add packages/keel-core/src/keel_core/knowledge/search.py tests/integration/test_knowledge_search.py
git commit -m "feat(knowledge): add hybrid retrieval" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 6: Add Structured Tool Citations and `kb_search`

**Files:**
- Modify: `packages/keel-core/src/keel_core/protocols.py`
- Modify: `packages/keel-core/src/keel_core/events.py`
- Modify: `packages/keel-core/src/keel_core/loop.py`
- Create: `packages/keel-core/src/keel_core/knowledge/tools.py`
- Modify: `packages/keel-core/src/keel_core/__init__.py`
- Modify: `packages/keel-server/src/keel_server/runtime.py`
- Test: `tests/unit/test_contracts.py`
- Test: `tests/unit/test_loop.py`
- Test: `tests/unit/test_loop_suspend_resume.py`
- Test: `tests/unit/test_knowledge_tools.py`
- Test: `tests/integration/test_web_app.py`
- Test: `tests/integration/test_slice_e2e.py`

**Interfaces:**
- Produces generic `Citation`, additive `ToolResult.citations`, durable event citations, and the registered read-only `kb_search` tool.

- [ ] **Step 1: Write RED citation propagation tests**

```python
result = ToolResult(
    ok=True,
    output="[1] Guide.md#chunk-1\nInstall Keel.",
    citations=[Citation(id="cite_1", label="Guide.md#chunk-1", source="knowledge", metadata={})],
    taint=ContentTaint.tainted,
)
assert result.citations[0].id == "cite_1"
```

Assert both normal and resumed loop paths persist `payload["citations"]`, while provider projection still uses only `payload["output"]`.

Add one composed safety acceptance to `test_slice_e2e.py`: configure `kb_search`,
`email_send`, and `ConfusedDeputyEngine`; execute `kb_search` in one provider/tool batch;
assert its durable result is tainted and cited; attempt `email_send` in a later batch; assert an
approval is created and the send action has not run.

- [ ] **Step 2: Extend contracts and event writes additively**

Use `Field(default_factory=dict/list)`; never mutate existing event text. Keep old events valid when `citations` is absent.

- [ ] **Step 3: Implement `KnowledgeSearchTool`**

Validate exact JSON schema (`kb_id`, `query`, optional `k` 1..10), call `KnowledgeSearcher`, cap numbered snippets to `knowledge_tool_output_max_chars`, convert domain citations into generic citations, and always return `ContentTaint.tainted`, including empty/degraded results.

- [ ] **Step 4: Register the tool in web runtime**

Construct it only when both engine and embedder/search store are available; append its name to the read-only permission list and agent toolset. Do not add outbound connectors to the web agent.

- [ ] **Step 5: Run tests and quality gates**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_contracts.py tests/unit/test_loop.py tests/unit/test_loop_suspend_resume.py tests/unit/test_knowledge_tools.py tests/integration/test_web_app.py tests/integration/test_slice_e2e.py -q
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/protocols.py packages/keel-core/src/keel_core/events.py packages/keel-core/src/keel_core/loop.py packages/keel-core/src/keel_core/knowledge/tools.py packages/keel-server/src/keel_server/runtime.py
.\.venv\Scripts\python.exe -m mypy packages/keel-core/src/keel_core packages/keel-server/src/keel_server/runtime.py
```

- [ ] **Step 6: Commit**

```powershell
git add packages/keel-core/src/keel_core/protocols.py packages/keel-core/src/keel_core/events.py packages/keel-core/src/keel_core/loop.py packages/keel-core/src/keel_core/knowledge/tools.py packages/keel-core/src/keel_core/__init__.py packages/keel-server/src/keel_server/runtime.py tests/unit/test_contracts.py tests/unit/test_loop.py tests/unit/test_loop_suspend_resume.py tests/unit/test_knowledge_tools.py tests/integration/test_web_app.py tests/integration/test_slice_e2e.py
git commit -m "feat(knowledge): add cited search tool" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 7: Implement Ingest/Delete Handlers and Production Registry

**Files:**
- Create: `packages/keel-core/src/keel_core/knowledge/jobs.py`
- Create: `packages/keel-worker/src/keel_worker/knowledge.py`
- Modify: `packages/keel-worker/src/keel_worker/main.py`
- Test: `tests/unit/test_knowledge_jobs.py`
- Test: `tests/unit/test_worker_tasks.py`
- Test: `tests/integration/test_knowledge_worker.py`

**Interfaces:**
- Core produces worker-agnostic `KnowledgeJobHandlers`; worker produces
  `knowledge_job_definitions()` and registers `knowledge.ingest` / `knowledge.delete`.

- [ ] **Step 1: Write RED handler/hook tests**

Cover payload `extra="forbid"`, embedder pin mismatch, batch checkpoints, idempotent upsert, stale desired early exit, zombie write refusal, active switch, observed cancellation, queued-after-retry cancellation, failure cleanup, hook retry, delete non-cancellability, and purge idempotency.

- [ ] **Step 2: Implement ingest handler**

Parse the IDs-only payload before side effects; load version content, MIME type,
`chunking_version`, `target_chars`, and `overlap_chars`, plus the KB pinned model/dim, from durable
rows; strictly derive source type from the version MIME type. Then normalize/chunk, verify the
embedder pin, embed batches of `knowledge_embedding_batch_size`, checkpoint after each batch,
write only while the version is indexing, and activate atomically. Return bounded `JobResult`
IDs/counts only.

- [ ] **Step 3: Implement terminal hooks**

`KnowledgeJobHandlers.ingest_cancelled()` deletes incomplete chunks and marks cancelled unless
delete already won. `ingest_failed()` deletes incomplete chunks and marks failed unless delete
already won. Both are idempotent and preserve an old active version.

- [ ] **Step 4: Implement delete handler**

Lock and purge one document or every document in a deleted KB; clear raw content/source URI/description, delete chunks, retain minimal tombstones. Register with `CancelMode.disabled` and `max_attempts=2_147_483_647`.

- [ ] **Step 5: Wire worker startup**

In `keel_worker.knowledge`, wrap the core handler methods as `JobDefinition` values. Replace
`_empty_job_registry()` with a factory that receives the scope-bound `PostgresKnowledgeStore`,
startup embedder, and settings, then registers exactly the two production definitions.

- [ ] **Step 6: Run unit and real arq acceptance**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
.\.venv\Scripts\python.exe -m pytest tests/unit/test_knowledge_jobs.py tests/unit/test_worker_tasks.py tests/integration/test_knowledge_worker.py -q
```

- [ ] **Step 7: Commit**

```powershell
git add packages/keel-core/src/keel_core/knowledge/jobs.py packages/keel-worker/src/keel_worker/knowledge.py packages/keel-worker/src/keel_worker/main.py tests/unit/test_knowledge_jobs.py tests/unit/test_worker_tasks.py tests/integration/test_knowledge_worker.py
git commit -m "feat(knowledge): add durable ingest jobs" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 8: Add the Idempotent Knowledge REST API

**Files:**
- Create: `packages/keel-core/src/keel_core/knowledge/service.py`
- Create: `packages/keel-server/src/keel_server/api/knowledge.py`
- Modify: `packages/keel-server/src/keel_server/app.py`
- Test: `tests/integration/test_knowledge_api.py`
- Test: `tests/unit/test_server.py`

**Interfaces:**
- Produces every endpoint in design §16 and crash-safe resource/job replay.

- [ ] **Step 1: Write RED API tests**

Test viewer reads/search, viewer mutation denial, operator CRUD, missing/unsafe/oversized
`Idempotency-Key`, same-key replay, different-body 409, model snapshot, first-failure update
recovery, no-active reindex 409, immediate delete hide, cross-scope 404, disabled delete-job
cancel 409, crash-after-resource-commit retry attaching the same job, and malformed document
content whose raw input must not appear in the 422 body.

- [ ] **Step 2: Implement `KnowledgeService`**

For each mutation: validate canonical request, acquire ledger entry, create/update/tombstone resource transactionally, enqueue deterministic `knowledge.ingest` or `knowledge.delete`, best-effort arq dispatch, attach job IDs, and return the authoritative resource/version/job. Never return success without a durable job row for async work.

- [ ] **Step 3: Implement router DTOs and error mapping**

Use `Header(alias="Idempotency-Key")`, viewer router default, operator dependencies on mutations,
404 for malformed/foreign IDs, 409 for conflicts/no-active/not-cancellable, 413 for document
bytes, and 503 when create-KB has no configured embedder. Register a
`RequestValidationError` sanitizer that returns only bounded `loc/type/msg` fields and strips
Pydantic's raw `input`/unsafe context before serializing any 422 response.

- [ ] **Step 4: Wire app state**

With Postgres enabled, construct one scope-bound store/service and include `knowledge.router`; with the memory event-store profile, read endpoints return a clear unavailable response rather than a fake successful KB.

- [ ] **Step 5: Run API tests and quality gates**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests/integration/test_knowledge_api.py tests/unit/test_server.py -q
.\.venv\Scripts\python.exe -m ruff check packages/keel-core/src/keel_core/knowledge/service.py packages/keel-server/src/keel_server/api/knowledge.py packages/keel-server/src/keel_server/app.py tests/integration/test_knowledge_api.py tests/unit/test_server.py
.\.venv\Scripts\python.exe -m mypy packages/keel-core/src/keel_core/knowledge/service.py packages/keel-server/src/keel_server
```

- [ ] **Step 6: Commit**

```powershell
git add packages/keel-core/src/keel_core/knowledge/service.py packages/keel-server/src/keel_server/api/knowledge.py packages/keel-server/src/keel_server/app.py tests/integration/test_knowledge_api.py tests/unit/test_server.py
git commit -m "feat(knowledge): add scoped REST API" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 9: Add the Minimal React Knowledge Page

**Files:**
- Create: `web/src/features/knowledge/types.ts`
- Create: `web/src/features/knowledge/useKnowledge.ts`
- Create: `web/src/features/knowledge/KnowledgePage.tsx`
- Create: `web/src/features/knowledge/KnowledgePage.test.tsx`
- Modify: `web/src/lib/api.ts`
- Modify: `web/src/router.tsx`
- Modify: `web/src/components/Sidebar.tsx`
- Modify: `web/src/test/handlers.ts`
- Test: `web/src/components/AppShell.test.tsx`

**Interfaces:**
- Consumes the REST API; produces `/knowledge` management/search UI with job polling.

- [ ] **Step 1: Write RED page tests**

Test route/sidebar, empty state, create/select KB, add/update/reindex/delete document, optimistic hide, terminal job refetch, search snippets/citations/mode badge, and bounded error states.

- [ ] **Step 2: Extend the API helper**

Allow callers to pass headers without losing JSON content type:

```ts
function jsonInit(method: string, body: unknown, init?: RequestInit): RequestInit {
  const headers = new Headers(init?.headers);
  headers.set("Content-Type", "application/json");
  return {
    ...init,
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  };
}

post: <T>(path: string, body?: unknown, init?: RequestInit) =>
  req<T>(path, jsonInit("POST", body, init))
```

Implement equivalent `put`/`del` support.

- [ ] **Step 3: Implement hooks and stable mutation keys**

Generate one UUID idempotency key per user action, not per React retry/render. Poll `/v1/jobs/{id}` only while queued/running, then invalidate KB/document queries.

- [ ] **Step 4: Implement the page**

Use existing `Topbar`, `Card`, `Button`, `Badge`, `Banner`, and `Skeleton`; no new component library. Keep text/Markdown in a textarea, show document status/version/progress, and render structured citation title/chunk/range.

- [ ] **Step 5: Run web tests/lint/build**

```powershell
Set-Location web
npm ci
npm run test
npm run lint
npm run build
```

- [ ] **Step 6: Commit**

```powershell
git add web/src/features/knowledge web/src/lib/api.ts web/src/router.tsx web/src/components/Sidebar.tsx web/src/components/AppShell.test.tsx web/src/test/handlers.ts
git commit -m "feat(web): add knowledge management page" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 10: Add Replayable Knowledge Evals

**Files:**
- Create: `packages/keel-worker/src/keel_worker/knowledge_evals/__init__.py`
- Create: `packages/keel-worker/src/keel_worker/knowledge_evals/models.py`
- Create: `packages/keel-worker/src/keel_worker/knowledge_evals/runner.py`
- Create: `packages/keel-worker/src/keel_worker/knowledge_evals/cli.py`
- Create: `scripts/run_knowledge_evals.py`
- Create: `evals/datasets/knowledge/v1.jsonl`
- Create: `evals/cassettes/knowledge/v1-embeddings.json`
- Test: `tests/unit/test_knowledge_eval_models.py`
- Test: `tests/unit/test_knowledge_eval_runner.py`

**Interfaces:**
- Produces independent replay/live/record Knowledge evals with exit codes 0 quality-pass, 1 gate-fail, 2 infrastructure/dataset/cassette failure.

- [ ] **Step 1: Write RED strict-model and scoring tests**

Cases cover English paraphrase, Chinese lexical/semantic, heading citation, multi-document rank, no-answer, embedding failure degradation, deleted-doc leakage, and prompt-injection taint.

- [ ] **Step 2: Implement strict dataset models**

Forbid unknown keys, pin version 1, and assert expected semantic locators by `content_sha256 + version + ordinal + offsets`, never generated UUIDs.
Treat those offsets as exact chunk locators; hard-split chunks may leave only Unicode-whitespace gaps,
so eval and citation assertions must not require contiguous full-document chunk coverage.

- [ ] **Step 3: Implement runner and gates**

Reuse `EmbeddingCassette`, `ReplayEmbedder`, `RecordingEmbedder`, `FailingEmbedder`, and eval DB guards. Seed one isolated scope per case, ingest synchronously through production chunk/store/search code, execute queries, delete where requested, calculate Recall@5/MRR/citation precision/leakage/taint/degraded pass rates, and enforce design thresholds.

- [ ] **Step 4: Add dataset and record cassette**

Run live record only against `keel_test`:

```powershell
$env:KEEL_EVAL_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe scripts/run_knowledge_evals.py --mode live --record
```

Then prove replay makes no live fallback:

```powershell
.\.venv\Scripts\python.exe scripts/run_knowledge_evals.py --mode replay
```

- [ ] **Step 5: Run unit tests and quality gates**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_knowledge_eval_models.py tests/unit/test_knowledge_eval_runner.py -q
.\.venv\Scripts\python.exe -m ruff check packages/keel-worker/src/keel_worker/knowledge_evals scripts/run_knowledge_evals.py tests/unit/test_knowledge_eval_models.py tests/unit/test_knowledge_eval_runner.py
.\.venv\Scripts\python.exe -m mypy packages/keel-worker/src/keel_worker/knowledge_evals
```

- [ ] **Step 6: Commit**

```powershell
git add packages/keel-worker/src/keel_worker/knowledge_evals scripts/run_knowledge_evals.py evals/datasets/knowledge/v1.jsonl evals/cassettes/knowledge/v1-embeddings.json tests/unit/test_knowledge_eval_models.py tests/unit/test_knowledge_eval_runner.py
git commit -m "feat(evals): add knowledge replay suite" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

### Task 11: Verify the Complete Slice and Update Status

**Files:**
- Modify: `docs/STATUS.md`
- Modify only if verification finds a directly coupled defect: files changed in Tasks 1-10.

**Interfaces:**
- Produces the verified, merge-ready RAG/KB vertical slice and records the next milestone.

- [ ] **Step 1: Run all Python non-integration tests**

```powershell
.\.venv\Scripts\python.exe -m pytest -m "not integration" -q
```

- [ ] **Step 2: Run all integration tests**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_EVAL_DATABASE_URL = $env:KEEL_TEST_DATABASE_URL
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
.\.venv\Scripts\python.exe -m pytest -m integration -q
```

- [ ] **Step 3: Run repo-wide Python quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m mypy packages
```

- [ ] **Step 4: Run all web gates**

```powershell
Set-Location web
npm run test
npm run lint
npm run build
```

- [ ] **Step 5: Verify migration head and replay evals**

```powershell
Set-Location ..
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_EVAL_DATABASE_URL = $env:KEEL_TEST_DATABASE_URL
.\.venv\Scripts\python.exe -m alembic -c alembic.ini current
.\.venv\Scripts\python.exe scripts/run_knowledge_evals.py --mode replay
```

Expected head: `0010_knowledge_base`.

- [ ] **Step 6: Run isolated live smoke**

Create a KB, ingest Markdown with headings and prompt-injection text, wait for the real arq job, search lexical/hybrid, verify structured citation + taint, update while old active remains available, delete and verify immediate zero hits, then wait for purge and assert zero chunks/raw content. Do not use production scope/database/Redis.

- [ ] **Step 7: Run focused final review**

Review only high-risk contracts: scope/RLS, same-KB FKs, request-ledger races, job hook ordering, zombie writes, active/desired switching, delete purge, taint/citations, and model pinning. Fix every high-confidence finding and rerun the affected gates.

- [ ] **Step 8: Update `docs/STATUS.md`**

Mark RAG/KB complete with the actual test/eval baseline and set Event Upcasters + Retention/Erasure as the next slice.

- [ ] **Step 9: Commit**

```powershell
git add docs/STATUS.md
git commit -m "docs: record RAG knowledge baseline" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" -m "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

## Self-Review Result

- Spec coverage: schema, lifecycle, current-only fingerprint dedupe, job hooks/cancel modes, deterministic chunking, hybrid/degraded retrieval, citations, taint, API/RBAC/idempotency, UI, evals, purge, observability, and rollout all map to a task.
- Placeholder scan: no implementation step is deferred to an unspecified future task; non-goals remain explicit.
- Type consistency: `CancelMode`, hook signatures, Knowledge enums/models, `KnowledgeSearcher.search`, payload models, and worker registry names are used consistently across tasks.
- Dependency order: jobs/schema precede stores; stores/chunker precede search/jobs; citations precede tool/runtime; handlers precede API enqueue; API precedes web; production retrieval precedes evals.
