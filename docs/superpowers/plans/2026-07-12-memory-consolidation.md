# Keel Memory Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A scheduled, unattended `memory-consolidator` agent reads recent trusted Web conversations in bounded batches and distills durable knowledge into (a) **proposals** for the persona/human Core blocks (human-approved, never auto-applied) and (b) **archival** memory rows with provenance and dedupe — advancing a leased global cursor only when the run genuinely succeeds.

**Architecture:** A daily schedule enqueues `run_agent("memory-consolidation:web:local")`; the worker claims a per-scope cursor lease, reads an id-ordered batch from the `events` log (excluding `digest:`/`consolidation:` sessions), and runs a constrained agent whose only two tools are `memory_propose_rewrite` (writes a `memory_proposals` row) and `archival_consolidate_insert` (writes a deduped `archival` row). A per-run `ConsolidationRunContext` enforces the "cite a real user event id" evidence rule and counts validation errors; the cursor advances only when `reason == completed AND validation_errors == 0`. Core memory is mutated exclusively through the human-approved proposal-apply CAS path.

**Tech Stack:** Python 3.12, uv workspace, FastAPI, arq (Redis), SQLAlchemy async + Alembic (Postgres), pgvector, pytest (`asyncio_mode=auto`), ruff, mypy --strict.

## Global Constraints

- **Run uv as `python -m uv`** (uv is not on PATH). Unit tests: `python -m uv run pytest <path> -v`.
- **Lint/type gates (must stay green after every task):** `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages` (strict).
- **ruff lint selects `E,F,I,UP,B,ASYNC`** (line length 100). `BLE` is NOT selected, so a defensive `except Exception` needs no `noqa`. Put `from __future__ import annotations` at the top of every new module.
- **Integration tests fail closed.** `tests/integration/conftest.py::_require_test_database_url` requires `KEEL_TEST_DATABASE_URL` to be set and to name the database exactly `keel_test`. Every integration `Run` command below **must** set it. On this PowerShell host set it first:
  ```powershell
  $env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"
  python -m uv run pytest <path> -v
  ```
  The bash equivalent (CI/Linux) is `KEEL_TEST_DATABASE_URL=postgresql+psycopg://keel:keel@localhost:5432/keel_test python -m uv run pytest <path> -v`.
- **Windows host caveat:** `tests/integration/conftest.py` installs `WindowsSelectorEventLoopPolicy`, so Postgres-backed integration tests run on this host against a live `keel_test` database. If no Postgres is reachable the integration fixtures skip; run them in `docker compose`/CI when a local server is unavailable.
- **Commits (every task):** stage with `git add <paths>` (repo `core.safecrlf=false`). Commit with two `-m` flags — subject first, then BOTH trailers in one quoted paragraph so git parses them as a single trailer block:
  ```bash
  git commit -m "<type>(consolidation): <subject>" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
  Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
  ```
- **Core memory is never auto-written.** `memory_propose_rewrite` only inserts a `memory_proposals` row. `memory_blocks` change exclusively through the human-approved `MemoryProposalStore.approve()` CAS path.
- **Every saved item must be grounded in at least one user message from the current batch** (user-evidence rule); assistant-only content can never justify a write.
- **Cursor advances only on genuine success:** `should_advance_cursor(reason, validation_errors)` returns `True` iff `reason == StopReason.completed and validation_errors == 0`. Any tool-level infra failure increments `validation_errors`, which blocks advancement; whole-batch retry is safe because both writes are idempotent (proposal idempotency key + archival content hash).
- **Single scope for this slice:** durable scope is `web:local` (schedule id `memory-consolidation:web:local`, agent id `memory-consolidator`). Multi-scope scheduling is out of scope.
- **Out of scope (do NOT build):** evals/Langfuse datasets, Memory UI, archival auto-deletion / semantic-merge / retention, RAG / Knowledge Base, IM-digest session consolidation, Core proposal auto-approval, multi-tenant cross-scope scheduling, LLM structured-output gateway.

## File Structure

**Create**
- `packages/keel-core/src/keel_core/consolidation/__init__.py` — package init; final public export surface (finalized in Task 11).
- `packages/keel-core/src/keel_core/consolidation/hashing.py` — `normalize_whitespace`, `archival_content_hash`, `consolidation_idempotency_key`.
- `packages/keel-core/src/keel_core/consolidation/context.py` — `ConsolidationRunContext`, `should_advance_cursor`.
- `packages/keel-core/src/keel_core/consolidation/cursor.py` — `ConsolidationLease`, `ConsolidationCursorStore`.
- `packages/keel-core/src/keel_core/consolidation/reader.py` — `ConsolidationMessage`, `ConsolidationBatch`, `ConsolidationBatchReader`.
- `packages/keel-core/src/keel_core/consolidation/proposals.py` — `MemoryProposal`, `ProposalOutcome`, `ProposalResolution`, `MemoryProposalStore`.
- `packages/keel-core/src/keel_core/consolidation/tools.py` — `validate_propose_rewrite`, `validate_archival_insert`, `ProposeRewriteTool`, `ArchivalConsolidateInsertTool`, tool-name constants.
- `packages/keel-core/src/keel_core/consolidation/agent.py` — `CONSOLIDATION_SYSTEM_INSTRUCTION`, `MEMORY_CONSOLIDATOR_AGENT_ID`, `consolidation_schedule_id`, `consolidation_session_id`, `format_consolidation_prompt`, `consolidation_system_context`, `consolidation_permissions`, `consolidation_registry`, `build_consolidation_agent`.
- `migrations/versions/0008_memory_consolidation.py` — `consolidation_cursors` + `memory_proposals` tables + RLS; `archival` provenance columns + dedupe index.
- `scripts/seed_consolidation_schedule.py` — idempotent daily-schedule seeder.
- Tests: `tests/unit/test_consolidation_hashing.py`, `tests/unit/test_consolidation_context.py`, `tests/unit/test_consolidation_tools.py`, `tests/unit/test_consolidation_agent.py`, `tests/unit/test_consolidation_exports.py`, `tests/integration/test_consolidation_migration.py`, `tests/integration/test_consolidation_cursor.py`, `tests/integration/test_consolidation_reader.py`, `tests/integration/test_consolidation_proposals.py`, `tests/integration/test_consolidation_tools_postgres.py`, `tests/integration/test_consolidation_worker.py`, `tests/integration/test_memory_proposals_api.py`, `tests/integration/test_consolidation_e2e.py`.

**Modify**
- `packages/keel-core/src/keel_core/config.py` — add 7 `consolidation_*` settings.
- `packages/keel-core/src/keel_core/memory.py` — add `PostgresMemoryStore.versions()`.
- `packages/keel-core/src/keel_core/search.py` — add `ArchivalStore.add_consolidated()`.
- `packages/keel-worker/src/keel_worker/main.py` — refactor `run_agent` into an agent dispatcher; add `_run_digest`, `consolidate_memory`; register the task; add `ctx["embedder"]` in startup.
- `packages/keel-server/src/keel_server/api/v1.py` — add proposals list/approve/reject + manual consolidation-run endpoints with RBAC.
- `tests/integration/conftest.py` — add `consolidation_cursors`, `memory_proposals` to the `TRUNCATE` list.
- `tests/unit/test_config.py` — assert the new settings defaults.
- `tests/unit/test_worker_tasks.py` — assert the `run_agent` dispatcher routing.

**File responsibilities (map before tasks)**

| File | Owns | Consumes | Produced by |
| --- | --- | --- | --- |
| `config.py` | `consolidation_*` tunables | env (`KEEL_` prefix) | Task 1 |
| `0008_memory_consolidation.py` | schema: cursors, proposals, archival provenance | migration 0007 head | Task 2 |
| `hashing.py` | canonical text + hash + idempotency key | stdlib only | Task 3 |
| `context.py` | per-run evidence set + error count + advance predicate | `types.StopReason` | Task 4 |
| `cursor.py` | atomic lease claim/complete/fail | `consolidation_cursors` | Task 5 |
| `reader.py` | bounded id-ordered batch + user-evidence set | `events` table, `config` | Task 6 |
| `proposals.py` | proposal persist + list + approve CAS + reject | `memory_proposals`, `memory_blocks`, `hashing` | Task 7 |
| `memory.py::versions()` | current block versions for the prompt | `memory_blocks` | Task 7 |
| `search.py::add_consolidated()` | deduped archival write w/ provenance | `archival`, `hashing` | Task 8 |
| `tools.py` | 2 constrained tools + pure validators | `context`, `proposals`, `search`, `hashing`, `config` | Task 8 |
| `agent.py` | agent spec, prompt, permissions, registry | `tools`, `reader`, `loop`, `permissions` | Task 9 |
| `worker/main.py` | dispatch + orchestrate the run | all consolidation modules | Task 10 |
| `seed_consolidation_schedule.py` | idempotent schedule seed | `schedules`, `agent.py` | Task 10 |
| `api/v1.py` | proposal REST + manual run + RBAC | `proposals`, `auth` | Task 11 |
| `consolidation/__init__.py` | public export surface | all submodules | Task 11 |

---

## Task 1: Consolidation configuration settings

**Files:**
- Modify: `packages/keel-core/src/keel_core/config.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces (all on `Settings`, `KEEL_` env prefix): `consolidation_min_messages: int = 10`, `consolidation_batch_messages: int = 50`, `consolidation_input_max_chars: int = 20_000`, `consolidation_message_max_chars: int = 4_000`, `consolidation_archival_min_confidence: float = 0.8`, `consolidation_lease_seconds: int = 600`, `consolidation_token_budget: int = 4_000`, `consolidation_semantic_dedupe_distance: float = 0.05` (cosine ceiling for the exact-source archival retry merge; calibrated from live replay max 0.0468; `0` disables the semantic pass).
- Consumes: nothing.

- [ ] **Step 1: Write the failing test** — append to `tests/unit/test_config.py`:

```python
def test_consolidation_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.consolidation_min_messages == 10
    assert settings.consolidation_batch_messages == 50
    assert settings.consolidation_input_max_chars == 20_000
    assert settings.consolidation_message_max_chars == 4_000
    assert settings.consolidation_archival_min_confidence == 0.8
    assert settings.consolidation_lease_seconds == 600
    assert settings.consolidation_token_budget == 4_000
    assert settings.consolidation_semantic_dedupe_distance == 0.05


def test_consolidation_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_CONSOLIDATION_MIN_MESSAGES", "3")
    monkeypatch.setenv("KEEL_CONSOLIDATION_ARCHIVAL_MIN_CONFIDENCE", "0.5")
    monkeypatch.setenv("KEEL_CONSOLIDATION_SEMANTIC_DEDUPE_DISTANCE", "0.2")

    from keel_core.config import Settings

    settings = Settings()
    assert settings.consolidation_min_messages == 3
    assert settings.consolidation_archival_min_confidence == 0.5
    assert settings.consolidation_semantic_dedupe_distance == 0.2
```

- [ ] **Step 2: Run RED** — `python -m uv run pytest tests/unit/test_config.py::test_consolidation_defaults -v`
  - Expected failure: `AttributeError: 'Settings' object has no attribute 'consolidation_min_messages'`.

- [ ] **Step 3: Implement** — in `packages/keel-core/src/keel_core/config.py`, add the fields to the `Settings` class next to the existing `memory_block_max_chars` / embedding fields (keep alphabetically-adjacent grouping and the existing `Field`/plain-default style):

```python
    consolidation_min_messages: int = 10
    consolidation_batch_messages: int = 50
    consolidation_input_max_chars: int = 20_000
    consolidation_message_max_chars: int = 4_000
    consolidation_archival_min_confidence: float = 0.8
    consolidation_lease_seconds: int = 600
    consolidation_token_budget: int = 4_000
    # Exact-source archival retry merge: cosine ceiling; 0 disables the semantic pass.
    consolidation_semantic_dedupe_distance: float = 0.05
```

- [ ] **Step 4: Run GREEN** — `python -m uv run pytest tests/unit/test_config.py -v` (all config tests pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/config.py tests/unit/test_config.py
git commit -m "feat(consolidation): add consolidation settings" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 2: Migration 0008 — cursors, proposals, archival provenance

**Files:**
- Create: `migrations/versions/0008_memory_consolidation.py`
- Modify: `tests/integration/conftest.py` (add `consolidation_cursors`, `memory_proposals` to `TRUNCATE`)
- Test: `tests/integration/test_consolidation_migration.py`

**Interfaces:**
- Produces tables/columns:
  - `consolidation_cursors(scope_id text PK, last_event_id bigint NOT NULL DEFAULT 0, last_run_at timestamptz, last_status text, lease_token text, lease_expires_at timestamptz, updated_at timestamptz NOT NULL DEFAULT now())` + RLS.
  - `memory_proposals(id text PK, scope_id text NOT NULL, block text NOT NULL CHECK block IN ('persona','human'), expected_version int NOT NULL, proposed_value text NOT NULL, reason text NOT NULL, confidence double precision NOT NULL CHECK 0<=confidence<=1, source_event_ids bigint[] NOT NULL, idempotency_key text NOT NULL, status text NOT NULL DEFAULT 'pending' CHECK status IN ('pending','applied','rejected','stale'), created_at timestamptz NOT NULL DEFAULT now(), resolved_at timestamptz, resolved_by text, UNIQUE(scope_id, idempotency_key))` + index `(scope_id, status)` + RLS.
  - `archival` gains `origin text NOT NULL DEFAULT 'agent'`, `content_hash text`, `source_event_ids bigint[]`; partial unique index `ix_archival_scope_content_hash ON archival(scope_id, content_hash) WHERE content_hash IS NOT NULL`.
- Consumes: revision `0007_message_embeddings` (down_revision).

- [ ] **Step 1: Write the failing test** — create `tests/integration/test_consolidation_migration.py`:

```python
"""Schema assertions for migration 0008 (memory consolidation)."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


async def _columns(engine: AsyncEngine, table: str) -> dict[str, str]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = :t"
                ),
                {"t": table},
            )
        ).all()
    return {str(r.column_name): str(r.data_type) for r in rows}


async def test_consolidation_cursors_table(migrated_db: AsyncEngine) -> None:
    cols = await _columns(migrated_db, "consolidation_cursors")
    assert cols["scope_id"] == "text"
    assert cols["last_event_id"] == "bigint"
    assert "lease_token" in cols
    assert "lease_expires_at" in cols


async def test_memory_proposals_table(migrated_db: AsyncEngine) -> None:
    cols = await _columns(migrated_db, "memory_proposals")
    assert cols["block"] == "text"
    assert cols["expected_version"] == "integer"
    assert cols["source_event_ids"] == "ARRAY"
    assert cols["status"] == "text"


async def test_archival_provenance_columns(migrated_db: AsyncEngine) -> None:
    cols = await _columns(migrated_db, "archival")
    assert cols["origin"] == "text"
    assert "content_hash" in cols
    assert cols["source_event_ids"] == "ARRAY"


async def test_archival_content_hash_unique_index(migrated_db: AsyncEngine) -> None:
    async with migrated_db.connect() as conn:
        exists = (
            await conn.execute(
                text(
                    "SELECT 1 FROM pg_indexes WHERE indexname = "
                    "'ix_archival_scope_content_hash'"
                )
            )
        ).one_or_none()
    assert exists is not None
```

- [ ] **Step 2: Run RED** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_migration.py -v`
  - Expected failure: `sqlalchemy...UndefinedTable`-style error / assertion failure because `consolidation_cursors` and the new `archival` columns do not exist at head 0007.

- [ ] **Step 3: Implement the migration** — create `migrations/versions/0008_memory_consolidation.py`:

```python
"""memory consolidation: cursors, proposals, archival provenance.

Revision ID: 0008_memory_consolidation
Revises: 0007_message_embeddings
Create Date: 2026-07-12
"""

from __future__ import annotations

from alembic import op

revision = "0008_memory_consolidation"
down_revision = "0007_message_embeddings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE consolidation_cursors (
            scope_id text PRIMARY KEY,
            last_event_id bigint NOT NULL DEFAULT 0,
            last_run_at timestamptz,
            last_status text,
            lease_token text,
            lease_expires_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("ALTER TABLE consolidation_cursors ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON consolidation_cursors "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )

    op.execute(
        """
        CREATE TABLE memory_proposals (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            block text NOT NULL CHECK (block IN ('persona', 'human')),
            expected_version integer NOT NULL,
            proposed_value text NOT NULL,
            reason text NOT NULL,
            confidence double precision NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
            source_event_ids bigint[] NOT NULL,
            idempotency_key text NOT NULL,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'applied', 'rejected', 'stale')),
            created_at timestamptz NOT NULL DEFAULT now(),
            resolved_at timestamptz,
            resolved_by text,
            UNIQUE (scope_id, idempotency_key)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_memory_proposals_scope_status "
        "ON memory_proposals (scope_id, status)"
    )
    op.execute("ALTER TABLE memory_proposals ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON memory_proposals "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )

    op.execute("ALTER TABLE archival ADD COLUMN origin text NOT NULL DEFAULT 'agent'")
    op.execute("ALTER TABLE archival ADD COLUMN content_hash text")
    op.execute("ALTER TABLE archival ADD COLUMN source_event_ids bigint[]")
    op.execute(
        "CREATE UNIQUE INDEX ix_archival_scope_content_hash "
        "ON archival (scope_id, content_hash) WHERE content_hash IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_archival_scope_content_hash")
    op.execute("ALTER TABLE archival DROP COLUMN IF EXISTS source_event_ids")
    op.execute("ALTER TABLE archival DROP COLUMN IF EXISTS content_hash")
    op.execute("ALTER TABLE archival DROP COLUMN IF EXISTS origin")
    op.execute("DROP TABLE IF EXISTS memory_proposals")
    op.execute("DROP TABLE IF EXISTS consolidation_cursors")
```

- [ ] **Step 4: Update the integration TRUNCATE list** — in `tests/integration/conftest.py`, prepend the two new tables to the existing `TRUNCATE` statement in the `migrated_db` fixture (do not reorder or drop any existing table). Replace:

```python
            text(
                "TRUNCATE message_embeddings, events, sessions, memory_blocks, "
                "memory_block_versions, connector_tokens, archival, schedules, approvals"
            )
```

  with:

```python
            text(
                "TRUNCATE consolidation_cursors, memory_proposals, "
                "message_embeddings, events, sessions, memory_blocks, "
                "memory_block_versions, connector_tokens, archival, schedules, approvals"
            )
```

- [ ] **Step 5: Run GREEN** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_migration.py -v` (4 pass). Confirm the migration is linear: `python -m uv run alembic heads` shows a single head `0008_memory_consolidation`.

- [ ] **Step 6: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 7: Commit**

```bash
git add migrations/versions/0008_memory_consolidation.py tests/integration/conftest.py tests/integration/test_consolidation_migration.py
git commit -m "feat(consolidation): add migration 0008 for cursors, proposals, archival provenance" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 3: Hashing + idempotency keys (`consolidation.hashing`)

**Files:**
- Create: `packages/keel-core/src/keel_core/consolidation/__init__.py` (docstring only for now; full re-exports land in Task 11)
- Create: `packages/keel-core/src/keel_core/consolidation/hashing.py`
- Test: `tests/unit/test_consolidation_hashing.py`

**Interfaces:**
- Produces: `normalize_whitespace(text: str) -> str`; `archival_content_hash(content: str) -> str`; `consolidation_idempotency_key(scope_id: str, block: str, expected_version: int, proposed_value: str, source_event_ids: Sequence[int]) -> str`.
- Consumes: nothing (pure stdlib).

- [ ] **Step 1: Write the failing test** — create `tests/unit/test_consolidation_hashing.py`:

```python
"""Unit tests for consolidation hashing + idempotency keys."""

from __future__ import annotations

from keel_core.consolidation.hashing import (
    archival_content_hash,
    consolidation_idempotency_key,
    normalize_whitespace,
)


def test_normalize_whitespace_collapses_and_strips() -> None:
    assert normalize_whitespace("  hello   world \n foo ") == "hello world foo"


def test_archival_content_hash_ignores_whitespace_and_case_differences() -> None:
    assert archival_content_hash("hello world") == archival_content_hash("  hello   world ")
    assert archival_content_hash("Hello World") == archival_content_hash("hello world")
    assert archival_content_hash("hello world") != archival_content_hash("hello mars")


def test_idempotency_key_is_stable_and_order_independent() -> None:
    a = consolidation_idempotency_key("web:local", "human", 0, "likes tea", [3, 1, 2])
    b = consolidation_idempotency_key("web:local", "human", 0, "likes tea", [2, 3, 1])
    assert a == b
    assert len(a) == 64


def test_idempotency_key_varies_with_every_input() -> None:
    base = consolidation_idempotency_key("web:local", "human", 0, "likes tea", [1])
    assert base != consolidation_idempotency_key("web:local", "human", 1, "likes tea", [1])
    assert base != consolidation_idempotency_key("web:local", "persona", 0, "likes tea", [1])
    assert base != consolidation_idempotency_key("web:local", "human", 0, "likes coffee", [1])
    assert base != consolidation_idempotency_key("other", "human", 0, "likes tea", [1])
```

- [ ] **Step 2: Run RED** — `python -m uv run pytest tests/unit/test_consolidation_hashing.py -v`
  - Expected failure: `ModuleNotFoundError: No module named 'keel_core.consolidation'`.

- [ ] **Step 3: Implement** — create `packages/keel-core/src/keel_core/consolidation/__init__.py`:

```python
"""Memory consolidation subsystem (spec 2026-07-12-memory-consolidation).

A scheduled, unattended agent reviews recent conversation and durably records
lasting facts: it *proposes* core-memory rewrites (human-reviewed) and inserts
deduplicated, provenance-tagged archival passages. The public surface is
finalized in the package re-exports below (see the individual submodules).
"""

from __future__ import annotations
```

  Then create `packages/keel-core/src/keel_core/consolidation/hashing.py`:

```python
"""Deterministic hashing for consolidation dedupe + write idempotency.

``archival_content_hash`` keys archival dedupe on case-folded, whitespace-normalized
content so trivially reformatted passages collapse to one row. ``consolidation_idempotency_key``
makes a proposal write idempotent under whole-batch retry: the same (scope, block,
expected_version, value, cited events) always yields the same key, and the cited-event
list is order-independent.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence


def normalize_whitespace(text: str) -> str:
    """Collapse every run of whitespace to a single space and strip the ends."""
    return " ".join(text.split())


def archival_content_hash(content: str) -> str:
    """A stable SHA-256 of case-folded, whitespace-normalized content."""
    normalized = normalize_whitespace(content).casefold()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def consolidation_idempotency_key(
    scope_id: str,
    block: str,
    expected_version: int,
    proposed_value: str,
    source_event_ids: Sequence[int],
) -> str:
    """A stable SHA-256 identifying one proposal write (order-independent event ids)."""
    payload = json.dumps(
        {
            "scope_id": scope_id,
            "block": block,
            "expected_version": expected_version,
            "proposed_value": proposed_value,
            "source_event_ids": sorted({int(i) for i in source_event_ids}),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
```

- [ ] **Step 4: Run GREEN** — `python -m uv run pytest tests/unit/test_consolidation_hashing.py -v` (4 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/consolidation/__init__.py packages/keel-core/src/keel_core/consolidation/hashing.py tests/unit/test_consolidation_hashing.py
git commit -m "feat(consolidation): add content hashing + idempotency keys" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 4: Run context + cursor-advance predicate (`consolidation.context`)

**Files:**
- Create: `packages/keel-core/src/keel_core/consolidation/context.py`
- Test: `tests/unit/test_consolidation_context.py`

**Interfaces:**
- Produces: `ConsolidationRunContext(allowed_event_ids: frozenset[int], allowed_user_event_ids: frozenset[int], successful_actions: int = 0, validation_errors: int = 0)` (mutable dataclass — tools increment the counters); `should_advance_cursor(reason: StopReason, validation_errors: int) -> bool`.
- Consumes: `keel_core.types.StopReason`.

- [ ] **Step 1: Write the failing test** — create `tests/unit/test_consolidation_context.py`:

```python
"""Unit tests for the consolidation run context + cursor-advance predicate."""

from __future__ import annotations

from keel_core.consolidation.context import ConsolidationRunContext, should_advance_cursor
from keel_core.types import StopReason


def test_run_context_defaults() -> None:
    ctx = ConsolidationRunContext(
        allowed_event_ids=frozenset({1, 2}),
        allowed_user_event_ids=frozenset({1}),
    )
    assert ctx.successful_actions == 0
    assert ctx.validation_errors == 0
    assert 1 in ctx.allowed_user_event_ids
    assert ctx.allowed_user_event_ids <= ctx.allowed_event_ids


def test_counters_are_mutable() -> None:
    ctx = ConsolidationRunContext(allowed_event_ids=frozenset(), allowed_user_event_ids=frozenset())
    ctx.validation_errors += 1
    ctx.successful_actions += 2
    assert ctx.validation_errors == 1
    assert ctx.successful_actions == 2


def test_should_advance_only_on_clean_completion() -> None:
    assert should_advance_cursor(StopReason.completed, 0) is True
    assert should_advance_cursor(StopReason.completed, 1) is False
    assert should_advance_cursor(StopReason.error, 0) is False
    assert should_advance_cursor(StopReason.max_iterations, 0) is False
    assert should_advance_cursor(StopReason.suspended, 0) is False
```

- [ ] **Step 2: Run RED** — `python -m uv run pytest tests/unit/test_consolidation_context.py -v`
  - Expected failure: `ModuleNotFoundError: No module named 'keel_core.consolidation.context'`.

- [ ] **Step 3: Implement** — create `packages/keel-core/src/keel_core/consolidation/context.py`:

```python
"""Per-run consolidation state + the cursor-advance safety predicate.

``ConsolidationRunContext`` carries the batch's allowed event ids (grounding the
user-evidence rule) and mutable success/validation counters the tools bump. The
executor swallows tool exceptions into failed ``ToolResult``s, so a run can reach
``completed`` even after a tool failed; ``should_advance_cursor`` therefore refuses
to advance unless the run completed cleanly AND no validation/infra error was
recorded — otherwise the same batch is safely retried on the next run.
"""

from __future__ import annotations

from dataclasses import dataclass

from keel_core.types import StopReason


@dataclass
class ConsolidationRunContext:
    """Mutable per-run state shared with the consolidation tools."""

    allowed_event_ids: frozenset[int]
    allowed_user_event_ids: frozenset[int]
    successful_actions: int = 0
    validation_errors: int = 0


def should_advance_cursor(reason: StopReason, validation_errors: int) -> bool:
    """Advance the cursor only on a clean completion with zero recorded errors."""
    return reason is StopReason.completed and validation_errors == 0
```

- [ ] **Step 4: Run GREEN** — `python -m uv run pytest tests/unit/test_consolidation_context.py -v` (3 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/consolidation/context.py tests/unit/test_consolidation_context.py
git commit -m "feat(consolidation): add run context + cursor-advance predicate" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 5: Cursor lease store (`consolidation.cursor`)

**Files:**
- Create: `packages/keel-core/src/keel_core/consolidation/cursor.py`
- Test: `tests/integration/test_consolidation_cursor.py`

**Interfaces:**
- Produces: `ConsolidationLease(scope_id: str, token: str, last_event_id: int)` (frozen); `ConsolidationCursorState(last_event_id: int, last_status: str | None, lease_token: str | None)` (frozen); `ConsolidationCursorStore(engine: AsyncEngine, scope_id: str)` with `claim(now: datetime, *, lease_seconds: int = 600) -> ConsolidationLease | None`, `complete(lease, last_event_id: int, status: str) -> None`, `fail(lease, status: str = "error") -> None`, `get() -> ConsolidationCursorState | None`.
- Consumes: `consolidation_cursors` table (Task 2).

> Deviation from spec §5 `claim(scope_id, now, ...)`: the store binds `scope_id` at construction (consistent with every other scope-bound store — `PostgresMemoryStore`, `PostgresScheduleStore`, `ArchivalStore`), so `claim` takes only `now`.

- [ ] **Step 1: Write the failing test** — create `tests/integration/test_consolidation_cursor.py`:

```python
"""Integration: consolidation cursor lease (claim / complete / fail / steal)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.cursor import ConsolidationCursorStore

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 12, 9, 0, tzinfo=UTC)


async def test_claim_is_exclusive_until_released(migrated_db: AsyncEngine) -> None:
    store = ConsolidationCursorStore(migrated_db, "cur:excl")
    lease = await store.claim(_NOW, lease_seconds=600)
    assert lease is not None
    assert lease.last_event_id == 0
    assert await store.claim(_NOW, lease_seconds=600) is None  # busy


async def test_complete_advances_and_releases(migrated_db: AsyncEngine) -> None:
    store = ConsolidationCursorStore(migrated_db, "cur:done")
    lease = await store.claim(_NOW)
    assert lease is not None
    await store.complete(lease, 42, "completed")
    state = await store.get()
    assert state is not None
    assert state.last_event_id == 42
    assert state.last_status == "completed"
    assert state.lease_token is None
    next_lease = await store.claim(_NOW)
    assert next_lease is not None and next_lease.last_event_id == 42


async def test_fail_releases_without_advancing(migrated_db: AsyncEngine) -> None:
    store = ConsolidationCursorStore(migrated_db, "cur:fail")
    lease = await store.claim(_NOW)
    assert lease is not None
    await store.fail(lease, "error")
    state = await store.get()
    assert state is not None
    assert state.last_event_id == 0
    assert state.last_status == "error"
    reclaimed = await store.claim(_NOW)
    assert reclaimed is not None and reclaimed.last_event_id == 0


async def test_expired_lease_can_be_stolen(migrated_db: AsyncEngine) -> None:
    store = ConsolidationCursorStore(migrated_db, "cur:steal")
    first = await store.claim(_NOW, lease_seconds=600)
    assert first is not None
    assert await store.claim(_NOW, lease_seconds=600) is None
    stolen = await store.claim(_NOW + timedelta(seconds=601), lease_seconds=600)
    assert stolen is not None and stolen.token != first.token
```

- [ ] **Step 2: Run RED** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_cursor.py -v`
  - Expected failure: `ModuleNotFoundError: No module named 'keel_core.consolidation.cursor'`.

- [ ] **Step 3: Implement** — create `packages/keel-core/src/keel_core/consolidation/cursor.py`:

```python
"""At-most-one consolidation lease + durable cursor (spec §5).

One row per scope in ``consolidation_cursors`` tracks the last consolidated event id
and a time-boxed lease. ``claim`` atomically takes the lease (or steals an expired one)
via a scoped CAS ``UPDATE ... RETURNING``; a concurrent claim gets ``None`` (busy).
``complete`` advances the cursor and releases the lease in one statement; ``fail``
releases without advancing so the same batch is retried. Scope-bound + RLS (ADR-0009).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


@dataclass(frozen=True)
class ConsolidationLease:
    """A held consolidation lease: the CAS token + the cursor at claim time."""

    scope_id: str
    token: str
    last_event_id: int


@dataclass(frozen=True)
class ConsolidationCursorState:
    """A read-only snapshot of a scope's cursor row (tests / observability)."""

    last_event_id: int
    last_status: str | None
    lease_token: str | None


class ConsolidationCursorStore:
    """Scope-bound durable cursor + lease over ``consolidation_cursors``."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def claim(
        self, now: datetime, *, lease_seconds: int = 600
    ) -> ConsolidationLease | None:
        token = uuid.uuid4().hex
        expires_at = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "INSERT INTO consolidation_cursors (scope_id) VALUES (:scope) "
                    "ON CONFLICT (scope_id) DO NOTHING"
                ),
                {"scope": self._scope_id},
            )
            row = (
                await conn.execute(
                    text(
                        "UPDATE consolidation_cursors "
                        "SET lease_token = :token, lease_expires_at = :expires, "
                        "updated_at = now() "
                        "WHERE scope_id = :scope "
                        "AND (lease_token IS NULL OR lease_expires_at < :now) "
                        "RETURNING last_event_id"
                    ),
                    {
                        "token": token,
                        "expires": expires_at,
                        "scope": self._scope_id,
                        "now": now,
                    },
                )
            ).one_or_none()
        if row is None:
            return None
        return ConsolidationLease(self._scope_id, token, int(row.last_event_id))

    async def complete(
        self, lease: ConsolidationLease, last_event_id: int, status: str
    ) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "UPDATE consolidation_cursors "
                    "SET last_event_id = :leid, last_status = :status, last_run_at = now(), "
                    "lease_token = NULL, lease_expires_at = NULL, updated_at = now() "
                    "WHERE scope_id = :scope AND lease_token = :token"
                ),
                {
                    "leid": last_event_id,
                    "status": status,
                    "scope": self._scope_id,
                    "token": lease.token,
                },
            )

    async def fail(self, lease: ConsolidationLease, status: str = "error") -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "UPDATE consolidation_cursors "
                    "SET last_status = :status, last_run_at = now(), "
                    "lease_token = NULL, lease_expires_at = NULL, updated_at = now() "
                    "WHERE scope_id = :scope AND lease_token = :token"
                ),
                {"status": status, "scope": self._scope_id, "token": lease.token},
            )

    async def get(self) -> ConsolidationCursorState | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "SELECT last_event_id, last_status, lease_token "
                        "FROM consolidation_cursors WHERE scope_id = :scope"
                    ),
                    {"scope": self._scope_id},
                )
            ).one_or_none()
        if row is None:
            return None
        return ConsolidationCursorState(
            int(row.last_event_id),
            None if row.last_status is None else str(row.last_status),
            None if row.lease_token is None else str(row.lease_token),
        )
```

- [ ] **Step 4: Run GREEN** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_cursor.py -v` (4 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/consolidation/cursor.py tests/integration/test_consolidation_cursor.py
git commit -m "feat(consolidation): add cursor lease store" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 6: Bounded batch reader (`consolidation.reader`)

**Files:**
- Create: `packages/keel-core/src/keel_core/consolidation/reader.py`
- Test: `tests/integration/test_consolidation_reader.py`

**Interfaces:**
- Produces: `ConsolidationMessage(event_id: int, session_id: str, role: Literal["user", "assistant"], content: str)` (frozen); `ConsolidationBatch(messages: tuple[ConsolidationMessage, ...], user_event_ids: frozenset[int], max_event_id: int, eligible_count: int)` (frozen); `ConsolidationBatchReader(engine, scope_id).read(after_event_id: int, *, limit: int = 50, message_max_chars: int = 4000, input_max_chars: int = 20000) -> ConsolidationBatch`.
- Consumes: `events` table (`type = 'message.token'`, `payload->>'role'/'text'/'partial'`).

- [ ] **Step 1: Write the failing test** — create `tests/integration/test_consolidation_reader.py`:

```python
"""Integration: bounded consolidation batch reader (filters, truncation, budget)."""

from __future__ import annotations

import json
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.reader import ConsolidationBatchReader

pytestmark = pytest.mark.integration


async def _emit(
    conn: Any,
    scope: str,
    session_id: str,
    seq: int,
    role: str,
    content: str,
    *,
    etype: str = "message.token",
    partial: bool = False,
) -> int:
    row = (
        await conn.execute(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts, payload) "
                "VALUES (:session, :scope, :seq, :type, now(), CAST(:payload AS jsonb)) "
                "RETURNING id"
            ),
            {
                "session": session_id,
                "scope": scope,
                "seq": seq,
                "type": etype,
                "payload": json.dumps({"role": role, "text": content, "partial": partial}),
            },
        )
    ).one()
    return int(row.id)


async def test_reader_filters_and_orders(migrated_db: AsyncEngine) -> None:
    scope = "rdr:filter"
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        u1 = await _emit(conn, scope, "chat:a", 1, "user", "hello")
        a1 = await _emit(conn, scope, "chat:a", 2, "assistant", "hi there")
        await _emit(conn, scope, "chat:a", 3, "assistant", "streaming", partial=True)
        await _emit(conn, scope, "digest:x", 1, "user", "digest noise")
        await _emit(conn, scope, "consolidation:x:run", 1, "user", "self noise")
        await _emit(conn, scope, "chat:a", 4, "system", "system noise")

    batch = await ConsolidationBatchReader(migrated_db, scope).read(0)
    assert [m.event_id for m in batch.messages] == [u1, a1]
    assert batch.user_event_ids == frozenset({u1})
    assert batch.max_event_id == a1
    assert batch.eligible_count == 2


async def test_reader_truncates_and_respects_budget(migrated_db: AsyncEngine) -> None:
    scope = "rdr:budget"
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        first = await _emit(conn, scope, "chat:b", 1, "user", "x" * 100)
        await _emit(conn, scope, "chat:b", 2, "user", "y" * 100)

    reader = ConsolidationBatchReader(migrated_db, scope)
    truncated = await reader.read(0, message_max_chars=10)
    assert all(len(m.content) == 10 for m in truncated.messages)

    budgeted = await reader.read(0, message_max_chars=100, input_max_chars=100)
    assert [m.event_id for m in budgeted.messages] == [first]
    assert budgeted.max_event_id == first
    assert budgeted.eligible_count == 2


async def test_reader_after_cursor_skips_consumed(migrated_db: AsyncEngine) -> None:
    scope = "rdr:cursor"
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        first = await _emit(conn, scope, "chat:c", 1, "user", "one")
        second = await _emit(conn, scope, "chat:c", 2, "user", "two")

    batch = await ConsolidationBatchReader(migrated_db, scope).read(first)
    assert [m.event_id for m in batch.messages] == [second]
    assert batch.max_event_id == second
```

- [ ] **Step 2: Run RED** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_reader.py -v`
  - Expected failure: `ModuleNotFoundError: No module named 'keel_core.consolidation.reader'`.

- [ ] **Step 3: Implement** — create `packages/keel-core/src/keel_core/consolidation/reader.py`:

```python
"""Bounded, scope-global batch reader for consolidation input (spec §6).

Reads the next window of durable ``message.token`` events after a cursor across ALL of
a scope's sessions (excluding the digest's and consolidation's own sessions), in id
order. Each message is truncated to ``message_max_chars`` and the batch stops before it
would exceed ``input_max_chars`` (always yielding at least one message). ``user_event_ids``
and ``max_event_id`` feed the user-evidence rule and the cursor advance. Scope-bound + RLS.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")

_SELECT_BATCH = text(
    "SELECT id, session_id, payload->>'role' AS role, payload->>'text' AS text "
    "FROM events "
    "WHERE scope_id = :scope "
    "AND type = 'message.token' "
    "AND id > :after "
    "AND payload->>'role' IN ('user', 'assistant') "
    "AND COALESCE((payload->>'partial')::boolean, false) = false "
    "AND length(COALESCE(payload->>'text', '')) > 0 "
    "AND session_id NOT LIKE 'digest:%' "
    "AND session_id NOT LIKE 'consolidation:%' "
    "ORDER BY id ASC "
    "LIMIT :limit"
)


@dataclass(frozen=True)
class ConsolidationMessage:
    """One conversation message admitted into a consolidation batch."""

    event_id: int
    session_id: str
    role: Literal["user", "assistant"]
    content: str


@dataclass(frozen=True)
class ConsolidationBatch:
    """A bounded window of messages plus the metadata the run needs."""

    messages: tuple[ConsolidationMessage, ...]
    user_event_ids: frozenset[int]
    max_event_id: int
    eligible_count: int


class ConsolidationBatchReader:
    """Scope-bound reader of the next consolidation window after a cursor."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def read(
        self,
        after_event_id: int,
        *,
        limit: int = 50,
        message_max_chars: int = 4000,
        input_max_chars: int = 20000,
    ) -> ConsolidationBatch:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    _SELECT_BATCH,
                    {"scope": self._scope_id, "after": after_event_id, "limit": limit},
                )
            ).all()

        messages: list[ConsolidationMessage] = []
        user_ids: set[int] = set()
        total = 0
        max_event_id = after_event_id
        for row in rows:
            content = str(row.text)[:message_max_chars]
            if messages and total + len(content) > input_max_chars:
                break
            role = cast(Literal["user", "assistant"], str(row.role))
            event_id = int(row.id)
            messages.append(
                ConsolidationMessage(
                    event_id=event_id,
                    session_id=str(row.session_id),
                    role=role,
                    content=content,
                )
            )
            if role == "user":
                user_ids.add(event_id)
            total += len(content)
            max_event_id = event_id
        return ConsolidationBatch(
            messages=tuple(messages),
            user_event_ids=frozenset(user_ids),
            max_event_id=max_event_id,
            eligible_count=len(rows),
        )
```

- [ ] **Step 4: Run GREEN** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_reader.py -v` (3 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/consolidation/reader.py tests/integration/test_consolidation_reader.py
git commit -m "feat(consolidation): add bounded batch reader" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 7: Proposal store + atomic apply + `memory.versions()`

**Files:**
- Create: `packages/keel-core/src/keel_core/consolidation/proposals.py`
- Modify: `packages/keel-core/src/keel_core/memory.py` (add `PostgresMemoryStore.versions()`)
- Test: `tests/integration/test_consolidation_proposals.py`

**Interfaces:**
- Produces: `ProposalOutcome` StrEnum `{applied, rejected, stale, not_found, already_resolved}`; `ProposalResolution(outcome: ProposalOutcome, version: int | None = None)` (frozen); `MemoryProposal` dataclass; `MemoryProposalStore(engine, scope_id)` with `propose(*, block, proposed_value, reason, confidence, source_event_ids: list[int]) -> tuple[str, bool]`, `list_proposals(*, status: str | None = None) -> list[MemoryProposal]`, `get(proposal_id) -> MemoryProposal | None`, `approve(proposal_id, resolved_by) -> ProposalResolution`, `reject(proposal_id, resolved_by) -> ProposalResolution`; `PostgresMemoryStore.versions() -> dict[str, int]`.
- Consumes: `consolidation_idempotency_key` (Task 3); `memory_proposals`, `memory_blocks`, `memory_block_versions` tables.

- [ ] **Step 1: Write the failing test** — create `tests/integration/test_consolidation_proposals.py`:

```python
"""Integration: memory proposal store — idempotency + atomic CAS apply."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.proposals import MemoryProposalStore, ProposalOutcome
from keel_core.memory import PostgresMemoryStore

pytestmark = pytest.mark.integration


async def test_propose_is_idempotent(migrated_db: AsyncEngine) -> None:
    store = MemoryProposalStore(migrated_db, "prop:idem")
    pid1, created1 = await store.propose(
        block="human", proposed_value="likes tea", reason="stated",
        confidence=0.9, source_event_ids=[1, 2],
    )
    pid2, created2 = await store.propose(
        block="human", proposed_value="likes tea", reason="stated",
        confidence=0.9, source_event_ids=[2, 1],
    )
    assert created1 is True
    assert created2 is False
    assert pid1 == pid2
    assert len(await store.list_proposals(status="pending")) == 1


async def test_approve_creates_block_when_absent(migrated_db: AsyncEngine) -> None:
    store = MemoryProposalStore(migrated_db, "prop:new")
    memory = PostgresMemoryStore(migrated_db, "prop:new")
    pid, _ = await store.propose(
        block="human", proposed_value="likes tea", reason="stated",
        confidence=0.9, source_event_ids=[1],
    )
    resolution = await store.approve(pid, "reviewer")
    assert resolution.outcome is ProposalOutcome.applied
    assert resolution.version == 1
    assert await memory.get("human") == "likes tea"
    assert (await memory.versions())["human"] == 1
    assert await memory.history("human") == [(1, "likes tea")]


async def test_approve_updates_existing_block(migrated_db: AsyncEngine) -> None:
    memory = PostgresMemoryStore(migrated_db, "prop:upd")
    await memory.set("persona", "v1")  # version 1
    store = MemoryProposalStore(migrated_db, "prop:upd")
    pid, _ = await store.propose(
        block="persona", proposed_value="v2", reason="clarified",
        confidence=0.9, source_event_ids=[1],
    )
    resolution = await store.approve(pid, "reviewer")
    assert resolution.outcome is ProposalOutcome.applied
    assert resolution.version == 2
    assert await memory.get("persona") == "v2"


async def test_approve_goes_stale_when_block_changed(migrated_db: AsyncEngine) -> None:
    memory = PostgresMemoryStore(migrated_db, "prop:stale")
    store = MemoryProposalStore(migrated_db, "prop:stale")
    pid, _ = await store.propose(
        block="human", proposed_value="likes tea", reason="stated",
        confidence=0.9, source_event_ids=[1],
    )  # expected_version 0 (block absent)
    await memory.set("human", "written directly")  # now version 1
    resolution = await store.approve(pid, "reviewer")
    assert resolution.outcome is ProposalOutcome.stale
    assert await memory.get("human") == "written directly"


async def test_reject_and_double_resolve(migrated_db: AsyncEngine) -> None:
    store = MemoryProposalStore(migrated_db, "prop:rej")
    pid, _ = await store.propose(
        block="human", proposed_value="likes tea", reason="stated",
        confidence=0.9, source_event_ids=[1],
    )
    assert (await store.reject(pid, "reviewer")).outcome is ProposalOutcome.rejected
    assert (await store.approve(pid, "reviewer")).outcome is ProposalOutcome.already_resolved
    assert (await store.approve("missing", "reviewer")).outcome is ProposalOutcome.not_found
```

- [ ] **Step 2: Run RED** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_proposals.py -v`
  - Expected failure: `ModuleNotFoundError: No module named 'keel_core.consolidation.proposals'`.

- [ ] **Step 3a: Implement `versions()`** — in `packages/keel-core/src/keel_core/memory.py`, add this method to `PostgresMemoryStore` immediately after `blocks()`:

```python
    async def versions(self) -> dict[str, int]:
        """Return all of the scope's memory blocks as ``{key: version}``."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text("SELECT key, version FROM memory_blocks WHERE scope_id = :scope"),
                    {"scope": self._scope_id},
                )
            ).all()
        return {str(row.key): int(row.version) for row in rows}
```

- [ ] **Step 3b: Implement the store** — create `packages/keel-core/src/keel_core/consolidation/proposals.py`:

```python
"""Human-reviewed core-memory proposals + atomic apply (spec §11).

Consolidation never edits core memory directly: it writes a *proposal* keyed by an
idempotency key (safe under whole-batch retry). A human approve/reject resolves it.
Approve is an atomic compare-and-set against the block's expected version — a proposal
raised against version N applies only while the block is still at N, else it goes
``stale`` (the block changed underneath it). Scope-bound + RLS (ADR-0009).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.hashing import consolidation_idempotency_key

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


class ProposalOutcome(StrEnum):
    """The result of resolving a proposal."""

    applied = "applied"
    rejected = "rejected"
    stale = "stale"
    not_found = "not_found"
    already_resolved = "already_resolved"


@dataclass(frozen=True)
class ProposalResolution:
    """Outcome of an approve/reject, plus the new block version when applied."""

    outcome: ProposalOutcome
    version: int | None = None


@dataclass
class MemoryProposal:
    """A pending or resolved core-memory rewrite proposal."""

    id: str
    scope_id: str
    block: str
    expected_version: int
    proposed_value: str
    reason: str
    confidence: float
    source_event_ids: list[int]
    status: str
    created_at: datetime
    resolved_at: datetime | None = None
    resolved_by: str | None = None


def _to_proposal(row: Any) -> MemoryProposal:
    return MemoryProposal(
        id=row["id"],
        scope_id=row["scope_id"],
        block=row["block"],
        expected_version=int(row["expected_version"]),
        proposed_value=row["proposed_value"],
        reason=row["reason"],
        confidence=float(row["confidence"]),
        source_event_ids=[int(i) for i in (row["source_event_ids"] or [])],
        status=row["status"],
        created_at=row["created_at"],
        resolved_at=row["resolved_at"],
        resolved_by=row["resolved_by"],
    )


class MemoryProposalStore:
    """Scope-bound store of core-memory rewrite proposals over ``memory_proposals``."""

    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def propose(
        self,
        *,
        block: str,
        proposed_value: str,
        reason: str,
        confidence: float,
        source_event_ids: list[int],
    ) -> tuple[str, bool]:
        """Insert a proposal idempotently. Returns ``(proposal_id, created)``."""
        proposal_id = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            current = (
                await conn.execute(
                    text(
                        "SELECT version FROM memory_blocks "
                        "WHERE scope_id = :scope AND key = :block"
                    ),
                    {"scope": self._scope_id, "block": block},
                )
            ).one_or_none()
            expected_version = 0 if current is None else int(current.version)
            key = consolidation_idempotency_key(
                self._scope_id, block, expected_version, proposed_value, source_event_ids
            )
            inserted = (
                await conn.execute(
                    text(
                        "INSERT INTO memory_proposals "
                        "(id, scope_id, block, expected_version, proposed_value, reason, "
                        "confidence, source_event_ids, idempotency_key, status, created_at) "
                        "VALUES (:id, :scope, :block, :expected, :value, :reason, :confidence, "
                        "CAST(:ids AS bigint[]), :key, 'pending', now()) "
                        "ON CONFLICT (scope_id, idempotency_key) DO NOTHING "
                        "RETURNING id"
                    ),
                    {
                        "id": proposal_id,
                        "scope": self._scope_id,
                        "block": block,
                        "expected": expected_version,
                        "value": proposed_value,
                        "reason": reason,
                        "confidence": confidence,
                        "ids": list(source_event_ids),
                        "key": key,
                    },
                )
            ).one_or_none()
            if inserted is not None:
                return str(inserted.id), True
            existing = (
                await conn.execute(
                    text(
                        "SELECT id FROM memory_proposals "
                        "WHERE scope_id = :scope AND idempotency_key = :key"
                    ),
                    {"scope": self._scope_id, "key": key},
                )
            ).one()
        return str(existing.id), False

    async def list_proposals(self, *, status: str | None = None) -> list[MemoryProposal]:
        query = "SELECT * FROM memory_proposals WHERE scope_id = :scope"
        params: dict[str, str] = {"scope": self._scope_id}
        if status is not None:
            query += " AND status = :status"
            params["status"] = status
        query += " ORDER BY created_at"
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (await conn.execute(text(query), params)).mappings().all()
        return [_to_proposal(r) for r in rows]

    async def get(self, proposal_id: str) -> MemoryProposal | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM memory_proposals "
                            "WHERE scope_id = :scope AND id = :id"
                        ),
                        {"scope": self._scope_id, "id": proposal_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_proposal(row)

    async def approve(self, proposal_id: str, resolved_by: str) -> ProposalResolution:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            proposal = (
                await conn.execute(
                    text(
                        "SELECT block, expected_version, proposed_value, status "
                        "FROM memory_proposals WHERE scope_id = :scope AND id = :id FOR UPDATE"
                    ),
                    {"scope": self._scope_id, "id": proposal_id},
                )
            ).one_or_none()
            if proposal is None:
                return ProposalResolution(ProposalOutcome.not_found)
            if proposal.status != "pending":
                return ProposalResolution(ProposalOutcome.already_resolved)

            block = str(proposal.block)
            expected = int(proposal.expected_version)
            value = str(proposal.proposed_value)
            if expected == 0:
                applied = (
                    await conn.execute(
                        text(
                            "INSERT INTO memory_blocks (scope_id, key, value, version) "
                            "VALUES (:scope, :block, :value, 1) "
                            "ON CONFLICT (scope_id, key) DO NOTHING"
                        ),
                        {"scope": self._scope_id, "block": block, "value": value},
                    )
                ).rowcount == 1
                new_version = 1
            else:
                applied = (
                    await conn.execute(
                        text(
                            "UPDATE memory_blocks "
                            "SET value = :value, version = version + 1, updated_at = now() "
                            "WHERE scope_id = :scope AND key = :block AND version = :expected"
                        ),
                        {
                            "value": value,
                            "scope": self._scope_id,
                            "block": block,
                            "expected": expected,
                        },
                    )
                ).rowcount == 1
                new_version = expected + 1

            if not applied:
                await conn.execute(
                    text(
                        "UPDATE memory_proposals SET status = 'stale', resolved_at = now(), "
                        "resolved_by = :by WHERE scope_id = :scope AND id = :id"
                    ),
                    {"by": resolved_by, "scope": self._scope_id, "id": proposal_id},
                )
                return ProposalResolution(ProposalOutcome.stale)

            await conn.execute(
                text(
                    "INSERT INTO memory_block_versions (scope_id, key, version, value) "
                    "VALUES (:scope, :block, :version, :value)"
                ),
                {
                    "scope": self._scope_id,
                    "block": block,
                    "version": new_version,
                    "value": value,
                },
            )
            await conn.execute(
                text(
                    "UPDATE memory_proposals SET status = 'applied', resolved_at = now(), "
                    "resolved_by = :by WHERE scope_id = :scope AND id = :id"
                ),
                {"by": resolved_by, "scope": self._scope_id, "id": proposal_id},
            )
        return ProposalResolution(ProposalOutcome.applied, new_version)

    async def reject(self, proposal_id: str, resolved_by: str) -> ProposalResolution:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE memory_proposals SET status = 'rejected', resolved_at = now(), "
                    "resolved_by = :by "
                    "WHERE scope_id = :scope AND id = :id AND status = 'pending'"
                ),
                {"by": resolved_by, "scope": self._scope_id, "id": proposal_id},
            )
            if result.rowcount == 1:
                return ProposalResolution(ProposalOutcome.rejected)
            exists = (
                await conn.execute(
                    text(
                        "SELECT status FROM memory_proposals "
                        "WHERE scope_id = :scope AND id = :id"
                    ),
                    {"scope": self._scope_id, "id": proposal_id},
                )
            ).one_or_none()
        if exists is None:
            return ProposalResolution(ProposalOutcome.not_found)
        return ProposalResolution(ProposalOutcome.already_resolved)
```

- [ ] **Step 4: Run GREEN** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_proposals.py -v` (5 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/consolidation/proposals.py packages/keel-core/src/keel_core/memory.py tests/integration/test_consolidation_proposals.py
git commit -m "feat(consolidation): add proposal store with atomic CAS apply" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 8: Two constrained tools + validators + `ArchivalStore.add_consolidated`

**Files:**
- Create: `packages/keel-core/src/keel_core/consolidation/tools.py`
- Modify: `packages/keel-core/src/keel_core/search.py` (add module-level `_union_ids` + `ArchivalStore.add_consolidated`)
- Test (unit): `tests/unit/test_consolidation_tools.py`
- Test (integration): `tests/integration/test_consolidation_tools_postgres.py`

**Interfaces:**
- Produces: `validate_propose_rewrite(args, run_context, *, block_max_chars: int) -> str | None`; `validate_archival_insert(args, run_context, *, min_confidence: float, content_max_chars: int) -> str | None`; `ProposeRewriteTool(engine, run_context, *, block_max_chars: int = 2000)` (`name = "memory_propose_rewrite"`, `writes = True`); `ArchivalConsolidateInsertTool(engine, embedder, run_context, *, min_confidence: float = 0.8, content_max_chars: int = 2000, semantic_dedupe_distance: float = 0.05)` (`name = "archival_consolidate_insert"`, `writes = True`); `ArchivalStore.add_consolidated(content: str, *, source_event_ids: Sequence[int], semantic_dedupe_distance: float = 0.05) -> tuple[int, bool]` (normalizes sources to a sorted-unique array and merges a rephrased retry only when an existing `consolidation` row of the same `(model, dim)` has the *exact same* array within `semantic_dedupe_distance`; `0` disables the pass; range `0..2`).
- Consumes: `ConsolidationRunContext` (Task 4), `MemoryProposalStore` (Task 7), `ArchivalStore` + `Embedder`, `archival_content_hash` (Task 3).

- [ ] **Step 1: Write the failing tests** — create `tests/unit/test_consolidation_tools.py`:

```python
"""Unit tests for consolidation tool validators + fail-closed behavior."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import create_async_engine

from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.tools import (
    ArchivalConsolidateInsertTool,
    ProposeRewriteTool,
    validate_archival_insert,
    validate_propose_rewrite,
)
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ToolContext

_DUMMY_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel"


def _ctx() -> ConsolidationRunContext:
    return ConsolidationRunContext(
        allowed_event_ids=frozenset({1, 2, 3}),
        allowed_user_event_ids=frozenset({1}),
    )


def _tool_ctx() -> ToolContext:
    return ToolContext(scope_id="web:local", session_id="consolidation:web:local:r1")


def test_propose_validator_accepts_grounded_write() -> None:
    assert (
        validate_propose_rewrite(
            {"block": "human", "proposed_value": "likes tea", "reason": "stated",
             "source_event_ids": [1, 2]},
            _ctx(),
            block_max_chars=2000,
        )
        is None
    )


def test_propose_validator_rejects_unknown_block() -> None:
    error = validate_propose_rewrite(
        {"block": "system", "proposed_value": "x", "reason": "y", "source_event_ids": [1]},
        _ctx(),
        block_max_chars=2000,
    )
    assert error is not None and "block" in error


def test_propose_validator_requires_user_evidence() -> None:
    error = validate_propose_rewrite(
        {"block": "human", "proposed_value": "x", "reason": "y", "source_event_ids": [2, 3]},
        _ctx(),
        block_max_chars=2000,
    )
    assert error is not None and "user message" in error


def test_propose_validator_rejects_out_of_batch_citation() -> None:
    error = validate_propose_rewrite(
        {"block": "human", "proposed_value": "x", "reason": "y", "source_event_ids": [1, 99]},
        _ctx(),
        block_max_chars=2000,
    )
    assert error is not None and "current batch" in error


def test_archival_validator_enforces_confidence_floor() -> None:
    error = validate_archival_insert(
        {"content": "fact", "confidence": 0.5, "source_event_ids": [1]},
        _ctx(),
        min_confidence=0.8,
        content_max_chars=2000,
    )
    assert error is not None and "threshold" in error


def test_archival_validator_rejects_boolean_confidence() -> None:
    error = validate_archival_insert(
        {"content": "fact", "confidence": True, "source_event_ids": [1]},
        _ctx(),
        min_confidence=0.8,
        content_max_chars=2000,
    )
    assert error is not None and "number" in error


async def test_propose_tool_records_validation_error_without_db() -> None:
    engine = create_async_engine(_DUMMY_URL)
    run_context = _ctx()
    tool = ProposeRewriteTool(engine, run_context)
    result = await tool.run(
        {"block": "system", "proposed_value": "x", "reason": "y", "source_event_ids": [1]},
        _tool_ctx(),
    )
    assert result.ok is False
    assert run_context.validation_errors == 1
    assert run_context.successful_actions == 0
    await engine.dispose()


async def test_archival_tool_records_validation_error_without_db() -> None:
    engine = create_async_engine(_DUMMY_URL)
    run_context = _ctx()
    tool = ArchivalConsolidateInsertTool(engine, FakeEmbedder(), run_context)
    result = await tool.run(
        {"content": "fact", "confidence": 0.1, "source_event_ids": [1]},
        _tool_ctx(),
    )
    assert result.ok is False
    assert run_context.validation_errors == 1
    await engine.dispose()
```

  Then create `tests/integration/test_consolidation_tools_postgres.py`:

```python
"""Integration: archival add_consolidated dedupe/merge + tool success path."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.tools import ArchivalConsolidateInsertTool
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ToolContext
from keel_core.search import ArchivalStore

pytestmark = pytest.mark.integration


async def _archival_rows(engine: AsyncEngine, scope: str) -> list[tuple[str, list[int]]]:
    async with engine.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        rows = (
            await conn.execute(
                text(
                    "SELECT origin, source_event_ids FROM archival "
                    "WHERE scope_id = :s ORDER BY id"
                ),
                {"s": scope},
            )
        ).all()
    return [(str(r.origin), [int(i) for i in r.source_event_ids]) for r in rows]


async def test_add_consolidated_dedupes_and_merges(migrated_db: AsyncEngine) -> None:
    scope = "tool:dedupe"
    store = ArchivalStore(migrated_db, scope, FakeEmbedder())

    id_a, created_a = await store.add_consolidated("Likes  tea", source_event_ids=[1])
    id_dup, created_dup = await store.add_consolidated("Likes tea", source_event_ids=[2])
    id_b, created_b = await store.add_consolidated("likes tea", source_event_ids=[3])

    assert created_a is True
    assert created_dup is False  # whitespace-only difference -> same content hash
    assert id_dup == id_a
    assert created_b is False  # case-only difference -> same normalized content hash
    assert id_b == id_a

    rows = await _archival_rows(migrated_db, scope)
    assert len(rows) == 1
    assert all(origin == "consolidation" for origin, _ in rows)
    assert [ids for _, ids in rows] == [[1, 2, 3]]


async def test_archival_tool_success_path(migrated_db: AsyncEngine) -> None:
    scope = "tool:ok"
    run_context = ConsolidationRunContext(
        allowed_event_ids=frozenset({10, 11}),
        allowed_user_event_ids=frozenset({10}),
    )
    tool = ArchivalConsolidateInsertTool(
        migrated_db, FakeEmbedder(), run_context, min_confidence=0.8
    )
    result = await tool.run(
        {"content": "user prefers dark mode", "confidence": 0.95, "source_event_ids": [10, 11]},
        ToolContext(scope_id=scope, session_id=f"consolidation:{scope}:r"),
    )
    assert result.ok is True
    assert run_context.successful_actions == 1
    assert run_context.validation_errors == 0
    rows = await _archival_rows(migrated_db, scope)
    assert rows == [("consolidation", [10, 11])]
```

- [ ] **Step 2: Run RED** — `python -m uv run pytest tests/unit/test_consolidation_tools.py -v`
  - Expected failure: `ModuleNotFoundError: No module named 'keel_core.consolidation.tools'`.

- [ ] **Step 3a: Implement `add_consolidated`** — in `packages/keel-core/src/keel_core/search.py`:
  1. add `import logging` to the standard-library import block and a module-level `logger = logging.getLogger("keel.core.search")`; keep `from collections.abc import Sequence` before `from dataclasses import dataclass`;
  2. add this module-level helper directly below `_vector_literal`:

```python
def _union_ids(existing: Sequence[int] | None, new: Sequence[int]) -> list[int]:
    """Sorted set-union of two event-id lists (stable provenance merge)."""
    return sorted({*(existing or []), *new})
```

  3. add this method to `ArchivalStore`, directly after `add`. Exact content-hash
     dedupe runs first; a rephrased retry then merges **only** when an existing
     `consolidation` row of the same `(model, dim)` carries the *exact same*
     normalized `source_event_ids` and its embedding is within
     `semantic_dedupe_distance` (cosine). `0` disables that pass; the merge is
     logged at INFO (scope, row id, distance, source-id count — never content):

```python
    async def add_consolidated(
        self,
        content: str,
        *,
        source_event_ids: Sequence[int],
        semantic_dedupe_distance: float = 0.05,
    ) -> tuple[int, bool]:
        """Insert a consolidation-origin passage with retry-safe deduplication.

        Returns ``(row_id, created)``. On a content-hash hit the existing row's
        ``source_event_ids`` are merged (union) and no new embedding is computed.
        A rephrased retry also merges, but only when it cites the *exact same*
        normalized ``source_event_ids`` as an existing ``consolidation`` row of the
        same ``(model, dim)`` whose current embedding is within
        ``semantic_dedupe_distance`` (cosine). This targets provider rephrasing of
        one source-backed fact — it is deliberately NOT a general semantic merge.
        ``semantic_dedupe_distance == 0`` disables the semantic pass; exact
        content-hash dedupe still applies.
        """
        from keel_core.consolidation.hashing import archival_content_hash

        if not 0.0 <= semantic_dedupe_distance <= 2.0:
            raise ValueError("semantic_dedupe_distance must be between 0 and 2")
        content_hash = archival_content_hash(content)
        ids = sorted({int(i) for i in source_event_ids})
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            existing = (
                await conn.execute(
                    text(
                        "SELECT id, source_event_ids FROM archival "
                        "WHERE scope_id = :scope AND content_hash = :hash "
                        "FOR UPDATE"
                    ),
                    {"scope": self._scope_id, "hash": content_hash},
                )
            ).one_or_none()
            if existing is not None:
                merged = _union_ids(existing.source_event_ids, ids)
                await conn.execute(
                    text(
                        "UPDATE archival SET source_event_ids = CAST(:ids AS bigint[]) "
                        "WHERE scope_id = :scope AND id = :id"
                    ),
                    {"ids": merged, "scope": self._scope_id, "id": int(existing.id)},
                )
                return int(existing.id), False
        embedding = (await self._embedder.embed([content]))[0]
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            if semantic_dedupe_distance > 0:
                semantic_existing = (
                    await conn.execute(
                        text(
                            "SELECT id, embedding <=> CAST(:emb AS vector) AS distance "
                            "FROM archival "
                            "WHERE scope_id = :scope AND origin = 'consolidation' "
                            "AND model = :model AND dim = :dim "
                            "AND source_event_ids = CAST(:ids AS bigint[]) "
                            "AND embedding <=> CAST(:emb AS vector) < :distance "
                            "ORDER BY embedding <=> CAST(:emb AS vector) "
                            "LIMIT 1 FOR UPDATE"
                        ),
                        {
                            "scope": self._scope_id,
                            "model": self._embedder.model,
                            "dim": self._embedder.dim,
                            "ids": ids,
                            "emb": _vector_literal(embedding),
                            "distance": semantic_dedupe_distance,
                        },
                    )
                ).one_or_none()
                if semantic_existing is not None:
                    logger.info(
                        "consolidation semantic dedupe merge scope=%s row_id=%s "
                        "distance=%.6f source_ids=%d",
                        self._scope_id,
                        int(semantic_existing.id),
                        float(semantic_existing.distance),
                        len(ids),
                    )
                    return int(semantic_existing.id), False
            row = (
                await conn.execute(
                    text(
                        "INSERT INTO archival "
                        "(scope_id, content, model, dim, embedding, origin, content_hash, "
                        "source_event_ids) "
                        "VALUES (:scope, :content, :model, :dim, CAST(:emb AS vector), "
                        "'consolidation', :hash, CAST(:ids AS bigint[])) "
                        "ON CONFLICT (scope_id, content_hash) WHERE content_hash IS NOT NULL "
                        "DO UPDATE SET source_event_ids = ("
                        "  SELECT array_agg(DISTINCT eid ORDER BY eid) "
                        "  FROM unnest(archival.source_event_ids || excluded.source_event_ids) "
                        "  AS eid"
                        ") "
                        "RETURNING id, (xmax = 0) AS inserted"
                    ),
                    {
                        "scope": self._scope_id,
                        "content": content,
                        "model": self._embedder.model,
                        "dim": self._embedder.dim,
                        "emb": _vector_literal(embedding),
                        "hash": content_hash,
                        "ids": ids,
                    },
                )
            ).one()
        return int(row.id), bool(row.inserted)
```

- [ ] **Step 3b: Implement the tools** — create `packages/keel-core/src/keel_core/consolidation/tools.py`:

```python
"""The two constrained consolidation tools + their validators (spec §8–§10).

Both tools fail closed: an invalid call records a validation error on the shared
``ConsolidationRunContext`` (so the cursor will not advance) and returns a failed
``ToolResult``. The tool executor turns any raised exception into a failed result, so
infra errors are caught here too and counted. Every write must cite ``source_event_ids``
from the current batch, and at least one cited event must be a user message (the
user-evidence rule).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.proposals import MemoryProposalStore
from keel_core.embeddings import Embedder
from keel_core.protocols import ToolContext, ToolResult
from keel_core.search import ArchivalStore

_VALID_BLOCKS = ("persona", "human")


def _coerce_ids(raw: object) -> list[int] | None:
    if not isinstance(raw, list):
        return None
    ids: list[int] = []
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, int):
            return None
        ids.append(item)
    return ids


def _citation_error(ids: list[int] | None, run_context: ConsolidationRunContext) -> str | None:
    if ids is None:
        return "source_event_ids must be a list of integers"
    if not ids:
        return "source_event_ids must cite at least one event"
    cited = set(ids)
    if not cited <= run_context.allowed_event_ids:
        return "source_event_ids must reference events from the current batch"
    if not (cited & run_context.allowed_user_event_ids):
        return "at least one source event must be a user message"
    return None


def validate_propose_rewrite(
    args: dict[str, Any], run_context: ConsolidationRunContext, *, block_max_chars: int
) -> str | None:
    block = str(args.get("block", ""))
    if block not in _VALID_BLOCKS:
        return f"unknown block {block!r}; must be 'persona' or 'human'"
    value = str(args.get("proposed_value", ""))
    if not value.strip():
        return "proposed_value must be non-empty"
    if len(value) > block_max_chars:
        return f"proposed_value exceeds {block_max_chars} chars"
    if not str(args.get("reason", "")).strip():
        return "reason must be non-empty"
    return _citation_error(_coerce_ids(args.get("source_event_ids")), run_context)


def validate_archival_insert(
    args: dict[str, Any],
    run_context: ConsolidationRunContext,
    *,
    min_confidence: float,
    content_max_chars: int,
) -> str | None:
    content = str(args.get("content", ""))
    if not content.strip():
        return "content must be non-empty"
    if len(content) > content_max_chars:
        return f"content exceeds {content_max_chars} chars"
    confidence = args.get("confidence")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        return "confidence must be a number"
    if not (min_confidence <= float(confidence) <= 1.0):
        return f"confidence {confidence} is below the threshold {min_confidence}"
    return _citation_error(_coerce_ids(args.get("source_event_ids")), run_context)


class ProposeRewriteTool:
    """``memory_propose_rewrite``: propose a human-reviewed core-memory edit."""

    name = "memory_propose_rewrite"
    description = (
        "Propose a human-reviewed rewrite of a core memory block ('persona' or 'human'). "
        "The proposal is NOT applied automatically."
    )
    writes = True

    def __init__(
        self,
        engine: AsyncEngine,
        run_context: ConsolidationRunContext,
        *,
        block_max_chars: int = 2000,
    ) -> None:
        self._engine = engine
        self._run_context = run_context
        self._block_max_chars = block_max_chars

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "block": {"type": "string", "enum": list(_VALID_BLOCKS)},
                "proposed_value": {"type": "string"},
                "reason": {"type": "string"},
                "confidence": {"type": "number"},
                "source_event_ids": {"type": "array", "items": {"type": "integer"}},
            },
            "required": ["block", "proposed_value", "reason", "source_event_ids"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        error = validate_propose_rewrite(
            args, self._run_context, block_max_chars=self._block_max_chars
        )
        if error is not None:
            self._run_context.validation_errors += 1
            return ToolResult(ok=False, output=f"rejected: {error}")
        try:
            store = MemoryProposalStore(self._engine, ctx.scope_id)
            proposal_id, created = await store.propose(
                block=str(args["block"]),
                proposed_value=str(args["proposed_value"]),
                reason=str(args["reason"]),
                confidence=float(args.get("confidence", 1.0)),
                source_event_ids=_coerce_ids(args["source_event_ids"]) or [],
            )
        except Exception as exc:  # infra failure must block the cursor
            self._run_context.validation_errors += 1
            return ToolResult(ok=False, output=f"error: {exc.__class__.__name__}")
        self._run_context.successful_actions += 1
        state = "created" if created else "already proposed"
        return ToolResult(ok=True, output=f"proposal {proposal_id} {state}")


class ArchivalConsolidateInsertTool:
    """``archival_consolidate_insert``: persist a durable, deduplicated fact."""

    name = "archival_consolidate_insert"
    description = (
        "Persist a durable, standalone fact to archival memory for later recall. "
        "Deduplicated by content; requires a confidence and cited source events."
    )
    writes = True

    def __init__(
        self,
        engine: AsyncEngine,
        embedder: Embedder,
        run_context: ConsolidationRunContext,
        *,
        min_confidence: float = 0.8,
        content_max_chars: int = 2000,
    ) -> None:
        self._engine = engine
        self._embedder = embedder
        self._run_context = run_context
        self._min_confidence = min_confidence
        self._content_max_chars = content_max_chars

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "confidence": {"type": "number"},
                "source_event_ids": {"type": "array", "items": {"type": "integer"}},
            },
            "required": ["content", "confidence", "source_event_ids"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        error = validate_archival_insert(
            args,
            self._run_context,
            min_confidence=self._min_confidence,
            content_max_chars=self._content_max_chars,
        )
        if error is not None:
            self._run_context.validation_errors += 1
            return ToolResult(ok=False, output=f"rejected: {error}")
        try:
            store = ArchivalStore(self._engine, ctx.scope_id, self._embedder)
            row_id, created = await store.add_consolidated(
                str(args["content"]),
                source_event_ids=_coerce_ids(args["source_event_ids"]) or [],
            )
        except Exception as exc:  # infra failure must block the cursor
            self._run_context.validation_errors += 1
            return ToolResult(ok=False, output=f"error: {exc.__class__.__name__}")
        self._run_context.successful_actions += 1
        state = "inserted" if created else "merged"
        return ToolResult(ok=True, output=f"archival {row_id} {state}")
```

- [ ] **Step 4: Run GREEN** — unit: `python -m uv run pytest tests/unit/test_consolidation_tools.py -v` (8 pass). Integration: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_tools_postgres.py -v` (2 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/consolidation/tools.py packages/keel-core/src/keel_core/search.py tests/unit/test_consolidation_tools.py tests/integration/test_consolidation_tools_postgres.py
git commit -m "feat(consolidation): add constrained propose + archival tools" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 9: Consolidation agent glue (`consolidation.agent`)

**Files:**
- Create: `packages/keel-core/src/keel_core/consolidation/agent.py`
- Test: `tests/unit/test_consolidation_agent.py`

**Interfaces:**
- Produces: `MEMORY_CONSOLIDATOR_AGENT_ID = "memory-consolidator"`; `CONSOLIDATION_SYSTEM_INSTRUCTION: str`; `consolidation_session_id(scope_id: str, run_id: str) -> str`; `consolidation_schedule_id(scope_id: str) -> str`; `build_consolidation_agent(scope_id: str, model: str, *, token_budget: int, max_iterations: int = 8) -> AgentSpec`; `consolidation_permissions() -> RuleBasedPermissionEngine`; `consolidation_registry(engine, embedder, run_context, settings) -> ToolRegistry`; `consolidation_system_context() -> str` (async); `format_consolidation_prompt(blocks: dict[str, str], versions: dict[str, int], messages: Sequence[ConsolidationMessage]) -> str`.
- Consumes: `AgentSpec`/`Scope`, `ToolRegistry`, `Rule`/`RuleBasedPermissionEngine`, `ProposeRewriteTool`/`ArchivalConsolidateInsertTool` (Task 8), `ConsolidationRunContext` (Task 4), `ConsolidationMessage` (Task 6), `Settings`.

- [ ] **Step 1: Write the failing test** — create `tests/unit/test_consolidation_agent.py`:

```python
"""Unit tests for the consolidation agent glue (identity, prompt, permissions)."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import create_async_engine

from keel_core.config import Settings
from keel_core.consolidation.agent import (
    CONSOLIDATION_SYSTEM_INSTRUCTION,
    MEMORY_CONSOLIDATOR_AGENT_ID,
    build_consolidation_agent,
    consolidation_permissions,
    consolidation_registry,
    consolidation_schedule_id,
    consolidation_session_id,
    consolidation_system_context,
    format_consolidation_prompt,
)
from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.reader import ConsolidationMessage
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ToolContext
from keel_core.types import PermissionDecision, ScopeKind, TrustLevel

_DUMMY_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel"


def test_agent_identity_and_toolset() -> None:
    agent = build_consolidation_agent("web:local", "gpt-4o-mini", token_budget=4000)
    assert agent.id == MEMORY_CONSOLIDATOR_AGENT_ID
    assert agent.model == "gpt-4o-mini"
    assert agent.scope.kind is ScopeKind.system
    assert agent.scope.trust is TrustLevel.trusted
    assert agent.max_iterations == 8
    assert agent.token_budget == 4000
    assert agent.toolset == ["memory_propose_rewrite", "archival_consolidate_insert"]


def test_permissions_allow_only_the_two_tools() -> None:
    ctx = ToolContext(scope_id="web:local", session_id="s")
    perms = consolidation_permissions()
    assert perms.evaluate("memory_propose_rewrite", {}, ctx) is PermissionDecision.allow
    assert perms.evaluate("archival_consolidate_insert", {}, ctx) is PermissionDecision.allow
    assert perms.evaluate("memory_append", {}, ctx) is PermissionDecision.deny


def test_registry_advertises_exactly_two_tools() -> None:
    engine = create_async_engine(_DUMMY_URL)
    run_context = ConsolidationRunContext(
        allowed_event_ids=frozenset(), allowed_user_event_ids=frozenset()
    )
    registry = consolidation_registry(engine, FakeEmbedder(), run_context, Settings())
    names = {schema["function"]["name"] for schema in registry.schemas()}
    assert names == {"memory_propose_rewrite", "archival_consolidate_insert"}


async def test_system_context_returns_instruction() -> None:
    assert await consolidation_system_context() == CONSOLIDATION_SYSTEM_INSTRUCTION


def test_prompt_includes_versions_and_events() -> None:
    prompt = format_consolidation_prompt(
        {"persona": "helpful", "human": ""},
        {"persona": 3},
        [
            ConsolidationMessage(event_id=7, session_id="chat:a", role="user", content="I love tea"),
            ConsolidationMessage(event_id=8, session_id="chat:a", role="assistant", content="Noted"),
        ],
    )
    assert "persona (version 3)" in prompt
    assert "human (version 0)" in prompt
    assert "7 [user] I love tea" in prompt
    assert "8 [assistant] Noted" in prompt


def test_session_id_matches_exclusion_prefix() -> None:
    assert consolidation_session_id("web:local", "abc") == "consolidation:web:local:abc"
    assert consolidation_session_id("web:local", "abc").startswith("consolidation:")


def test_schedule_id_is_stable() -> None:
    assert consolidation_schedule_id("web:local") == "memory-consolidation:web:local"
```

- [ ] **Step 2: Run RED** — `python -m uv run pytest tests/unit/test_consolidation_agent.py -v`
  - Expected failure: `ModuleNotFoundError: No module named 'keel_core.consolidation.agent'`.

- [ ] **Step 3: Implement** — create `packages/keel-core/src/keel_core/consolidation/agent.py`:

```python
"""The memory-consolidation agent: identity, prompt, permissions, toolset (spec §7).

A system-scoped, trusted agent with exactly two write tools and a fail-closed (default
deny) permission engine. ``consolidation_system_context`` supplies the standing
instruction as an ephemeral system message (the loop inserts it at index 0), while
``format_consolidation_prompt`` renders the current core memory + the message batch as
the run's single user turn.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.agents import AgentSpec, Scope
from keel_core.config import Settings
from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.reader import ConsolidationMessage
from keel_core.consolidation.tools import ArchivalConsolidateInsertTool, ProposeRewriteTool
from keel_core.embeddings import Embedder
from keel_core.loop import ToolRegistry
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.types import PermissionDecision, ScopeKind, TrustLevel

MEMORY_CONSOLIDATOR_AGENT_ID = "memory-consolidator"

CONSOLIDATION_SYSTEM_INSTRUCTION = (
    "You are Keel's memory consolidation worker. You review a batch of recent "
    "conversation messages and durably record only well-grounded, lasting facts.\n"
    "Rules:\n"
    "- Never invent facts. Every write MUST cite source_event_ids drawn from the batch, "
    "and at least one cited event MUST be a user message.\n"
    "- Use memory_propose_rewrite to propose an edit to the 'persona' or 'human' "
    "core-memory block. These are proposals for human review; they are NOT applied "
    "automatically.\n"
    "- Use archival_consolidate_insert for durable standalone facts worth recalling "
    "later; set an honest confidence in [0, 1].\n"
    "- Prefer a few high-value writes. If nothing is worth recording, make no tool "
    "calls and end your turn."
)


def consolidation_session_id(scope_id: str, run_id: str) -> str:
    """A per-run session id (matches the reader's ``consolidation:%`` exclusion)."""
    return f"consolidation:{scope_id}:{run_id}"


def consolidation_schedule_id(scope_id: str) -> str:
    """The stable schedule id for a scope's daily consolidation."""
    return f"memory-consolidation:{scope_id}"


def build_consolidation_agent(
    scope_id: str, model: str, *, token_budget: int, max_iterations: int = 8
) -> AgentSpec:
    """The consolidation agent (system-scoped, trusted, two write tools)."""
    return AgentSpec(
        id=MEMORY_CONSOLIDATOR_AGENT_ID,
        name="Keel Memory Consolidator",
        model=model,
        scope=Scope(id=scope_id, kind=ScopeKind.system, trust=TrustLevel.trusted),
        persona="You consolidate durable memory from recent conversations.",
        toolset=["memory_propose_rewrite", "archival_consolidate_insert"],
        max_iterations=max_iterations,
        token_budget=token_budget,
    )


def consolidation_permissions() -> RuleBasedPermissionEngine:
    """Allow the two consolidation tools; deny everything else (fail closed)."""
    return RuleBasedPermissionEngine(
        [
            Rule("memory_propose_rewrite", PermissionDecision.allow),
            Rule("archival_consolidate_insert", PermissionDecision.allow),
        ],
        default=PermissionDecision.deny,
    )


def consolidation_registry(
    engine: AsyncEngine,
    embedder: Embedder,
    run_context: ConsolidationRunContext,
    settings: Settings,
) -> ToolRegistry:
    """The consolidation toolset bound to a run's shared context."""
    return ToolRegistry(
        [
            ProposeRewriteTool(engine, run_context),
            ArchivalConsolidateInsertTool(
                engine,
                embedder,
                run_context,
                min_confidence=settings.consolidation_archival_min_confidence,
                semantic_dedupe_distance=settings.consolidation_semantic_dedupe_distance,
            ),
        ]
    )


async def consolidation_system_context() -> str:
    """The standing instruction, supplied as an ephemeral system message."""
    return CONSOLIDATION_SYSTEM_INSTRUCTION


def format_consolidation_prompt(
    blocks: dict[str, str],
    versions: dict[str, int],
    messages: Sequence[ConsolidationMessage],
) -> str:
    """Render the current core memory + the message batch as the run's user turn."""
    lines = ["# Current core memory", ""]
    for key in ("persona", "human"):
        lines.append(f"## {key} (version {versions.get(key, 0)})")
        lines.append(blocks.get(key) or "(empty)")
        lines.append("")
    lines.append("# Recent conversation batch")
    lines.append("Each line is 'event_id [role] text'. Cite these event_ids in source_event_ids.")
    lines.append("")
    for message in messages:
        lines.append(f"{message.event_id} [{message.role}] {message.content}")
    lines.append("")
    lines.append("Record only durable, well-grounded facts. Make no tool calls if none qualify.")
    return "\n".join(lines)
```

- [ ] **Step 4: Run GREEN** — `python -m uv run pytest tests/unit/test_consolidation_agent.py -v` (7 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/consolidation/agent.py tests/unit/test_consolidation_agent.py
git commit -m "feat(consolidation): add consolidation agent glue" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 10: Worker dispatch + consolidation flow + daily seed

**Files:**
- Modify: `packages/keel-worker/src/keel_worker/main.py`
- Create: `scripts/seed_consolidation_schedule.py`
- Test (unit): `tests/unit/test_worker_tasks.py` (extend — dispatch routing)
- Test (integration): `tests/integration/test_consolidation_worker.py`

**Interfaces:**
- Produces: `run_agent(ctx, schedule_id) -> str` refactored into a dispatcher keyed on `row.agent_id`; `_run_digest(ctx: dict[str, Any], row: ScheduleRow, settings: Settings) -> str` (the extracted digest body, now scheduling `mark_run` internally); `consolidate_memory(ctx: dict[str, Any], row: ScheduleRow, settings: Settings) -> str`; `startup` adds `ctx["embedder"] = LiteLLMEmbedder(...)`; seed `seed(scope_id: str, engine: AsyncEngine) -> str`.
- Consumes: Task 9 agent glue (`build_consolidation_agent`, `consolidation_registry`, `consolidation_permissions`, `consolidation_system_context`, `consolidation_session_id`, `format_consolidation_prompt`, `MEMORY_CONSOLIDATOR_AGENT_ID`); `ConsolidationCursorStore` (T5); `ConsolidationBatchReader` (T6); `ConsolidationRunContext`/`should_advance_cursor` (T4); `PostgresMemoryStore.versions()` (T7); `PostgresEventStore`; `LiteLLMEmbedder`; `Settings` consolidation fields (T1); `ScheduleRow`/`PostgresScheduleStore`.

- [ ] **Step 1: Write the failing tests**

  Extend `tests/unit/test_worker_tasks.py`: add `import pytest` to the import block, then append these two dispatch tests (they exercise the refactored `run_agent` router without a database — the consolidation branch is monkeypatched):

```python
async def test_run_agent_dispatches_to_consolidation(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    async def fake_consolidate(ctx: dict[str, Any], row: ScheduleRow, settings: Any) -> str:
        seen.append(row.id)
        return "completed"

    monkeypatch.setattr("keel_worker.main.consolidate_memory", fake_consolidate)
    row = ScheduleRow(
        id="memory-consolidation:u:1",
        scope_id="u:1",
        agent_id="memory-consolidator",
        session_id="consolidation:u:1",
        trigger_kind="interval",
        spec="86400",
        next_run_at=_NOW,
        interval_s=86400,
    )
    ctx: dict[str, Any] = {"schedules": InMemoryScheduleStore([row])}
    assert await run_agent(ctx, "memory-consolidation:u:1") == "completed"
    assert seen == ["memory-consolidation:u:1"]


async def test_run_agent_rejects_unknown_agent_id() -> None:
    row = ScheduleRow(
        id="weird",
        scope_id="u:1",
        agent_id="mystery",
        session_id="s",
        trigger_kind="interval",
        spec="86400",
        next_run_at=_NOW,
        interval_s=86400,
    )
    ctx: dict[str, Any] = {"schedules": InMemoryScheduleStore([row])}
    assert await run_agent(ctx, "weird") == "unsupported"
```

  Create `tests/integration/test_consolidation_worker.py`:

```python
"""Integration: consolidation worker flow (busy, skipped) + seed idempotency."""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import get_settings
from keel_core.consolidation.cursor import ConsolidationCursorStore
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ProviderChunk
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_scheduler.store import PostgresScheduleStore, ScheduleRow
from keel_worker.main import consolidate_memory

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_NOW = datetime(2026, 7, 12, 9, 0, tzinfo=UTC)


def _row(scope: str) -> ScheduleRow:
    return ScheduleRow(
        id=f"memory-consolidation:{scope}",
        scope_id=scope,
        agent_id="memory-consolidator",
        session_id=f"consolidation:{scope}",
        trigger_kind="interval",
        spec="86400",
        next_run_at=_NOW,
        interval_s=86400,
    )


async def _emit(conn: Any, scope: str, session_id: str, seq: int, role: str, content: str) -> int:
    row = (
        await conn.execute(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts, payload) VALUES "
                "(:session, :scope, :seq, 'message.token', now(), CAST(:payload AS jsonb)) "
                "RETURNING id"
            ),
            {
                "session": session_id,
                "scope": scope,
                "seq": seq,
                "payload": json.dumps({"role": role, "text": content}),
            },
        )
    ).one()
    return int(row.id)


async def _seed_schedule(engine: AsyncEngine, scope: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        await conn.execute(
            text(
                "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                "spec, next_run_at, interval_s, enabled) VALUES "
                "(:id, :s, 'memory-consolidator', :sess, 'interval', '86400', now(), 86400, true) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": f"memory-consolidation:{scope}", "s": scope, "sess": f"consolidation:{scope}"},
        )


def _ctx(engine: AsyncEngine, scope: str) -> dict[str, Any]:
    return {
        "engine": engine,
        "provider": ScriptedProviderGateway(
            [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
        ),
        "embedder": FakeEmbedder(),
        "schedules": PostgresScheduleStore(engine, scope),
    }


async def test_consolidate_skips_below_min(migrated_db: AsyncEngine) -> None:
    scope = "wrk:skip"
    await _seed_schedule(migrated_db, scope)
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        await _emit(conn, scope, "chat:a", 1, "user", "just one message")

    result = await consolidate_memory(_ctx(migrated_db, scope), _row(scope), get_settings())

    assert result == "skipped"
    state = await ConsolidationCursorStore(migrated_db, scope).get()
    assert state is not None
    assert state.last_status == "skipped"
    assert state.last_event_id == 0


async def test_consolidate_reports_busy_when_leased(migrated_db: AsyncEngine) -> None:
    scope = "wrk:busy"
    # Pre-take a live lease so the worker's own claim loses.
    held = await ConsolidationCursorStore(migrated_db, scope).claim(datetime.now(UTC))
    assert held is not None

    result = await consolidate_memory(_ctx(migrated_db, scope), _row(scope), get_settings())

    assert result == "busy"


async def test_seed_consolidation_schedule_is_idempotent(migrated_db: AsyncEngine) -> None:
    spec = importlib.util.spec_from_file_location(
        "seed_consolidation_schedule",
        _REPO_ROOT / "scripts" / "seed_consolidation_schedule.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    scope = "wrk:seed"
    first = await module.seed(scope, migrated_db)
    second = await module.seed(scope, migrated_db)

    assert first == second == f"memory-consolidation:{scope}"
    rows = await PostgresScheduleStore(migrated_db, scope).list_all()
    assert [r.id for r in rows] == [first]
    assert rows[0].agent_id == "memory-consolidator"
```

- [ ] **Step 2: Run RED**
  - Unit: `python -m uv run pytest tests/unit/test_worker_tasks.py -v`
    - Expected failure: `AttributeError: <module 'keel_worker.main' ...> does not have the attribute 'consolidate_memory'` (the monkeypatch target does not exist yet) and `test_run_agent_rejects_unknown_agent_id` fails because the current `run_agent` ignores `agent_id`.
  - Integration: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_worker.py -v`
    - Expected failure: `ImportError: cannot import name 'consolidate_memory' from 'keel_worker.main'`.

- [ ] **Step 3: Implement**

  **3a — `packages/keel-worker/src/keel_worker/main.py` imports.** Add `import uuid` and the consolidation imports. Replace:

```python
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
```

  with:

```python
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
```

  Insert this consolidation import block immediately before the existing
  `from keel_core.digest import (` block (so ruff/isort keeps `consolidation` before
  `digest`):

```python
from keel_core.consolidation.agent import (
    MEMORY_CONSOLIDATOR_AGENT_ID,
    build_consolidation_agent,
    consolidation_permissions,
    consolidation_registry,
    consolidation_session_id,
    consolidation_system_context,
    format_consolidation_prompt,
)
from keel_core.consolidation.context import ConsolidationRunContext, should_advance_cursor
from keel_core.consolidation.cursor import ConsolidationCursorStore
from keel_core.consolidation.reader import ConsolidationBatchReader
```

  Then replace:

```python
from keel_core.loop import ToolRegistry, admit, resume, run
from keel_core.observability import configure_logging, configure_tracing
from keel_scheduler.store import due_tick
```

  with:

```python
from keel_core.loop import ToolRegistry, admit, resume, run
from keel_core.memory import PostgresMemoryStore
from keel_core.observability import configure_logging, configure_tracing
from keel_core.state import PostgresEventStore
from keel_scheduler.store import ScheduleRow, due_tick
```

  **3b — dispatcher refactor.** Replace the entire existing `run_agent` function (the `async def run_agent(ctx, schedule_id) -> str:` block, from its `"""Start an unattended digest run..."""` docstring through `return result.reason.value`) with the extracted digest runner, the dispatcher, and the consolidation flow:

```python
async def _run_digest(ctx: dict[str, Any], row: ScheduleRow, settings: Settings) -> str:
    """Start an unattended digest run for a due schedule; suspend on a gated send."""
    store, approvals, provider = ctx["store"], ctx["approvals"], ctx["provider"]
    agent = build_digest_agent(row.scope_id).model_copy(update={"model": settings.default_model})
    # The scheduled trigger is a *user* turn (the agent's standing behavior is its
    # persona/system prompt); a system-only message list is rejected by chat providers.
    await admit(store, row.session_id, row.scope_id, DIGEST_INSTRUCTION)
    result = await run(
        agent=agent,
        session_id=row.session_id,
        store=store,
        provider=provider,
        registry=_digest_registry(ctx, settings, row.scope_id),
        permissions=digest_permissions(),
        approvals=approvals,
        expires_at=datetime.now(UTC) + timedelta(hours=settings.approval_timeout_hours),
    )
    await ctx["schedules"].mark_run(row.id, row.next_run_at, result.reason.value)
    return result.reason.value


async def run_agent(ctx: dict[str, Any], schedule_id: str) -> str:
    """Dispatch a due schedule to its agent runner (digest or memory consolidation)."""
    settings = get_settings()
    row = await ctx["schedules"].get(schedule_id)
    if row is None:
        return "missing"
    if row.agent_id == MEMORY_CONSOLIDATOR_AGENT_ID:
        return await consolidate_memory(ctx, row, settings)
    if row.agent_id == "digest":
        return await _run_digest(ctx, row, settings)
    logger.warning("run_agent: unsupported agent_id %r (schedule %s)", row.agent_id, schedule_id)
    return "unsupported"


async def consolidate_memory(ctx: dict[str, Any], row: ScheduleRow, settings: Settings) -> str:
    """Run one memory-consolidation pass for a due schedule (lease -> batch -> agent run).

    Fail-closed: the cursor only advances past the batch when the run reaches ``completed``
    with zero validation errors; otherwise the lease is released with an ``error`` status
    and the same window is retried on the next tick. Never raises — the arq task returns a
    status string so one bad scope cannot crash the worker.
    """
    engine = ctx["engine"]
    provider = ctx["provider"]
    embedder = ctx["embedder"]
    schedules = ctx["schedules"]
    scope_id = row.scope_id

    cursors = ConsolidationCursorStore(engine, scope_id)
    lease = await cursors.claim(
        datetime.now(UTC), lease_seconds=settings.consolidation_lease_seconds
    )
    if lease is None:
        return "busy"  # another worker holds a live lease for this scope
    try:
        reader = ConsolidationBatchReader(engine, scope_id)
        batch = await reader.read(
            lease.last_event_id,
            limit=settings.consolidation_batch_messages,
            message_max_chars=settings.consolidation_message_max_chars,
            input_max_chars=settings.consolidation_input_max_chars,
        )
        if batch.eligible_count < settings.consolidation_min_messages:
            await cursors.fail(lease, "skipped")
            await schedules.mark_run(row.id, row.next_run_at, "skipped")
            return "skipped"

        run_context = ConsolidationRunContext(
            allowed_event_ids=frozenset(message.event_id for message in batch.messages),
            allowed_user_event_ids=batch.user_event_ids,
        )
        memory = PostgresMemoryStore(engine, scope_id)
        store = PostgresEventStore(engine, scope_id)
        run_id = uuid.uuid4().hex
        session_id = consolidation_session_id(scope_id, run_id)
        prompt = format_consolidation_prompt(
            await memory.blocks(), await memory.versions(), batch.messages
        )
        await admit(store, session_id, scope_id, prompt)
        result = await run(
            agent=build_consolidation_agent(
                scope_id,
                settings.default_model,
                token_budget=settings.consolidation_token_budget,
            ),
            session_id=session_id,
            store=store,
            provider=provider,
            registry=consolidation_registry(engine, embedder, run_context, settings),
            permissions=consolidation_permissions(),
            run_id=run_id,
            system_context=consolidation_system_context,
        )
        if should_advance_cursor(result.reason, run_context.validation_errors):
            await cursors.complete(lease, batch.max_event_id, "completed")
            await schedules.mark_run(row.id, row.next_run_at, "completed")
            return "completed"
        await cursors.fail(lease, "error")
        await schedules.mark_run(row.id, row.next_run_at, "error")
        return "error"
    except Exception:
        logger.exception("consolidation run failed for scope %s", scope_id)
        await cursors.fail(lease, "error")
        await schedules.mark_run(row.id, row.next_run_at, "error")
        return "error"
```

  **3c — `startup` embedder.** Inside `startup`, remove the now-redundant local import `from keel_core.state import PostgresEventStore` (it is imported at module top in 3a) and add the embedder import. Replace:

```python
    from keel_core.approvals import PostgresApprovalStore
    from keel_core.providers import LiteLLMGateway
    from keel_core.state import PostgresEventStore
    from keel_scheduler.store import PostgresClaimStore, PostgresScheduleStore
```

  with:

```python
    from keel_core.approvals import PostgresApprovalStore
    from keel_core.embeddings import LiteLLMEmbedder
    from keel_core.providers import LiteLLMGateway
    from keel_scheduler.store import PostgresClaimStore, PostgresScheduleStore
```

  Then register the embedder in `ctx` (the consolidation flow reads `ctx["embedder"]`). Replace:

```python
    ctx["provider"] = LiteLLMGateway()
    ctx["enqueue"] = lambda name, *args: redis.enqueue_job(name, *args)
```

  with:

```python
    ctx["provider"] = LiteLLMGateway()
    ctx["embedder"] = LiteLLMEmbedder(
        settings.embedding_model,
        settings.embedding_dim,
        send_dimensions=settings.embedding_send_dimensions,
        timeout_seconds=settings.embedding_timeout_seconds,
    )
    ctx["enqueue"] = lambda name, *args: redis.enqueue_job(name, *args)
```

  **3d — seed script.** Create `scripts/seed_consolidation_schedule.py`:

```python
"""Seed one memory-consolidation schedule for a scope (idempotent).

    python -m uv run python scripts/seed_consolidation_schedule.py --scope web:local

Creates a schedule that is immediately due (``next_run_at = now``, interval 1 day) so the
worker's ``scheduler_tick`` picks it up and dispatches a consolidation run. Safe to re-run.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import get_settings
from keel_core.consolidation.agent import consolidation_schedule_id
from keel_core.db import make_async_engine


async def seed(scope_id: str, engine: AsyncEngine) -> str:
    """Insert the consolidation schedule for ``scope_id`` (idempotent); return its id."""
    schedule_id = consolidation_schedule_id(scope_id)
    async with engine.begin() as conn:
        await conn.execute(
            text("select set_config('app.scope_id', :scope, true)"), {"scope": scope_id}
        )
        await conn.execute(
            text(
                "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                "spec, next_run_at, interval_s, enabled) VALUES "
                "(:id, :scope, 'memory-consolidator', :session, 'interval', '86400', "
                ":now, 86400, true) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {
                "id": schedule_id,
                "scope": scope_id,
                "session": f"consolidation:{scope_id}",
                "now": datetime.now(UTC),
            },
        )
    return schedule_id


async def _run(scope_id: str) -> None:
    engine = make_async_engine(get_settings())
    try:
        schedule_id = await seed(scope_id, engine)
        print(f"seeded schedule {schedule_id}")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed a memory-consolidation schedule.")
    parser.add_argument("--scope", default="web:local")
    args = parser.parse_args()
    if sys.platform == "win32":  # psycopg async needs a selector loop on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(_run(args.scope))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run GREEN**
  - Unit: `python -m uv run pytest tests/unit/test_worker_tasks.py -v` (existing digest tests still pass via the `agent_id == "digest"` branch; the two new dispatch tests pass).
  - Integration: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_worker.py -v` (3 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-worker/src/keel_worker/main.py scripts/seed_consolidation_schedule.py tests/unit/test_worker_tasks.py tests/integration/test_consolidation_worker.py
git commit -m "feat(consolidation): dispatch consolidation runs from the worker + seed" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 11: Proposal REST API + RBAC + manual run + package exports

**Files:**
- Modify: `packages/keel-server/src/keel_server/api/v1.py`
- Modify: `packages/keel-core/src/keel_core/consolidation/__init__.py` (finalize the public export surface)
- Test (integration): `tests/integration/test_memory_proposals_api.py`
- Test (unit): `tests/unit/test_consolidation_exports.py`

**Interfaces:**
- Produces: `GET /v1/memory/proposals?status=` (viewer); `POST /v1/memory/proposals/{proposal_id}/approve` (operator); `POST /v1/memory/proposals/{proposal_id}/reject` (operator); `POST /v1/memory/consolidation/run` (operator, enqueues `run_agent` for the scope's consolidation schedule); module helpers `_proposal_store(request)`, `_proposal_dict(proposal)`, `_resolution_response(resolution)`; finalized `keel_core.consolidation` re-exports + `__all__`.
- Consumes: `MemoryProposalStore`/`MemoryProposal`/`ProposalOutcome`/`ProposalResolution` (T7); `consolidation_schedule_id` (T9); `require_role`/`Role` (auth); `request.app.state.engine`/`durable_scope`/`enqueue`.

- [ ] **Step 1: Write the failing tests**

  Create `tests/unit/test_consolidation_exports.py`:

```python
"""Smoke test: the consolidation package surface imports and wires a 2-tool registry."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import create_async_engine

from keel_core import consolidation
from keel_core.config import Settings
from keel_core.consolidation import ConsolidationRunContext, consolidation_registry
from keel_core.embeddings import FakeEmbedder

_DUMMY_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel"

_EXPECTED = (
    "normalize_whitespace",
    "archival_content_hash",
    "consolidation_idempotency_key",
    "ConsolidationRunContext",
    "should_advance_cursor",
    "ConsolidationLease",
    "ConsolidationCursorState",
    "ConsolidationCursorStore",
    "ConsolidationMessage",
    "ConsolidationBatch",
    "ConsolidationBatchReader",
    "MemoryProposal",
    "ProposalOutcome",
    "ProposalResolution",
    "MemoryProposalStore",
    "validate_propose_rewrite",
    "validate_archival_insert",
    "ProposeRewriteTool",
    "ArchivalConsolidateInsertTool",
    "MEMORY_CONSOLIDATOR_AGENT_ID",
    "CONSOLIDATION_SYSTEM_INSTRUCTION",
    "consolidation_schedule_id",
    "consolidation_session_id",
    "build_consolidation_agent",
    "consolidation_permissions",
    "consolidation_registry",
    "consolidation_system_context",
    "format_consolidation_prompt",
)


def test_public_surface_is_exported() -> None:
    for name in _EXPECTED:
        assert name in consolidation.__all__, name
        assert hasattr(consolidation, name), name


async def test_registry_wires_two_tools() -> None:
    engine = create_async_engine(_DUMMY_URL)
    try:
        run_context = ConsolidationRunContext(
            allowed_event_ids=frozenset(), allowed_user_event_ids=frozenset()
        )
        registry = consolidation_registry(engine, FakeEmbedder(), run_context, Settings())
        assert len(registry.schemas()) == 2
    finally:
        await engine.dispose()
```

  Create `tests/integration/test_memory_proposals_api.py`:

```python
"""Integration: /v1/memory proposals list/approve/reject + manual consolidation run."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation import MemoryProposalStore
from keel_core.memory import PostgresMemoryStore
from keel_server.api.v1 import router

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def proposals_client(
    migrated_db: AsyncEngine,
) -> AsyncIterator[tuple[httpx.AsyncClient, str, list[tuple[Any, ...]]]]:
    scope = f"u:{uuid.uuid4().hex}"
    enqueued: list[tuple[Any, ...]] = []

    async def enqueue(name: str, *args: object) -> None:
        enqueued.append((name, *args))

    app = FastAPI()
    app.include_router(router)
    app.state.engine = migrated_db
    app.state.durable_scope = scope
    app.state.enqueue = enqueue
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, scope, enqueued


async def test_list_approve_reject_and_run(
    proposals_client: tuple[httpx.AsyncClient, str, list[tuple[Any, ...]]],
    migrated_db: AsyncEngine,
) -> None:
    client, scope, enqueued = proposals_client
    store = MemoryProposalStore(migrated_db, scope)
    memory = PostgresMemoryStore(migrated_db, scope)

    pid_human, created_human = await store.propose(
        block="human",
        proposed_value="likes tea",
        reason="stated in chat",
        confidence=0.9,
        source_event_ids=[1],
    )
    pid_persona, _ = await store.propose(
        block="persona",
        proposed_value="concise and warm",
        reason="tone feedback",
        confidence=0.8,
        source_event_ids=[2],
    )
    assert created_human is True

    pending = (await client.get("/v1/memory/proposals", params={"status": "pending"})).json()
    assert {p["id"] for p in pending} == {pid_human, pid_persona}
    assert all(p["status"] == "pending" for p in pending)

    approved = await client.post(f"/v1/memory/proposals/{pid_human}/approve")
    assert approved.status_code == 200
    assert approved.json() == {"ok": True, "status": "applied", "version": 1}
    assert await memory.get("human") == "likes tea"

    rejected = await client.post(f"/v1/memory/proposals/{pid_persona}/reject")
    assert rejected.status_code == 200
    assert rejected.json() == {"ok": True, "status": "rejected", "version": None}

    missing = await client.post("/v1/memory/proposals/does-not-exist/approve")
    assert missing.status_code == 404
    assert missing.json() == {"ok": False, "status": "not_found", "version": None}

    still_pending = (
        await client.get("/v1/memory/proposals", params={"status": "pending"})
    ).json()
    assert still_pending == []

    ran = await client.post("/v1/memory/consolidation/run")
    assert ran.json() == {"ok": True}
    assert enqueued == [("run_agent", f"memory-consolidation:{scope}")]
```

- [ ] **Step 2: Run RED**
  - Unit: `python -m uv run pytest tests/unit/test_consolidation_exports.py -v`
    - Expected failure: `ImportError: cannot import name 'ConsolidationRunContext' from 'keel_core.consolidation'` (the package `__init__` is docstring-only until this task).
  - Integration: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_memory_proposals_api.py -v`
    - Expected failure: the first `client.get("/v1/memory/proposals")` returns `404 Not Found` (route not registered), so `pending` is not JSON-decodable as a list / the id-set assertion fails.

- [ ] **Step 3: Implement**

  **3a — finalize the package surface.** Replace the entire contents of `packages/keel-core/src/keel_core/consolidation/__init__.py` (created docstring-only in Task 3) with:

```python
"""Memory consolidation subsystem (spec 2026-07-12-memory-consolidation).

A scheduled, unattended agent reviews recent conversation and durably records
lasting facts: it *proposes* core-memory rewrites (human-reviewed) and inserts
deduplicated, provenance-tagged archival passages. This package's public surface
is re-exported below; the submodules hold the implementation.
"""

from __future__ import annotations

from keel_core.consolidation.agent import (
    CONSOLIDATION_SYSTEM_INSTRUCTION,
    MEMORY_CONSOLIDATOR_AGENT_ID,
    build_consolidation_agent,
    consolidation_permissions,
    consolidation_registry,
    consolidation_schedule_id,
    consolidation_session_id,
    consolidation_system_context,
    format_consolidation_prompt,
)
from keel_core.consolidation.context import ConsolidationRunContext, should_advance_cursor
from keel_core.consolidation.cursor import (
    ConsolidationCursorState,
    ConsolidationCursorStore,
    ConsolidationLease,
)
from keel_core.consolidation.hashing import (
    archival_content_hash,
    consolidation_idempotency_key,
    normalize_whitespace,
)
from keel_core.consolidation.proposals import (
    MemoryProposal,
    MemoryProposalStore,
    ProposalOutcome,
    ProposalResolution,
)
from keel_core.consolidation.reader import (
    ConsolidationBatch,
    ConsolidationBatchReader,
    ConsolidationMessage,
)
from keel_core.consolidation.tools import (
    ArchivalConsolidateInsertTool,
    ProposeRewriteTool,
    validate_archival_insert,
    validate_propose_rewrite,
)

__all__ = [
    "CONSOLIDATION_SYSTEM_INSTRUCTION",
    "MEMORY_CONSOLIDATOR_AGENT_ID",
    "ArchivalConsolidateInsertTool",
    "ConsolidationBatch",
    "ConsolidationBatchReader",
    "ConsolidationCursorState",
    "ConsolidationCursorStore",
    "ConsolidationLease",
    "ConsolidationMessage",
    "ConsolidationRunContext",
    "MemoryProposal",
    "MemoryProposalStore",
    "ProposalOutcome",
    "ProposalResolution",
    "ProposeRewriteTool",
    "archival_content_hash",
    "build_consolidation_agent",
    "consolidation_idempotency_key",
    "consolidation_permissions",
    "consolidation_registry",
    "consolidation_schedule_id",
    "consolidation_session_id",
    "consolidation_system_context",
    "format_consolidation_prompt",
    "normalize_whitespace",
    "should_advance_cursor",
    "validate_archival_insert",
    "validate_propose_rewrite",
]
```

  **3b — v1 imports.** In `packages/keel-server/src/keel_server/api/v1.py`, widen the responses import and add the consolidation imports. Replace:

```python
from fastapi.responses import StreamingResponse
```

  with:

```python
from fastapi.responses import JSONResponse, StreamingResponse
```

  Add alongside the other `keel_core` imports, immediately after the existing
  `from keel_core.approvals import ...` line (so ruff/isort keeps `api`, `approvals`,
  then `consolidation`):

```python
from keel_core.consolidation import (
    MemoryProposal,
    MemoryProposalStore,
    ProposalOutcome,
    ProposalResolution,
    consolidation_schedule_id,
)
```

  **3c — helpers + routes.** Append to the end of `packages/keel-server/src/keel_server/api/v1.py`:

```python
def _proposal_store(request: Request) -> tuple[MemoryProposalStore, str]:
    engine = getattr(request.app.state, "engine", None)
    scope: str = getattr(request.app.state, "durable_scope", "web:local")
    if engine is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "datastore unavailable")
    return MemoryProposalStore(engine, scope), scope


def _proposal_dict(proposal: MemoryProposal) -> dict[str, object]:
    return {
        "id": proposal.id,
        "block": proposal.block,
        "expected_version": proposal.expected_version,
        "proposed_value": proposal.proposed_value,
        "reason": proposal.reason,
        "confidence": proposal.confidence,
        "source_event_ids": proposal.source_event_ids,
        "status": proposal.status,
        "created_at": proposal.created_at.isoformat(),
        "resolved_at": proposal.resolved_at.isoformat() if proposal.resolved_at else None,
        "resolved_by": proposal.resolved_by,
    }


def _resolution_response(resolution: ProposalResolution) -> JSONResponse:
    """Map a proposal resolution to its HTTP response (404 missing, 409 conflict, 200 ok)."""
    outcome = resolution.outcome
    if outcome is ProposalOutcome.not_found:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"ok": False, "status": outcome.value, "version": None},
        )
    if outcome in (ProposalOutcome.stale, ProposalOutcome.already_resolved):
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"ok": False, "status": outcome.value, "version": None},
        )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"ok": True, "status": outcome.value, "version": resolution.version},
    )


@router.get("/memory/proposals", summary="List core-memory rewrite proposals for the scope")
async def list_memory_proposals(
    request: Request, status_filter: str | None = Query(None, alias="status")
) -> list[dict[str, object]]:
    """Core-memory rewrite proposals awaiting (or past) human review."""
    store, _ = _proposal_store(request)
    return [_proposal_dict(p) for p in await store.list_proposals(status=status_filter)]


@router.post(
    "/memory/proposals/{proposal_id}/approve",
    summary="Approve a proposal (atomically apply it to core memory)",
    dependencies=[Depends(require_role(Role.operator))],
)
async def approve_memory_proposal(proposal_id: str, request: Request) -> JSONResponse:
    """Apply the proposal under an optimistic version check; stale ones 409."""
    store, _ = _proposal_store(request)
    return _resolution_response(await store.approve(proposal_id, "web"))


@router.post(
    "/memory/proposals/{proposal_id}/reject",
    summary="Reject a proposal (no change to core memory)",
    dependencies=[Depends(require_role(Role.operator))],
)
async def reject_memory_proposal(proposal_id: str, request: Request) -> JSONResponse:
    """Mark the proposal rejected; core memory is untouched."""
    store, _ = _proposal_store(request)
    return _resolution_response(await store.reject(proposal_id, "web"))


@router.post(
    "/memory/consolidation/run",
    summary="Enqueue a memory-consolidation run for the scope now",
    dependencies=[Depends(require_role(Role.operator))],
)
async def run_consolidation(request: Request) -> dict[str, bool]:
    """Manually trigger the scope's consolidation schedule (same path as the daily tick)."""
    enqueue = getattr(request.app.state, "enqueue", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    if enqueue is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "job queue unavailable")
    await enqueue("run_agent", consolidation_schedule_id(scope))
    return {"ok": True}
```

- [ ] **Step 4: Run GREEN**
  - Unit: `python -m uv run pytest tests/unit/test_consolidation_exports.py -v` (2 pass).
  - Integration: `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_memory_proposals_api.py -v` (1 pass).

- [ ] **Step 5: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/consolidation/__init__.py packages/keel-server/src/keel_server/api/v1.py tests/integration/test_memory_proposals_api.py tests/unit/test_consolidation_exports.py
git commit -m "feat(consolidation): expose proposal REST API + package surface" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 12: End-to-end acceptance (scripted provider + real Postgres)

This is the capstone acceptance test for spec §17: it drives the whole chain
(`scheduler dispatch -> lease -> batch -> agent run -> constrained tools -> proposal +
archival persistence -> cursor advance`) against a real Postgres with a
`ScriptedProviderGateway` and `FakeEmbedder` (no network). It exercises three behaviors:
clean completion + persistence, dedupe on re-run, and a citation-validation failure that
must NOT advance the cursor.

**Files:**
- Test (integration): `tests/integration/test_consolidation_e2e.py`

**Interfaces:**
- Consumes: `run_agent` (Task 10 dispatcher); `consolidation_schedule_id` (Task 9); `MemoryProposalStore`/`ConsolidationCursorStore` (T5/T7); `PostgresMemoryStore`; `PostgresScheduleStore`; `ScriptedProviderGateway`/`ProviderChunk`/`ToolCall`/`FinishReason`/`FakeEmbedder`. Produces: no new source (acceptance only).

- [ ] **Step 1: Write the acceptance test** — create `tests/integration/test_consolidation_e2e.py`:

```python
"""End-to-end acceptance: consolidation dispatch -> tools -> persistence (spec §17)."""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation import (
    ConsolidationCursorStore,
    MemoryProposalStore,
    consolidation_schedule_id,
)
from keel_core.embeddings import FakeEmbedder
from keel_core.memory import PostgresMemoryStore
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_scheduler.store import PostgresScheduleStore
from keel_worker.main import run_agent

pytestmark = pytest.mark.integration

_SET_SCOPE = text("select set_config('app.scope_id', :s, true)")


async def _emit(conn: Any, scope: str, session_id: str, seq: int, role: str, content: str) -> int:
    row = (
        await conn.execute(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts, payload) VALUES "
                "(:session, :scope, :seq, 'message.token', now(), CAST(:payload AS jsonb)) "
                "RETURNING id"
            ),
            {
                "session": session_id,
                "scope": scope,
                "seq": seq,
                "payload": json.dumps({"role": role, "text": content, "partial": False}),
            },
        )
    ).one()
    return int(row.id)


async def _seed_schedule(engine: AsyncEngine, scope: str) -> str:
    schedule_id = consolidation_schedule_id(scope)
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        await conn.execute(
            text(
                "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                "spec, next_run_at, interval_s, enabled) VALUES "
                "(:id, :s, 'memory-consolidator', :sess, 'interval', '86400', now(), 86400, true) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": schedule_id, "s": scope, "sess": f"consolidation:{scope}"},
        )
    return schedule_id


def _ctx(engine: AsyncEngine, scope: str, provider: ScriptedProviderGateway) -> dict[str, Any]:
    return {
        "engine": engine,
        "provider": provider,
        "embedder": FakeEmbedder(),
        "schedules": PostgresScheduleStore(engine, scope),
    }


def _scripted(*, propose_ids: list[int], archival_ids: list[int]) -> ScriptedProviderGateway:
    """A run that proposes a 'human' rewrite, inserts one archival fact, then ends."""
    return ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c1",
                        name="memory_propose_rewrite",
                        arguments={
                            "block": "human",
                            "proposed_value": "Prefers tea in the morning.",
                            "reason": "the user said so",
                            "confidence": 0.9,
                            "source_event_ids": propose_ids,
                        },
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c2",
                        name="archival_consolidate_insert",
                        arguments={
                            "content": "The user prefers tea in the morning.",
                            "confidence": 0.95,
                            "source_event_ids": archival_ids,
                        },
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )


async def _archival(engine: AsyncEngine, scope: str) -> list[tuple[str, bool, list[int]]]:
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        rows = (
            await conn.execute(
                text(
                    "SELECT origin, content_hash, source_event_ids FROM archival "
                    "WHERE scope_id = :s ORDER BY id"
                ),
                {"s": scope},
            )
        ).all()
    return [
        (str(r.origin), r.content_hash is not None, [int(i) for i in (r.source_event_ids or [])])
        for r in rows
    ]


async def test_e2e_consolidation_persists_then_dedupes(migrated_db: AsyncEngine) -> None:
    scope = f"e2e:{uuid.uuid4().hex}"
    session = f"chat:{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        ids = [
            await _emit(conn, scope, session, i, "user" if i % 2 else "assistant", f"message {i}")
            for i in range(1, 11)
        ]
    user_id, max_id = ids[0], ids[-1]
    schedule_id = await _seed_schedule(migrated_db, scope)

    result = await run_agent(
        _ctx(migrated_db, scope, _scripted(propose_ids=[user_id], archival_ids=[user_id])),
        schedule_id,
    )
    assert result == "completed"

    # Core memory is unchanged: the rewrite is a *proposal* awaiting human review.
    assert await PostgresMemoryStore(migrated_db, scope).get("human") is None
    proposals = await MemoryProposalStore(migrated_db, scope).list_proposals(status="pending")
    assert len(proposals) == 1
    assert proposals[0].block == "human"
    assert proposals[0].source_event_ids == [user_id]

    assert await _archival(migrated_db, scope) == [("consolidation", True, [user_id])]

    cursor = await ConsolidationCursorStore(migrated_db, scope).get()
    assert cursor is not None
    assert cursor.last_event_id == max_id
    assert cursor.last_status == "completed"

    # Re-run the identical batch (reset the cursor): idempotent proposal + archival merge.
    async with migrated_db.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        await conn.execute(
            text("DELETE FROM consolidation_cursors WHERE scope_id = :s"), {"s": scope}
        )

    rerun = await run_agent(
        _ctx(migrated_db, scope, _scripted(propose_ids=[user_id], archival_ids=[user_id])),
        schedule_id,
    )
    assert rerun == "completed"
    still = await MemoryProposalStore(migrated_db, scope).list_proposals(status="pending")
    assert len(still) == 1  # ON CONFLICT DO NOTHING -> the same proposal
    assert await _archival(migrated_db, scope) == [("consolidation", True, [user_id])]


async def test_e2e_validation_error_blocks_cursor(migrated_db: AsyncEngine) -> None:
    scope = f"e2e:{uuid.uuid4().hex}"
    session = f"chat:{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:
        await conn.execute(_SET_SCOPE, {"s": scope})
        for i in range(1, 11):
            await _emit(conn, scope, session, i, "user", f"message {i}")
    schedule_id = await _seed_schedule(migrated_db, scope)

    # The model cites an event id that is not in the batch -> both tools fail validation.
    result = await run_agent(
        _ctx(migrated_db, scope, _scripted(propose_ids=[999999], archival_ids=[999999])),
        schedule_id,
    )
    assert result == "error"

    cursor = await ConsolidationCursorStore(migrated_db, scope).get()
    assert cursor is not None
    assert cursor.last_event_id == 0  # NOT advanced: the batch is safely retried next tick
    assert cursor.last_status == "error"
    assert await MemoryProposalStore(migrated_db, scope).list_proposals() == []
    assert await _archival(migrated_db, scope) == []
```

- [ ] **Step 2: Run RED** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_e2e.py -v`
  - This is the acceptance capstone: if written early (per TDD) before Tasks 3–11 exist, it fails with `ModuleNotFoundError: No module named 'keel_core.consolidation'`. Once Tasks 1–11 are implemented, it passes directly — it asserts the fully wired chain and no new source is added by this task.

- [ ] **Step 3: Run GREEN** — `$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest tests/integration/test_consolidation_e2e.py -v` (2 pass).

- [ ] **Step 4: Quality gates** — `python -m uv run ruff check .` · `python -m uv run ruff format --check .` · `python -m uv run mypy packages`.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_consolidation_e2e.py
git commit -m "test(consolidation): end-to-end acceptance for the consolidation flow" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Full verification & live check

The acceptance gate for the whole slice (spec §18–§19). This is **not** a TDD task — no
new code and no commit. Run it after Tasks 1–12 are merged.

### Automated full-suite gates

```powershell
# 1. Unit + non-integration suite (no database required)
python -m uv run pytest -m "not integration" -q

# 2. Postgres integration suite — the fixture fails closed, so the URL is REQUIRED
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
python -m uv run pytest -m integration -q

# 3. Lint + format + strict types
python -m uv run ruff check .
python -m uv run ruff format --check .
python -m uv run mypy packages

# 4. Migration is linear and single-headed at 0008
python -m uv run alembic upgrade head
python -m uv run alembic heads   # -> exactly one head: 0008_memory_consolidation

# 5. Seed scripts import + run (idempotent on re-run)
python -m uv run python scripts/seed_consolidation_schedule.py --scope web:local
python -m uv run python scripts/seed_digest_schedule.py --scope web:local
```

Expected: all suites green; `alembic heads` prints a single revision `0008_memory_consolidation`; both seed scripts exit `0` and are idempotent.

### Live verification (spec §18 — manual, against a real deployment)

Run against a live web scope with a real provider once the automated gates pass:

- [ ] 1. Apply the migration and seed the daily schedule (`scripts/seed_consolidation_schedule.py --scope web:local`).
- [ ] 2. Create at least 10 trusted Web `user`/`assistant` messages in a chat session.
- [ ] 3. Trigger a manual run (`POST /v1/memory/consolidation/run`); confirm the schedule status becomes `completed`.
- [ ] 4. Confirm archival auto-insert: `origin='consolidation'`, `content_hash` non-null, `source_event_ids` correct.
- [ ] 5. Confirm the Core block is unchanged and only a `pending` proposal was produced.
- [ ] 6. Approve the proposal (`POST /v1/memory/proposals/{id}/approve`): block `version` grows, a `memory_block_versions` row is appended, the new value is visible in the next chat turn.
- [ ] 7. After a proposal exists, manually edit the same block, then approve: expect HTTP 409, proposal `stale`, and the newer block value is NOT overwritten.
- [ ] 8. Retry the same batch: no duplicate proposal or archival row. Proposal uses its
  idempotency key; Archival uses exact content hash first, then an exact-source semantic
  retry merge (identical normalized `source_event_ids` + `cosine distance <
  consolidation_semantic_dedupe_distance`, default `0.05`) for provider rephrasing. A
  merely-overlapping source set stays distinct; the merge is logged at INFO.
- [ ] 9. With fewer than 10 messages: status `skipped`, cursor does not advance, batch accumulates for the next run.
- [ ] 10. Manual + scheduled run at the same time: exactly one wins the lease, the other returns `busy`.

### Definition of done (spec §19)

- [ ] Daily + manual consolidation both execute.
- [ ] Core is modified only through proposal + approve (never auto-applied).
- [ ] Archival auto-writes carry provenance, a confidence gate, and idempotent dedupe.
- [ ] Cursor/lease are safe under concurrency, failure, and crash.
- [ ] Every write carries user-message evidence.
- [ ] Scope isolation, CAS-stale, and retry-idempotency have integration tests.
- [ ] Full tests, ruff, and mypy pass.

---

## Self-Review

Performed with fresh eyes against the spec per the writing-plans skill. Three checks
below; every issue found was fixed inline before finalizing.

### 1. Spec coverage

| Spec section | Requirement | Task(s) |
|---|---|---|
| §1 背景与现状 | Motivation / context (no deliverable) | — (context only) |
| §2.1 本切片交付 | In-scope deliverables | Tasks 1–12 |
| §2.2 不在本切片 | Out of scope (evals, Memory UI, archival deletion, RAG, IM/digest consolidation) | Global Constraints → "Out of scope" |
| §3 核心架构 | cursor → batch → agent → tools → proposal/archival flow | Tasks 4, 5, 6, 9, 10 |
| §4.1 `consolidation_cursors` | Cursor table + RLS | Task 2 |
| §4.2 `memory_proposals` | Proposal table + RLS | Task 2 |
| §4.3 Archival provenance | `origin` / `content_hash` / `source_event_ids` + partial unique index | Task 2 |
| §5.1 Claim | Lease claim (INSERT-then-lease CAS) | Task 5 |
| §5.2 Complete / fail | complete advances cursor; fail preserves it | Task 5 |
| §6 Batch reader | Bounded reader + `user_event_ids` + digest/consolidation session exclusions | Task 6 |
| §7.1 AgentSpec | Dedicated system-scope agent, 2-tool toolset | Task 9 |
| §7.2 Prompt | Consolidation system instruction + prompt render | Task 9 |
| §8 Tool 1 `memory_propose_rewrite` | Propose-only tool + citation validators | Task 8 |
| §9 Tool 2 `archival_consolidate_insert` | Archival tool + confidence gate + dedupe | Task 8 |
| §10 Run context 与 cursor 成功条件 | Run-context counters + `should_advance_cursor` | Task 4 |
| §11.1 Atomic Core optimistic CAS | Approve = insert (v0) / CAS-update (v>0) → applied/stale | Task 7 |
| §11.2 Reject | Reject transition | Task 7 |
| §12 Worker 与 schedule | Dispatcher + `consolidate_memory` + daily seed | Task 10 |
| §13.1 Proposal list | `GET /v1/memory/proposals` | Task 11 |
| §13.2 Approve / reject | `POST …/approve` \| `…/reject` + 409 semantics | Task 11 |
| §13.3 Manual run | `POST /v1/memory/consolidation/run` | Task 11 |
| §14 配置 | 7 consolidation settings | Task 1 |
| §15 错误、重试与可观测性 | fail preserves cursor; try/except logs + `mark_run("error")`; whole-batch retry idempotent | Tasks 4, 5, 10, 12 |
| §16 安全边界 | citation validators, default-deny permissions, system scope, session exclusions, RBAC | Tasks 6, 8, 9, 11 |
| §17.1 Unit | hashing / context / agent unit tests | Tasks 3, 4, 9 |
| §17.2 Postgres integration | migration / cursor / reader / proposal / tools integration | Tasks 2, 5, 6, 7, 8 |
| §17.3 Worker | dispatcher + flow tests | Task 10 |
| §17.4 API | proposal API integration | Task 11 |
| §17.5 Scripted-provider e2e | end-to-end acceptance | Task 12 |
| §18 Live verification | manual checklist + full-suite gates | Full verification & live check |
| §19 完成标准 | definition of done | Full verification & live check |

**No gaps:** every spec section maps to a task or the verification section. The user's
explicit coverage list (migration 0008, cursor lease, proposals + atomic apply, archival
provenance/dedupe, bounded batch reader + user-evidence rule, two constrained tools,
dedicated agent/run context, cursor success predicate, worker dispatch + daily seed,
API/RBAC/manual enqueue, config/exports, scripted-provider e2e, full/live verification)
is fully covered by Tasks 1–12 + the verification section.

### 2. Placeholder scan

Searched the plan for unfinished-marker phrases, vague implementation directives,
and cross-task shorthand, plus bare `...` inside code. **Result: no incomplete
instructions.** The
only `...` occurrences are legitimate Python type annotations (`tuple[ConsolidationMessage, ...]`,
`tuple[Any, ...]`), real error-string reprs (`AttributeError: <module 'keel_worker.main' ...>`),
or prose describing an existing line — none are missing code. Every code step is complete
and runnable; every command has an explicit expected result; no step references a symbol
that is not defined in, or consumed from, a named task. Credential-masking check: `0`
literal masked strings; the integration URL is the literal
`postgresql+psycopg://keel:keel@localhost:5432/keel_test` throughout.

**Issue found and fixed during this scan:** the integration RED/GREEN commands in Tasks 2,
5, 6, 7 (and the integration half of Task 8's GREEN) referenced `KEEL_TEST_DATABASE_URL`
only in prose ("with `KEEL_TEST_DATABASE_URL` set"). Because the `migrated_db` fixture now
fails closed, every integration command was rewritten to set the variable explicitly inline
—`$env:KEEL_TEST_DATABASE_URL="postgresql+psycopg://keel:keel@localhost:5432/keel_test"; python -m uv run pytest …`—
matching Tasks 10–12. All 15 integration test invocations now set the URL on the same line.

### 3. Type & signature consistency

Cross-checked the names/types shared across task boundaries; all align:

- `consolidation_schedule_id(scope_id: str) -> str` — defined Task 9; consumed identically
  by Task 10 (seed + dispatcher lookup), Task 11 (manual-run enqueue), and Task 12 (e2e
  seed). Centralized as a helper (no duplicated string literal); this replaced an earlier
  `CONSOLIDATION_SCHEDULE_ID` module constant, and the rename was propagated to the File
  Structure map and Tasks 9/10/11/12.
- `consolidation_session_id(scope: str, run_id: str) -> str` — defined Task 9; used in Task 10's run.
- `ConsolidationRunContext(allowed_event_ids, allowed_user_event_ids, successful_actions=0, validation_errors=0)`
  — defined Task 4; constructed in Task 10; mutated by Task 8 tools; asserted in Task 12.
- `should_advance_cursor(reason: StopReason, validation_errors: int) -> bool` — defined Task 4; called in Task 10.
- `ConsolidationCursorStore.claim/complete/fail/get` + `ConsolidationLease` / `ConsolidationCursorState`
  — defined Task 5; used in Task 10; asserted in Task 12.
- `ConsolidationBatchReader.read(after, *, limit, message_max_chars, input_max_chars) -> ConsolidationBatch(messages, user_event_ids, max_event_id, eligible_count)`
  — defined Task 6; consumed in Task 10; the event-row shape is mirrored by Task 12's `_emit`.
- `MemoryProposalStore.propose/list_proposals/get/approve/reject` → `(id, created)` /
  `ProposalResolution(outcome: ProposalOutcome, version: int | None)` — defined Task 7;
  consumed by the Task 11 API (`_resolution_response`, `_proposal_dict`); asserted in Task 12.
- `ArchivalStore.add_consolidated(content, *, source_event_ids) -> tuple[int, bool]` — defined Task 8; exercised in Task 12.
- Tool contract `ToolResult(ok=..., output=...)` and the arg schemas for
  `memory_propose_rewrite` / `archival_consolidate_insert` — defined Task 8; scripted verbatim in Task 12's `_scripted`.
- Config fields `consolidation_min_messages … consolidation_token_budget` plus `consolidation_semantic_dedupe_distance` — defined Task 1; read in Tasks 6, 8, 9, 10.

No signature drift found. The one reconciliation (constant → `consolidation_schedule_id()`
helper) was applied everywhere it is referenced.

---

## Execution

Plan complete and saved. Two execution options per the writing-plans skill:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task via
   `superpowers:subagent-driven-development`, with a two-stage review between tasks.
2. **Inline Execution** — batch execution with review checkpoints via `superpowers:executing-plans`.
