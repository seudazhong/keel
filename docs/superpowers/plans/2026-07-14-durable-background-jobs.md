# Durable Background Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a scope-bound, Postgres-backed durable background-job substrate with at-least-once arq delivery, DB leases, progress, cooperative cancellation, bounded retry, exactly-once assistant result injection, and read/cancel APIs, without registering a production job kind yet.

**Architecture:** `keel_core.jobs` owns the strict contracts plus matching in-memory and Postgres state machines; Postgres remains lifecycle source of truth while arq is only a repeatable delivery mechanism. `keel_worker.jobs` owns the allow-listed registry, `JobContext`, execution/retry orchestration, and recovery dispatcher; the server exposes only scope-bound list/detail/cancel operations. Every terminal Postgres transition and optional session injection shares one transaction and the event projector coalesces only model-facing adjacent plain assistant messages, leaving the durable event log unchanged.

**Tech Stack:** Python 3.12, Pydantic Settings, frozen dataclasses / `StrEnum`, SQLAlchemy 2 async + psycopg3, Alembic/Postgres RLS, arq 0.28 + Redis, FastAPI, OpenTelemetry, pytest/pytest-asyncio, ruff, mypy strict. No new dependency.

## Global Constraints

These constraints come from the approved design
`docs/superpowers/specs/2026-07-14-durable-background-jobs-design.md` and
ADR-0010. **Every task implicitly includes them.**

- **Postgres is authoritative.** Redis/arq delivery may be lost or duplicated; a `jobs` row is the lifecycle source of truth.
- **Delivery is at least once; handler effects are not exactly once.** Lease-token CAS prevents concurrent owners, but every future handler must make its own external/database effects idempotent by `job.id` or a business idempotency key.
- **Terminal injection is exactly once.** Every terminal transition, including queued cancellation and crash-attempt exhaustion, updates the job and appends the optional assistant event in one Postgres transaction.
- **Current scope stays singular but never implicit.** The durable scope remains `web:local`; stores are constructed with it, server state carries it, arq receives `run_job(scope_id, job_id)`, and workers reject an argument that differs from `ctx["durable_scope"]`.
- **RLS fails closed.** Every Postgres job transaction calls `set_config('app.scope_id', scope, true)` and every query also filters `scope_id`.
- **No production job kind in this slice.** `startup()` creates an empty `JobRegistry`; only tests register deterministic `test.*` handlers. Do not add a demo/no-op production kind or a public kind-registration path.
- **No generic create, retry, or inject API.** Feature services will call `enqueue_once()` with typed, validated payloads; this slice exposes only list/detail/cancel.
- **Cancellation is cooperative.** Queued jobs cancel immediately; running jobs persist `cancel_requested_at`; `JobContext.progress()` / `checkpoint()` observe it. A handler that returns before observing cancellation succeeds.
- **Handler lease discipline:** one non-interruptible handler operation must finish in less than `lease_seconds / 2`; longer handlers call `checkpoint()` at natural boundaries. The future `knowledge.ingest` handler must checkpoint after every embedding batch.
- **Retry is deterministic and bounded.** `delay = min(job_retry_base_seconds * 2 ** (attempt - 1), job_retry_max_seconds)`; no jitter.
- **Attempt ceilings are atomic.** Both queued claim and expired-running reclaim include `attempt < max_attempts` in the SQL `WHERE`, not only in dispatcher filtering.
- **Progress is a latest snapshot, not a resume cursor.** It is non-negative, `current <= total` when total exists, non-decreasing within one attempt, and reset on each successful claim.
- **Payload/result safety:** payload and result must be JSON objects; payload/result are each at most 65,536 UTF-8 JSON bytes; result messages are at most 8,000 characters; public error messages are at most 2,000 characters. Structured log/span fields never include payload, result, result message, or secrets; unknown exceptions may emit an operator traceback as required by design §12.1, while persisted public errors stay generic.
- **Typed handler boundary:** every future production handler must parse its allow-listed payload into its own Pydantic model before side effects. Never serialize/import a callable, module path, `eval` expression, or shell command as a job kind/payload execution mechanism.
- **Result injection:** use `message.token` with `role="assistant"`, `partial=false`, `job_id`, `job_kind`, and `job_status`; never start a provider run automatically.
- **Model projection only:** coalesce adjacent plain assistant messages with `"\n\n"` for provider requests; never cross a user/system/tool role or an assistant message carrying `tool_calls`; preserve durable/UI event boundaries.
- **No hard kill, DAG engine, arbitrary function import/eval, job-history table, high-frequency progress SSE, Jobs React page, generic retry endpoint, or multi-scope discovery in this slice.**
- **Database isolation is mandatory.** Every destructive/integration command sets `KEEL_TEST_DATABASE_URL` explicitly and the database name must be exactly `keel_test`; `tests/integration/conftest.py` re-checks `SELECT current_database()`. Redis smoke uses an isolated queue name and test Redis URL.
- **Command convention (Windows):** run `.\.venv\Scripts\python.exe` directly. Unit tests need no services. Integration commands set:
  ```powershell
  $env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
  $env:KEEL_EVAL_DATABASE_URL = $env:KEEL_TEST_DATABASE_URL
  $env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
  ```
- **Quality gates after every code task:** run the task’s targeted tests, then
  `.\.venv\Scripts\python.exe -m ruff check <changed paths>`,
  `.\.venv\Scripts\python.exe -m ruff format --check <changed paths>`, and the narrowest applicable
  `.\.venv\Scripts\python.exe -m mypy ...`. Task 17 reviews only its documentation diff; the final
  task runs repo-wide gates.
- **Commit convention (both trailers, every implementation commit):**
  ```bash
  git commit -m "<type>(jobs): <subject>" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
  Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
  ```

## Architecture and File Map

### Create

| File | Responsibility |
|---|---|
| `packages/keel-core/src/keel_core/jobs.py` | Strict models/errors/helpers, `JobStore` protocol, in-memory store, Postgres store, terminal finalizer and result-injection payload construction. |
| `packages/keel-worker/src/keel_worker/jobs.py` | `JobDefinition`, empty-by-default registry, `JobContext`, `run_job`, retry/deferred enqueue, `dispatch_jobs`, structured logging/span attributes. |
| `migrations/versions/0009_background_jobs.py` | `jobs` table, checks, unique dedupe key, dispatch/lease/session indexes, RLS policy. |
| `tests/unit/test_jobs.py` | Contracts, bounds, in-memory enqueue/state-machine/progress/cancel/finalizer tests. |
| `tests/unit/test_worker_jobs.py` | Registry, context, run/retry/cancel/scope/dispatcher orchestration tests with injected handlers/stores. |
| `tests/integration/test_jobs_postgres.py` | Migration/RLS, Postgres dedupe/read/list, claim/reclaim, progress/heartbeat, cancellation, retry, atomic finalizers and exactly-once injection. |
| `tests/integration/test_jobs_api.py` | List/detail/cancel API filters, visibility and RBAC. |
| `tests/integration/test_jobs_worker.py` | Real Postgres + Redis/arq acceptance for lost/duplicate delivery, retry, crash reclaim, cancellation and injection. |

### Modify

| File | Change |
|---|---|
| `packages/keel-core/src/keel_core/config.py` | Add eight validated `job_*` settings from the spec. |
| `packages/keel-core/src/keel_core/state.py` | Extract reusable `append_event_in_transaction()`; add in-memory session-existence seam; keep `PostgresEventStore.append()` behavior unchanged. |
| `packages/keel-core/src/keel_core/projections.py` | Coalesce model-facing adjacent plain assistant messages only. |
| `packages/keel-core/src/keel_core/api.py` | Add strict job read DTO. |
| `packages/keel-worker/src/keel_worker/main.py` | Construct scope-bound job store + empty registry, forward enqueue keyword options, register `run_job` and dispatcher cron. |
| `packages/keel-server/src/keel_server/app.py` | Construct scope-bound job store and forward arq keyword options. |
| `packages/keel-server/src/keel_server/api/v1.py` | Add list/detail/cancel routes and explicit store/scope validation. |
| `tests/integration/conftest.py` | Truncate `jobs` before sessions/events and preserve the exact-`keel_test` guard. |
| `tests/unit/test_config.py` | Job setting defaults, overrides and invalid values. |
| `tests/unit/test_projections.py` | Adjacent-assistant and tool-boundary/provider-request compatibility tests. |
| `tests/unit/test_worker_tasks.py` | WorkerSettings function/cron registration and empty production registry checks. |
| `tests/unit/test_server.py` | App job-store factory and enqueue-option forwarding tests. |
| `tests/integration/test_state_postgres.py` | In-transaction append, existing-session and rollback tests. |
| `docs/STATUS.md` | Mark durable background jobs complete and make RAG/KB the next slice. |

### Deliberately unchanged

- `docs/adr/0010-durable-background-jobs.md` is already accepted and needs no implementation edit.
- `keel-scheduler` retains ADR-0006 at-most-once schedule triggering; only rows already accepted into `jobs` use ADR-0010 semantics.
- No web UI file changes and no dependency/lockfile changes.

## Authoritative Cross-Task Interfaces

The following names and signatures are authoritative for every task. A later task must not
rename fields, invert booleans, or introduce a second representation.

```python
# packages/keel-core/src/keel_core/jobs.py
class JobStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


@dataclass(frozen=True)
class JobLimits:
    payload_max_bytes: int = 65_536
    result_max_bytes: int = 65_536
    result_message_max_chars: int = 8_000
    error_message_max_chars: int = 2_000

    @classmethod
    def from_settings(cls, settings: Settings) -> JobLimits: ...
    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]: ...
    def validate_result(self, result: dict[str, Any]) -> dict[str, Any]: ...
    def result_message(self, value: str) -> str: ...
    def error_message(self, value: str) -> str: ...


@dataclass(frozen=True)
class JobRecord:
    id: str
    scope_id: str
    kind: str
    status: JobStatus
    payload: dict[str, Any]
    target_session_id: str | None
    idempotency_key: str
    attempt: int
    max_attempts: int
    next_attempt_at: datetime
    lease_token: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    cancel_requested_at: datetime | None
    progress_current: int
    progress_total: int | None
    progress_message: str | None
    progress_updated_at: datetime | None
    result: dict[str, Any] | None
    result_message: str | None
    error_kind: str | None
    error_message: str | None
    injected_event_seq: int | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class JobLease:
    job_id: str
    scope_id: str
    token: str
    kind: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    lease_seconds: int


@dataclass(frozen=True)
class JobResult:
    data: dict[str, Any]
    message: str


@dataclass(frozen=True)
class JobError:
    kind: str
    message: str


@dataclass(frozen=True)
class JobProgressResult:
    record: JobRecord
    cancel_requested: bool


class RetryableJobError(Exception):
    code: str
    public_message: str


class PermanentJobError(Exception):
    code: str
    public_message: str


class JobValidationError(Exception):
    code: str
    public_message: str


class JobLeaseLostError(Exception): ...
class JobCancellationRequested(Exception): ...


class JobStore(Protocol):
    @property
    def scope_id(self) -> str: ...

    async def enqueue_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]: ...

    async def get(self, job_id: str) -> JobRecord | None: ...

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> list[JobRecord]: ...

    async def dispatchable(self, now: datetime, limit: int) -> list[str]: ...
    async def exhausted(self, now: datetime, limit: int) -> list[str]: ...

    async def claim(
        self, job_id: str, now: datetime, lease_seconds: int
    ) -> JobLease | None: ...

    async def heartbeat(self, lease: JobLease, now: datetime) -> bool:
        """Refresh the lease; return whether cancellation is requested."""
        ...

    async def progress(
        self,
        lease: JobLease,
        *,
        current: int,
        total: int | None,
        message: str | None,
        now: datetime,
    ) -> JobProgressResult: ...

    async def request_cancel(self, job_id: str, now: datetime) -> JobRecord | None: ...

    async def requeue(
        self, lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord: ...

    async def succeed(
        self, lease: JobLease, result: JobResult, now: datetime
    ) -> JobRecord: ...

    async def fail_terminal(
        self, lease: JobLease, error: JobError, now: datetime
    ) -> JobRecord: ...

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord: ...
    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...


def retry_delay_seconds(attempt: int, base_seconds: int, max_seconds: int) -> int: ...
```

```python
# packages/keel-core/src/keel_core/state.py
async def append_event_in_transaction(
    conn: AsyncConnection,
    event: Event,
    *,
    require_existing_session: bool = False,
) -> int: ...
```

```python
# packages/keel-worker/src/keel_worker/jobs.py
JobHandler = Callable[["JobContext", dict[str, Any]], Awaitable[JobResult]]
JobClock = Callable[[], datetime]
EnqueueJob = Callable[..., Awaitable[None]]


@dataclass(frozen=True)
class JobDefinition:
    kind: str
    handler: JobHandler
    max_attempts: int = 3
    lease_seconds: int = 300


class JobRegistry:
    def register(self, definition: JobDefinition) -> None: ...
    def get(self, kind: str) -> JobDefinition | None: ...
    def kinds(self) -> tuple[str, ...]: ...


class JobContext:
    job_id: str
    scope_id: str
    attempt: int

    async def progress(
        self,
        current: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None: ...

    async def checkpoint(self) -> None: ...


async def run_job(
    ctx: dict[str, Any], scope_id: str, job_id: str
) -> str: ...


async def dispatch_jobs(ctx: dict[str, Any]) -> int: ...
```

The worker context keys are also fixed:

```python
ctx["jobs"]          # JobStore, bound to ctx["durable_scope"]
ctx["job_registry"]  # JobRegistry; empty in production for this slice
ctx["durable_scope"] # "web:local"
ctx["enqueue"]       # async (name, *args, **options), forwards arq options
ctx["job_settings"]  # Settings; retry, dispatcher, fallback lease, and size limits
ctx["job_clock"]     # optional deterministic JobClock used by tests
```

## Dependency Order

| Task | Depends on | Reviewable deliverable |
|---|---|---|
| 1 | — | Frozen contracts/errors/settings/bounds |
| 2 | 1 | Migration 0009 + isolated DB/RLS proof |
| 3 | — | Reusable transactional event append |
| 4 | 1, 3 | In-memory enqueue/read/dispatch selection |
| 5 | 4 | In-memory lease/heartbeat/progress |
| 6 | 5 | In-memory cancel/retry/terminal injection |
| 7 | 1, 2 | Postgres enqueue/read/list/selection |
| 8 | 7 | Postgres claim/reclaim/heartbeat/progress/requeue |
| 9 | 3, 8 | Postgres cancellation/finalizers/exact injection |
| 10 | 3 | Model-facing assistant coalescing |
| 11 | 1, 6, 9 | Registry + `JobContext` |
| 12 | 11 | Core `run_job` success/permanent/cancel/scope paths |
| 13 | 12 | Retry/deferred enqueue + dispatcher/exhaustion |
| 14 | 13 | Worker/server construction and arq wiring |
| 15 | 9, 14 | Server DTO/routes/RBAC |
| 16 | 10, 13, 15 | Postgres+Redis/arq acceptance |
| 17 | 16 | Status documentation |
| 18 | all | Whole-branch gates + live isolated smoke |

---

## Task 1: Strict job contracts, public errors, limits, settings, and retry math

**Files:**
- Create: `packages/keel-core/src/keel_core/jobs.py`
- Modify: `packages/keel-core/src/keel_core/config.py`
- Test: `tests/unit/test_jobs.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces every `keel_core.jobs` model/error/protocol/helper in the authoritative interface block.
- Produces validated `Settings` fields:
  `job_lease_seconds=300`, `job_dispatch_limit=100`,
  `job_retry_base_seconds=5`, `job_retry_max_seconds=300`,
  `job_payload_max_bytes=65_536`, `job_result_max_bytes=65_536`,
  `job_result_message_max_chars=8_000`, `job_error_message_max_chars=2_000`.
- `max_attempts` remains `JobDefinition` policy; do **not** add `job_max_attempts` to `Settings`.
- Consumes only stdlib, `pydantic.Field`, current `Settings`, and the approved public exception
  contract; no new dependency or alternate error hierarchy.

- [ ] **Step 1: Write RED contract and bounds tests** — create
  `tests/unit/test_jobs.py` with these initial tests:

```python
"""Strict durable-job contracts, limits and deterministic retry math."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from keel_core.config import Settings
from keel_core.jobs import (
    JobError,
    JobLease,
    JobLimits,
    JobResult,
    JobStatus,
    JobValidationError,
    PermanentJobError,
    RetryableJobError,
    retry_delay_seconds,
)


def test_job_contracts_are_frozen_and_typed() -> None:
    lease = JobLease(
        job_id="job_1",
        scope_id="web:local",
        token="lease_1",
        kind="test.echo",
        payload={"value": 1},
        attempt=1,
        max_attempts=3,
        lease_seconds=300,
    )
    assert lease.attempt == 1
    with pytest.raises(FrozenInstanceError):
        lease.attempt = 2  # type: ignore[misc]
    assert JobStatus("succeeded") is JobStatus.succeeded
    with pytest.raises(ValueError):
        JobStatus("done")


def test_public_handler_errors_keep_code_and_safe_message() -> None:
    retryable = RetryableJobError("provider_timeout", "Provider timed out.")
    permanent = PermanentJobError("invalid_document", "Document is invalid.")
    assert (retryable.code, retryable.public_message) == (
        "provider_timeout",
        "Provider timed out.",
    )
    assert (permanent.code, permanent.public_message) == (
        "invalid_document",
        "Document is invalid.",
    )
    assert str(retryable) == "Provider timed out."
    assert str(JobValidationError("invalid_payload", "Payload is invalid.")) == (
        "invalid_payload: Payload is invalid."
    )
    with pytest.raises(ValueError, match="code"):
        RetryableJobError("", "safe")
    with pytest.raises(ValueError, match="public_message"):
        PermanentJobError("bad", " ")


def test_limits_validate_json_bytes_and_clip_public_text() -> None:
    limits = JobLimits(
        payload_max_bytes=12,
        result_max_bytes=12,
        result_message_max_chars=5,
        error_message_max_chars=4,
    )
    assert limits.validate_payload({"a": 1}) == {"a": 1}
    assert limits.validate_result({"a": 1}) == {"a": 1}
    with pytest.raises(JobValidationError, match="payload_too_large"):
        limits.validate_payload({"long": "value"})
    with pytest.raises(JobValidationError, match="result_too_large"):
        limits.validate_result({"long": "value"})
    with pytest.raises(JobValidationError, match="json_object_required"):
        limits.validate_payload(["not", "an", "object"])  # type: ignore[arg-type]
    with pytest.raises(JobValidationError, match="json_serializable"):
        limits.validate_result({"bad": object()})
    with pytest.raises(JobValidationError, match="json_serializable"):
        limits.validate_payload({"bad": float("nan")})
    assert limits.result_message("123456") == "12345"
    assert limits.error_message("12345") == "1234"


def test_job_limits_are_built_from_settings() -> None:
    settings = Settings(
        job_payload_max_bytes=101,
        job_result_max_bytes=102,
        job_result_message_max_chars=103,
        job_error_message_max_chars=104,
    )
    assert JobLimits.from_settings(settings) == JobLimits(
        payload_max_bytes=101,
        result_max_bytes=102,
        result_message_max_chars=103,
        error_message_max_chars=104,
    )


def test_result_and_error_models_reject_empty_required_text() -> None:
    assert JobResult(data={"ok": True}, message="done").message == "done"
    assert JobError(kind="provider_timeout", message="safe").kind == "provider_timeout"
    with pytest.raises(ValueError, match="message"):
        JobResult(data={}, message="")
    with pytest.raises(ValueError, match="kind"):
        JobError(kind="", message="safe")


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 5), (2, 10), (3, 20), (7, 300)],
)
def test_retry_delay_is_deterministic_and_capped(attempt: int, expected: int) -> None:
    assert retry_delay_seconds(attempt, 5, 300) == expected


def test_retry_delay_rejects_non_positive_inputs() -> None:
    with pytest.raises(ValueError):
        retry_delay_seconds(0, 5, 300)
    with pytest.raises(ValueError):
        retry_delay_seconds(1, 0, 300)
```

- [ ] **Step 2: Extend config RED tests** — append to `tests/unit/test_config.py`:

```python
def test_job_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.job_lease_seconds == 300
    assert settings.job_dispatch_limit == 100
    assert settings.job_retry_base_seconds == 5
    assert settings.job_retry_max_seconds == 300
    assert settings.job_payload_max_bytes == 65_536
    assert settings.job_result_max_bytes == 65_536
    assert settings.job_result_message_max_chars == 8_000
    assert settings.job_error_message_max_chars == 2_000


def test_job_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_JOB_LEASE_SECONDS", "45")
    monkeypatch.setenv("KEEL_JOB_DISPATCH_LIMIT", "17")
    monkeypatch.setenv("KEEL_JOB_RETRY_BASE_SECONDS", "2")

    from keel_core.config import Settings

    settings = Settings()
    assert settings.job_lease_seconds == 45
    assert settings.job_dispatch_limit == 17
    assert settings.job_retry_base_seconds == 2


def test_job_settings_reject_non_positive_values() -> None:
    from pydantic import ValidationError

    from keel_core.config import Settings

    with pytest.raises(ValidationError):
        Settings(job_lease_seconds=0)
    with pytest.raises(ValidationError):
        Settings(job_result_max_bytes=-1)
```

- [ ] **Step 3: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py tests\unit\test_config.py -v
```

Expected: collection fails with `ModuleNotFoundError: No module named 'keel_core.jobs'`; after
creating an empty module, config tests fail because `Settings` lacks `job_lease_seconds`.

- [ ] **Step 4: Add validated settings** — import `Field` from Pydantic and add this block to
  `Settings` after consolidation settings:

```python
    # Durable background jobs (ADR-0010).
    job_lease_seconds: int = Field(default=300, gt=0)
    job_dispatch_limit: int = Field(default=100, gt=0)
    job_retry_base_seconds: int = Field(default=5, gt=0)
    job_retry_max_seconds: int = Field(default=300, gt=0)
    job_payload_max_bytes: int = Field(default=65_536, gt=0)
    job_result_max_bytes: int = Field(default=65_536, gt=0)
    job_result_message_max_chars: int = Field(default=8_000, gt=0)
    job_error_message_max_chars: int = Field(default=2_000, gt=0)
```

Use `from pydantic import Field`; do not add a client-controlled attempt setting.

- [ ] **Step 5: Implement the strict core contract** — create
  `packages/keel-core/src/keel_core/jobs.py` with:

```python
"""Durable background-job contracts and stores (ADR-0010)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from keel_core.config import Settings


class JobStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class _PublicJobException(Exception):
    def __init__(self, code: str, public_message: str) -> None:
        code = code.strip()
        public_message = public_message.strip()
        if not code:
            raise ValueError("code must not be empty")
        if not public_message:
            raise ValueError("public_message must not be empty")
        self.code = code
        self.public_message = public_message
        super().__init__(public_message)


class RetryableJobError(_PublicJobException):
    """A handler failure that may be retried while attempts remain."""


class PermanentJobError(_PublicJobException):
    """A handler failure that must become terminal immediately."""


class JobValidationError(_PublicJobException):
    """A bounded public validation failure raised by the framework/store."""

    def __str__(self) -> str:
        return f"{self.code}: {self.public_message}"


class JobLeaseLostError(Exception):
    """The operation did not hold the current running lease token."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"job lease lost: {job_id}")


class JobCancellationRequested(Exception):
    """Internal cooperative-cancellation signal raised at a checkpoint."""


def _validated_json_object(
    value: dict[str, Any], *, field: str, max_bytes: int
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise JobValidationError("json_object_required", f"{field} must be a JSON object")
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise JobValidationError(
            "json_serializable", f"{field} must contain only JSON-serializable values"
        ) from exc
    if len(encoded) > max_bytes:
        raise JobValidationError(
            f"{field}_too_large", f"{field} exceeds {max_bytes} UTF-8 JSON bytes"
        )
    return value


@dataclass(frozen=True)
class JobLimits:
    payload_max_bytes: int = 65_536
    result_max_bytes: int = 65_536
    result_message_max_chars: int = 8_000
    error_message_max_chars: int = 2_000

    def __post_init__(self) -> None:
        if min(
            self.payload_max_bytes,
            self.result_max_bytes,
            self.result_message_max_chars,
            self.error_message_max_chars,
        ) <= 0:
            raise ValueError("job limits must be positive")

    @classmethod
    def from_settings(cls, settings: Settings) -> JobLimits:
        return cls(
            payload_max_bytes=settings.job_payload_max_bytes,
            result_max_bytes=settings.job_result_max_bytes,
            result_message_max_chars=settings.job_result_message_max_chars,
            error_message_max_chars=settings.job_error_message_max_chars,
        )

    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _validated_json_object(
            payload, field="payload", max_bytes=self.payload_max_bytes
        )

    def validate_result(self, result: dict[str, Any]) -> dict[str, Any]:
        return _validated_json_object(
            result, field="result", max_bytes=self.result_max_bytes
        )

    def result_message(self, value: str) -> str:
        return value[: self.result_message_max_chars]

    def error_message(self, value: str) -> str:
        return value[: self.error_message_max_chars]


@dataclass(frozen=True)
class JobRecord:
    id: str
    scope_id: str
    kind: str
    status: JobStatus
    payload: dict[str, Any]
    target_session_id: str | None
    idempotency_key: str
    attempt: int
    max_attempts: int
    next_attempt_at: datetime
    lease_token: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    cancel_requested_at: datetime | None
    progress_current: int
    progress_total: int | None
    progress_message: str | None
    progress_updated_at: datetime | None
    result: dict[str, Any] | None
    result_message: str | None
    error_kind: str | None
    error_message: str | None
    injected_event_seq: int | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class JobLease:
    job_id: str
    scope_id: str
    token: str
    kind: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    lease_seconds: int


@dataclass(frozen=True)
class JobResult:
    data: dict[str, Any]
    message: str

    def __post_init__(self) -> None:
        if not self.message.strip():
            raise ValueError("JobResult.message must not be empty")


@dataclass(frozen=True)
class JobError:
    kind: str
    message: str

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("JobError.kind must not be empty")
        if not self.message.strip():
            raise ValueError("JobError.message must not be empty")


@dataclass(frozen=True)
class JobProgressResult:
    record: JobRecord
    cancel_requested: bool


class JobStore(Protocol):
    @property
    def scope_id(self) -> str: ...

    async def enqueue_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]: ...

    async def get(self, job_id: str) -> JobRecord | None: ...

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> list[JobRecord]: ...

    async def dispatchable(self, now: datetime, limit: int) -> list[str]: ...
    async def exhausted(self, now: datetime, limit: int) -> list[str]: ...

    async def claim(
        self, job_id: str, now: datetime, lease_seconds: int
    ) -> JobLease | None: ...

    async def heartbeat(self, lease: JobLease, now: datetime) -> bool: ...

    async def progress(
        self,
        lease: JobLease,
        *,
        current: int,
        total: int | None,
        message: str | None,
        now: datetime,
    ) -> JobProgressResult: ...

    async def request_cancel(self, job_id: str, now: datetime) -> JobRecord | None: ...

    async def requeue(
        self, lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord: ...

    async def succeed(
        self, lease: JobLease, result: JobResult, now: datetime
    ) -> JobRecord: ...

    async def fail_terminal(
        self, lease: JobLease, error: JobError, now: datetime
    ) -> JobRecord: ...

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord: ...
    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None: ...


def retry_delay_seconds(attempt: int, base_seconds: int, max_seconds: int) -> int:
    if attempt < 1 or base_seconds < 1 or max_seconds < 1:
        raise ValueError("attempt, base_seconds and max_seconds must be positive")
    return min(base_seconds * 2 ** (attempt - 1), max_seconds)
```

Leave store implementations for Tasks 4–9; do not add stub production behavior beyond the
protocol.

- [ ] **Step 6: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py tests\unit\test_config.py -v
```

Expected: all tests in both files pass.

- [ ] **Step 7: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\jobs.py packages\keel-core\src\keel_core\config.py tests\unit\test_jobs.py tests\unit\test_config.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\jobs.py packages\keel-core\src\keel_core\config.py tests\unit\test_jobs.py tests\unit\test_config.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: all three commands exit `0`.

- [ ] **Step 8: Commit**

```bash
git add packages/keel-core/src/keel_core/jobs.py packages/keel-core/src/keel_core/config.py tests/unit/test_jobs.py tests/unit/test_config.py
git commit -m "feat(jobs): add strict job contracts and settings" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 2: Migration 0009, isolated DB fixture, schema checks, and jobs RLS

**Files:**
- Create: `migrations/versions/0009_background_jobs.py`
- Modify: `tests/integration/conftest.py`
- Create: `tests/integration/test_jobs_postgres.py`
- Verify unchanged guard: `tests/integration/test_db_guard.py`

**Interfaces:**
- Produces the exact `jobs` table/checks/indexes/RLS policy in design §6.
- Revision is `0009_background_jobs`; down revision is `0008_memory_consolidation`.
- The fixture truncates `jobs` before `events`/`sessions`.
- Consumes the existing exact-`keel_test` URL guard and `SELECT current_database()` re-check; do not add a fallback to `KEEL_DATABASE_URL`.

- [ ] **Step 1: Write RED migration/schema tests** — create
  `tests/integration/test_jobs_postgres.py` with:

```python
"""Postgres durable-job state machine and exactly-once injection."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


async def _columns(engine: AsyncEngine, table: str) -> dict[str, tuple[str, str]]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns WHERE table_name = :table"
                ),
                {"table": table},
            )
        ).all()
    return {
        str(row.column_name): (str(row.data_type), str(row.is_nullable))
        for row in rows
    }


async def test_jobs_migration_has_required_columns_checks_and_indexes(
    migrated_db: AsyncEngine,
) -> None:
    columns = await _columns(migrated_db, "jobs")
    assert columns["id"] == ("text", "NO")
    assert columns["scope_id"] == ("text", "NO")
    assert columns["payload"] == ("jsonb", "NO")
    assert columns["attempt"] == ("integer", "NO")
    assert columns["lease_expires_at"] == ("timestamp with time zone", "YES")
    assert columns["progress_current"] == ("bigint", "NO")
    assert columns["injected_event_seq"] == ("bigint", "YES")

    async with migrated_db.connect() as conn:
        checks = "\n".join(
            str(value)
            for value in (
                await conn.execute(
                    text(
                        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                        "WHERE conrelid = 'jobs'::regclass"
                    )
                )
            ).scalars()
        )
        indexes = {
            str(value)
            for value in (
                await conn.execute(
                    text("SELECT indexname FROM pg_indexes WHERE tablename = 'jobs'")
                )
            ).scalars()
        }
    assert "queued" in checks and "cancelled" in checks
    assert "attempt >= 0" in checks
    assert "max_attempts >= 1" in checks
    assert {
        "jobs_pkey",
        "jobs_scope_id_kind_idempotency_key_key",
        "ix_jobs_dispatch",
        "ix_jobs_lease_expiry",
        "ix_jobs_target_session",
    } <= indexes


async def test_jobs_rls_is_enabled_and_fails_closed(migrated_db: AsyncEngine) -> None:
    role = f"jobs_rls_{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:
        await conn.execute(text(f'CREATE ROLE "{role}" NOSUPERUSER'))
        await conn.execute(text(f'GRANT SELECT ON jobs TO "{role}"'))
        await conn.execute(
            text(
                "INSERT INTO jobs "
                "(id, scope_id, kind, idempotency_key, max_attempts) VALUES "
                "('job_a', 'scope:a', 'test.echo', 'a', 3), "
                "('job_b', 'scope:b', 'test.echo', 'b', 3)"
            )
        )
    try:
        async with migrated_db.connect() as conn:
            await conn.execute(text(f'SET ROLE "{role}"'))
            await conn.execute(text("SET app.scope_id = 'scope:a'"))
            assert (await conn.execute(text("SELECT id FROM jobs"))).scalars().all() == [
                "job_a"
            ]
            await conn.execute(text("RESET app.scope_id"))
            assert (await conn.execute(text("SELECT id FROM jobs"))).scalars().all() == []
            await conn.execute(text("RESET ROLE"))
    finally:
        async with migrated_db.begin() as conn:
            await conn.execute(
                text(f'REVOKE ALL PRIVILEGES ON TABLE jobs FROM "{role}"')
            )
            await conn.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
```

- [ ] **Step 2: Run the DB guard before any migration test**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_db_guard.py -v
```

Expected: all guard tests pass, including refusal of missing/malformed/live `keel` URLs.

- [ ] **Step 3: Run migration RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_postgres.py::test_jobs_migration_has_required_columns_checks_and_indexes -v
```

Expected: FAIL because relation `jobs` does not exist / `columns["id"]` is missing.

- [ ] **Step 4: Create migration 0009** — use the exact schema:

```python
"""durable background jobs.

Revision ID: 0009_background_jobs
Revises: 0008_memory_consolidation
Create Date: 2026-07-14
"""

from __future__ import annotations

from alembic import op

revision = "0009_background_jobs"
down_revision = "0008_memory_consolidation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE jobs (
            id text PRIMARY KEY,
            scope_id text NOT NULL,
            kind text NOT NULL,
            status text NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            target_session_id text,
            idempotency_key text NOT NULL,
            attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
            max_attempts integer NOT NULL CHECK (max_attempts >= 1),
            next_attempt_at timestamptz NOT NULL DEFAULT now(),
            lease_token text,
            lease_expires_at timestamptz,
            heartbeat_at timestamptz,
            cancel_requested_at timestamptz,
            progress_current bigint NOT NULL DEFAULT 0 CHECK (progress_current >= 0),
            progress_total bigint CHECK (progress_total IS NULL OR progress_total >= 0),
            progress_message text,
            progress_updated_at timestamptz,
            result jsonb,
            result_message text,
            error_kind text,
            error_message text,
            injected_event_seq bigint,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz,
            finished_at timestamptz,
            UNIQUE (scope_id, kind, idempotency_key)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_jobs_dispatch ON jobs (scope_id, status, next_attempt_at)"
    )
    op.execute(
        "CREATE INDEX ix_jobs_lease_expiry ON jobs (scope_id, lease_expires_at) "
        "WHERE status = 'running'"
    )
    op.execute(
        "CREATE INDEX ix_jobs_target_session ON jobs (scope_id, target_session_id) "
        "WHERE target_session_id IS NOT NULL"
    )
    op.execute("ALTER TABLE jobs ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY scope_isolation ON jobs "
        "USING (scope_id = current_setting('app.scope_id', true)) "
        "WITH CHECK (scope_id = current_setting('app.scope_id', true))"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS jobs")
```

- [ ] **Step 5: Update destructive fixture ordering** — change the `TRUNCATE` text in
  `tests/integration/conftest.py` to:

```python
                "TRUNCATE jobs, consolidation_cursors, memory_proposals, "
                "message_embeddings, events, sessions, memory_blocks, "
                "memory_block_versions, connector_tokens, archival, schedules, approvals"
```

Keep `_require_test_database_url()` and the actual-database re-check unchanged.

- [ ] **Step 6: Run GREEN**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_db_guard.py tests\integration\test_jobs_postgres.py -v
.\.venv\Scripts\python.exe -m alembic heads
```

Expected: tests pass; `alembic heads` prints exactly `0009_background_jobs (head)`.

- [ ] **Step 7: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check migrations\versions\0009_background_jobs.py tests\integration\conftest.py tests\integration\test_jobs_postgres.py
.\.venv\Scripts\python.exe -m ruff format --check migrations\versions\0009_background_jobs.py tests\integration\conftest.py tests\integration\test_jobs_postgres.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 8: Commit**

```bash
git add migrations/versions/0009_background_jobs.py tests/integration/conftest.py tests/integration/test_jobs_postgres.py
git commit -m "feat(jobs): add migration 0009 with scoped RLS" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 3: Reusable in-transaction event append and existing-session guard

**Files:**
- Modify: `packages/keel-core/src/keel_core/state.py`
- Modify: `tests/integration/test_state_postgres.py`

**Interfaces:**
- Produces `append_event_in_transaction(conn, event, *, require_existing_session=False) -> int`.
- `PostgresEventStore.append()` remains scope-checked and transaction-owning, but delegates sequence allocation + insert to the helper.
- `require_existing_session=True` updates only an existing `(id, scope_id)` session and raises `LookupError` without creating one.
- Produces `InMemoryEventStore.has_session(session_id, scope_id) -> bool`.
- Consumed by both in-memory target validation and Postgres terminal finalization.

- [ ] **Step 1: Write RED transactional tests** — append to
  `tests/integration/test_state_postgres.py`:

```python
from sqlalchemy import text

from keel_core.state import append_event_in_transaction


async def test_append_event_in_outer_transaction_rolls_back(
    migrated_db: AsyncEngine,
) -> None:
    session_id = f"s-{uuid.uuid4().hex}"
    event = _msg(session_id, "A", "rolled back")

    with pytest.raises(RuntimeError, match="force rollback"):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SELECT set_config('app.scope_id', 'A', true)"))
            assert await append_event_in_transaction(conn, event) == 1
            raise RuntimeError("force rollback")

    assert [row async for row in PostgresEventStore(migrated_db, "A").read(session_id)] == []


async def test_append_event_can_require_an_existing_same_scope_session(
    migrated_db: AsyncEngine,
) -> None:
    missing = _msg(f"missing-{uuid.uuid4().hex}", "A", "no implicit session")
    with pytest.raises(LookupError, match="session"):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SELECT set_config('app.scope_id', 'A', true)"))
            await append_event_in_transaction(
                conn, missing, require_existing_session=True
            )

    session_id = f"s-{uuid.uuid4().hex}"
    await PostgresEventStore(migrated_db, "A").append(_msg(session_id, "A", "seed"))
    event = _msg(session_id, "A", "injected")
    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', 'A', true)"))
        assert (
            await append_event_in_transaction(
                conn, event, require_existing_session=True
            )
            == 2
        )
    assert [e.seq async for e in PostgresEventStore(migrated_db, "A").read(session_id)] == [
        1,
        2,
    ]
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_state_postgres.py::test_append_event_in_outer_transaction_rolls_back tests\integration\test_state_postgres.py::test_append_event_can_require_an_existing_same_scope_session -v
```

Expected: collection fails because `append_event_in_transaction` is not exported.

- [ ] **Step 3: Extract the helper** — import `AsyncConnection`, add
  `InMemoryEventStore.has_session`, and move the current sequence allocation + event insert into:

```python
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class InMemoryEventStore:
    # existing methods stay unchanged

    def has_session(self, session_id: SessionId, scope_id: ScopeId) -> bool:
        return any(
            event.scope_id == scope_id for event in self._events.get(session_id, [])
        )


async def append_event_in_transaction(
    conn: AsyncConnection,
    event: Event,
    *,
    require_existing_session: bool = False,
) -> int:
    """Allocate a session sequence and append an event inside the caller's transaction."""
    params = {"sid": event.session_id, "scope": event.scope_id}
    if require_existing_session:
        row = (
            await conn.execute(
                text(
                    "UPDATE sessions SET next_seq = next_seq + 1, updated_at = now() "
                    "WHERE id = :sid AND scope_id = :scope "
                    "RETURNING next_seq - 1 AS seq"
                ),
                params,
            )
        ).one_or_none()
        if row is None:
            raise LookupError(
                f"session {event.session_id!r} does not exist in scope {event.scope_id!r}"
            )
    else:
        row = (
            await conn.execute(
                text(
                    "INSERT INTO sessions (id, scope_id, next_seq) "
                    "VALUES (:sid, :scope, 2) "
                    "ON CONFLICT (id) DO UPDATE "
                    "SET next_seq = sessions.next_seq + 1, updated_at = now() "
                    "RETURNING next_seq - 1 AS seq"
                ),
                params,
            )
        ).one()
    seq = int(row.seq)
    event.seq = seq
    await conn.execute(
        text(
            "INSERT INTO events "
            "(session_id, scope_id, seq, type, version, run_id, ts, payload) "
            "VALUES (:sid, :scope, :seq, :type, :version, :run_id, :ts, "
            "CAST(:payload AS jsonb))"
        ),
        {
            **params,
            "seq": seq,
            "type": str(event.type),
            "version": event.version,
            "run_id": event.run_id,
            "ts": event.ts,
            "payload": json.dumps(event.payload, default=str),
        },
    )
    return seq
```

Refactor `PostgresEventStore.append()` to retain the existing cross-scope check and:

```python
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await append_event_in_transaction(conn, event)
```

Do not set the scope GUC inside the helper; the transaction owner must do that explicitly.

- [ ] **Step 4: Run GREEN and regression tests**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_state_postgres.py -v
.\.venv\Scripts\python.exe -m pytest tests\unit\test_loop.py tests\unit\test_loop_memory.py tests\unit\test_projections.py -v
```

Expected: all selected tests pass; existing append/read/resume behavior is unchanged.

- [ ] **Step 5: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\state.py tests\integration\test_state_postgres.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\state.py tests\integration\test_state_postgres.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/state.py tests/integration/test_state_postgres.py
git commit -m "refactor(jobs): expose transactional event append" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 4: In-memory enqueue dedupe, target validation, reads, filters, and recovery selection

**Files:**
- Modify: `packages/keel-core/src/keel_core/jobs.py`
- Extend: `tests/unit/test_jobs.py`

**Interfaces:**
- Produces `InMemoryJobStore(scope_id, *, events=None, limits=None)`.
- `events` is an optional `InMemoryEventStore`; a non-null target session is accepted only
  when that event store already has an event for the same `(session_id, scope_id)`.
- `enqueue_once()` is atomic under an `asyncio.Lock` and dedupes on
  `(scope_id, kind, idempotency_key)`; the first request wins even if a retry supplies
  a non-serializable/oversized payload or a different/missing target. Normalize and validate
  the dedupe identity first, but return an existing row before validating retry-only
  payload/target fields.
- `list()` is newest-first and validates `1 <= limit <= 100`.
- `dispatchable()` returns due queued jobs and expired-running jobs with attempts left;
  `exhausted()` returns expired-running jobs at the ceiling. Running-row cases become
  executable after Task 5.

- [ ] **Step 1: Write RED in-memory enqueue/read tests** — extend imports in
  `tests/unit/test_jobs.py` and append:

```python
from datetime import UTC, datetime, timedelta

from keel_core.events import Event, EventType
from keel_core.jobs import InMemoryJobStore, JobStatus
from keel_core.state import InMemoryEventStore

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


async def _session(events: InMemoryEventStore, session_id: str, scope: str) -> None:
    await events.append(
        Event(
            type=EventType.message_token,
            seq=0,
            session_id=session_id,
            scope_id=scope,
            ts=_NOW,
            payload={"role": "user", "text": "seed"},
        )
    )


async def test_in_memory_enqueue_once_dedupes_and_first_request_wins() -> None:
    store = InMemoryJobStore("web:local")
    first, created = await store.enqueue_once(
        kind="test.echo",
        payload={"value": 1},
        target_session_id=None,
        idempotency_key="request-1",
        max_attempts=3,
        now=_NOW,
    )
    duplicate, duplicate_created = await store.enqueue_once(
        kind="test.echo",
        payload={"bad": object()},
        target_session_id="missing-on-retry",
        idempotency_key="request-1",
        max_attempts=1,
        now=_NOW + timedelta(seconds=1),
    )

    assert created is True
    assert duplicate_created is False
    assert duplicate.id == first.id
    assert duplicate.payload == {"value": 1}
    assert duplicate.max_attempts == 3
    assert first.status is JobStatus.queued
    assert first.attempt == 0
    assert first.next_attempt_at == _NOW


@pytest.mark.parametrize(
    ("kind", "key", "max_attempts", "error_code"),
    [
        (" ", "request", 3, "invalid_kind"),
        ("test.echo", " ", 3, "invalid_idempotency_key"),
        ("test.echo", "request", 0, "invalid_max_attempts"),
    ],
)
async def test_in_memory_enqueue_rejects_invalid_identity_and_attempt_policy(
    kind: str,
    key: str,
    max_attempts: int,
    error_code: str,
) -> None:
    store = InMemoryJobStore("web:local")
    with pytest.raises(JobValidationError, match=error_code):
        await store.enqueue_once(
            kind=kind,
            payload={},
            target_session_id=None,
            idempotency_key=key,
            max_attempts=max_attempts,
            now=_NOW,
        )


async def test_in_memory_enqueue_validates_target_session_and_scope() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)

    accepted, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="accepted",
        max_attempts=3,
        now=_NOW,
    )
    assert accepted.target_session_id == "target"

    with pytest.raises(JobValidationError, match="target_session_not_found"):
        await store.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id="missing",
            idempotency_key="missing",
            max_attempts=3,
            now=_NOW,
        )

    other_scope = InMemoryJobStore("scope:other", events=events)
    with pytest.raises(JobValidationError, match="target_session_not_found"):
        await other_scope.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id="target",
            idempotency_key="cross-scope",
            max_attempts=3,
            now=_NOW,
        )


async def test_in_memory_get_and_list_are_copied_filtered_and_newest_first() -> None:
    store = InMemoryJobStore("web:local")
    older, _ = await store.enqueue_once(
        kind="test.a",
        payload={"nested": {"value": 1}},
        target_session_id=None,
        idempotency_key="older",
        max_attempts=3,
        now=_NOW,
    )
    newer, _ = await store.enqueue_once(
        kind="test.b",
        payload={},
        target_session_id=None,
        idempotency_key="newer",
        max_attempts=3,
        now=_NOW + timedelta(seconds=1),
    )

    fetched = await store.get(older.id)
    assert fetched is not None
    fetched.payload["nested"]["value"] = 99
    assert (await store.get(older.id)).payload == {"nested": {"value": 1}}  # type: ignore[union-attr]
    assert [row.id for row in await store.list()] == [newer.id, older.id]
    assert [row.id for row in await store.list(kind="test.a")] == [older.id]
    assert await store.list(status=JobStatus.running) == []
    with pytest.raises(ValueError, match="limit"):
        await store.list(limit=0)
    with pytest.raises(ValueError, match="limit"):
        await store.list(limit=101)


async def test_in_memory_dispatch_selection_respects_due_time_and_limit() -> None:
    store = InMemoryJobStore("web:local")
    due, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="due",
        max_attempts=3,
        now=_NOW,
    )
    later, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="later",
        max_attempts=3,
        now=_NOW + timedelta(minutes=5),
    )

    assert await store.dispatchable(_NOW, 1) == [due.id]
    assert later.id not in await store.dispatchable(_NOW, 100)
    assert await store.exhausted(_NOW, 100) == []
```

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py -v
```

Expected: collection fails because `InMemoryJobStore` is not defined.

- [ ] **Step 3: Add in-memory storage primitives** — add imports:

```python
import asyncio
import copy
import uuid
from dataclasses import replace
from datetime import UTC, timedelta

from keel_core.state import InMemoryEventStore
```

Add helpers:

```python
def _utcnow() -> datetime:
    return datetime.now(UTC)


def _copy_record(record: JobRecord) -> JobRecord:
    return replace(
        record,
        payload=copy.deepcopy(record.payload),
        result=copy.deepcopy(record.result),
    )


def _validate_limit(limit: int) -> None:
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")


def _validate_progress(current: int, total: int | None) -> None:
    if current < 0 or (total is not None and (total < 0 or current > total)):
        raise JobValidationError(
            "invalid_progress",
            "progress requires current >= 0 and current <= total when total is set",
        )
```

Then start `InMemoryJobStore`:

```python
class InMemoryJobStore:
    """Deterministic scope-bound JobStore for unit tests and the lite profile."""

    def __init__(
        self,
        scope_id: str,
        *,
        events: InMemoryEventStore | None = None,
        limits: JobLimits | None = None,
    ) -> None:
        if not scope_id.strip():
            raise ValueError("scope_id must not be empty")
        self._scope_id = scope_id
        self._events = events
        self._limits = limits or JobLimits()
        self._rows: dict[str, JobRecord] = {}
        self._dedupe: dict[tuple[str, str, str], str] = {}
        self._lock = asyncio.Lock()

    @property
    def scope_id(self) -> str:
        return self._scope_id
```

- [ ] **Step 4: Implement enqueue/get/list exactly**:

```python
    async def enqueue_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]:
        kind = kind.strip()
        idempotency_key = idempotency_key.strip()
        if not kind:
            raise JobValidationError("invalid_kind", "kind must not be empty")
        if not idempotency_key:
            raise JobValidationError(
                "invalid_idempotency_key", "idempotency_key must not be empty"
            )
        if max_attempts < 1:
            raise JobValidationError(
                "invalid_max_attempts", "max_attempts must be at least 1"
            )
        timestamp = now or _utcnow()
        key = (self._scope_id, kind, idempotency_key)
        async with self._lock:
            existing_id = self._dedupe.get(key)
            if existing_id is not None:
                return _copy_record(self._rows[existing_id]), False
            safe_payload = copy.deepcopy(self._limits.validate_payload(payload))
            if target_session_id is not None and (
                self._events is None
                or not self._events.has_session(target_session_id, self._scope_id)
            ):
                raise JobValidationError(
                    "target_session_not_found",
                    "target session does not exist in the current scope",
                )
            job_id = f"job_{uuid.uuid4().hex}"
            record = JobRecord(
                id=job_id,
                scope_id=self._scope_id,
                kind=kind,
                status=JobStatus.queued,
                payload=safe_payload,
                target_session_id=target_session_id,
                idempotency_key=idempotency_key,
                attempt=0,
                max_attempts=max_attempts,
                next_attempt_at=timestamp,
                lease_token=None,
                lease_expires_at=None,
                heartbeat_at=None,
                cancel_requested_at=None,
                progress_current=0,
                progress_total=None,
                progress_message=None,
                progress_updated_at=None,
                result=None,
                result_message=None,
                error_kind=None,
                error_message=None,
                injected_event_seq=None,
                created_at=timestamp,
                updated_at=timestamp,
                started_at=None,
                finished_at=None,
            )
            self._rows[job_id] = record
            self._dedupe[key] = job_id
            return _copy_record(record), True

    async def get(self, job_id: str) -> JobRecord | None:
        async with self._lock:
            record = self._rows.get(job_id)
            return None if record is None else _copy_record(record)

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> list[JobRecord]:
        _validate_limit(limit)
        async with self._lock:
            rows = [
                row
                for row in self._rows.values()
                if (status is None or row.status is status)
                and (kind is None or row.kind == kind)
            ]
            rows.sort(key=lambda row: (row.created_at, row.id), reverse=True)
            return [_copy_record(row) for row in rows[:limit]]
```

- [ ] **Step 5: Implement recovery selectors**:

```python
    async def dispatchable(self, now: datetime, limit: int) -> list[str]:
        _validate_limit(limit)
        async with self._lock:
            rows = [
                row
                for row in self._rows.values()
                if row.attempt < row.max_attempts
                and (
                    (
                        row.status is JobStatus.queued
                        and row.next_attempt_at <= now
                    )
                    or (
                        row.status is JobStatus.running
                        and row.lease_expires_at is not None
                        and row.lease_expires_at <= now
                    )
                )
            ]
            rows.sort(key=lambda row: (row.next_attempt_at, row.created_at, row.id))
            return [row.id for row in rows[:limit]]

    async def exhausted(self, now: datetime, limit: int) -> list[str]:
        _validate_limit(limit)
        async with self._lock:
            rows = [
                row
                for row in self._rows.values()
                if row.status is JobStatus.running
                and row.lease_expires_at is not None
                and row.lease_expires_at <= now
                and row.attempt >= row.max_attempts
            ]
            rows.sort(key=lambda row: (row.lease_expires_at, row.created_at, row.id))
            return [row.id for row in rows[:limit]]
```

- [ ] **Step 6: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py -v
```

Expected: all Task 1 and Task 4 tests pass.

- [ ] **Step 7: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\jobs.py tests\unit\test_jobs.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\jobs.py tests\unit\test_jobs.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 8: Commit**

```bash
git add packages/keel-core/src/keel_core/jobs.py tests/unit/test_jobs.py
git commit -m "feat(jobs): add in-memory enqueue and recovery selection" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 5: In-memory claim/reclaim, attempt ceiling, heartbeat, and progress

**Files:**
- Modify: `packages/keel-core/src/keel_core/jobs.py`
- Extend: `tests/unit/test_jobs.py`

**Interfaces:**
- Implements `claim`, `heartbeat`, and `progress` on `InMemoryJobStore`.
- A claim succeeds only for a due queued row or an expired running row and only while
  `attempt < max_attempts`; every success increments attempt, replaces the lease token,
  refreshes heartbeat/expiry, sets `started_at` only once, and resets progress.
- `heartbeat()` raises `JobLeaseLostError` for stale tokens and returns
  `cancel_requested_at is not None`.
- `progress()` has the same lease refresh, validates bounds and monotonicity, and returns
  `JobProgressResult`.

- [ ] **Step 1: Write RED state-machine tests** — append to `tests/unit/test_jobs.py`:

```python
from keel_core.jobs import JobLeaseLostError


async def _queued(
    store: InMemoryJobStore,
    key: str,
    *,
    max_attempts: int = 3,
) -> str:
    row, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key=key,
        max_attempts=max_attempts,
        now=_NOW,
    )
    return row.id


async def test_claim_is_exclusive_and_reclaim_replaces_token_and_resets_progress() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "claim")
    first = await store.claim(job_id, _NOW, 60)
    assert first is not None
    assert first.attempt == 1
    assert await store.claim(job_id, _NOW, 60) is None

    await store.progress(
        first,
        current=3,
        total=10,
        message="first attempt",
        now=_NOW + timedelta(seconds=10),
    )
    second = await store.claim(job_id, _NOW + timedelta(seconds=71), 60)
    assert second is not None
    assert second.attempt == 2
    assert second.token != first.token
    record = await store.get(job_id)
    assert record is not None
    assert record.progress_current == 0
    assert record.progress_total is None
    assert record.started_at == _NOW


async def test_claim_attempt_ceiling_is_enforced_inside_store() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "ceiling", max_attempts=1)
    lease = await store.claim(job_id, _NOW, 10)
    assert lease is not None
    assert await store.claim(job_id, _NOW + timedelta(seconds=11), 10) is None
    assert await store.dispatchable(_NOW + timedelta(seconds=11), 100) == []
    assert await store.exhausted(_NOW + timedelta(seconds=11), 100) == [job_id]


async def test_heartbeat_refreshes_lease_and_stale_token_is_rejected() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "heartbeat")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    assert await store.heartbeat(lease, _NOW + timedelta(seconds=30)) is False
    record = await store.get(job_id)
    assert record is not None
    assert record.heartbeat_at == _NOW + timedelta(seconds=30)
    assert record.lease_expires_at == _NOW + timedelta(seconds=90)

    reclaimed = await store.claim(job_id, _NOW + timedelta(seconds=91), 60)
    assert reclaimed is not None
    with pytest.raises(JobLeaseLostError):
        await store.heartbeat(lease, _NOW + timedelta(seconds=92))


async def test_progress_is_monotonic_bounded_and_refreshes_lease() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "progress")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None

    updated = await store.progress(
        lease,
        current=4,
        total=10,
        message="embedding batch 2",
        now=_NOW + timedelta(seconds=5),
    )
    assert updated.record.progress_current == 4
    assert updated.record.progress_total == 10
    assert updated.record.progress_message == "embedding batch 2"
    assert updated.record.lease_expires_at == _NOW + timedelta(seconds=65)
    assert updated.cancel_requested is False

    with pytest.raises(JobValidationError, match="progress_regression"):
        await store.progress(
            lease, current=3, total=10, message=None, now=_NOW + timedelta(seconds=6)
        )
    with pytest.raises(JobValidationError, match="invalid_progress"):
        await store.progress(
            lease, current=11, total=10, message=None, now=_NOW + timedelta(seconds=6)
        )
    with pytest.raises(JobValidationError, match="invalid_progress"):
        await store.progress(
            lease, current=-1, total=None, message=None, now=_NOW + timedelta(seconds=6)
        )
```

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py -v
```

Expected: `AttributeError` because the three methods do not exist.

- [ ] **Step 3: Add lease validation helpers** inside `InMemoryJobStore`:

```python
    def _owned(self, lease: JobLease) -> JobRecord:
        row = self._rows.get(lease.job_id)
        if (
            row is None
            or lease.scope_id != self._scope_id
            or row.status is not JobStatus.running
            or row.lease_token != lease.token
        ):
            raise JobLeaseLostError(lease.job_id)
        return row

```

- [ ] **Step 4: Implement atomic claim/reclaim**:

```python
    async def claim(
        self, job_id: str, now: datetime, lease_seconds: int
    ) -> JobLease | None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        async with self._lock:
            row = self._rows.get(job_id)
            if row is None or row.attempt >= row.max_attempts:
                return None
            due_queued = (
                row.status is JobStatus.queued and row.next_attempt_at <= now
            )
            expired_running = (
                row.status is JobStatus.running
                and row.lease_expires_at is not None
                and row.lease_expires_at <= now
            )
            if not (due_queued or expired_running):
                return None
            token = uuid.uuid4().hex
            claimed = replace(
                row,
                status=JobStatus.running,
                attempt=row.attempt + 1,
                lease_token=token,
                lease_expires_at=now + timedelta(seconds=lease_seconds),
                heartbeat_at=now,
                progress_current=0,
                progress_total=None,
                progress_message=None,
                progress_updated_at=None,
                updated_at=now,
                started_at=row.started_at or now,
            )
            self._rows[job_id] = claimed
            return JobLease(
                job_id=job_id,
                scope_id=self._scope_id,
                token=token,
                kind=claimed.kind,
                payload=copy.deepcopy(claimed.payload),
                attempt=claimed.attempt,
                max_attempts=claimed.max_attempts,
                lease_seconds=lease_seconds,
            )
```

- [ ] **Step 5: Implement heartbeat and progress**:

```python
    async def heartbeat(self, lease: JobLease, now: datetime) -> bool:
        async with self._lock:
            row = self._owned(lease)
            updated = replace(
                row,
                heartbeat_at=now,
                lease_expires_at=now + timedelta(seconds=lease.lease_seconds),
                updated_at=now,
            )
            self._rows[row.id] = updated
            return updated.cancel_requested_at is not None

    async def progress(
        self,
        lease: JobLease,
        *,
        current: int,
        total: int | None,
        message: str | None,
        now: datetime,
    ) -> JobProgressResult:
        _validate_progress(current, total)
        async with self._lock:
            row = self._owned(lease)
            if current < row.progress_current:
                raise JobValidationError(
                    "progress_regression",
                    "progress current cannot decrease within one attempt",
                )
            updated = replace(
                row,
                progress_current=current,
                progress_total=total,
                progress_message=message,
                progress_updated_at=now,
                heartbeat_at=now,
                lease_expires_at=now + timedelta(seconds=lease.lease_seconds),
                updated_at=now,
            )
            self._rows[row.id] = updated
            return JobProgressResult(
                record=_copy_record(updated),
                cancel_requested=updated.cancel_requested_at is not None,
            )
```

- [ ] **Step 6: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py -v
```

Expected: all job unit tests pass.

- [ ] **Step 7: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\jobs.py tests\unit\test_jobs.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\jobs.py tests\unit\test_jobs.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 8: Commit**

```bash
git add packages/keel-core/src/keel_core/jobs.py tests/unit/test_jobs.py
git commit -m "feat(jobs): add in-memory leases heartbeat and progress" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 6: In-memory cancellation, requeue, terminal finalizers, and exactly-once injection

**Files:**
- Modify: `packages/keel-core/src/keel_core/jobs.py`
- Extend: `tests/unit/test_jobs.py`

**Interfaces:**
- Completes `InMemoryJobStore` protocol parity.
- `request_cancel`: missing → `None`; queued → atomic `cancelled` + optional injection;
  running → set request only; terminal → idempotently return current record.
- `requeue`: running/current lease only, attempts must remain, clears lease and stores bounded
  error without injecting.
- `succeed`, `fail_terminal`, `finish_cancelled`, and `fail_exhausted` share one locked
  finalizer and inject at most one assistant event.
- Success wins if the handler returns before observing a pending cancel request.

- [ ] **Step 1: Write RED cancellation/retry/finalizer tests** — append:

```python
from keel_core.jobs import JobError, JobResult


async def test_queued_cancel_is_terminal_idempotent_and_injects_once() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="cancel-queued",
        max_attempts=3,
        now=_NOW,
    )

    cancelled = await store.request_cancel(job.id, _NOW + timedelta(seconds=1))
    again = await store.request_cancel(job.id, _NOW + timedelta(seconds=2))
    assert cancelled is not None and cancelled.status is JobStatus.cancelled
    assert again is not None and again.injected_event_seq == cancelled.injected_event_seq
    injected = [
        event
        for event in events.snapshot("target")
        if event.payload.get("job_id") == job.id
    ]
    assert len(injected) == 1
    assert injected[0].payload == {
        "role": "assistant",
        "text": "后台任务 test.echo 已取消。",
        "partial": False,
        "job_id": job.id,
        "job_kind": "test.echo",
        "job_status": "cancelled",
    }


async def test_running_cancel_is_observed_by_heartbeat_but_success_can_win() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "cancel-running")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None

    requested = await store.request_cancel(job_id, _NOW + timedelta(seconds=1))
    assert requested is not None and requested.status is JobStatus.running
    assert requested.cancel_requested_at == _NOW + timedelta(seconds=1)
    assert await store.heartbeat(lease, _NOW + timedelta(seconds=2)) is True

    succeeded = await store.succeed(
        lease,
        JobResult(data={"count": 1}, message="completed before checkpoint"),
        _NOW + timedelta(seconds=3),
    )
    assert succeeded.status is JobStatus.succeeded


async def test_requeue_is_non_terminal_and_does_not_inject() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="retry",
        max_attempts=3,
        now=_NOW,
    )
    lease = await store.claim(job.id, _NOW, 60)
    assert lease is not None
    retry_at = _NOW + timedelta(seconds=5)
    queued = await store.requeue(
        lease,
        JobError("provider_timeout", "safe public message"),
        retry_at,
        _NOW + timedelta(seconds=1),
    )
    assert queued.status is JobStatus.queued
    assert queued.next_attempt_at == retry_at
    assert queued.error_kind == "provider_timeout"
    assert queued.injected_event_seq is None
    assert not any(e.payload.get("job_id") == job.id for e in events.snapshot("target"))


@pytest.mark.parametrize("terminal", ["succeeded", "failed", "cancelled"])
async def test_terminal_finalizers_inject_exactly_once(terminal: str) -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key=f"terminal-{terminal}",
        max_attempts=3,
        now=_NOW,
    )
    lease = await store.claim(job.id, _NOW, 60)
    assert lease is not None

    if terminal == "succeeded":
        result = await store.succeed(
            lease, JobResult(data={"ok": True}, message="done"), _NOW
        )
    elif terminal == "failed":
        result = await store.fail_terminal(
            lease, JobError("bad_input", "safe"), _NOW
        )
    else:
        result = await store.finish_cancelled(lease, _NOW)

    assert result.status.value == terminal
    with pytest.raises(JobLeaseLostError):
        await store.succeed(lease, JobResult(data={}, message="again"), _NOW)
    assert len(
        [e for e in events.snapshot("target") if e.payload.get("job_id") == job.id]
    ) == 1


async def test_fail_exhausted_uses_terminal_finalizer_once() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="exhausted",
        max_attempts=1,
        now=_NOW,
    )
    assert await store.claim(job.id, _NOW, 10) is not None
    failed = await store.fail_exhausted(job.id, _NOW + timedelta(seconds=11))
    assert failed is not None and failed.status is JobStatus.failed
    assert failed.error_kind == "attempts_exhausted"
    assert await store.fail_exhausted(job.id, _NOW + timedelta(seconds=12)) is None
    assert len(
        [e for e in events.snapshot("target") if e.payload.get("job_id") == job.id]
    ) == 1


async def test_failed_rerun_requires_a_new_enqueue_request_key() -> None:
    store = InMemoryJobStore("web:local")
    first, _ = await store.enqueue_once(
        kind="test.echo",
        payload={"version": 1},
        target_session_id=None,
        idempotency_key="request-1",
        max_attempts=1,
        now=_NOW,
    )
    lease = await store.claim(first.id, _NOW, 10)
    assert lease is not None
    await store.fail_terminal(lease, JobError("bad_input", "safe"), _NOW)

    duplicate, duplicate_created = await store.enqueue_once(
        kind="test.echo",
        payload={"version": 2},
        target_session_id=None,
        idempotency_key="request-1",
        max_attempts=1,
        now=_NOW + timedelta(seconds=1),
    )
    rerun, rerun_created = await store.enqueue_once(
        kind="test.echo",
        payload={"version": 2},
        target_session_id=None,
        idempotency_key="request-2",
        max_attempts=1,
        now=_NOW + timedelta(seconds=1),
    )
    assert duplicate_created is False
    assert duplicate.id == first.id
    assert duplicate.status is JobStatus.failed
    assert rerun_created is True
    assert rerun.id != first.id
    assert rerun.status is JobStatus.queued
```

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py -v
```

Expected: `AttributeError` for `request_cancel`, `requeue`, and terminal methods.

- [ ] **Step 3: Add finalizer payload helpers** at module level:

```python
from keel_core.events import Event, EventType


def _terminal_message(
    record: JobRecord,
    status: JobStatus,
    *,
    result: JobResult | None,
    error: JobError | None,
) -> str:
    if status is JobStatus.succeeded:
        assert result is not None
        return result.message
    if status is JobStatus.failed:
        assert error is not None
        return f"后台任务 {record.kind} 失败：{error.kind}"
    return f"后台任务 {record.kind} 已取消。"


def _injection_event(
    record: JobRecord, status: JobStatus, text_value: str, now: datetime
) -> Event:
    assert record.target_session_id is not None
    return Event(
        type=EventType.message_token,
        seq=0,
        session_id=record.target_session_id,
        scope_id=record.scope_id,
        ts=now,
        payload={
            "role": "assistant",
            "text": text_value,
            "partial": False,
            "job_id": record.id,
            "job_kind": record.kind,
            "job_status": status.value,
        },
    )
```

- [ ] **Step 4: Implement one locked in-memory finalizer**:

```python
    async def _finalize_locked(
        self,
        row: JobRecord,
        *,
        status: JobStatus,
        now: datetime,
        result: JobResult | None = None,
        error: JobError | None = None,
    ) -> JobRecord:
        safe_result = (
            None
            if result is None
            else copy.deepcopy(self._limits.validate_result(result.data))
        )
        safe_error = (
            None
            if error is None
            else JobError(error.kind, self._limits.error_message(error.message))
        )
        text_value = self._limits.result_message(
            _terminal_message(row, status, result=result, error=safe_error)
        )
        injected_seq = row.injected_event_seq
        if row.target_session_id is not None and injected_seq is None:
            if self._events is None or not self._events.has_session(
                row.target_session_id, self._scope_id
            ):
                raise JobValidationError(
                    "target_session_not_found",
                    "target session does not exist in the current scope",
                )
            event = _injection_event(row, status, text_value, now)
            await self._events.append(event)
            injected_seq = event.seq
        updated = replace(
            row,
            status=status,
            lease_token=None,
            lease_expires_at=None,
            heartbeat_at=None,
            result=safe_result if status is JobStatus.succeeded else None,
            result_message=text_value,
            error_kind=safe_error.kind if safe_error is not None else None,
            error_message=safe_error.message if safe_error is not None else None,
            injected_event_seq=injected_seq,
            updated_at=now,
            finished_at=now,
        )
        self._rows[row.id] = updated
        return _copy_record(updated)
```

- [ ] **Step 5: Implement cancellation and requeue**:

```python
    async def request_cancel(self, job_id: str, now: datetime) -> JobRecord | None:
        async with self._lock:
            row = self._rows.get(job_id)
            if row is None:
                return None
            if row.status is JobStatus.queued:
                return await self._finalize_locked(
                    row, status=JobStatus.cancelled, now=now
                )
            if row.status is JobStatus.running:
                updated = replace(
                    row,
                    cancel_requested_at=row.cancel_requested_at or now,
                    updated_at=now,
                )
                self._rows[job_id] = updated
                return _copy_record(updated)
            return _copy_record(row)

    async def requeue(
        self, lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord:
        async with self._lock:
            row = self._owned(lease)
            if row.attempt >= row.max_attempts:
                raise JobValidationError(
                    "attempts_exhausted", "job has no retry attempts remaining"
                )
            updated = replace(
                row,
                status=JobStatus.queued,
                next_attempt_at=retry_at,
                lease_token=None,
                lease_expires_at=None,
                heartbeat_at=None,
                error_kind=error.kind,
                error_message=self._limits.error_message(error.message),
                updated_at=now,
            )
            self._rows[row.id] = updated
            return _copy_record(updated)
```

- [ ] **Step 6: Implement terminal methods**:

```python
    async def succeed(
        self, lease: JobLease, result: JobResult, now: datetime
    ) -> JobRecord:
        async with self._lock:
            row = self._owned(lease)
            return await self._finalize_locked(
                row, status=JobStatus.succeeded, now=now, result=result
            )

    async def fail_terminal(
        self, lease: JobLease, error: JobError, now: datetime
    ) -> JobRecord:
        async with self._lock:
            row = self._owned(lease)
            return await self._finalize_locked(
                row, status=JobStatus.failed, now=now, error=error
            )

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord:
        async with self._lock:
            row = self._owned(lease)
            return await self._finalize_locked(
                row, status=JobStatus.cancelled, now=now
            )

    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        async with self._lock:
            row = self._rows.get(job_id)
            if (
                row is None
                or row.status is not JobStatus.running
                or row.lease_expires_at is None
                or row.lease_expires_at > now
                or row.attempt < row.max_attempts
            ):
                return None
            return await self._finalize_locked(
                row,
                status=JobStatus.failed,
                now=now,
                error=JobError(
                    "attempts_exhausted",
                    "job attempts were exhausted after worker lease expiry",
                ),
            )
```

- [ ] **Step 7: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py -v
```

Expected: all in-memory lifecycle tests pass, including one injected event per terminal job.

- [ ] **Step 8: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\jobs.py tests\unit\test_jobs.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\jobs.py tests\unit\test_jobs.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 9: Commit**

```bash
git add packages/keel-core/src/keel_core/jobs.py tests/unit/test_jobs.py
git commit -m "feat(jobs): add in-memory cancellation retry and finalizers" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 7: Postgres enqueue dedupe, target policy, reads, filters, and dispatcher queries

**Files:**
- Modify: `packages/keel-core/src/keel_core/jobs.py`
- Extend: `tests/integration/test_jobs_postgres.py`

**Interfaces:**
- Produces `PostgresJobStore(engine, scope_id, *, limits=None)`.
- Every method sets `_SET_SCOPE`, filters `scope_id`, and maps rows through one
  `_to_job_record()` helper.
- `enqueue_once()` first returns an existing `(scope_id, kind, idempotency_key)` winner before
  validating retry payload/target fields; for a new key it validates the target in the same
  transaction, inserts with `ON CONFLICT ... DO NOTHING`, and selects a concurrent winner.
- `dispatchable()` and `exhausted()` exactly match design §9.3.
- Consumes migration 0009 and `JobLimits`; no Redis dependency.

- [ ] **Step 1: Write RED Postgres enqueue/read tests** — extend imports and append:

```python
import asyncio
from datetime import UTC, datetime, timedelta

from keel_core.jobs import (
    JobStatus,
    JobValidationError,
    PostgresJobStore,
)
from keel_core.loop import admit
from keel_core.state import PostgresEventStore

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


async def _target(engine: AsyncEngine, scope: str, session_id: str) -> None:
    await admit(PostgresEventStore(engine, scope), session_id, scope, "seed")


async def test_postgres_enqueue_once_is_concurrently_deduped(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "jobs:dedupe")

    async def enqueue(value: int) -> tuple[str, bool, dict[str, object]]:
        row, created = await store.enqueue_once(
            kind="test.echo",
            payload={"value": value},
            target_session_id=None,
            idempotency_key="request-1",
            max_attempts=3,
            now=_NOW,
        )
        return row.id, created, row.payload

    first, second = await asyncio.gather(enqueue(1), enqueue(2))
    assert first[0] == second[0]
    assert sorted([first[1], second[1]]) == [False, True]
    assert first[2] == second[2]
    assert first[2] in ({"value": 1}, {"value": 2})


async def test_postgres_existing_dedupe_precedes_retry_payload_and_target_validation(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "jobs:dedupe-order")
    first, created = await store.enqueue_once(
        kind="test.echo",
        payload={"value": 1},
        target_session_id=None,
        idempotency_key="request-1",
        max_attempts=3,
        now=_NOW,
    )
    duplicate, duplicate_created = await store.enqueue_once(
        kind="test.echo",
        payload={"bad": object()},
        target_session_id="missing-on-retry",
        idempotency_key="request-1",
        max_attempts=1,
        now=_NOW + timedelta(seconds=1),
    )

    assert created is True
    assert duplicate_created is False
    assert duplicate.id == first.id
    assert duplicate.payload == {"value": 1}
    assert duplicate.target_session_id is None
    assert duplicate.max_attempts == 3


async def test_postgres_enqueue_target_must_exist_in_bound_scope(
    migrated_db: AsyncEngine,
) -> None:
    await _target(migrated_db, "scope:a", "target-a")
    accepted, _ = await PostgresJobStore(migrated_db, "scope:a").enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target-a",
        idempotency_key="same-scope",
        max_attempts=3,
        now=_NOW,
    )
    assert accepted.target_session_id == "target-a"

    with pytest.raises(JobValidationError, match="target_session_not_found"):
        await PostgresJobStore(migrated_db, "scope:b").enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id="target-a",
            idempotency_key="cross-scope",
            max_attempts=3,
            now=_NOW,
        )


async def test_postgres_get_list_filters_limit_and_scope(
    migrated_db: AsyncEngine,
) -> None:
    a = PostgresJobStore(migrated_db, "scope:list:a")
    b = PostgresJobStore(migrated_db, "scope:list:b")
    older, _ = await a.enqueue_once(
        kind="test.a",
        payload={},
        target_session_id=None,
        idempotency_key="older",
        max_attempts=3,
        now=_NOW,
    )
    newer, _ = await a.enqueue_once(
        kind="test.b",
        payload={},
        target_session_id=None,
        idempotency_key="newer",
        max_attempts=3,
        now=_NOW + timedelta(seconds=1),
    )
    await b.enqueue_once(
        kind="test.a",
        payload={},
        target_session_id=None,
        idempotency_key="other",
        max_attempts=3,
        now=_NOW + timedelta(seconds=2),
    )

    assert (await a.get(older.id)).id == older.id  # type: ignore[union-attr]
    assert await b.get(older.id) is None
    assert [row.id for row in await a.list()] == [newer.id, older.id]
    assert [row.id for row in await a.list(kind="test.a", limit=1)] == [older.id]
    assert await a.list(status=JobStatus.running) == []


async def test_postgres_dispatchable_and_exhausted_queries(
    migrated_db: AsyncEngine,
) -> None:
    scope = "scope:dispatch"
    store = PostgresJobStore(migrated_db, scope)
    due, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="due",
        max_attempts=3,
        now=_NOW,
    )
    later, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="later",
        max_attempts=3,
        now=_NOW + timedelta(minutes=5),
    )
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"), {"scope": scope}
        )
        await conn.execute(
            text(
                "INSERT INTO jobs "
                "(id, scope_id, kind, status, idempotency_key, attempt, max_attempts, "
                "next_attempt_at, lease_expires_at) VALUES "
                "('expired-left', :scope, 'test.echo', 'running', 'expired-left', 1, 3, "
                ":now, :expired), "
                "('expired-done', :scope, 'test.echo', 'running', 'expired-done', 3, 3, "
                ":now, :expired)"
            ),
            {
                "scope": scope,
                "now": _NOW,
                "expired": _NOW - timedelta(seconds=1),
            },
        )

    dispatchable = await store.dispatchable(_NOW, 100)
    assert due.id in dispatchable
    assert later.id not in dispatchable
    assert "expired-left" in dispatchable
    assert "expired-done" not in dispatchable
    assert await store.exhausted(_NOW, 100) == ["expired-done"]
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_postgres.py -v
```

Expected: collection fails because `PostgresJobStore` is not defined.

- [ ] **Step 3: Add shared validation and row mapping** — refactor the duplicated in-memory
  identity validation into:

```python
from collections.abc import Mapping

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


def _validate_enqueue_fields(
    kind: str, idempotency_key: str, max_attempts: int
) -> tuple[str, str]:
    kind = kind.strip()
    idempotency_key = idempotency_key.strip()
    if not kind:
        raise JobValidationError("invalid_kind", "kind must not be empty")
    if not idempotency_key:
        raise JobValidationError(
            "invalid_idempotency_key", "idempotency_key must not be empty"
        )
    if max_attempts < 1:
        raise JobValidationError(
            "invalid_max_attempts", "max_attempts must be at least 1"
        )
    return kind, idempotency_key


def _to_job_record(row: Mapping[str, Any]) -> JobRecord:
    payload = row["payload"] if isinstance(row["payload"], dict) else {}
    result = row["result"] if isinstance(row["result"], dict) else None
    return JobRecord(
        id=str(row["id"]),
        scope_id=str(row["scope_id"]),
        kind=str(row["kind"]),
        status=JobStatus(str(row["status"])),
        payload=copy.deepcopy(payload),
        target_session_id=row["target_session_id"],
        idempotency_key=str(row["idempotency_key"]),
        attempt=int(row["attempt"]),
        max_attempts=int(row["max_attempts"]),
        next_attempt_at=row["next_attempt_at"],
        lease_token=row["lease_token"],
        lease_expires_at=row["lease_expires_at"],
        heartbeat_at=row["heartbeat_at"],
        cancel_requested_at=row["cancel_requested_at"],
        progress_current=int(row["progress_current"]),
        progress_total=(
            None if row["progress_total"] is None else int(row["progress_total"])
        ),
        progress_message=row["progress_message"],
        progress_updated_at=row["progress_updated_at"],
        result=copy.deepcopy(result),
        result_message=row["result_message"],
        error_kind=row["error_kind"],
        error_message=row["error_message"],
        injected_event_seq=(
            None
            if row["injected_event_seq"] is None
            else int(row["injected_event_seq"])
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )
```

Make `InMemoryJobStore.enqueue_once()` call `_validate_enqueue_fields()` rather than retaining
its local copy.

- [ ] **Step 4: Add the Postgres constructor and enqueue**:

```python
class PostgresJobStore:
    """Scope-bound durable JobStore over Postgres with RLS defense in depth."""

    def __init__(
        self,
        engine: AsyncEngine,
        scope_id: str,
        *,
        limits: JobLimits | None = None,
    ) -> None:
        if not scope_id.strip():
            raise ValueError("scope_id must not be empty")
        self._engine = engine
        self._scope_id = scope_id
        self._limits = limits or JobLimits()

    @property
    def scope_id(self) -> str:
        return self._scope_id

    async def enqueue_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        target_session_id: str | None,
        idempotency_key: str,
        max_attempts: int,
        now: datetime | None = None,
    ) -> tuple[JobRecord, bool]:
        kind, idempotency_key = _validate_enqueue_fields(
            kind, idempotency_key, max_attempts
        )
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            existing_sql = text(
                "SELECT * FROM jobs WHERE scope_id = :scope "
                "AND kind = :kind AND idempotency_key = :key"
            )
            dedupe_params = {
                "scope": self._scope_id,
                "kind": kind,
                "key": idempotency_key,
            }
            existing = (
                (await conn.execute(existing_sql, dedupe_params))
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                return _to_job_record(existing), False

            safe_payload = self._limits.validate_payload(payload)
            if target_session_id is not None:
                exists = (
                    await conn.execute(
                        text(
                            "SELECT 1 FROM sessions "
                            "WHERE id = :session AND scope_id = :scope"
                        ),
                        {"session": target_session_id, "scope": self._scope_id},
                    )
                ).one_or_none()
                if exists is None:
                    raise JobValidationError(
                        "target_session_not_found",
                        "target session does not exist in the current scope",
                    )
            timestamp = now or _utcnow()
            job_id = f"job_{uuid.uuid4().hex}"
            inserted = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO jobs "
                            "(id, scope_id, kind, payload, target_session_id, "
                            "idempotency_key, max_attempts, next_attempt_at, "
                            "created_at, updated_at) VALUES "
                            "(:id, :scope, :kind, CAST(:payload AS jsonb), :target, "
                            ":key, :max_attempts, :now, :now, :now) "
                            "ON CONFLICT (scope_id, kind, idempotency_key) DO NOTHING "
                            "RETURNING *"
                        ),
                        {
                            "id": job_id,
                            "scope": self._scope_id,
                            "kind": kind,
                            "payload": json.dumps(safe_payload, ensure_ascii=False),
                            "target": target_session_id,
                            "key": idempotency_key,
                            "max_attempts": max_attempts,
                            "now": timestamp,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if inserted is not None:
                return _to_job_record(inserted), True
            existing = (
                (await conn.execute(existing_sql, dedupe_params))
                .mappings()
                .one()
            )
            return _to_job_record(existing), False
```

- [ ] **Step 5: Implement get/list**:

```python
    async def get(self, job_id: str) -> JobRecord | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM jobs "
                            "WHERE id = :id AND scope_id = :scope"
                        ),
                        {"id": job_id, "scope": self._scope_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_job_record(row)

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> list[JobRecord]:
        _validate_limit(limit)
        clauses = ["scope_id = :scope"]
        params: dict[str, Any] = {"scope": self._scope_id, "limit": limit}
        if status is not None:
            clauses.append("status = :status")
            params["status"] = status.value
        if kind is not None:
            clauses.append("kind = :kind")
            params["kind"] = kind
        sql = (
            "SELECT * FROM jobs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at DESC, id DESC LIMIT :limit"
        )
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (await conn.execute(text(sql), params)).mappings().all()
        return [_to_job_record(row) for row in rows]
```

- [ ] **Step 6: Implement dispatch selection**:

```python
    async def dispatchable(self, now: datetime, limit: int) -> list[str]:
        _validate_limit(limit)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT id FROM jobs WHERE scope_id = :scope "
                        "AND attempt < max_attempts AND ("
                        "  (status = 'queued' AND next_attempt_at <= :now) OR "
                        "  (status = 'running' AND lease_expires_at <= :now)"
                        ") ORDER BY next_attempt_at, created_at, id LIMIT :limit"
                    ),
                    {"scope": self._scope_id, "now": now, "limit": limit},
                )
            ).scalars().all()
        return [str(value) for value in rows]

    async def exhausted(self, now: datetime, limit: int) -> list[str]:
        _validate_limit(limit)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT id FROM jobs WHERE scope_id = :scope "
                        "AND status = 'running' AND lease_expires_at <= :now "
                        "AND attempt >= max_attempts "
                        "ORDER BY lease_expires_at, created_at, id LIMIT :limit"
                    ),
                    {"scope": self._scope_id, "now": now, "limit": limit},
                )
            ).scalars().all()
        return [str(value) for value in rows]
```

- [ ] **Step 7: Run GREEN**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_postgres.py -v
```

Expected: migration/RLS plus enqueue/read/list/selection tests pass.

- [ ] **Step 8: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\jobs.py tests\integration\test_jobs_postgres.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\jobs.py tests\integration\test_jobs_postgres.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 9: Commit**

```bash
git add packages/keel-core/src/keel_core/jobs.py tests/integration/test_jobs_postgres.py
git commit -m "feat(jobs): add Postgres enqueue dedupe and reads" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 8: Postgres atomic claim/reclaim, heartbeat, progress, stale-lease rejection, and requeue

**Files:**
- Modify: `packages/keel-core/src/keel_core/jobs.py`
- Extend: `tests/integration/test_jobs_postgres.py`

**Interfaces:**
- Implements `claim`, `heartbeat`, `progress`, and `requeue` on `PostgresJobStore`.
- Claim is one `UPDATE ... WHERE ... RETURNING *` whose SQL contains
  `attempt < max_attempts` and both valid source states.
- Progress uses `SELECT ... FOR UPDATE` so it can distinguish lease loss from a monotonicity
  violation and return a precise bounded public error.
- Requeue is current-token-only, non-terminal, clears lease, records error, and never injects.

- [ ] **Step 1: Write RED transition tests** — append:

```python
from keel_core.jobs import JobError, JobLeaseLostError


async def _pg_job(
    store: PostgresJobStore,
    key: str,
    *,
    max_attempts: int = 3,
    target_session_id: str | None = None,
) -> str:
    row, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=target_session_id,
        idempotency_key=key,
        max_attempts=max_attempts,
        now=_NOW,
    )
    return row.id


async def test_postgres_concurrent_claim_has_one_winner(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:claim")
    job_id = await _pg_job(store, "one-winner")
    first, second = await asyncio.gather(
        store.claim(job_id, _NOW, 60),
        store.claim(job_id, _NOW, 60),
    )
    assert sum(lease is not None for lease in (first, second)) == 1
    winner = first or second
    assert winner is not None and winner.attempt == 1


async def test_postgres_expired_reclaim_increments_attempt_replaces_token_and_resets_progress(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:reclaim")
    job_id = await _pg_job(store, "reclaim")
    first = await store.claim(job_id, _NOW, 10)
    assert first is not None
    await store.progress(
        first, current=2, total=5, message="attempt 1", now=_NOW + timedelta(seconds=1)
    )
    second = await store.claim(job_id, _NOW + timedelta(seconds=12), 10)
    assert second is not None
    assert second.attempt == 2
    assert second.token != first.token
    row = await store.get(job_id)
    assert row is not None
    assert row.progress_current == 0
    assert row.progress_total is None
    with pytest.raises(JobLeaseLostError):
        await store.heartbeat(first, _NOW + timedelta(seconds=13))
    with pytest.raises(JobLeaseLostError):
        await store.progress(
            first,
            current=3,
            total=5,
            message="stale attempt",
            now=_NOW + timedelta(seconds=13),
        )


async def test_postgres_claim_sql_enforces_attempt_ceiling(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:ceiling")
    job_id = await _pg_job(store, "ceiling", max_attempts=1)
    assert await store.claim(job_id, _NOW, 10) is not None
    assert await store.claim(job_id, _NOW + timedelta(seconds=11), 10) is None
    assert await store.exhausted(_NOW + timedelta(seconds=11), 100) == [job_id]


async def test_postgres_progress_heartbeat_cancel_flag_and_validation(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:progress")
    job_id = await _pg_job(store, "progress")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    result = await store.progress(
        lease,
        current=4,
        total=10,
        message="batch 2",
        now=_NOW + timedelta(seconds=5),
    )
    assert result.record.progress_current == 4
    assert result.record.lease_expires_at == _NOW + timedelta(seconds=65)
    assert result.cancel_requested is False
    with pytest.raises(JobValidationError, match="progress_regression"):
        await store.progress(
            lease, current=3, total=10, message=None, now=_NOW + timedelta(seconds=6)
        )
    with pytest.raises(JobValidationError, match="invalid_progress"):
        await store.progress(
            lease, current=11, total=10, message=None, now=_NOW + timedelta(seconds=6)
        )


async def test_postgres_requeue_is_due_later_and_has_no_terminal_injection(
    migrated_db: AsyncEngine,
) -> None:
    scope = "scope:requeue"
    await _target(migrated_db, scope, "target-requeue")
    store = PostgresJobStore(migrated_db, scope)
    job_id = await _pg_job(
        store, "requeue", target_session_id="target-requeue"
    )
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    retry_at = _NOW + timedelta(seconds=5)
    row = await store.requeue(
        lease,
        JobError("provider_timeout", "safe"),
        retry_at,
        _NOW + timedelta(seconds=1),
    )
    assert row.status is JobStatus.queued
    assert row.next_attempt_at == retry_at
    assert row.injected_event_seq is None
    assert job_id not in await store.dispatchable(_NOW + timedelta(seconds=4), 100)
    assert job_id in await store.dispatchable(retry_at, 100)
    assert not any(
        event.payload.get("job_id") == job_id
        async for event in PostgresEventStore(migrated_db, scope).read("target-requeue")
    )
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_postgres.py -v
```

Expected: `AttributeError` for transition methods.

- [ ] **Step 3: Implement the atomic claim SQL**:

```python
    async def claim(
        self, job_id: str, now: datetime, lease_seconds: int
    ) -> JobLease | None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        token = uuid.uuid4().hex
        expires = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE jobs SET "
                            "status = 'running', attempt = attempt + 1, "
                            "lease_token = :token, lease_expires_at = :expires, "
                            "heartbeat_at = :now, progress_current = 0, "
                            "progress_total = NULL, progress_message = NULL, "
                            "progress_updated_at = NULL, updated_at = :now, "
                            "started_at = COALESCE(started_at, :now) "
                            "WHERE id = :id AND scope_id = :scope "
                            "AND attempt < max_attempts AND ("
                            "  (status = 'queued' AND next_attempt_at <= :now) OR "
                            "  (status = 'running' AND lease_expires_at <= :now)"
                            ") RETURNING *"
                        ),
                        {
                            "token": token,
                            "expires": expires,
                            "now": now,
                            "id": job_id,
                            "scope": self._scope_id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        record = _to_job_record(row)
        return JobLease(
            job_id=record.id,
            scope_id=record.scope_id,
            token=token,
            kind=record.kind,
            payload=record.payload,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            lease_seconds=lease_seconds,
        )
```

- [ ] **Step 4: Add a locked lease-row helper and heartbeat**:

```python
    async def _locked_owned_row(
        self, conn: Any, lease: JobLease
    ) -> Mapping[str, Any]:
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT * FROM jobs WHERE id = :id AND scope_id = :scope "
                        "FOR UPDATE"
                    ),
                    {"id": lease.job_id, "scope": self._scope_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["status"] != JobStatus.running.value
            or row["lease_token"] != lease.token
        ):
            raise JobLeaseLostError(lease.job_id)
        return row

    async def heartbeat(self, lease: JobLease, now: datetime) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await self._locked_owned_row(conn, lease)
            cancel_requested_at = (
                await conn.execute(
                    text(
                        "UPDATE jobs SET heartbeat_at = :now, "
                        "lease_expires_at = :expires, updated_at = :now "
                        "WHERE id = :id AND scope_id = :scope AND lease_token = :token "
                        "RETURNING cancel_requested_at"
                    ),
                    {
                        "now": now,
                        "expires": now + timedelta(seconds=lease.lease_seconds),
                        "id": lease.job_id,
                        "scope": self._scope_id,
                        "token": lease.token,
                    },
                )
            ).scalar_one()
        return cancel_requested_at is not None
```

Use `AsyncConnection` rather than `Any` for `conn` in the actual implementation; the abbreviated
snippet avoids repeating the import already added in Task 3.

- [ ] **Step 5: Implement progress under row lock**:

```python
    async def progress(
        self,
        lease: JobLease,
        *,
        current: int,
        total: int | None,
        message: str | None,
        now: datetime,
    ) -> JobProgressResult:
        _validate_progress(current, total)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = await self._locked_owned_row(conn, lease)
            if current < int(locked["progress_current"]):
                raise JobValidationError(
                    "progress_regression",
                    "progress current cannot decrease within one attempt",
                )
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE jobs SET progress_current = :current, "
                            "progress_total = :total, progress_message = :message, "
                            "progress_updated_at = :now, heartbeat_at = :now, "
                            "lease_expires_at = :expires, updated_at = :now "
                            "WHERE id = :id AND scope_id = :scope "
                            "AND status = 'running' AND lease_token = :token "
                            "RETURNING *"
                        ),
                        {
                            "current": current,
                            "total": total,
                            "message": message,
                            "now": now,
                            "expires": now
                            + timedelta(seconds=lease.lease_seconds),
                            "id": lease.job_id,
                            "scope": self._scope_id,
                            "token": lease.token,
                        },
                    )
                )
                .mappings()
                .one()
            )
        record = _to_job_record(row)
        return JobProgressResult(
            record=record,
            cancel_requested=record.cancel_requested_at is not None,
        )
```

Both stores already call the module-level `_validate_progress()` introduced in Task 4; do
not duplicate or move it in this task.

- [ ] **Step 6: Implement requeue**:

```python
    async def requeue(
        self, lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = await self._locked_owned_row(conn, lease)
            if int(locked["attempt"]) >= int(locked["max_attempts"]):
                raise JobValidationError(
                    "attempts_exhausted", "job has no retry attempts remaining"
                )
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE jobs SET status = 'queued', "
                            "next_attempt_at = :retry_at, lease_token = NULL, "
                            "lease_expires_at = NULL, heartbeat_at = NULL, "
                            "error_kind = :error_kind, error_message = :error_message, "
                            "updated_at = :now "
                            "WHERE id = :id AND scope_id = :scope "
                            "AND status = 'running' AND lease_token = :token "
                            "RETURNING *"
                        ),
                        {
                            "retry_at": retry_at,
                            "error_kind": error.kind,
                            "error_message": self._limits.error_message(error.message),
                            "now": now,
                            "id": lease.job_id,
                            "scope": self._scope_id,
                            "token": lease.token,
                        },
                    )
                )
                .mappings()
                .one()
            )
        return _to_job_record(row)
```

- [ ] **Step 7: Run GREEN**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_postgres.py -v
```

Expected: all Postgres claim/reclaim/progress/requeue tests pass.

- [ ] **Step 8: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\jobs.py tests\integration\test_jobs_postgres.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\jobs.py tests\integration\test_jobs_postgres.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 9: Commit**

```bash
git add packages/keel-core/src/keel_core/jobs.py tests/integration/test_jobs_postgres.py
git commit -m "feat(jobs): add Postgres lease progress and requeue transitions" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 9: Postgres request-cancel and atomic terminal finalizers with exactly-once injection

**Files:**
- Modify: `packages/keel-core/src/keel_core/jobs.py`
- Extend: `tests/integration/test_jobs_postgres.py`

**Interfaces:**
- Completes `PostgresJobStore`.
- One private `_finalize_in_transaction()` owns success/failure/cancel/exhaustion.
- Lock order is job row first, target session row second.
- `_finalize_in_transaction(..., locked=...)` accepts a caller-owned locked mapping so queued
  `request_cancel()` never takes the same row lock twice or opens a nested transaction.
- A terminal transition validates current status/token (or exhaustion predicate), appends through
  `append_event_in_transaction(..., require_existing_session=True)`, stores
  `injected_event_seq`, updates terminal fields, and commits once.
- Duplicate normal finalization raises `JobLeaseLostError`; duplicate exhaustion returns `None`;
  terminal cancel is idempotent.

- [ ] **Step 1: Write RED finalizer tests** — append:

```python
from keel_core.events import EventType
from keel_core.jobs import JobResult


async def _job_events(
    engine: AsyncEngine, scope: str, session_id: str, job_id: str
) -> list[object]:
    return [
        event
        async for event in PostgresEventStore(engine, scope).read(session_id)
        if event.payload.get("job_id") == job_id
    ]


async def test_postgres_queued_cancel_is_atomic_and_idempotent(
    migrated_db: AsyncEngine,
) -> None:
    scope, session_id = "scope:cancel:q", "target-cancel-q"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    job_id = await _pg_job(store, "cancel-q", target_session_id=session_id)

    first = await store.request_cancel(job_id, _NOW)
    second = await store.request_cancel(job_id, _NOW + timedelta(seconds=1))
    assert first is not None and first.status is JobStatus.cancelled
    assert second is not None
    assert second.injected_event_seq == first.injected_event_seq
    events = await _job_events(migrated_db, scope, session_id, job_id)
    assert len(events) == 1
    assert events[0].type is EventType.message_token  # type: ignore[union-attr]


async def test_postgres_running_cancel_sets_request_and_success_can_win(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:cancel:r")
    job_id = await _pg_job(store, "cancel-r")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    requested = await store.request_cancel(job_id, _NOW + timedelta(seconds=1))
    assert requested is not None and requested.status is JobStatus.running
    assert await store.heartbeat(lease, _NOW + timedelta(seconds=2)) is True
    result = await store.succeed(
        lease,
        JobResult(data={"ok": True}, message="completed"),
        _NOW + timedelta(seconds=3),
    )
    assert result.status is JobStatus.succeeded


async def test_postgres_reclaimed_stale_lease_cannot_complete(
    migrated_db: AsyncEngine,
) -> None:
    store = PostgresJobStore(migrated_db, "scope:stale-complete")
    job_id = await _pg_job(store, "stale-complete")
    first = await store.claim(job_id, _NOW, 10)
    assert first is not None
    second = await store.claim(job_id, _NOW + timedelta(seconds=11), 10)
    assert second is not None

    with pytest.raises(JobLeaseLostError):
        await store.succeed(
            first,
            JobResult(data={"owner": "stale"}, message="must not commit"),
            _NOW + timedelta(seconds=12),
        )
    running = await store.get(job_id)
    assert running is not None
    assert running.status is JobStatus.running
    assert running.attempt == 2
    await store.succeed(
        second,
        JobResult(data={"owner": "current"}, message="current owner completed"),
        _NOW + timedelta(seconds=12),
    )


@pytest.mark.parametrize(
    ("terminal", "expected_text"),
    [
        ("succeeded", "indexed 42 chunks"),
        ("failed", "后台任务 test.echo 失败：embedding_timeout"),
        ("cancelled", "后台任务 test.echo 已取消。"),
    ],
)
async def test_postgres_terminal_transition_injects_one_assistant_event(
    migrated_db: AsyncEngine,
    terminal: str,
    expected_text: str,
) -> None:
    scope = f"scope:terminal:{terminal}"
    session_id = f"target-{terminal}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    job_id = await _pg_job(
        store, f"terminal-{terminal}", target_session_id=session_id
    )
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None

    if terminal == "succeeded":
        row = await store.succeed(
            lease,
            JobResult(data={"chunks": 42}, message="indexed 42 chunks"),
            _NOW,
        )
    elif terminal == "failed":
        row = await store.fail_terminal(
            lease, JobError("embedding_timeout", "safe public error"), _NOW
        )
    else:
        row = await store.finish_cancelled(lease, _NOW)

    assert row.status.value == terminal
    if terminal == "succeeded":
        assert row.result == {"chunks": 42}
    else:
        assert row.result is None
    assert row.injected_event_seq is not None
    injected = await _job_events(migrated_db, scope, session_id, job_id)
    assert len(injected) == 1
    assert injected[0].payload == {  # type: ignore[union-attr]
        "role": "assistant",
        "text": expected_text,
        "partial": False,
        "job_id": job_id,
        "job_kind": "test.echo",
        "job_status": terminal,
    }
    with pytest.raises(JobLeaseLostError):
        await store.succeed(
            lease, JobResult(data={}, message="duplicate"), _NOW
        )
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1


async def test_postgres_finalizer_rolls_back_event_and_job_together(
    migrated_db: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import keel_core.jobs as jobs_module

    scope, session_id = "scope:rollback", "target-rollback"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    job_id = await _pg_job(store, "rollback", target_session_id=session_id)
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    real_append = jobs_module.append_event_in_transaction

    async def append_then_fail(*args: object, **kwargs: object) -> int:
        await real_append(*args, **kwargs)  # type: ignore[arg-type]
        raise RuntimeError("after event insert")

    monkeypatch.setattr(jobs_module, "append_event_in_transaction", append_then_fail)
    with pytest.raises(RuntimeError, match="after event insert"):
        await store.succeed(
            lease, JobResult(data={"ok": True}, message="done"), _NOW
        )
    assert (await store.get(job_id)).status is JobStatus.running  # type: ignore[union-attr]
    assert await _job_events(migrated_db, scope, session_id, job_id) == []


async def test_postgres_fail_exhausted_is_terminal_and_exactly_once(
    migrated_db: AsyncEngine,
) -> None:
    scope, session_id = "scope:exhausted", "target-exhausted"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    job_id = await _pg_job(
        store, "exhausted", max_attempts=1, target_session_id=session_id
    )
    assert await store.claim(job_id, _NOW, 10) is not None
    failed = await store.fail_exhausted(job_id, _NOW + timedelta(seconds=11))
    assert failed is not None
    assert failed.status is JobStatus.failed
    assert failed.error_kind == "attempts_exhausted"
    assert await store.fail_exhausted(job_id, _NOW + timedelta(seconds=12)) is None
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_postgres.py -v
```

Expected: finalizer/cancel methods are missing.

- [ ] **Step 3: Import the transactional append seam**:

```python
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.state import append_event_in_transaction
```

- [ ] **Step 4: Implement the shared terminal transaction**. The actual helper must contain
  these exact phases in this order:

```python
    async def _finalize_in_transaction(
        self,
        conn: AsyncConnection,
        *,
        job_id: str,
        status: JobStatus,
        now: datetime,
        lease_token: str | None = None,
        result: JobResult | None = None,
        error: JobError | None = None,
        exhaustion: bool = False,
        locked: Mapping[str, Any] | None = None,
    ) -> JobRecord | None:
        if locked is None:
            locked = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM jobs WHERE id = :id AND scope_id = :scope "
                            "FOR UPDATE"
                        ),
                        {"id": job_id, "scope": self._scope_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        if locked is None:
            return None
        current = _to_job_record(locked)
        if exhaustion:
            if (
                current.status is not JobStatus.running
                or current.lease_expires_at is None
                or current.lease_expires_at > now
                or current.attempt < current.max_attempts
            ):
                return None
        elif lease_token is not None:
            if (
                current.status is not JobStatus.running
                or current.lease_token != lease_token
            ):
                raise JobLeaseLostError(job_id)
        elif current.status is not JobStatus.queued:
            raise JobLeaseLostError(job_id)

        safe_result = (
            None
            if result is None
            else self._limits.validate_result(result.data)
        )
        safe_error = (
            None
            if error is None
            else JobError(error.kind, self._limits.error_message(error.message))
        )
        text_value = self._limits.result_message(
            _terminal_message(
                current, status, result=result, error=safe_error
            )
        )
        injected_seq = current.injected_event_seq
        if current.target_session_id is not None and injected_seq is None:
            target = (
                await conn.execute(
                    text(
                        "SELECT id FROM sessions WHERE id = :session "
                        "AND scope_id = :scope FOR UPDATE"
                    ),
                    {
                        "session": current.target_session_id,
                        "scope": self._scope_id,
                    },
                )
            ).one_or_none()
            if target is None:
                raise JobValidationError(
                    "target_session_not_found",
                    "target session does not exist in the current scope",
                )
            injected_seq = await append_event_in_transaction(
                conn,
                _injection_event(current, status, text_value, now),
                require_existing_session=True,
            )

        stored_result = safe_result if status is JobStatus.succeeded else None
        result_assignment = "result = NULL"
        params: dict[str, Any] = {
            "status": status.value,
            "result_message": text_value,
            "error_kind": None if safe_error is None else safe_error.kind,
            "error_message": None if safe_error is None else safe_error.message,
            "injected_seq": injected_seq,
            "now": now,
            "id": job_id,
            "scope": self._scope_id,
        }
        if stored_result is not None:
            result_assignment = "result = CAST(:result AS jsonb)"
            params["result"] = json.dumps(stored_result, ensure_ascii=False)
        update_sql = text(
            "UPDATE jobs SET status = :status, "
            "lease_token = NULL, lease_expires_at = NULL, heartbeat_at = NULL, "
            f"{result_assignment}, result_message = :result_message, "
            "error_kind = :error_kind, error_message = :error_message, "
            "injected_event_seq = :injected_seq, updated_at = :now, "
            "finished_at = :now "
            "WHERE id = :id AND scope_id = :scope RETURNING *"
        )
        row = (
            (await conn.execute(update_sql, params))
            .mappings()
            .one()
        )
        return _to_job_record(row)
```

The two fixed `result_assignment` variants are mandatory: failure/cancellation writes SQL `NULL`;
success binds serialized JSON. Do not pass Python `None` through a JSON cast.

- [ ] **Step 5: Implement request-cancel around the shared finalizer**:

```python
    async def request_cancel(self, job_id: str, now: datetime) -> JobRecord | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            locked = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM jobs WHERE id = :id AND scope_id = :scope "
                            "FOR UPDATE"
                        ),
                        {"id": job_id, "scope": self._scope_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if locked is None:
                return None
            record = _to_job_record(locked)
            if record.status is JobStatus.queued:
                return await self._finalize_in_transaction(
                    conn,
                    job_id=job_id,
                    status=JobStatus.cancelled,
                    now=now,
                    locked=locked,
                )
            if record.status is JobStatus.running:
                row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE jobs SET "
                                "cancel_requested_at = COALESCE(cancel_requested_at, :now), "
                                "updated_at = :now "
                                "WHERE id = :id AND scope_id = :scope RETURNING *"
                            ),
                            {
                                "now": now,
                                "id": job_id,
                                "scope": self._scope_id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                return _to_job_record(row)
            return record
```

The `locked=locked` argument is required. `_finalize_in_transaction()` must use that mapping
directly, so this path contains exactly one job-row `SELECT ... FOR UPDATE` and one transaction.

- [ ] **Step 6: Implement terminal entry points**:

```python
    async def succeed(
        self, lease: JobLease, result: JobResult, now: datetime
    ) -> JobRecord:
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = await self._finalize_in_transaction(
                conn,
                job_id=lease.job_id,
                status=JobStatus.succeeded,
                now=now,
                lease_token=lease.token,
                result=result,
            )
        if row is None:
            raise JobLeaseLostError(lease.job_id)
        return row

    async def fail_terminal(
        self, lease: JobLease, error: JobError, now: datetime
    ) -> JobRecord:
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = await self._finalize_in_transaction(
                conn,
                job_id=lease.job_id,
                status=JobStatus.failed,
                now=now,
                lease_token=lease.token,
                error=error,
            )
        if row is None:
            raise JobLeaseLostError(lease.job_id)
        return row

    async def finish_cancelled(self, lease: JobLease, now: datetime) -> JobRecord:
        if lease.scope_id != self._scope_id:
            raise JobLeaseLostError(lease.job_id)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = await self._finalize_in_transaction(
                conn,
                job_id=lease.job_id,
                status=JobStatus.cancelled,
                now=now,
                lease_token=lease.token,
            )
        if row is None:
            raise JobLeaseLostError(lease.job_id)
        return row

    async def fail_exhausted(self, job_id: str, now: datetime) -> JobRecord | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            return await self._finalize_in_transaction(
                conn,
                job_id=job_id,
                status=JobStatus.failed,
                now=now,
                error=JobError(
                    "attempts_exhausted",
                    "job attempts were exhausted after worker lease expiry",
                ),
                exhaustion=True,
            )
```

- [ ] **Step 7: Run GREEN and all store parity tests**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py tests\integration\test_jobs_postgres.py -v
```

Expected: all in-memory and Postgres state-machine tests pass; rollback test proves neither
terminal row nor injected event survives a failed transaction.

- [ ] **Step 8: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\jobs.py tests\unit\test_jobs.py tests\integration\test_jobs_postgres.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\jobs.py tests\unit\test_jobs.py tests\integration\test_jobs_postgres.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 9: Commit**

```bash
git add packages/keel-core/src/keel_core/jobs.py tests/integration/test_jobs_postgres.py
git commit -m "feat(jobs): atomically finalize and inject job results" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 10: Model-facing adjacent plain-assistant coalescing

**Files:**
- Modify: `packages/keel-core/src/keel_core/projections.py`
- Modify: `tests/unit/test_projections.py`

**Interfaces:**
- `project_messages(events)` keeps its signature.
- It merges only adjacent projected messages where both are
  `{"role": "assistant", "content": ...}` and neither carries `tool_calls`.
- It joins content with exactly `"\n\n"`.
- It never alters source events or crosses user/system/tool messages.
- Consumed by the existing `loop._build_request()` for both OpenAI-compatible and Anthropic
  model IDs.

- [ ] **Step 1: Write RED projector tests** — append:

```python
import pytest

from keel_core.agents import AgentSpec, Scope
from keel_core.loop import ToolRegistry, _build_request
from keel_core.state import InMemoryEventStore
from keel_core.types import ScopeKind


def test_adjacent_plain_assistant_events_coalesce_only_in_projection() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "user", "text": "start"}),
        _event(2, EventType.message_token, {"role": "assistant", "text": "answer"}),
        _event(
            3,
            EventType.message_token,
            {
                "role": "assistant",
                "text": "background result",
                "job_id": "job_1",
                "partial": False,
            },
        ),
        _event(4, EventType.message_token, {"role": "user", "text": "continue"}),
    ]
    assert len(events) == 4
    assert project_messages(events) == [
        {"role": "user", "content": "start"},
        {
            "role": "assistant",
            "content": "answer\n\nbackground result",
        },
        {"role": "user", "content": "continue"},
    ]


def test_plain_assistant_does_not_merge_across_tool_call_or_tool_result() -> None:
    events = [
        _event(1, EventType.message_token, {"role": "assistant", "text": "checking"}),
        _event(2, EventType.tool_call, {"tool": "read", "call_id": "c1", "args": {}}),
        _event(3, EventType.tool_result, {"call_id": "c1", "ok": True, "output": "body"}),
        _event(
            4,
            EventType.message_token,
            {"role": "assistant", "text": "background result"},
        ),
    ]
    messages = project_messages(events)
    assert [message["role"] for message in messages] == ["assistant", "tool", "assistant"]
    assert messages[0]["tool_calls"][0]["id"] == "c1"
    assert messages[2]["content"] == "background result"


@pytest.mark.parametrize(
    "model",
    ["openai/gpt-4o-mini", "anthropic/claude-3-5-sonnet-20241022"],
)
async def test_next_provider_request_has_no_adjacent_assistant_roles(model: str) -> None:
    store = InMemoryEventStore()
    for event in [
        _event(1, EventType.message_token, {"role": "user", "text": "start"}),
        _event(2, EventType.message_token, {"role": "assistant", "text": "answer"}),
        _event(
            3,
            EventType.message_token,
            {"role": "assistant", "text": "background result", "job_id": "job_1"},
        ),
        _event(4, EventType.message_token, {"role": "user", "text": "continue"}),
    ]:
        await store.append(event)
    agent = AgentSpec(
        id="projection-test",
        name="Projection Test",
        model=model,
        scope=Scope(id="u:1", kind=ScopeKind.personal),
    )

    request = await _build_request(agent, store, "s1", ToolRegistry())
    assert [message["role"] for message in request.messages] == [
        "user",
        "assistant",
        "user",
    ]
    assert request.messages[1]["content"] == "answer\n\nbackground result"
```

When appending the `_event()` instances to `InMemoryEventStore`, their input `seq` values are
reassigned by the store; assertions intentionally depend only on order/content.

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_projections.py -v
```

Expected: adjacent assistant test fails because two separate assistant dictionaries are emitted.

- [ ] **Step 3: Add a model-message append helper** inside `project_messages()`:

```python
    def append_message(message: dict[str, Any]) -> None:
        is_plain_assistant = (
            message.get("role") == "assistant" and "tool_calls" not in message
        )
        previous = messages[-1] if messages else None
        previous_is_plain_assistant = (
            previous is not None
            and previous.get("role") == "assistant"
            and "tool_calls" not in previous
        )
        if is_plain_assistant and previous_is_plain_assistant:
            previous["content"] = (
                f"{str(previous.get('content', ''))}\n\n"
                f"{str(message.get('content', ''))}"
            )
            return
        messages.append(message)
```

Change `flush()` from `messages.append(pending_assistant)` to
`append_message(pending_assistant)`. Change direct user/system and tool-result appends to
`append_message(...)` too, so every boundary decision has one path. Do not merge an assistant
dictionary after `tool_calls` have been attached.

- [ ] **Step 4: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_projections.py tests\unit\test_loop.py tests\unit\test_providers.py -v
```

Expected: all selected tests pass; existing tool-call threading remains unchanged.

- [ ] **Step 5: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\projections.py tests\unit\test_projections.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\projections.py tests\unit\test_projections.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core
```

Expected: exit `0`.

- [ ] **Step 6: Commit**

```bash
git add packages/keel-core/src/keel_core/projections.py tests/unit/test_projections.py
git commit -m "fix(jobs): coalesce adjacent assistant context for providers" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 11: Worker job registry and cooperative `JobContext`

**Files:**
- Create: `packages/keel-worker/src/keel_worker/jobs.py`
- Create: `tests/unit/test_worker_jobs.py`

**Interfaces:**
- Produces `JobHandler`, `JobClock`, `EnqueueJob`, `JobDefinition`, `JobRegistry`,
  and `JobContext` exactly as declared above.
- Registry rejects blank/duplicate kinds, `max_attempts < 1`, and `lease_seconds < 1`.
- `JobContext.progress()` and `checkpoint()` refresh the lease; either raises
  `JobCancellationRequested` when the store reports cancellation.
- Production registration remains empty; this task defines no handler.

- [ ] **Step 1: Write RED registry/context tests** — create:

```python
"""Durable-job worker registry, context and orchestration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from keel_core.jobs import (
    InMemoryJobStore,
    JobCancellationRequested,
    JobResult,
)
from keel_worker.jobs import JobContext, JobDefinition, JobRegistry

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


async def _handler(context: JobContext, payload: dict[str, object]) -> JobResult:
    return JobResult(data={"attempt": context.attempt, **payload}, message="done")


def test_registry_is_empty_by_default_and_rejects_duplicates() -> None:
    registry = JobRegistry()
    assert registry.kinds() == ()
    definition = JobDefinition(kind="test.echo", handler=_handler)
    registry.register(definition)
    assert registry.get("test.echo") is definition
    assert registry.kinds() == ("test.echo",)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(definition)
    with pytest.raises(ValueError):
        JobDefinition(kind="", handler=_handler)
    with pytest.raises(ValueError):
        JobDefinition(kind="test.bad", handler=_handler, max_attempts=0)
    with pytest.raises(ValueError):
        JobDefinition(kind="test.bad", handler=_handler, lease_seconds=0)


async def test_job_context_progress_updates_record_and_exposes_identity() -> None:
    store = InMemoryJobStore("web:local")
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="context",
        max_attempts=3,
        now=_NOW,
    )
    lease = await store.claim(job.id, _NOW, 60)
    assert lease is not None
    context = JobContext(store, lease, clock=lambda: _NOW + timedelta(seconds=5))
    await context.progress(2, total=10, message="batch 1")

    record = await store.get(job.id)
    assert record is not None
    assert (context.job_id, context.scope_id, context.attempt) == (
        job.id,
        "web:local",
        1,
    )
    assert record.progress_current == 2
    assert record.heartbeat_at == _NOW + timedelta(seconds=5)


async def test_job_context_checkpoint_raises_cooperative_cancel() -> None:
    store = InMemoryJobStore("web:local")
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="cancel-context",
        max_attempts=3,
        now=_NOW,
    )
    lease = await store.claim(job.id, _NOW, 60)
    assert lease is not None
    await store.request_cancel(job.id, _NOW + timedelta(seconds=1))
    context = JobContext(store, lease, clock=lambda: _NOW + timedelta(seconds=2))

    with pytest.raises(JobCancellationRequested):
        await context.checkpoint()
    with pytest.raises(JobCancellationRequested):
        await context.progress(1)
```

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_jobs.py -v
```

Expected: collection fails because `keel_worker.jobs` does not exist.

- [ ] **Step 3: Implement registry and aliases** — create the module with:

```python
"""Allow-listed durable-job worker orchestration (ADR-0010)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from keel_core.jobs import (
    JobCancellationRequested,
    JobLease,
    JobResult,
    JobStore,
)

JobHandler = Callable[["JobContext", dict[str, Any]], Awaitable[JobResult]]
JobClock = Callable[[], datetime]
EnqueueJob = Callable[..., Awaitable[None]]


@dataclass(frozen=True)
class JobDefinition:
    kind: str
    handler: JobHandler
    max_attempts: int = 3
    lease_seconds: int = 300

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("job kind must not be empty")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.lease_seconds < 1:
            raise ValueError("lease_seconds must be at least 1")


class JobRegistry:
    def __init__(self) -> None:
        self._definitions: dict[str, JobDefinition] = {}

    def register(self, definition: JobDefinition) -> None:
        if definition.kind in self._definitions:
            raise ValueError(f"job kind already registered: {definition.kind}")
        self._definitions[definition.kind] = definition

    def get(self, kind: str) -> JobDefinition | None:
        return self._definitions.get(kind)

    def kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._definitions))
```

- [ ] **Step 4: Implement `JobContext`**:

```python
class JobContext:
    def __init__(
        self,
        store: JobStore,
        lease: JobLease,
        *,
        clock: JobClock,
    ) -> None:
        self._store = store
        self._lease = lease
        self._clock = clock
        self.job_id = lease.job_id
        self.scope_id = lease.scope_id
        self.attempt = lease.attempt

    async def progress(
        self,
        current: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        update = await self._store.progress(
            self._lease,
            current=current,
            total=total,
            message=message,
            now=self._clock(),
        )
        if update.cancel_requested:
            raise JobCancellationRequested

    async def checkpoint(self) -> None:
        if await self._store.heartbeat(self._lease, self._clock()):
            raise JobCancellationRequested
```

- [ ] **Step 5: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_jobs.py -v
```

Expected: all registry/context tests pass.

- [ ] **Step 6: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-worker\src\keel_worker\jobs.py tests\unit\test_worker_jobs.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-worker\src\keel_worker\jobs.py tests\unit\test_worker_jobs.py
.\.venv\Scripts\python.exe -m mypy packages\keel-worker
```

Expected: exit `0`.

- [ ] **Step 7: Commit**

```bash
git add packages/keel-worker/src/keel_worker/jobs.py tests/unit/test_worker_jobs.py
git commit -m "feat(jobs): add worker registry and job context" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 12: `run_job` execution, scope validation, cancellation, errors, retry, and observability

**Files:**
- Modify: `packages/keel-worker/src/keel_worker/jobs.py`
- Extend: `tests/unit/test_worker_jobs.py`

**Interfaces:**
- Produces `run_job(ctx, scope_id, job_id) -> str`.
- Uses `ctx["job_clock"]` when present, else `datetime.now(UTC)`.
- Advisory pre-read finds the kind/terminal state and lease duration; `claim()` remains the
  atomic authority.
- Unknown kind is a permanent terminal `unknown_job_kind`; no import/eval/dynamic load.
- `asyncio.CancelledError` is re-raised with the row still running.
- Retryable and unknown exceptions requeue while attempts remain, otherwise fail terminal.
- Delayed retry calls `enqueue("run_job", scope_id, job_id, _defer_until=retry_at)` best effort.
- Span `job.execute` sets only id/kind/scope/attempt/status; logs contain only safe metadata.

- [ ] **Step 1: Write RED run tests** — add `asyncio`, `logging`, and `Any`; add the Settings
  import; replace the existing job imports with the combined blocks below; then append the tests:

```python
import asyncio
import logging
from typing import Any

from keel_core.config import Settings
from keel_core.jobs import (
    InMemoryJobStore,
    JobCancellationRequested,
    JobResult,
    JobStatus,
    PermanentJobError,
    RetryableJobError,
)
from keel_worker.jobs import JobContext, JobDefinition, JobRegistry, run_job


class _Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


async def _enqueued_job(
    store: InMemoryJobStore,
    *,
    key: str,
    kind: str = "test.echo",
    payload: dict[str, Any] | None = None,
    max_attempts: int = 3,
) -> str:
    job, _ = await store.enqueue_once(
        kind=kind,
        payload=payload or {},
        target_session_id=None,
        idempotency_key=key,
        max_attempts=max_attempts,
        now=_NOW,
    )
    return job.id


def _ctx(
    store: InMemoryJobStore,
    registry: JobRegistry,
    clock: _Clock,
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]],
) -> dict[str, Any]:
    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        enqueued.append((name, args, options))

    return {
        "jobs": store,
        "job_registry": registry,
        "durable_scope": "web:local",
        "enqueue": enqueue,
        "job_clock": clock,
    }


async def test_run_job_executes_registered_handler_and_succeeds() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    calls: list[tuple[int, dict[str, Any]]] = []

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        calls.append((context.attempt, payload))
        await context.progress(1, total=1, message="done")
        return JobResult(data={"echo": payload["value"]}, message="completed")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=60))
    job_id = await _enqueued_job(
        store, key="success", payload={"value": "secret-value"}
    )
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    result = await run_job(_ctx(store, registry, _Clock(_NOW), enqueued), "web:local", job_id)

    assert result == JobStatus.succeeded.value
    assert calls == [(1, {"value": "secret-value"})]
    row = await store.get(job_id)
    assert row is not None and row.result == {"echo": "secret-value"}
    assert enqueued == []


async def test_run_job_rejects_argument_scope_before_store_access() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    job_id = await _enqueued_job(store, key="scope")
    result = await run_job(
        _ctx(store, registry, _Clock(_NOW), []),
        "scope:other",
        job_id,
    )
    assert result == "scope_mismatch"
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]


async def test_run_job_rejects_store_bound_to_another_scope() -> None:
    store = InMemoryJobStore("scope:store")
    job_id = await _enqueued_job(store, key="store-scope")
    result = await run_job(
        _ctx(store, JobRegistry(), _Clock(_NOW), []),
        "web:local",
        job_id,
    )
    assert result == "scope_mismatch"
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]


async def test_run_job_unknown_kind_is_permanent_and_never_imported() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _enqueued_job(store, key="unknown", kind="python.module:function")
    result = await run_job(
        _ctx(store, JobRegistry(), _Clock(_NOW), []),
        "web:local",
        job_id,
    )
    row = await store.get(job_id)
    assert result == JobStatus.failed.value
    assert row is not None and row.error_kind == "unknown_job_kind"
    assert row.attempt == 1


async def test_run_job_permanent_error_executes_once() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        raise PermanentJobError("invalid_document", "Document is invalid.")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=60))
    job_id = await _enqueued_job(store, key="permanent")
    assert (
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []), "web:local", job_id
        )
        == JobStatus.failed.value
    )
    assert calls == 1
    assert (await store.get(job_id)).error_kind == "invalid_document"  # type: ignore[union-attr]


async def test_run_job_retryable_error_requeues_and_defers_delivery() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("provider_timeout", "Provider timed out.")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=60))
    job_id = await _enqueued_job(store, key="retry")
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    clock = _Clock(_NOW)
    result = await run_job(_ctx(store, registry, clock, enqueued), "web:local", job_id)
    row = await store.get(job_id)
    assert result == JobStatus.queued.value
    assert row is not None and row.next_attempt_at == _NOW + timedelta(seconds=5)
    assert enqueued == [
        (
            "run_job",
            ("web:local", job_id),
            {"_defer_until": _NOW + timedelta(seconds=5)},
        )
    ]


async def test_run_job_observes_persisted_cancel_before_reclaimed_handler() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    called = False

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal called
        called = True
        return JobResult(data={}, message="should not run")

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=10))
    job_id = await _enqueued_job(store, key="cancel")
    first = await store.claim(job_id, _NOW, 10)
    assert first is not None
    await store.request_cancel(job_id, _NOW + timedelta(seconds=1))
    clock = _Clock(_NOW + timedelta(seconds=11))
    result = await run_job(_ctx(store, registry, clock, []), "web:local", job_id)
    assert result == JobStatus.cancelled.value
    assert called is False


async def test_run_job_propagates_asyncio_cancelled_error() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise asyncio.CancelledError

    registry.register(JobDefinition("test.echo", handler, max_attempts=3, lease_seconds=60))
    job_id = await _enqueued_job(store, key="worker-shutdown")
    with pytest.raises(asyncio.CancelledError):
        await run_job(
            _ctx(store, registry, _Clock(_NOW), []), "web:local", job_id
        )
    assert (await store.get(job_id)).status is JobStatus.running  # type: ignore[union-attr]


async def test_run_job_logs_no_payload_or_result(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="keel.worker.jobs")
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()
    registry.register(JobDefinition("test.echo", _handler, lease_seconds=60))
    job_id = await _enqueued_job(
        store, key="redaction", payload={"secret": "DO-NOT-LOG"}
    )
    await run_job(
        _ctx(store, registry, _Clock(_NOW), []), "web:local", job_id
    )
    assert "DO-NOT-LOG" not in caplog.text
    assert "done" not in caplog.text
```

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_jobs.py -v
```

Expected: import fails because `run_job` is not defined.

- [ ] **Step 3: Add execution helpers and safe imports**:

```python
import asyncio
import logging
from datetime import UTC, timedelta
from time import perf_counter

from keel_core.jobs import (
    JobError,
    JobLeaseLostError,
    JobStatus,
    JobValidationError,
    PermanentJobError,
    RetryableJobError,
    retry_delay_seconds,
)
from keel_core.observability import get_tracer

logger = logging.getLogger("keel.worker.jobs")


def _clock(ctx: dict[str, Any]) -> JobClock:
    clock = ctx.get("job_clock")
    if callable(clock):
        return clock
    return lambda: datetime.now(UTC)


async def _current_status(store: JobStore, job_id: str) -> str:
    row = await store.get(job_id)
    return "missing" if row is None else row.status.value
```

- [ ] **Step 4: Implement retry-or-terminal helper**:

```python
async def _retry_or_fail(
    *,
    ctx: dict[str, Any],
    store: JobStore,
    lease: JobLease,
    error: JobError,
    now: datetime,
) -> JobStatus:
    if lease.attempt >= lease.max_attempts:
        row = await store.fail_terminal(lease, error, now)
        return row.status
    settings = ctx["job_settings"]
    retry_at = now + timedelta(
        seconds=retry_delay_seconds(
            lease.attempt,
            settings.job_retry_base_seconds,
            settings.job_retry_max_seconds,
        )
    )
    row = await store.requeue(lease, error, retry_at, now)
    try:
        enqueue: EnqueueJob = ctx["enqueue"]
        await enqueue(
            "run_job",
            lease.scope_id,
            lease.job_id,
            _defer_until=retry_at,
        )
    except Exception:
        logger.warning(
            "job retry enqueue failed scope=%s job=%s kind=%s attempt=%d",
            lease.scope_id,
            lease.job_id,
            lease.kind,
            lease.attempt,
            exc_info=True,
        )
    return row.status
```

Tests construct `Settings()` under `ctx["job_settings"]`; update `_ctx()` accordingly:

```text
        "job_settings": Settings(),
```

- [ ] **Step 5: Implement `run_job`**:

```python
async def run_job(ctx: dict[str, Any], scope_id: str, job_id: str) -> str:
    durable_scope = str(ctx["durable_scope"])
    if scope_id != durable_scope:
        logger.warning(
            "job scope mismatch configured=%s requested=%s job=%s",
            durable_scope,
            scope_id,
            job_id,
        )
        return "scope_mismatch"
    store: JobStore = ctx["jobs"]
    if store.scope_id != durable_scope:
        logger.error(
            "job store scope mismatch configured=%s store=%s",
            durable_scope,
            store.scope_id,
        )
        return "scope_mismatch"
    registry: JobRegistry = ctx["job_registry"]
    clock = _clock(ctx)
    before = await store.get(job_id)
    if before is None:
        return "missing"
    if before.status in {
        JobStatus.succeeded,
        JobStatus.failed,
        JobStatus.cancelled,
    }:
        return before.status.value
    definition = registry.get(before.kind)
    lease_seconds = (
        definition.lease_seconds
        if definition is not None
        else ctx["job_settings"].job_lease_seconds
    )
    lease = await store.claim(job_id, clock(), lease_seconds)
    if lease is None:
        return await _current_status(store, job_id)

    tracer = get_tracer("keel.worker.jobs")
    started = perf_counter()
    final_status = JobStatus.running
    with tracer.start_as_current_span("job.execute") as span:
        span.set_attribute("job.id", lease.job_id)
        span.set_attribute("job.kind", lease.kind)
        span.set_attribute("job.scope_id", lease.scope_id)
        span.set_attribute("job.attempt", lease.attempt)
        try:
            if definition is None:
                row = await store.fail_terminal(
                    lease,
                    JobError(
                        "unknown_job_kind",
                        "job kind is not registered on this worker",
                    ),
                    clock(),
                )
                final_status = row.status
                return row.status.value
            context = JobContext(store, lease, clock=clock)
            await context.checkpoint()
            result = await definition.handler(context, lease.payload)
            if not isinstance(result, JobResult):
                raise PermanentJobError(
                    "invalid_job_result",
                    "job handler must return JobResult",
                )
            row = await store.succeed(lease, result, clock())
            final_status = row.status
            return row.status.value
        except asyncio.CancelledError:
            raise
        except JobCancellationRequested:
            row = await store.finish_cancelled(lease, clock())
            final_status = row.status
            return row.status.value
        except PermanentJobError as exc:
            row = await store.fail_terminal(
                lease, JobError(exc.code, exc.public_message), clock()
            )
            final_status = row.status
            return row.status.value
        except JobValidationError as exc:
            row = await store.fail_terminal(
                lease, JobError(exc.code, exc.public_message), clock()
            )
            final_status = row.status
            return row.status.value
        except RetryableJobError as exc:
            final_status = await _retry_or_fail(
                ctx=ctx,
                store=store,
                lease=lease,
                error=JobError(exc.code, exc.public_message),
                now=clock(),
            )
            return final_status.value
        except JobLeaseLostError:
            status_value = await _current_status(store, job_id)
            try:
                final_status = JobStatus(status_value)
            except ValueError:
                pass
            return status_value
        except Exception:
            logger.exception(
                "job handler raised scope=%s job=%s kind=%s attempt=%d",
                lease.scope_id,
                lease.job_id,
                lease.kind,
                lease.attempt,
            )
            final_status = await _retry_or_fail(
                ctx=ctx,
                store=store,
                lease=lease,
                error=JobError(
                    "internal_error",
                    "job failed with a temporary internal error",
                ),
                now=clock(),
            )
            return final_status.value
        finally:
            span.set_attribute("job.status", final_status.value)
            logger.info(
                "job transition scope=%s job=%s kind=%s attempt=%d status=%s duration_ms=%d",
                lease.scope_id,
                lease.job_id,
                lease.kind,
                lease.attempt,
                final_status.value,
                int((perf_counter() - started) * 1000),
            )
```

The `logger.exception` traceback stays in process logs for operators, but the persisted
`error_message` remains the generic public sentence. The log statement must never interpolate
payload/result/message.

- [ ] **Step 6: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_jobs.py -v
```

Expected: all execution tests pass, including delayed enqueue options, scope rejection,
cooperative cancel, and propagated `CancelledError`.

- [ ] **Step 7: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-worker\src\keel_worker\jobs.py tests\unit\test_worker_jobs.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-worker\src\keel_worker\jobs.py tests\unit\test_worker_jobs.py
.\.venv\Scripts\python.exe -m mypy packages\keel-worker
```

Expected: exit `0`.

- [ ] **Step 8: Commit**

```bash
git add packages/keel-worker/src/keel_worker/jobs.py tests/unit/test_worker_jobs.py
git commit -m "feat(jobs): execute registered jobs with bounded retry" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 13: Dispatcher recovery, crash-attempt exhaustion, and retry edge cases

**Files:**
- Modify: `packages/keel-worker/src/keel_worker/jobs.py`
- Extend: `tests/unit/test_worker_jobs.py`

**Interfaces:**
- Produces `dispatch_jobs(ctx) -> int`.
- It queries one configured scope, best-effort enqueues every dispatchable ID, then individually
  calls `fail_exhausted()` for expired rows at the ceiling.
- Return value is successful Redis enqueue count plus successfully terminalized exhaustion count.
- One enqueue/finalizer failure is logged and does not stop later rows.
- Last-attempt retryable/unknown error calls `fail_terminal()` directly and never leaves an
  orphan queued row.

- [ ] **Step 1: Write RED dispatcher/exhaustion tests** — add `dispatch_jobs` to the existing
  combined `keel_worker.jobs` import, then append:

```python
async def test_retryable_last_attempt_fails_terminal_without_delayed_enqueue() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("provider_timeout", "Provider timed out.")

    registry.register(JobDefinition("test.echo", handler, max_attempts=1, lease_seconds=10))
    job_id = await _enqueued_job(store, key="last-attempt", max_attempts=1)
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    result = await run_job(
        _ctx(store, registry, _Clock(_NOW), enqueued), "web:local", job_id
    )
    row = await store.get(job_id)
    assert result == JobStatus.failed.value
    assert row is not None and row.status is JobStatus.failed
    assert row.error_kind == "provider_timeout"
    assert enqueued == []


async def test_failed_deferred_enqueue_leaves_due_job_for_dispatcher() -> None:
    store = InMemoryJobStore("web:local")
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise RetryableJobError("provider_timeout", "Provider timed out.")

    async def unavailable(
        name: str, *args: object, **options: object
    ) -> None:
        raise RuntimeError("redis unavailable")

    registry.register(JobDefinition("test.echo", handler, lease_seconds=10))
    job_id = await _enqueued_job(store, key="lost-defer")
    ctx = _ctx(store, registry, _Clock(_NOW), [])
    ctx["enqueue"] = unavailable
    assert await run_job(ctx, "web:local", job_id) == JobStatus.queued.value
    assert job_id in await store.dispatchable(_NOW + timedelta(seconds=5), 100)


async def test_dispatcher_enqueues_due_recovery_and_finalizes_exhaustion() -> None:
    store = InMemoryJobStore("web:local")
    due_id = await _enqueued_job(store, key="dispatch-due")
    exhausted_id = await _enqueued_job(
        store, key="dispatch-exhausted", max_attempts=1
    )
    assert await store.claim(exhausted_id, _NOW, 10) is not None
    clock = _Clock(_NOW + timedelta(seconds=11))
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    ctx = _ctx(store, JobRegistry(), clock, enqueued)

    assert await dispatch_jobs(ctx) == 2
    assert enqueued == [("run_job", ("web:local", due_id), {})]
    exhausted = await store.get(exhausted_id)
    assert exhausted is not None and exhausted.status is JobStatus.failed
    assert exhausted.error_kind == "attempts_exhausted"


async def test_dispatcher_continues_after_one_enqueue_failure() -> None:
    store = InMemoryJobStore("web:local")
    first = await _enqueued_job(store, key="dispatch-1")
    second = await _enqueued_job(store, key="dispatch-2")
    attempted: list[str] = []

    async def flaky(
        name: str, *args: object, **options: object
    ) -> None:
        job_id = str(args[1])
        attempted.append(job_id)
        if job_id == first:
            raise RuntimeError("first failed")

    ctx = {
        "jobs": store,
        "job_registry": JobRegistry(),
        "durable_scope": "web:local",
        "enqueue": flaky,
        "job_clock": _Clock(_NOW),
        "job_settings": Settings(job_dispatch_limit=100),
    }
    assert await dispatch_jobs(ctx) == 1
    assert set(attempted) == {first, second}


async def test_dispatcher_fails_closed_on_store_scope_mismatch() -> None:
    store = InMemoryJobStore("scope:store")
    job_id = await _enqueued_job(store, key="dispatch-scope")
    enqueued: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    ctx = _ctx(store, JobRegistry(), _Clock(_NOW), enqueued)

    assert await dispatch_jobs(ctx) == 0
    assert enqueued == []
    assert (await store.get(job_id)).status is JobStatus.queued  # type: ignore[union-attr]
```

Reuse the `Settings` import and `_ctx()` entry added in Task 12.

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_jobs.py -v
```

Expected: import fails because `dispatch_jobs` is not defined.

- [ ] **Step 3: Implement dispatcher**:

```python
async def dispatch_jobs(ctx: dict[str, Any]) -> int:
    store: JobStore = ctx["jobs"]
    scope_id = str(ctx["durable_scope"])
    if store.scope_id != scope_id:
        logger.error(
            "job dispatcher scope mismatch configured=%s store=%s",
            scope_id,
            store.scope_id,
        )
        return 0
    settings = ctx["job_settings"]
    now = _clock(ctx)()
    enqueue: EnqueueJob = ctx["enqueue"]
    processed = 0

    for job_id in await store.dispatchable(now, settings.job_dispatch_limit):
        try:
            await enqueue("run_job", scope_id, job_id)
            processed += 1
        except Exception:
            logger.warning(
                "job dispatch enqueue failed scope=%s job=%s",
                scope_id,
                job_id,
                exc_info=True,
            )

    for job_id in await store.exhausted(now, settings.job_dispatch_limit):
        try:
            row = await store.fail_exhausted(job_id, now)
            if row is not None:
                processed += 1
        except Exception:
            logger.exception(
                "job exhaustion finalizer failed scope=%s job=%s",
                scope_id,
                job_id,
            )
    return processed
```

- [ ] **Step 4: Confirm `_retry_or_fail` last-attempt branch precedes requeue** and delayed
  enqueue is wrapped exactly as in Task 12. No arq retry exception should escape after the DB row
  has returned to `queued`.

- [ ] **Step 5: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_jobs.py -v
```

Expected: all worker unit tests pass.

- [ ] **Step 6: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-worker\src\keel_worker\jobs.py tests\unit\test_worker_jobs.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-worker\src\keel_worker\jobs.py tests\unit\test_worker_jobs.py
.\.venv\Scripts\python.exe -m mypy packages\keel-worker
```

Expected: exit `0`.

- [ ] **Step 7: Commit**

```bash
git add packages/keel-worker/src/keel_worker/jobs.py tests/unit/test_worker_jobs.py
git commit -m "feat(jobs): recover delivery and finalize exhausted jobs" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 14: Worker/server construction, empty production registry, keyword-option enqueue, and cron wiring

**Files:**
- Modify: `packages/keel-worker/src/keel_worker/main.py`
- Modify: `packages/keel-server/src/keel_server/app.py`
- Modify: `tests/unit/test_worker_tasks.py`
- Modify: `tests/unit/test_server.py`

**Interfaces:**
- Worker startup adds `ctx["durable_scope"]`, `ctx["jobs"]`, `ctx["job_registry"]`,
  `ctx["job_settings"]`, and a keyword-forwarding `ctx["enqueue"]`.
- Server lifespan adds `app.state.jobs` and passes the same `_DURABLE_SCOPE` explicitly to
  `AgentRuntime`, approvals, and the jobs store.
- Both arq adapters accept `async enqueue(name, *args, **options)`.
- `WorkerSettings.functions` contains `run_job` and `dispatch_jobs`; cron runs dispatcher at
  seconds `{0, 30}`.
- The production registry is provably empty.

- [ ] **Step 1: Write RED wiring tests** — replace the existing `keel_worker.main` import with the
  combined import below, add the `keel_worker.jobs` import, then append the tests:

```python
from keel_worker.jobs import JobRegistry, dispatch_jobs, run_job
from keel_worker.main import (
    WorkerSettings,
    _empty_job_registry,
    _enqueue_arq,
    resume_run,
    run_agent,
    scheduler_tick,
)


def test_worker_registers_job_functions_and_dispatch_cron() -> None:
    assert run_job in WorkerSettings.functions
    assert dispatch_jobs in WorkerSettings.functions
    dispatch_cron = next(
        job for job in WorkerSettings.cron_jobs if job.coroutine is dispatch_jobs
    )
    assert dispatch_cron.second == {0, 30}


def test_production_job_registry_is_empty() -> None:
    registry = _empty_job_registry()
    assert isinstance(registry, JobRegistry)
    assert registry.kinds() == ()


async def test_worker_enqueue_adapter_forwards_arq_options() -> None:
    seen: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    class Pool:
        async def enqueue_job(
            self, name: str, *args: object, **options: object
        ) -> None:
            seen.append((name, args, options))

    defer_until = _NOW + timedelta(seconds=5)
    await _enqueue_arq(
        Pool(),
        "run_job",
        "web:local",
        "job_1",
        _defer_until=defer_until,
    )
    assert seen == [
        (
            "run_job",
            ("web:local", "job_1"),
            {"_defer_until": defer_until},
        )
    ]
```

In `tests/unit/test_server.py`, replace `from keel_server.app import create_app` with the combined
import below, add the other imports, define `_NOW`, then append the tests:

```python
from sqlalchemy.ext.asyncio import create_async_engine

from keel_core.config import Settings
from keel_core.jobs import InMemoryJobStore, PostgresJobStore
from keel_server.app import _build_job_store, _enqueue_arq, create_app


async def test_server_builds_scope_bound_job_store_for_both_profiles() -> None:
    settings = Settings()
    memory = _build_job_store(None, "web:local", settings)
    assert isinstance(memory, InMemoryJobStore)
    assert memory.scope_id == "web:local"

    engine = create_async_engine("postgresql+psycopg://keel:keel@localhost:5432/keel_test")
    assert engine.url.database == "keel_test"
    try:
        postgres = _build_job_store(engine, "web:local", settings)
        assert isinstance(postgres, PostgresJobStore)
        assert postgres.scope_id == "web:local"
    finally:
        await engine.dispose()


async def test_server_enqueue_adapter_forwards_keyword_options() -> None:
    seen: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    class Pool:
        async def enqueue_job(
            self, name: str, *args: object, **options: object
        ) -> None:
            seen.append((name, args, options))

    await _enqueue_arq(
        Pool(), "run_job", "web:local", "job_1", _defer_until=_NOW
    )
    assert seen == [
        ("run_job", ("web:local", "job_1"), {"_defer_until": _NOW})
    ]
```

Reuse the existing `_NOW` in `test_worker_tasks.py`; add this below the imports in
`test_server.py`:

```python
_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)
```

- [ ] **Step 2: Run RED**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_tasks.py tests\unit\test_server.py -v
```

Expected: imports fail for wiring helpers and job functions are absent from WorkerSettings.

- [ ] **Step 3: Add worker helpers/imports** to `keel_worker.main`:

```python
from keel_core.jobs import JobLimits, PostgresJobStore
from keel_worker.jobs import JobRegistry, dispatch_jobs, run_job


def _empty_job_registry() -> JobRegistry:
    # Intentionally empty until the RAG slice registers the first real kind.
    return JobRegistry()


async def _enqueue_arq(
    redis: Any, name: str, *args: object, **options: object
) -> None:
    await redis.enqueue_job(name, *args, **options)
```

Keep the existing single scope constant and rename it only if all uses change together:

```python
_DURABLE_SCOPE = "web:local"
```

- [ ] **Step 4: Extend worker startup** after creating the engine:

```python
    ctx["durable_scope"] = _DURABLE_SCOPE
    ctx["job_settings"] = settings
    ctx["jobs"] = PostgresJobStore(
        engine,
        _DURABLE_SCOPE,
        limits=JobLimits.from_settings(settings),
    )
    ctx["job_registry"] = _empty_job_registry()

    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        await _enqueue_arq(redis, name, *args, **options)

    ctx["enqueue"] = enqueue
```

Update existing event/approval/schedule stores to use `_DURABLE_SCOPE`; remove the old lambda
that dropped keyword options.

- [ ] **Step 5: Extend WorkerSettings**:

```python
    functions = [run_agent, resume_run, scheduler_tick, run_job, dispatch_jobs]
    cron_jobs = [
        cron(scheduler_tick, second={0, 30}),
        cron(dispatch_jobs, second={0, 30}),
    ]
```

The two crons may share cadence; their DB predicates and arq cron locking make repeated ticks
safe.

- [ ] **Step 6: Add server store/enqueue helpers**. Replace the existing config import with
  `from keel_core.config import Settings, get_settings, load_env_file`, then add:

```python
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings, get_settings, load_env_file
from keel_core.jobs import (
    InMemoryJobStore,
    JobLimits,
    JobStore,
    PostgresJobStore,
)

_DURABLE_SCOPE = "web:local"


def _build_job_store(
    engine: AsyncEngine | None,
    scope_id: str,
    settings: Settings,
) -> JobStore:
    limits = JobLimits.from_settings(settings)
    if engine is None:
        return InMemoryJobStore(scope_id, limits=limits)
    return PostgresJobStore(engine, scope_id, limits=limits)


async def _enqueue_arq(
    pool: Any, name: str, *args: object, **options: object
) -> None:
    await pool.enqueue_job(name, *args, **options)
```

The first slice has no production job kind, so the memory/lite store intentionally has no
target-session event store yet. When RAG registers the first production kind, its lite-profile
wiring must pass the runtime's `InMemoryEventStore` into `InMemoryJobStore(events=...)` before
allowing a non-null `target_session_id`.

In lifespan, set scope once and wire the store:

```python
    app.state.durable_scope = _DURABLE_SCOPE
    app.state.jobs = _build_job_store(engine, _DURABLE_SCOPE, settings)
```

Move `_DURABLE_SCOPE` above `_lifespan()`, pass `scope_id=_DURABLE_SCOPE` in the existing
`AgentRuntime(...)` constructor, and replace the literal approval-store scope with
`PostgresApprovalStore(engine, _DURABLE_SCOPE)`. There must be no server jobs/runtime/approval
construction path that relies on `AgentRuntime`'s default scope argument.

Replace the current arq closure with:

```python
        async def _enqueue(
            name: str, *args: object, **options: object
        ) -> None:
            await _enqueue_arq(arq_pool, name, *args, **options)
```

Keep approval store construction and all current shutdown behavior intact.

- [ ] **Step 7: Run GREEN**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_tasks.py tests\unit\test_server.py -v
```

Expected: all tests pass; production registry has zero kinds and both adapters preserve
`_defer_until`.

- [ ] **Step 8: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check packages\keel-worker\src\keel_worker\main.py packages\keel-server\src\keel_server\app.py tests\unit\test_worker_tasks.py tests\unit\test_server.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-worker\src\keel_worker\main.py packages\keel-server\src\keel_server\app.py tests\unit\test_worker_tasks.py tests\unit\test_server.py
.\.venv\Scripts\python.exe -m mypy packages\keel-worker packages\keel-server
```

Expected: exit `0`.

- [ ] **Step 9: Commit**

```bash
git add packages/keel-worker/src/keel_worker/main.py packages/keel-server/src/keel_server/app.py tests/unit/test_worker_tasks.py tests/unit/test_server.py
git commit -m "feat(jobs): wire durable stores registry and dispatcher" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 15: Strict job read DTOs and list/detail/cancel API with RBAC

**Files:**
- Modify: `packages/keel-core/src/keel_core/api.py`
- Modify: `packages/keel-server/src/keel_server/api/v1.py`
- Create: `tests/integration/test_jobs_api.py`

**Interfaces:**
- Produces `JobResponse.from_record(record)`.
- `GET /v1/jobs`: viewer, optional typed `status`, exact `kind`, `limit` 1–100,
  newest first/current scope only.
- `GET /v1/jobs/{job_id}`: viewer; missing/cross-scope returns 404.
- `POST /v1/jobs/{job_id}/cancel`: operator; queued returns cancelled, running returns
  running with `cancel_requested=true`, terminal returns current record.
- Response intentionally omits payload, `scope_id`, `idempotency_key`, and `lease_token`;
  result/error fields are already bounded by stores.
- No POST create, retry, or inject route.

- [ ] **Step 1: Write RED API integration tests** — create:

```python
"""Integration: scope-bound jobs list/detail/cancel API and RBAC."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.jobs import PostgresJobStore
from keel_server.api.v1 import router
from keel_server.auth import parse_api_keys

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


@pytest_asyncio.fixture
async def jobs_client(
    migrated_db: AsyncEngine,
) -> AsyncIterator[tuple[httpx.AsyncClient, str, PostgresJobStore]]:
    scope = "api:jobs"
    store = PostgresJobStore(migrated_db, scope)
    app = FastAPI()
    app.include_router(router)
    app.state.durable_scope = scope
    app.state.jobs = store
    app.state.api_keys = parse_api_keys("vw:viewer,op:operator")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client, scope, store


async def _seed(
    store: PostgresJobStore,
    key: str,
    *,
    kind: str = "test.echo",
    now: datetime = _NOW,
) -> str:
    row, _ = await store.enqueue_once(
        kind=kind,
        payload={"private": "not exposed"},
        target_session_id=None,
        idempotency_key=key,
        max_attempts=3,
        now=now,
    )
    return row.id


async def test_viewer_lists_filters_and_reads_detail(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    older = await _seed(store, "older", kind="test.a")
    newer = await _seed(
        store, "newer", kind="test.b", now=_NOW + timedelta(seconds=1)
    )
    headers = {"X-API-Key": "vw"}

    response = await client.get("/v1/jobs", headers=headers)
    assert response.status_code == 200
    assert [row["id"] for row in response.json()] == [newer, older]
    assert "payload" not in response.json()[0]
    assert "scope_id" not in response.json()[0]
    assert "lease_token" not in response.json()[0]
    assert "idempotency_key" not in response.json()[0]

    filtered = await client.get(
        "/v1/jobs", params={"kind": "test.a", "status": "queued", "limit": 1},
        headers=headers,
    )
    assert [row["id"] for row in filtered.json()] == [older]
    assert (
        await client.get("/v1/jobs", params={"limit": 0}, headers=headers)
    ).status_code == 422
    assert (
        await client.get("/v1/jobs", params={"limit": 101}, headers=headers)
    ).status_code == 422
    detail = await client.get(f"/v1/jobs/{older}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()["status"] == "queued"
    assert detail.json()["cancel_requested"] is False


async def test_jobs_api_hides_missing_and_cross_scope_rows(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
    migrated_db: AsyncEngine,
) -> None:
    client, _, _ = jobs_client
    other_id = await _seed(PostgresJobStore(migrated_db, "api:other"), "other")
    headers = {"X-API-Key": "vw"}
    assert (await client.get("/v1/jobs/missing", headers=headers)).status_code == 404
    assert (await client.get(f"/v1/jobs/{other_id}", headers=headers)).status_code == 404
    assert (
        await client.post(
            f"/v1/jobs/{other_id}/cancel", headers={"X-API-Key": "op"}
        )
    ).status_code == 404


@pytest.mark.parametrize("configured_scope", [None, "api:wrong"])
async def test_jobs_api_requires_explicit_matching_scope(
    migrated_db: AsyncEngine,
    configured_scope: str | None,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.state.jobs = PostgresJobStore(migrated_db, "api:expected")
    app.state.api_keys = parse_api_keys("vw:viewer")
    if configured_scope is not None:
        app.state.durable_scope = configured_scope
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/v1/jobs", headers={"X-API-Key": "vw"})
    assert response.status_code == 503


async def test_viewer_cannot_cancel_operator_can_and_terminal_is_idempotent(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    job_id = await _seed(store, "cancel")
    assert (
        await client.post(
            f"/v1/jobs/{job_id}/cancel", headers={"X-API-Key": "vw"}
        )
    ).status_code == 403

    first = await client.post(
        f"/v1/jobs/{job_id}/cancel", headers={"X-API-Key": "op"}
    )
    second = await client.post(
        f"/v1/jobs/{job_id}/cancel", headers={"X-API-Key": "op"}
    )
    assert first.status_code == 200
    assert first.json()["status"] == "cancelled"
    assert second.json()["status"] == "cancelled"


async def test_running_cancel_response_exposes_request_flag(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    job_id = await _seed(store, "running-cancel")
    assert await store.claim(job_id, _NOW, 60) is not None
    response = await client.post(
        f"/v1/jobs/{job_id}/cancel", headers={"X-API-Key": "op"}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "running"
    assert response.json()["cancel_requested"] is True


async def test_no_generic_create_retry_or_inject_routes(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, _ = jobs_client
    headers = {"X-API-Key": "op"}
    assert (await client.post("/v1/jobs", headers=headers, json={})).status_code == 405
    assert (await client.post("/v1/jobs/job_1/retry", headers=headers)).status_code == 404
    assert (await client.post("/v1/jobs/job_1/inject", headers=headers)).status_code == 404
```

- [ ] **Step 2: Run RED**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_api.py -v
```

Expected: requests return 404 because routes/DTO do not exist.

- [ ] **Step 3: Add strict DTO** — import `datetime`, `Any`, `ConfigDict`, `JobRecord`, and
  `JobStatus` in `keel_core.api`, then add:

```python
class JobResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    kind: str
    status: JobStatus
    target_session_id: str | None
    attempt: int
    max_attempts: int
    next_attempt_at: datetime
    lease_expires_at: datetime | None
    cancel_requested: bool
    progress_current: int
    progress_total: int | None
    progress_message: str | None
    progress_updated_at: datetime | None
    result: dict[str, Any] | None
    result_message: str | None
    error_kind: str | None
    error_message: str | None
    injected_event_seq: int | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    @classmethod
    def from_record(cls, record: JobRecord) -> JobResponse:
        return cls(
            id=record.id,
            kind=record.kind,
            status=record.status,
            target_session_id=record.target_session_id,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            next_attempt_at=record.next_attempt_at,
            lease_expires_at=record.lease_expires_at,
            cancel_requested=record.cancel_requested_at is not None,
            progress_current=record.progress_current,
            progress_total=record.progress_total,
            progress_message=record.progress_message,
            progress_updated_at=record.progress_updated_at,
            result=record.result,
            result_message=record.result_message,
            error_kind=record.error_kind,
            error_message=record.error_message,
            injected_event_seq=record.injected_event_seq,
            created_at=record.created_at,
            updated_at=record.updated_at,
            started_at=record.started_at,
            finished_at=record.finished_at,
        )
```

- [ ] **Step 4: Add explicit store/scope helper** to `v1.py`:

```python
from datetime import UTC, datetime

from keel_core.api import JobResponse
from keel_core.jobs import JobStatus, JobStore


def _jobs(request: Request) -> JobStore:
    store: JobStore | None = getattr(request.app.state, "jobs", None)
    scope: object = getattr(request.app.state, "durable_scope", None)
    if store is None or not isinstance(scope, str) or not scope:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "jobs datastore unavailable"
        )
    if store.scope_id != scope:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "jobs scope misconfigured"
        )
    return store
```

- [ ] **Step 5: Add the three routes**:

```python
@router.get("/jobs", response_model=list[JobResponse], summary="List durable jobs")
async def list_jobs(
    request: Request,
    status_filter: JobStatus | None = Query(None, alias="status"),
    kind: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
) -> list[JobResponse]:
    rows = await _jobs(request).list(
        status=status_filter,
        kind=kind,
        limit=limit,
    )
    return [JobResponse.from_record(row) for row in rows]


@router.get(
    "/jobs/{job_id}",
    response_model=JobResponse,
    summary="Get a durable job",
)
async def get_job(job_id: str, request: Request) -> JobResponse:
    row = await _jobs(request).get(job_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return JobResponse.from_record(row)


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobResponse,
    summary="Request durable-job cancellation",
    dependencies=[Depends(require_role(Role.operator))],
)
async def cancel_job(job_id: str, request: Request) -> JobResponse:
    row = await _jobs(request).request_cancel(job_id, datetime.now(UTC))
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return JobResponse.from_record(row)
```

Do not add a request body or generic mutation route.

- [ ] **Step 6: Run GREEN**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_api.py -v
```

Expected: all API/RBAC tests pass.

- [ ] **Step 7: Regression + quality gates**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
.\.venv\Scripts\python.exe -m pytest tests\unit\test_auth.py tests\unit\test_server.py tests\integration\test_jobs_api.py tests\integration\test_memory_proposals_api.py tests\integration\test_schedules_api.py -v
.\.venv\Scripts\python.exe -m ruff check packages\keel-core\src\keel_core\api.py packages\keel-server\src\keel_server\api\v1.py tests\integration\test_jobs_api.py
.\.venv\Scripts\python.exe -m ruff format --check packages\keel-core\src\keel_core\api.py packages\keel-server\src\keel_server\api\v1.py tests\integration\test_jobs_api.py
.\.venv\Scripts\python.exe -m mypy packages\keel-core packages\keel-server
```

Expected: all commands exit `0`.

- [ ] **Step 8: Commit**

```bash
git add packages/keel-core/src/keel_core/api.py packages/keel-server/src/keel_server/api/v1.py tests/integration/test_jobs_api.py
git commit -m "feat(jobs): expose list detail and cancel APIs" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 16: End-to-end Postgres + Redis/arq acceptance for lost/duplicate/retry/crash/cancel/injection

**Files:**
- Create: `tests/integration/test_jobs_worker.py`

**Interfaces:**
- Consumes the production `PostgresJobStore`, `JobRegistry`, `run_job`, and
  `dispatch_jobs`; handlers exist only as closures inside this test module.
- Uses real `keel_test` Postgres and real test Redis/arq with a unique queue name.
- Proves: missed immediate enqueue recovery, actual arq delivery, duplicate claim exclusion,
  two retries then success, permanent failure once, worker cancellation/lease reclaim,
  cooperative cancel, exhaustion failure injection, no automatic provider run, and legal
  next-turn projection.
- Produces no production job kind or source behavior.

- [ ] **Step 1: Create acceptance helpers and the actual arq smoke test**:

```python
"""Acceptance: durable jobs over real Postgres + Redis/arq with injected handlers."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from arq.connections import RedisSettings, create_pool
from arq.worker import Worker
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.events import Event, EventType
from keel_core.jobs import (
    JobResult,
    JobStatus,
    PermanentJobError,
    PostgresJobStore,
    RetryableJobError,
)
from keel_core.loop import admit
from keel_core.projections import project_messages
from keel_core.state import PostgresEventStore
from keel_worker.jobs import (
    JobContext,
    JobDefinition,
    JobRegistry,
    dispatch_jobs,
    run_job,
)

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


class _Clock:
    def __init__(self, value: datetime = _NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


async def _target(engine: AsyncEngine, scope: str, session_id: str) -> None:
    store = PostgresEventStore(engine, scope)
    await admit(store, session_id, scope, "start")
    await store.append(
        Event(
            type=EventType.message_token,
            seq=0,
            session_id=session_id,
            scope_id=scope,
            ts=_NOW,
            payload={"role": "assistant", "text": "previous answer", "partial": False},
        )
    )


async def _job(
    store: PostgresJobStore,
    *,
    key: str,
    target_session_id: str | None = None,
    max_attempts: int = 3,
    payload: dict[str, Any] | None = None,
) -> str:
    row, _ = await store.enqueue_once(
        kind="test.acceptance",
        payload=payload or {},
        target_session_id=target_session_id,
        idempotency_key=key,
        max_attempts=max_attempts,
        now=_NOW,
    )
    return row.id


def _ctx(
    store: PostgresJobStore,
    registry: JobRegistry,
    clock: _Clock,
    enqueue: Any,
) -> dict[str, Any]:
    return {
        "jobs": store,
        "job_registry": registry,
        "durable_scope": store.scope_id,
        "enqueue": enqueue,
        "job_clock": clock,
        "job_settings": Settings(
            job_retry_base_seconds=5,
            job_retry_max_seconds=300,
            job_dispatch_limit=100,
        ),
    }


async def _job_events(
    engine: AsyncEngine, scope: str, session_id: str, job_id: str
) -> list[Event]:
    return [
        event
        async for event in PostgresEventStore(engine, scope).read(session_id)
        if event.payload.get("job_id") == job_id
    ]


async def test_arq_burst_worker_executes_and_injects_once(
    migrated_db: AsyncEngine,
    redis_client: object,
) -> None:
    scope = f"accept:arq:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        await context.progress(1, total=1, message="done")
        return JobResult(data={"ok": True}, message="arq completed")

    registry.register(
        JobDefinition(
            "test.acceptance",
            handler,
            max_attempts=3,
            lease_seconds=30,
        )
    )
    job_id = await _job(store, key="arq", target_session_id=session_id)
    redis_url = os.environ.get("KEEL_TEST_REDIS_URL", "redis://localhost:6379/15")
    pool = await create_pool(RedisSettings.from_dsn(redis_url))
    queue_name = f"arq:jobs:{uuid.uuid4().hex}"

    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        await pool.enqueue_job(name, *args, _queue_name=queue_name, **options)

    try:
        await pool.enqueue_job(
            "run_job",
            scope,
            job_id,
            _queue_name=queue_name,
        )
        worker = Worker(
            functions=[run_job],
            queue_name=queue_name,
            redis_pool=pool,
            burst=True,
            handle_signals=False,
            ctx=_ctx(store, registry, _Clock(), enqueue),
        )
        await worker.async_run()
    finally:
        await pool.aclose()

    row = await store.get(job_id)
    assert row is not None and row.status is JobStatus.succeeded
    assert row.attempt == 1
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1
```

The `redis_client` fixture parameter intentionally forces the existing fail/skip check before
the arq pool is created; the body uses a separate arq-native pool and isolated queue.

- [ ] **Step 2: Add missed-enqueue, duplicate-delivery, and permanent-failure acceptance**:

```python
async def test_missed_immediate_enqueue_is_healed_and_duplicates_execute_once(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:duplicate:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return JobResult(data={"calls": calls}, message="duplicate-safe")

    registry.register(JobDefinition("test.acceptance", handler, lease_seconds=30))
    job_id = await _job(store, key="missed", target_session_id=session_id)
    recorded: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        recorded.append((name, args, options))

    clock = _Clock()
    ctx = _ctx(store, registry, clock, enqueue)
    # No immediate Redis call happened after enqueue_once(): dispatcher heals it.
    assert await dispatch_jobs(ctx) == 1
    assert recorded == [("run_job", (scope, job_id), {})]

    first, second = await asyncio.gather(
        run_job(ctx, scope, job_id),
        run_job(ctx, scope, job_id),
    )
    assert {first, second} <= {"running", "succeeded"}
    assert await run_job(ctx, scope, job_id) == JobStatus.succeeded.value
    assert calls == 1
    assert (await store.get(job_id)).status is JobStatus.succeeded  # type: ignore[union-attr]
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1


async def test_permanent_handler_executes_once_and_duplicate_delivery_stays_terminal(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:permanent:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        raise PermanentJobError("invalid_document", "Document is invalid.")

    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        return None

    registry.register(
        JobDefinition("test.acceptance", handler, max_attempts=3, lease_seconds=30)
    )
    job_id = await _job(store, key="permanent", target_session_id=session_id)
    ctx = _ctx(store, registry, _Clock(), enqueue)

    assert await run_job(ctx, scope, job_id) == JobStatus.failed.value
    assert await run_job(ctx, scope, job_id) == JobStatus.failed.value
    failed = await store.get(job_id)
    assert failed is not None and failed.error_kind == "invalid_document"
    assert calls == 1
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1
```

- [ ] **Step 3: Add deterministic retry acceptance**:

```python
async def test_retryable_handler_fails_twice_then_succeeds_third_attempt(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:retry:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RetryableJobError("embedding_timeout", "Embedding timed out.")
        return JobResult(data={"chunks": 42}, message="indexed 42 chunks")

    registry.register(
        JobDefinition(
            "test.acceptance",
            handler,
            max_attempts=3,
            lease_seconds=30,
        )
    )
    job_id = await _job(
        store, key="retry", target_session_id=session_id, max_attempts=3
    )
    deferred: list[datetime] = []

    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        deferred.append(options["_defer_until"])  # type: ignore[arg-type]

    clock = _Clock()
    ctx = _ctx(store, registry, clock, enqueue)
    assert await run_job(ctx, scope, job_id) == JobStatus.queued.value
    clock.advance(5)
    assert await run_job(ctx, scope, job_id) == JobStatus.queued.value
    clock.advance(10)
    assert await run_job(ctx, scope, job_id) == JobStatus.succeeded.value

    row = await store.get(job_id)
    assert row is not None
    assert row.attempt == 3
    assert row.result == {"chunks": 42}
    assert deferred == [
        _NOW + timedelta(seconds=5),
        _NOW + timedelta(seconds=15),
    ]
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1
```

- [ ] **Step 4: Add worker-crash reclaim and exhaustion acceptance**:

```python
async def test_worker_cancelled_error_leaves_lease_for_reclaim(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:crash:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    calls = 0

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise asyncio.CancelledError
        return JobResult(data={"recovered": True}, message="recovered")

    registry.register(
        JobDefinition("test.acceptance", handler, max_attempts=2, lease_seconds=10)
    )
    job_id = await _job(
        store, key="crash", target_session_id=session_id, max_attempts=2
    )

    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        return None

    clock = _Clock()
    ctx = _ctx(store, registry, clock, enqueue)
    with pytest.raises(asyncio.CancelledError):
        await run_job(ctx, scope, job_id)
    after_crash = await store.get(job_id)
    assert after_crash is not None and after_crash.status is JobStatus.running
    assert after_crash.attempt == 1
    assert await _job_events(migrated_db, scope, session_id, job_id) == []

    clock.advance(11)
    assert await run_job(ctx, scope, job_id) == JobStatus.succeeded.value
    recovered = await store.get(job_id)
    assert recovered is not None and recovered.attempt == 2
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1


async def test_crash_at_attempt_ceiling_is_failed_by_dispatcher(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:exhaust:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        raise asyncio.CancelledError

    registry.register(
        JobDefinition("test.acceptance", handler, max_attempts=1, lease_seconds=10)
    )
    job_id = await _job(
        store, key="exhaust", target_session_id=session_id, max_attempts=1
    )

    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        return None

    clock = _Clock()
    ctx = _ctx(store, registry, clock, enqueue)
    with pytest.raises(asyncio.CancelledError):
        await run_job(ctx, scope, job_id)
    clock.advance(11)
    assert await dispatch_jobs(ctx) == 1
    failed = await store.get(job_id)
    assert failed is not None and failed.status is JobStatus.failed
    assert failed.error_kind == "attempts_exhausted"
    assert len(await _job_events(migrated_db, scope, session_id, job_id)) == 1
```

- [ ] **Step 5: Add cooperative cancellation and next-turn projection acceptance**:

```python
async def test_running_cancel_is_observed_at_checkpoint_and_injection_does_not_run_model(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"accept:cancel:{uuid.uuid4().hex}"
    session_id = f"target:{uuid.uuid4().hex}"
    await _target(migrated_db, scope, session_id)
    store = PostgresJobStore(migrated_db, scope)
    registry = JobRegistry()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(context: JobContext, payload: dict[str, Any]) -> JobResult:
        entered.set()
        await release.wait()
        await context.checkpoint()
        return JobResult(data={}, message="must not win")

    registry.register(JobDefinition("test.acceptance", handler, lease_seconds=30))
    job_id = await _job(store, key="cancel", target_session_id=session_id)

    async def enqueue(
        name: str, *args: object, **options: object
    ) -> None:
        return None

    clock = _Clock()
    ctx = _ctx(store, registry, clock, enqueue)
    running = asyncio.create_task(run_job(ctx, scope, job_id))
    await entered.wait()
    requested = await store.request_cancel(job_id, clock())
    assert requested is not None and requested.status is JobStatus.running
    release.set()
    assert await running == JobStatus.cancelled.value

    events_before_next_turn = [
        event async for event in PostgresEventStore(migrated_db, scope).read(session_id)
    ]
    assert not any(event.type is EventType.run_started for event in events_before_next_turn)
    assert len(
        [event for event in events_before_next_turn if event.payload.get("job_id") == job_id]
    ) == 1

    await admit(PostgresEventStore(migrated_db, scope), session_id, scope, "what happened?")
    projected = project_messages(
        [event async for event in PostgresEventStore(migrated_db, scope).read(session_id)]
    )
    assert [message["role"] for message in projected] == ["user", "assistant", "user"]
    assert projected[1]["content"] == (
        "previous answer\n\n后台任务 test.acceptance 已取消。"
    )
```

- [ ] **Step 6: Run acceptance RED on the pre-implementation baseline**

If this acceptance file is applied before Tasks 1–15, run:

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_EVAL_DATABASE_URL = $env:KEEL_TEST_DATABASE_URL
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_worker.py -v
```

Expected on that baseline: collection fails with missing `keel_core.jobs` /
`keel_worker.jobs`. In dependency order, no production source is added in this task; proceed to
the GREEN command against Tasks 1–15.

- [ ] **Step 7: Run GREEN**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_worker.py -v
```

Expected: all acceptance cases pass; actual arq burst delivery uses an isolated queue and every
terminal target receives exactly one assistant event.

- [ ] **Step 8: Run the complete jobs slice**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py tests\unit\test_worker_jobs.py tests\unit\test_projections.py tests\unit\test_worker_tasks.py tests\unit\test_server.py tests\integration\test_jobs_postgres.py tests\integration\test_jobs_api.py tests\integration\test_jobs_worker.py -v
```

Expected: all selected tests pass.

- [ ] **Step 9: Quality gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check tests\integration\test_jobs_worker.py
.\.venv\Scripts\python.exe -m ruff format --check tests\integration\test_jobs_worker.py
.\.venv\Scripts\python.exe -m mypy packages
```

Expected: exit `0`.

- [ ] **Step 10: Commit**

```bash
git add tests/integration/test_jobs_worker.py
git commit -m "test(jobs): accept crash duplicate retry cancel and injection flows" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 17: Update living status after the slice is green

**Files:**
- Modify: `docs/STATUS.md`

**Interfaces:**
- Marks durable background jobs complete without claiming N-worker benchmarking, per-task
  routing, WeCom, Jobs UI, or RAG is complete.
- Makes RAG/Knowledge Base the next implementation slice.
- Keeps ADR-0006 schedule semantics distinct from ADR-0010 job semantics.

- [ ] **Step 1: Verify implementation evidence before editing status**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
.\.venv\Scripts\python.exe -m pytest tests\unit\test_jobs.py tests\unit\test_worker_jobs.py tests\integration\test_jobs_postgres.py tests\integration\test_jobs_api.py tests\integration\test_jobs_worker.py -q
```

Expected: all selected tests pass. If not, do not mark the capability complete.

- [ ] **Step 2: Make exact status edits**

1. In §1, replace the two-line M2 sentence with:

```markdown
- M2 已完成调度、审批、RBAC、基础 provider failover、Telegram 与 durable background jobs
  等切片，但 N-worker scale-out、per-task routing 和 WeCom 尚未完成。
```

2. In §2 “Scope、connectors 与 autonomy”, add:

```markdown
- Durable background jobs：
  - Postgres lifecycle source of truth + at-least-once arq delivery；
  - lease/heartbeat/reclaim、progress、cooperative cancellation；
  - bounded retry + dispatcher recovery；
  - terminal result exactly-once assistant injection；
  - list/detail/cancel API + RBAC。
```

3. Replace the M2 milestone row with:

```markdown
| **M2 Autonomy & scale** | 部分完成 | schedules、digest、durable approvals、RBAC、admin overview、Telegram、基础 failover、durable background jobs、progress/cancel/result injection | N-worker demo、per-task routing、WeCom |
```

4. In §6, rename the first two headings exactly:

```markdown
### 1. Durable background jobs（M2 prerequisite，已完成）
### 2. RAG/KB vertical slice（下一步）
```

Keep the completed jobs evidence under item 1 and the existing RAG sequence under item 2.
5. Replace §7’s current-next-step sentence with:

```markdown
立即进入 **RAG/Knowledge Base vertical slice 设计**；直接复用 ADR-0010 与
`2026-07-14-durable-background-jobs-design.md`，不重新定义 job lifecycle。
```

- [ ] **Step 3: Review the doc diff**

```powershell
git --no-pager diff -- docs\STATUS.md
```

Expected: only the capability/milestone/next-step statements above change; no test-count or
unrelated roadmap claims are guessed.

- [ ] **Step 4: Commit**

```bash
git add docs/STATUS.md
git commit -m "docs(jobs): mark durable background jobs complete" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
```

---

## Task 18: Whole-branch verification and live isolated smoke

**Files:**
- No source changes expected.
- If a gate fails, fix the owning earlier task and amend/add a focused commit with both trailers;
  then restart this task from Step 1.

**Interfaces:**
- Verifies migration head, every automated test layer, lint/format/types, empty production
  registry, API surface, and one real Redis/arq delivery against isolated `keel_test`.
- Produces verification evidence only; no commit when all gates are already green.

- [ ] **Step 1: Start isolated services and ensure `keel_test` exists**

```powershell
docker compose --profile dev up -d postgres redis
$exists = docker compose exec -T postgres psql -U keel -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='keel_test'"
if ($exists.Trim() -ne "1") {
    docker compose exec -T postgres createdb -U keel keel_test
}
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
```

Expected: Postgres and Redis are healthy; the destructive URL names exactly `keel_test`.

- [ ] **Step 2: Re-run the fail-closed database guard**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_db_guard.py -v
```

Expected: all guard tests pass.

- [ ] **Step 3: Verify the migration chain**

```powershell
$env:KEEL_DATABASE_URL = $env:KEEL_TEST_DATABASE_URL
.\.venv\Scripts\python.exe -m alembic upgrade head
.\.venv\Scripts\python.exe -m alembic heads
Remove-Item Env:KEEL_DATABASE_URL
```

Expected: upgrade exits `0`; exactly one head is printed:
`0009_background_jobs (head)`.

- [ ] **Step 4: Run all non-integration tests**

```powershell
.\.venv\Scripts\python.exe -m pytest -m "not integration" -q
```

Expected: all collected non-integration tests pass with no unexpected skip/failure.

- [ ] **Step 5: Run the full Postgres/Redis integration suite**

```powershell
.\.venv\Scripts\python.exe -m pytest -m integration -q
```

Expected: all integration tests pass; any service skip means the live smoke is not complete.

- [ ] **Step 6: Run repo-wide static gates**

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m mypy packages
```

Expected: all three exit `0`.

- [ ] **Step 7: Run the live isolated arq smoke by itself**

```powershell
$env:KEEL_TEST_DATABASE_URL = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
$env:KEEL_TEST_REDIS_URL = "redis://localhost:6379/15"
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs_worker.py::test_arq_burst_worker_executes_and_injects_once -vv -s
```

Expected: one real arq job is serialized through Redis, claimed from Postgres, executed by a
test-injected handler, marked `succeeded`, and injected once into its target session.

- [ ] **Step 8: Verify forbidden production surfaces remain absent**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_worker_tasks.py::test_production_job_registry_is_empty tests\integration\test_jobs_api.py::test_no_generic_create_retry_or_inject_routes -v
```

Expected: both tests pass.

- [ ] **Step 9: Inspect final branch state**

```powershell
git --no-pager status --short
git --no-pager diff --check
git --no-pager log --format="%h %s%n%b" -17
$commits = @(git rev-list --max-count=17 HEAD)
if ($commits.Count -ne 17) {
    throw "expected 17 implementation commits"
}
foreach ($commit in $commits) {
    $body = (git log -1 --format="%B" $commit) -join "`n"
    if (
        $body -notmatch "Co-authored-by: Copilot <223556219\+Copilot@users\.noreply\.github\.com>" -or
        $body -notmatch "Copilot-Session: e6e934ad-91c1-41c3-a46e-521cd446cb49"
    ) {
        throw "missing required trailers on $commit"
    }
}
```

Expected:
- no accidental generated/runtime files are staged;
- `git diff --check` prints nothing;
- every Task 1–17 commit has both `Co-authored-by` and `Copilot-Session` trailers;
- pre-existing unrelated untracked files are left untouched.

---

## Appendix A: Spec and ADR Coverage Matrix

| Requirement source | Requirement | Implementing task(s) |
|---|---|---|
| Spec §1–§2 | Generic durable substrate; explicit non-goals | Header + Global Constraints |
| Spec §3 | Reuse arq, schedules, approvals, event store/RBAC; close generic jobs gap | Architecture/File Map, Tasks 1–16 |
| Spec §4 | At-least-once, lease, handler idempotency, terminal atomicity, allow-list, explicit scope, no fake handler | Global Constraints, Tasks 4–16 |
| Spec §5 / ADR-0010 | DB-first enqueue, duplicate-safe delivery, bounded attempts, dispatcher recovery | Tasks 4, 7, 12, 13, 16 |
| Spec §6 | `jobs` schema/indexes/RLS; no `job_events` table | Task 2 |
| Spec §7 | queued/running/terminal transitions; reclaim; cancel-vs-complete semantics | Tasks 5, 6, 8, 9, 12, 16 |
| Spec §8 | Strict models + exact JobStore contract; in-memory and Postgres implementations | Tasks 1, 4–9 |
| Spec §9.1 | Enqueue dedupe + best-effort immediate delivery seam | Tasks 4, 7, 14, 16 |
| Spec §9.2 | No generic public create API | Global Constraints, Task 15, Task 18 |
| Spec §9.3 | 30-second recovery dispatcher; dispatchable/exhausted; attempt ceiling in claim | Tasks 7, 8, 13, 14, 16 |
| Spec §10 | Allow-listed registry; unknown kind permanent; `run_job`; no production registration | Tasks 11, 12, 14, 18 |
| Spec §11 | `JobContext.progress/checkpoint`, heartbeat, cooperative cancellation | Tasks 5, 8, 11, 12, 16 |
| Spec §12 | Retryable/permanent/unknown classification; deterministic backoff; `_defer_until`; final-attempt terminal failure | Tasks 1, 12, 13, 16 |
| Spec §13 | Durable latest progress; bounds/monotonicity; reset per attempt | Tasks 5, 8, 11, 15 |
| Spec §14.1–§14.3 | Assistant payload; reusable in-transaction append; all terminal transitions atomic/exactly once | Tasks 3, 6, 9, 16 |
| Spec §14.4 | Model-facing adjacent plain-assistant coalescing; tool boundary; OpenAI/Anthropic compatibility | Task 10, Task 16 |
| Spec §14.5 | Optional target; same-scope existing session; no auto-created target | Tasks 3, 4, 7, 9 |
| Spec §14.6 | Failed rerun uses a new request key; no generic retry API | Tasks 4, 6, 7, 15 |
| Spec §15 | Viewer list/detail; operator cancel; filters/limit; cross-scope 404 | Task 15 |
| Spec §16.1 | Explicit single configured scope in arq/store/API + worker validation + RLS | Tasks 2, 4, 7, 12, 14, 15 |
| Spec §16.2 | Allow-listed payload path, no callable/import/shell serialization, bounded data, safe logs | Tasks 1, 11, 12, 15 |
| Spec §17 | Eight job settings; definition-owned attempts; 30-second cadence | Tasks 1, 11, 14 |
| Spec §18 | `job.execute` span and safe structured logs | Task 12 |
| Spec §19 | Exact source/test file layout | Architecture/File Map |
| Spec §20.1 | Unit matrix: dedupe/transitions/terminal/progress/retry/unknown/cancel/duplicate/exhaustion/bounds | Tasks 1, 4–6, 10–13 |
| Spec §20.2 | Postgres matrix: migration/RLS/scope/dedupe/claim/reclaim/stale/retry/finalizer/double-finalize/atomicity | Tasks 2, 7–9 |
| Spec §20.3 | API/RBAC matrix | Task 15 |
| Spec §20.4 | Worker acceptance matrix | Task 16 |
| Spec §21 | Rollout with no production handler and isolated `keel_test` acceptance | Tasks 14, 16, 18 |
| Spec §22 | Full tests/Ruff/format/mypy, live isolated smoke, status update, RAG reuse | Tasks 17–18 |
| ADR-0010 Decision 1–8 | Postgres authority, at-least-once arq, leases, deterministic bounded attempts, cooperative cancel, idempotent handlers, atomic injection, recovery dispatcher | Global Constraints + Tasks 2–16 |
| ADR-0010 schedule distinction | ADR-0006 remains for schedule cursor semantics | Global Constraints, Architecture/File Map, Task 17 |
| User explicit requirement | Both commit trailers | Global Constraints + every Task 1–17 commit block |
| User explicit requirement | Fresh-subagent executable, dependency ordered, commit-sized TDD tasks | Header, Interface blocks, Dependency Order, Tasks 1–18 |

## Appendix B: Self-Review Result

Self-review performed against the approved design, ADR-0010, the current source/tests, and the
writing-plans checklist.

### 1. Coverage and omission review

- **Complete:** every spec section §1–§22 and every ADR decision maps to a task in Appendix A.
- **Explicit user list:** strict models/errors/settings (T1); migration/DB guard/RLS (T2);
  in-memory/Postgres stores (T4–T9); dedupe (T4/T7); claim/reclaim/ceiling (T5/T8);
  progress/heartbeat (T5/T8/T11); cancel/requeue/finalizers (T6/T8/T9);
  reusable transactional append (T3); exactly-once injection (T6/T9/T16);
  assistant coalescing (T10); registry/context/run (T11/T12); retry/deferred/dispatcher/exhaustion
  (T12/T13); APIs/RBAC (T15); wiring (T14); acceptance (T16); docs/status (T17);
  whole-branch/live smoke (T18).
- **No scope creep:** no Jobs UI, job history table, public create/retry/inject API, hard kill,
  multi-scope discovery, arbitrary Python execution, new dependency, or production handler.

### 2. Completeness and executability review

- Implementation Tasks 1–16 name exact files/interfaces, RED commands with expected failures,
  concrete code/SQL, GREEN/quality commands, and commit messages. Task 17 is an evidence-gated
  documentation commit; Task 18 is verification-only and creates no commit when green.
- Ellipses appear only in Protocol/type syntax or explanatory SQL/message notation; every
  implementation block later supplies the concrete behavior and no step relies on omitted code.
- Dynamic evidence (test totals and final commit hashes) is deliberately not guessed; Task 18
  commands capture the real branch result.
- Every integration command explicitly sets an exact `keel_test` URL; the live Redis smoke uses
  DB 15 plus a UUID queue name and does not flush another user's Redis data.
- Automated plan scans found no forbidden incomplete-work markers or live-database URL, all fenced
  Python snippets parse, Markdown fences are balanced, and every Task 1–17 commit block has both
  required trailers.

### 3. Type and signature consistency review

- `JobResult.data/message` and `JobError.kind/message` match everywhere; `JobLease` and the full
  `JobRecord` mapping are identical across both stores and worker tests. `JobResponse` is the
  explicitly documented safe subset rather than a second store model.
- `heartbeat()` consistently means “refresh or raise stale lease; return cancel requested.”
- `progress()` consistently returns `JobProgressResult(record, cancel_requested)`.
- Stores are constructor-bound to scope; arq still carries scope and `run_job` validates it against
  both configured scope and store scope.
- `JobDefinition.max_attempts` is enqueue policy; the durable row/lease is execution authority.
  There is no contradictory `Settings.job_max_attempts`.
- Both stores normalize the dedupe identity first and return an existing winner before validating
  a retry's differing payload or target; only a genuinely new key validates those creation fields.
- Retry uses attempt numbers starting at 1 and delays 5, 10, 20… capped at 300; the same clock
  drives row transitions and `_defer_until`.
- `request_cancel()` is the only terminal method without a lease and only finalizes a queued row;
  success/failure/cancelled execution finalizers require current token; exhaustion uses its own
  expired/ceiling predicate.
- Both stores use the same terminal message/payload builder, so in-memory and Postgres behavior
  cannot drift.
- `append_event_in_transaction()` receives a caller-owned transaction and never sets an implicit
  scope; Postgres job finalization sets RLS first and requires an existing session.
- `JobResponse` derives `cancel_requested` from the timestamp and intentionally omits payload,
  scope, idempotency key, and lease token without changing store contracts.
- Worker/server enqueue adapters both preserve arbitrary arq options, including `_defer_until`.

### 4. Review risks resolved in-plan

- **Cancel/complete race:** running cancellation does not block `succeed()`; a checkpoint-observed
  cancellation calls `finish_cancelled()`.
- **Crash-after-complete:** terminal status prevents a new claim; `injected_event_seq` plus row lock
  prevents a second event.
- **Crash at attempt ceiling:** dispatcher calls the same terminal finalizer, not a bare status
  update.
- **Stale arq delivery:** atomic claim SQL includes the attempt ceiling and due/expired predicates.
- **Cross-scope existence leak:** scope-bound get/cancel returns the same 404 as missing.
- **Target deletion/finalizer rollback:** missing target raises before append; the injected-append
  failure test separately proves a later transaction error leaves no partial event/status.
- **Model role legality:** only provider projection coalesces adjacent plain assistants; durable
  event/UI boundaries remain separate.
- **Production execution surface:** startup registry is empty and API has no create/retry/inject
  route; tests inject all handlers.

## Appendix C: Execution Handoff

Plan saved at `docs/superpowers/plans/2026-07-14-durable-background-jobs.md`.

1. **Subagent-Driven (recommended):** use `superpowers:subagent-driven-development`; dispatch one
   fresh subagent per task and review RED/GREEN evidence before advancing.
2. **Inline:** use `superpowers:executing-plans`; execute in dependency order with review
   checkpoints after Tasks 3, 6, 9, 13, 16, and 18.

Tasks 1, 4–6, 10–14 are service-free unit work. Tasks 2–3, 7–9, 15–18 require an explicitly
isolated `keel_test`; Tasks 16 and 18 also require test Redis. Do not start RAG/KB implementation
until Task 18 is green.
