# Durable Background Jobs 设计文档

> **状态：** Draft for implementation review  
> **日期：** 2026-07-14  
> **Milestone：** M2 prerequisite for M3 RAG/Knowledge Base  
> **相关文档：**
> [`STATUS.md`](../../STATUS.md)、
> [`IMPLEMENTATION-PLAN.md`](../../IMPLEMENTATION-PLAN.md) M2/M3、
> [`PRD.md`](../../PRD.md) FR-A3、
> [`ARCHITECTURE.md`](../../ARCHITECTURE.md)、
> [`0006-scheduler-and-queue.md`](../../adr/0006-scheduler-and-queue.md)、
> [`2026-07-07-keel-autonomy-slice-design.md`](./2026-07-07-keel-autonomy-slice-design.md)

## 1. 目标

为 Keel 增加一个通用、scope-bound、可恢复的后台任务底座，使长任务能够：

1. 持久化排队；
2. 被 arq 至少一次投递；
3. 通过数据库 lease 避免并发执行；
4. 上报持久化进度；
5. 接受 cooperative cancellation；
6. 在瞬时错误后 bounded retry；
7. 在 worker crash 后恢复；
8. 将终态结果 **exactly once** 回注目标 session；
9. 让 API/UI 查询任务状态。

第一位真实 consumer 是下一阶段的：

- `knowledge.ingest`
- `knowledge.reindex`
- `knowledge.delete`

arq/Redis 只是投递层；**Postgres `jobs` row 是任务生命周期的 source of truth。**

## 2. 非目标

首版不做：

- 任意 Python function/module 的远程执行；
- 用户通过通用 API 注册任意 job kind；
- hard kill 运行中的 Python coroutine/container；
- 完整 Jobs React 页面；
- 高频 progress SSE；
- 多租户 scope registry 与跨 scope worker discovery；
- N-worker benchmark；
- DAG/workflow engine；
- distributed transaction 到外部 provider；
- 无限日志流或大 result blob。

这些边界避免把 FR-A3 扩成 Celery/Temporal 替代品。

## 3. 现状与缺口

### 3.1 已有可复用能力

- arq worker 与 Redis enqueue：
  `keel_worker.main.WorkerSettings`。
- Scheduler at-most-once trigger：
  `due_tick()` 在 enqueue 前推进 `schedules.next_run_at`。
- Durable approval suspend/resume：
  event log 是 checkpoint。
- Consolidation lease/cursor：
  已验证 claim、expiry、retry、busy。
- `PostgresEventStore`：
  session `seq` 原子分配、scope/RLS、append-only events。
- RBAC：
  viewer 读、operator 修改、admin 管理。
- API scope：
  当前 server/worker 使用 configured durable scope `web:local`。

### 3.2 当前缺口

- 没有通用 `jobs` 表。
- 没有 queued/running/terminal 状态机。
- 没有 durable progress。
- interactive `interrupt` 只是 server 进程内 flag。
- 没有 generic worker lease/heartbeat。
- 没有 job attempt/backoff ledger。
- 没有 terminal result → session 的 exactly-once transaction。
- worker crash 后，Redis 已丢失的 enqueue 没有 DB sweeper 补投。

## 4. 核心决策

| 主题 | 决策 |
|---|---|
| 投递语义 | **At-least-once delivery**。API 先写 DB，再 best-effort enqueue；周期 dispatcher 补投。 |
| 并发控制 | 每次执行必须先取得 DB lease；重复 arq delivery 只有一个 claim 成功。 |
| 副作用语义 | Handler 可能重跑，必须按 `job.id` / `idempotency_key` 自身幂等。框架不承诺 handler exactly-once。 |
| 终态注入 | 终态 transition 与 session event append 在同一 Postgres transaction 中完成。 |
| 注入角色 | 作为 `message.token{role="assistant"}` 回注，不自动触发新 agent run。 |
| Progress | 首版持久化最新 progress，客户端 polling；不把每次 progress 写进 session event log。 |
| Cancellation | Cooperative；queued 立即取消，running 设置 request，由 handler checkpoint 观察。Handler 已返回结果时 success 胜出。 |
| Retry | DB 驱动、bounded exponential backoff、无 jitter（首版便于测试）。 |
| Job kinds | 由代码中的 allow-listed registry 定义；没有任意函数执行入口。 |
| Scope | 表、store、API、arq 参数都携带 scope；首版 worker 只接受 configured durable scope。 |
| 首个 production handler | 不引入假的 demo job；下一 slice 的 `knowledge.ingest` 是第一个真实 kind。 |

## 5. Delivery 与 correctness 语义

### 5.1 为什么不是 at-most-once job execution

Scheduler 的 I9 at-most-once 语义适合“定时触发”：

- cursor 先推进；
- crash 允许 0 次；
- 不允许 2 次。

文档 ingestion 等长任务不能接受“enqueue 丢失后永不执行”，因此采用：

```text
DB enqueue -> immediate arq enqueue (best effort)
                 |
                 +-> periodic DB dispatcher heals missed delivery
```

重复 delivery 是正常情况；lease + handler idempotency 负责收敛。

### 5.2 框架保证

框架保证：

- 一个有效 lease 同时只有一个 owner；
- terminal row 不再执行；
- progress/cancel/complete 必须持有当前 lease token；
- result injection 最多一次；
- **所有** terminal transition（含 crash-attempt exhaustion）与 result injection 原子；
- retry 次数有上限；
- scope/RLS fail closed。

框架不保证：

- handler 外部副作用 exactly once；
- worker 被强杀时立即取消；
- 每个 progress update 都实时推送到浏览器。

## 6. 数据模型

新增 migration：`0009_background_jobs`。

```sql
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
);

CREATE INDEX ix_jobs_dispatch
    ON jobs (scope_id, status, next_attempt_at);

CREATE INDEX ix_jobs_lease_expiry
    ON jobs (scope_id, lease_expires_at)
    WHERE status = 'running';

CREATE INDEX ix_jobs_target_session
    ON jobs (scope_id, target_session_id)
    WHERE target_session_id IS NOT NULL;
```

### 6.1 RLS

与现有 scoped tables 一致：

```sql
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
CREATE POLICY scope_isolation ON jobs
USING (scope_id = current_setting('app.scope_id', true))
WITH CHECK (scope_id = current_setting('app.scope_id', true));
```

所有 Postgres store transaction 都先：

```sql
SELECT set_config('app.scope_id', :scope, true)
```

### 6.2 为什么没有 `job_events` 表

首版只需要：

- 当前状态；
- 当前 progress；
- attempt/error/result；
- terminal session injection。

完整 transition/history 可在 event upcaster/data-lifecycle epic 中设计。现在增加另一套 append-only
event vocabulary 会扩大 schema evolution 债务。

## 7. 状态机

```text
                  claim(attempt += 1)
queued ----------------------------> running
  |                                    |
  | cancel                             | success
  v                                    v
cancelled                           succeeded
                                       ^
                                       |
running -- retryable + attempts left --+--> queued(next_attempt_at)
   |
   +-- permanent / attempts exhausted ------> failed
   |
   +-- cancel requested + checkpoint -------> cancelled

running -- expired lease + attempts left ----> running(new lease, attempt += 1)
```

Terminal：

- `succeeded`
- `failed`
- `cancelled`

Terminal row immutable；重复 delivery 读取后直接返回当前 status。

### 7.1 Cancel 与 complete race

- queued cancel：同一 transaction 直接 `cancelled`，并执行 terminal injection。
- running cancel：只设置 `cancel_requested_at`。
- handler 在 `progress()` / `checkpoint()` 时看到 cancel，抛出内部
  `JobCancellationRequested`。
- **Cancellation 是 cooperative，不是 rollback。**
- handler 若在下一个 checkpoint 前已经完成并返回结果，`succeed()` 正常提交；
  success 胜出，因为外部副作用可能已经完成。
- handler 若先在 checkpoint 观察到 cancel，则调用 `finish_cancelled()`；
  cancelled 胜出。
- terminal 后的 cancel 幂等返回当前 record。

## 8. Store contract

新增 `packages/keel-core/src/keel_core/jobs.py`。

### 8.1 Models

```python
class JobStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


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
    cancel_requested_at: datetime | None
    progress_current: int
    progress_total: int | None
    progress_message: str | None
    result: dict[str, Any] | None
    result_message: str | None
    error_kind: str | None
    error_message: str | None
    injected_event_seq: int | None
```

### 8.2 Protocol

```python
class JobStore(Protocol):
    async def enqueue_once(...) -> tuple[JobRecord, bool]: ...
    async def get(job_id: str) -> JobRecord | None: ...
    async def list(...filters...) -> list[JobRecord]: ...
    async def dispatchable(now: datetime, limit: int) -> list[str]: ...
    async def exhausted(now: datetime, limit: int) -> list[str]: ...
    async def claim(job_id: str, now: datetime, lease_seconds: int) -> JobLease | None: ...
    async def heartbeat(lease: JobLease, now: datetime) -> bool: ...
    async def progress(lease: JobLease, ...) -> JobProgressResult: ...
    async def request_cancel(job_id: str, now: datetime) -> JobRecord | None: ...
    async def requeue(
        lease: JobLease, error: JobError, retry_at: datetime, now: datetime
    ) -> JobRecord: ...
    async def succeed(lease: JobLease, result: JobResult, now: datetime) -> JobRecord: ...
    async def fail_terminal(
        lease: JobLease, error: JobError, now: datetime
    ) -> JobRecord: ...
    async def finish_cancelled(lease: JobLease, now: datetime) -> JobRecord: ...
    async def fail_exhausted(job_id: str, now: datetime) -> JobRecord | None: ...
```

提供：

- `InMemoryJobStore`：unit tests / lite。
- `PostgresJobStore`：生产。

## 9. Enqueue 与 dispatcher

### 9.1 Feature enqueue

业务 service（下一 slice 的 Knowledge API）调用：

```python
job, created = await jobs.enqueue_once(
    kind="knowledge.ingest",
    payload=validated_payload,
    target_session_id=originating_session,
    idempotency_key=request_idempotency_key,
    max_attempts=definition.max_attempts,
)
```

如果 `created=True`：

```python
await enqueue("run_job", scope_id, job.id)
```

enqueue 失败不回滚 DB row；row 保持 `queued`，dispatcher 会补投。

### 9.2 没有通用 public create API

首版不提供：

```text
POST /v1/jobs {kind, payload}
```

原因：

- 任意 job kind 是不必要的执行面；
- 每个 feature endpoint 应使用 typed payload；
- client 不能指定 retry/lease policy；
- RAG endpoint 才是第一个 enqueue 入口。

### 9.3 Recovery dispatcher

worker 新增：

```python
async def dispatch_jobs(ctx) -> int
```

每 30 秒：

1. 查询 configured scope 内：
   - `queued AND next_attempt_at <= now AND attempt < max_attempts`
   - `running AND lease_expires_at <= now AND attempt < max_attempts`
2. enqueue `run_job(scope_id, job_id)`。
3. 查询 lease 已过期且 `attempt >= max_attempts` 的 job。
4. **逐条**调用 `fail_exhausted(job_id)`；该方法与普通 terminal failure 一样，
   在一个 transaction 内写 `failed` + exactly-once session injection。

dispatcher 可以重复 enqueue；claim 负责互斥。

`claim()` 对两种 row 生效：

- due queued row；
- expired running row。

两条 atomic claim branch 都必须在 SQL `WHERE` 中包含：

```sql
attempt < max_attempts
```

不能只依赖 dispatcher 预筛选；stale/duplicate arq delivery 也不能突破 attempt ceiling。

每次成功 claim：

- `attempt += 1`；
- 生成新 lease token；
- 重置该 attempt 的 progress；
- 更新 `started_at`（首次）与 heartbeat/expiry。

## 10. Worker 与 handler registry

新增：

- `packages/keel-worker/src/keel_worker/jobs.py`
- `run_job(ctx, scope_id, job_id)` arq function
- `dispatch_jobs(ctx)` cron function

### 10.1 Registry

```python
@dataclass(frozen=True)
class JobDefinition:
    kind: str
    handler: JobHandler
    max_attempts: int = 3
    lease_seconds: int = 300


class JobRegistry:
    def register(definition: JobDefinition) -> None: ...
    def get(kind: str) -> JobDefinition | None: ...
```

只有 registry 中的 kind 能执行。Unknown kind：

- 不 import；
- 不 eval；
- 不动态加载；
- final `failed(error_kind="unknown_job_kind")`。

首版 production registry 可以为空；tests 注入 deterministic handler。下一 slice 注册：

- `knowledge.ingest`
- `knowledge.reindex`
- `knowledge.delete`

### 10.2 `run_job`

```text
validate scope == configured worker scope
  -> claim DB lease
  -> terminal/missed claim: return current status
  -> resolve handler
  -> handler(JobContext, payload)
  -> succeed / retry / fail / cancel
```

`asyncio.CancelledError` 不转换为业务失败；worker shutdown 让 lease 过期后恢复。

Handler contract：

- 单个不可中断操作必须小于 `lease_seconds / 2`；
- 更长 handler 必须在自然边界调用 `checkpoint()`；
- `knowledge.ingest` 至少在每个 embedding batch 后 checkpoint。

## 11. `JobContext`

```python
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
```

`progress()`：

- 更新 progress；
- 刷新 heartbeat/lease expiry；
- 检查 cancel；
- cancel 时抛 `JobCancellationRequested`。

Handler 必须在自然边界调用，例如：

- 文档读取完成；
- 每 N 个 chunks；
- embedding batch 完成；
- 每个 document 完成。

首版不注入线程、signal 或 task cancellation。

## 12. Retry 与错误分类

### 12.1 Error types

```python
class RetryableJobError(Exception):
    code: str
    public_message: str


class PermanentJobError(Exception):
    code: str
    public_message: str
```

规则：

- `PermanentJobError`：直接 final failed。
- `RetryableJobError`：attempt 未耗尽时回 queued。
- 未知 Exception：默认 retryable，直到耗尽；日志包含 traceback。
- validation/unknown kind/cross-scope：permanent。
- `asyncio.CancelledError`：向外传播，等待 lease recovery。

### 12.2 Backoff

```text
delay = min(
  job_retry_base_seconds * 2 ** (attempt - 1),
  job_retry_max_seconds
)
```

默认：

- `job_max_attempts = 3`（definition 可降低）；
- `job_retry_base_seconds = 5`；
- `job_retry_max_seconds = 300`。

无 jitter，保证测试与运行记录可预测。

Retryable error 的 worker flow：

1. 若 `lease.attempt < max_attempts`：
   - `requeue()`：`running -> queued`，保存 bounded error 与 `next_attempt_at`，
     **不做 terminal injection**；
   - best-effort 调 arq delayed enqueue（`_defer_until=next_attempt_at`）。
2. 若 `lease.attempt >= max_attempts`：
   - 调 `fail_terminal()`；
   - 写 final failed + exactly-once result injection；
   - 绝不再回 queued。
3. delayed enqueue 丢失时，DB dispatcher 仍会补投。

因此有效 retry latency 由 backoff 决定，而不是被 30 秒 recovery tick 强制取整。

worker enqueue seam 必须支持 keyword options：

```python
async def enqueue(name: str, *args: object, **options: object) -> None:
    await redis.enqueue_job(name, *args, **options)
```

以便透传 arq `_defer_until`。

## 13. Progress

Progress 是 **最新快照**：

- `current`
- `total`
- `message`
- `progress_updated_at`

约束：

- `current >= 0`
- `total is None or total >= 0`
- 若 total 非空，则 `current <= total`
- 同一 attempt 内 current 不可回退
- 新 attempt claim 时 progress 重置为 0

Progress 只是展示/heartbeat 数据，**不是 resume cursor**。首版 retry 会从 handler 开头
重新执行；RAG handler 必须通过 document/chunk 的业务幂等键避免重复持久化和重复计费。

首版通过：

- `GET /v1/jobs`
- `GET /v1/jobs/{id}`

polling 获取。后续 Jobs UI 可增加 SSE/Redis fanout，但不改变 store contract。

## 14. Terminal result 与 exactly-once session injection

### 14.1 用户确认的 UX

若 `target_session_id` 非空：

- 终态结果作为 **assistant message** 回注；
- 不自动触发新的 agent run；
- 用户下一次输入时，projection 会把该结果作为已有上下文。

### 14.2 Payload

使用现有 `message.token` event：

```json
{
  "role": "assistant",
  "text": "知识库文档已完成索引：42 个 chunks。",
  "partial": false,
  "job_id": "job_...",
  "job_kind": "knowledge.ingest",
  "job_status": "succeeded"
}
```

失败/取消使用 framework 生成的 bounded message：

```text
后台任务 knowledge.ingest 失败：embedding_timeout
后台任务 knowledge.ingest 已取消。
```

### 14.3 Atomic transaction

`PostgresJobStore.succeed/fail_terminal/finish_cancelled/fail_exhausted`
都通过同一个 terminal finalizer transaction：

1. `SELECT ... FOR UPDATE` job row；
2. 校验 status/lease token；
3. 校验 target session 属于同一 scope；
4. 若 `target_session_id != NULL` 且 `injected_event_seq IS NULL`：
   - 原子推进 `sessions.next_seq`；
   - 插入一个 `message.token`；
   - 保存 `injected_event_seq`；
5. 更新 terminal status/result/error/finished_at；
6. commit。

因此：

- transaction rollback：row 和 message 都不存在；
- commit 后 worker crash：重复 delivery 看到 terminal，不再注入；
- duplicate finalize：lease/status check 拒绝；
- result injection exactly once。

### 14.4 Model-facing role alternation

真实 UI/event log 仍保留独立的 injected assistant event。为了避免下一轮 provider request
出现两个连续 assistant messages，`project_messages()` 的 model-facing projection 必须：

- 合并相邻的 **plain assistant message**；
- 使用 `"\n\n"` 连接内容；
- 不跨 assistant `tool_calls`、`tool.result` 或其他 role 合并；
- 保留原始 event log 与 UI 展示边界。

增加 Anthropic/OpenAI-compatible projection tests，证明下一轮 request role sequence 合法。

### 14.5 Target session policy

- `target_session_id` 可为空。
- 非空时 enqueue 前必须已存在于同 scope。
- job framework 不自动创建用户 session。
- RAG feature 可选择：
  - 来自 chat：注入 originating session；
  - 来自管理页面：不指定 session，只在 Jobs/KB UI 展示。

### 14.6 Failed job 的重新执行

`idempotency_key` 表示“一次 enqueue request”，不是永久 resource version key。

- HTTP/client retry 使用同一个 key，返回同一 job。
- terminal failed 后的显式重新执行使用新的 request key，创建新 job。
- RAG 的 document/version 幂等由 Knowledge 数据模型保证，不依赖复用同一个 job row。
- 首版不提供 generic retry API；Knowledge endpoint 后续可接受新的 request idempotency key。

## 15. API 与 RBAC

新增 DTO 到 `keel_core.api`。

### 15.1 List

```http
GET /v1/jobs?status=running&kind=knowledge.ingest&limit=50
```

- Role：viewer。
- 只返回当前 scope。
- newest first。
- result/error message bounded。

### 15.2 Detail

```http
GET /v1/jobs/{job_id}
```

- Role：viewer。
- cross-scope / missing：404，避免 existence leak。

### 15.3 Cancel

```http
POST /v1/jobs/{job_id}/cancel
```

- Role：operator。
- queued：返回 `cancelled`。
- running：返回 `running` + `cancel_requested=true`。
- terminal：幂等返回当前 record。

不提供通用 create/retry/inject endpoint。

## 16. Scope 与安全

### 16.1 Scope

首版仍运行单 configured durable scope，但消除隐式 scope：

- arq args：`run_job(scope_id, job_id)`；
- worker 验证 `scope_id == ctx["durable_scope"]`；
- store 构造时绑定 scope；
- API 使用 `app.state.durable_scope`；
- RLS defense-in-depth。

未来多 scope worker 只替换 scope discovery/authorization，不改变 jobs schema。

### 16.2 Payload safety

- payload 只交给 allow-listed handler；
- handler 用 Pydantic model 校验；
- 禁止 import path / shell string / callable serialization；
- logs 不输出 payload/result；
- public error 不含 traceback、token、secret；
- result/payload 有大小限制。

默认限制：

- `job_payload_max_bytes = 64 KiB`
- `job_result_max_bytes = 64 KiB`
- `job_result_message_max_chars = 8_000`
- `job_error_message_max_chars = 2_000`

大结果必须写 artifact/document store，只在 job result 保存 reference。

## 17. Settings

新增 `KEEL_` settings：

```python
job_lease_seconds: int = 300
job_dispatch_limit: int = 100
job_retry_base_seconds: int = 5
job_retry_max_seconds: int = 300
job_payload_max_bytes: int = 65_536
job_result_max_bytes: int = 65_536
job_result_message_max_chars: int = 8_000
job_error_message_max_chars: int = 2_000
```

`max_attempts` 属于 `JobDefinition`，不由 API client 控制。

Recovery dispatcher 首版复用 30 秒 worker cron cadence；feature enqueue 仍会立即投递。

## 18. Observability

每次 execution 建立 span：

```text
job.execute
  job.id
  job.kind
  job.scope_id
  job.attempt
  job.status
```

结构化日志只包含：

- scope
- job id
- kind
- attempt
- transition
- error kind
- duration

绝不记录 payload、result、result_message 或 secret。

## 19. 文件布局

```text
migrations/versions/0009_background_jobs.py

packages/keel-core/src/keel_core/
  jobs.py                         # models, protocol, in-memory + Postgres store
  api.py                          # read/cancel DTOs
  state.py                        # reusable in-transaction append + plain-assistant projection support
  projections.py                  # merge adjacent plain assistant messages for provider requests
  config.py                       # limits/retry/lease settings

packages/keel-worker/src/keel_worker/
  jobs.py                         # registry, JobContext, execute/dispatch orchestration
  main.py                         # arq run_job + dispatch_jobs wiring

packages/keel-server/src/keel_server/
  api/v1.py                       # GET list/detail, POST cancel
  app.py                          # durable job store / scope wiring

tests/unit/
  test_jobs.py
  test_worker_jobs.py

tests/integration/
  test_jobs_postgres.py
  test_jobs_api.py
  test_jobs_worker.py
```

## 20. 测试矩阵

### 20.1 Unit

1. enqueue idempotency。
2. valid state transitions。
3. terminal immutable。
4. progress monotonic + bounds。
5. retry backoff。
6. unknown kind permanent fail。
7. queued cancel。
8. running cancel observed at checkpoint。
9. duplicate arq delivery only one claim。
10. retryable requeue 不注入 terminal message。
11. lease-exhaustion 使用 terminal finalizer。
12. retryable error 在最后 attempt 调 `fail_terminal()`，不产生 orphan queued row。
13. stale delivery 不能突破 `attempt < max_attempts`。
14. payload/result/error bounding。

### 20.2 Postgres integration

1. migration + RLS。
2. cross-scope list/get/cancel invisible。
3. `(scope, kind, idempotency_key)` dedupe。
4. concurrent claim：one winner。
5. expired lease reclaim。
6. stale lease token 不能 progress/complete。
7. retry requeue。
8. max attempts exhausted → failed + exactly-one failure injection。
9. queued cancel + injection atomic。
10. success + injection atomic。
11. fail + injection atomic。
12. double finalize → one message。
13. crash-after-complete simulation → duplicate delivery no second message。
14. target session cross-scope 拒绝。
15. expired lease reclaim increments attempt and replaces lease token。

### 20.3 API/RBAC

1. viewer list/detail。
2. viewer cancel → 403。
3. operator cancel。
4. missing/cross-scope → 404。
5. filters/limit。

### 20.4 Worker acceptance

1. immediate enqueue 丢失，dispatcher 补投。
2. two deliveries，one execution。
3. retryable handler fails twice, succeeds third次。
4. permanent handler executes once。
5. worker cancellation/restart 后 lease recovery。
6. result injected as assistant message。
7. injection 不触发 provider/new run。
8. next user turn 的 provider projection 不出现非法连续 assistant roles。

## 21. Rollout

1. 增加 migration/store/tests，不注册 production handler。
2. 增加 worker run/dispatch 与 API read/cancel。
3. 在 isolated `keel_test` 做 crash/retry/duplicate-delivery acceptance。
4. 合并后保持 jobs table 空。
5. RAG slice 注册第一个 production handler：
   `knowledge.ingest`。

不需要 feature flag：没有注册的 production kind 就不会创建 job。

## 22. 完成标准

Durable Background Jobs slice 完成必须满足：

- Postgres/RLS job state machine 落地；
- immediate enqueue + dispatcher recovery；
- lease/heartbeat/reclaim；
- progress/cancel；
- bounded retry；
- result injection exactly once；
- list/detail/cancel API + RBAC；
- duplicate delivery/crash windows 有 integration tests；
- full tests、Ruff、format、mypy 全绿；
- live isolated DB smoke；
- `docs/STATUS.md` 更新为 background jobs completed；
- RAG design 可直接引用本 spec，不重新发明 job lifecycle。
