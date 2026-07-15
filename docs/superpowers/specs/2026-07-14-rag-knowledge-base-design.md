# RAG / Knowledge Base Vertical Slice 设计文档

> **状态：** Reviewed / implementation-ready
> **日期：** 2026-07-14
> **Milestone：** M3 Knowledge & Quality
> **依赖：** Durable Background Jobs (`main@9c4d09f+`)
> **相关文档：**
> [`STATUS.md`](../../STATUS.md)、
> [`PRD.md`](../../PRD.md) FR-D5、
> [`ARCHITECTURE.md`](../../ARCHITECTURE.md) §8、
> [`ADR-0007`](../../adr/0007-embeddings-and-rerank.md)、
> [`ADR-0009`](../../adr/0009-product-form-and-primary-use-cases.md)、
> [`ADR-0010`](../../adr/0010-durable-background-jobs.md)

## 1. 目标

交付 Keel 的第一个完整 RAG/Knowledge Base vertical slice：

```text
create KB
  -> add/update text or Markdown document
  -> durable knowledge.ingest job
  -> deterministic chunk
  -> embed
  -> atomically activate document version
  -> hybrid retrieve
  -> API + kb_search tool return citations
  -> update/reindex/delete
  -> replayable knowledge evals
```

首版必须同时满足：

- KB 严格绑定当前 agent scope；
- 文档生命周期独立于 Archival memory；
- ingestion 可恢复、可取消、可重试；
- 更新失败不破坏旧 active version；
- 检索结果携带稳定 citation；
- 所有 KB 内容始终为 `tainted`；
- 删除后立即不可检索，异步物理清除内容/chunks；
- 最小 React 管理页可创建 KB、管理文档、查看 job 状态并搜索。

## 2. 已确认的产品决策

| 主题 | 决策 |
|---|---|
| 可见性 | **仅当前 agent scope 可见**；不做跨 scope grants。 |
| 删除 | **立即隐藏 + 异步 purge + 最小 tombstone**。 |
| 原文存储 | text/Markdown 原文首版存 **Postgres**，设置大小上限；以后可迁移 MinIO。 |
| 内容信任 | 所有 KB retrieval 一律 `ContentTaint.tainted`，无 trusted override。 |
| HTTP 幂等 | mutating Knowledge API 使用标准 **`Idempotency-Key` header** 和持久化 request ledger。 |
| UI | 最小管理页：KB 列表、文档创建/更新/删除、job 状态、搜索与 citations。 |

## 3. 非目标

首版不做：

- Google Drive / OneDrive / Notion connector ingest；
- PDF、HTML、Office、OCR、图片；
- binary multipart upload；
- MinIO/object storage；
- 跨 scope/team grants；
- trusted KB bypass；
- reranker；
- answer generation / citation-enforced final answer；
- arbitrary URLs/web crawler；
- full retention/erasure framework；
- complete Jobs UI；
- generic job create/retry API；
- multi-model migration in one KB；
- automatic cross-KB global search；
- chunk-level manual editing。

## 4. 为什么不能复用 `archival`

`archival` 适合：

- 轻量、standalone memory passage；
- 无文档层级；
- 无 active-version 切换；
- provenance 主要来自 session events；
- content/hash 粒度的 dedupe。

Knowledge Base 需要：

- KB/document/version/chunk 四层 lifecycle；
- 原文与 chunk offsets；
- update/reindex 的 atomic active switch；
- delete/tombstone/purge；
- citation 指向稳定 document version；
- document/job 状态；
- source metadata。

因此首版增加独立表，不把 `archival` 演化为通用文档仓库。

## 5. Architecture

```text
FastAPI / React
    |
    v
KnowledgeService ---------> Postgres KnowledgeStore
    |                             |
    | enqueue_once                | metadata/raw/version/chunks
    v                             |
PostgresJobStore                  |
    |                             |
    v                             |
arq run_job ----------------------+
    |
    v
KnowledgeJobHandlers
    -> chunker
    -> Embedder
    -> activate version

Agent loop
    -> kb_search Tool
    -> KnowledgeSearchStore
    -> lexical + vector + RRF
    -> cited + tainted ToolResult
```

### 5.1 模块边界

```text
packages/keel-core/src/keel_core/
  knowledge/
    models.py       # records, DTO-neutral domain models, citations
    chunking.py     # deterministic text/Markdown chunker
    store.py        # metadata/version/chunk lifecycle
    search.py       # hybrid retrieval + degraded mode
    tools.py        # kb_search
    jobs.py         # strict job payloads + core handlers/hooks

packages/keel-server/src/keel_server/
  api/knowledge.py  # scoped REST API

packages/keel-worker/src/keel_worker/
  knowledge.py      # wrap core handlers/hooks as JobDefinition entries
  jobs.py           # terminal lifecycle hooks for domain cleanup
  main.py           # register production knowledge job definitions

web/src/features/knowledge/
  types.ts
  useKnowledge.ts
  KnowledgePage.tsx
  KnowledgePage.test.tsx

evals/datasets/knowledge/v1.jsonl
evals/cassettes/knowledge/v1-embeddings.json
```

`keel_core.knowledge` 不依赖 server/web/worker。

## 6. 数据模型

新增 migration：`0010_knowledge_base`（revision name 以当前 Alembic head 顺延）。

所有表：

- `scope_id NOT NULL`
- query 显式过滤 scope
- transaction 设置 `app.scope_id`
- RLS policy fail closed
- 对重要关系使用 composite `(scope_id, id)` FK，避免 superuser 绕过 RLS 时跨 scope 引用。

### 6.1 `knowledge_bases`

```sql
CREATE TABLE knowledge_bases (
    id text PRIMARY KEY,
    scope_id text NOT NULL,
    name text NOT NULL,
    description text,
    embedding_model text NOT NULL,
    embedding_dim integer NOT NULL CHECK (embedding_dim > 0),
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'deleted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    UNIQUE (scope_id, id)
);
```

第一版 `(embedding_model, embedding_dim)` 创建后不可修改，并且不接受 client
指定：create KB 从 server 当前配置的 embedder snapshot 这两个值。未配置 embedder
时 create KB 返回 `503 embeddings_not_configured`。

worker ingest 前必须验证其 embedder model/dim 与 KB pinned values 一致；不一致是
permanent `embedding_configuration_mismatch`，不得把使用其他模型生成的向量标成
KB pinned model。search 遇到 server embedder 不匹配时跳过 semantic arm，明确返回
`lexical-degraded`。

### 6.2 `kb_documents`

```sql
CREATE TABLE kb_documents (
    id text PRIMARY KEY,
    scope_id text NOT NULL,
    kb_id text NOT NULL,
    title text NOT NULL,
    source_type text NOT NULL
        CHECK (source_type IN ('text', 'markdown')),
    source_uri text,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'active', 'failed', 'deleted')),
    desired_version_id text,
    active_version_id text,
    last_error_kind text,
    last_error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    UNIQUE (scope_id, id),
    UNIQUE (scope_id, kb_id, id),
    FOREIGN KEY (scope_id, kb_id)
        REFERENCES knowledge_bases(scope_id, id)
);
```

`desired_version_id` 防止较旧、较慢的 ingest 覆盖更新后的版本。

### 6.3 `kb_document_versions`

```sql
CREATE TABLE kb_document_versions (
    id text PRIMARY KEY,
    scope_id text NOT NULL,
    kb_id text NOT NULL,
    document_id text NOT NULL,
    version integer NOT NULL CHECK (version >= 1),
    content text,
    content_sha256 text NOT NULL,
    index_fingerprint text NOT NULL,
    mime_type text NOT NULL,
    chunking_version text NOT NULL,
    target_chars integer NOT NULL CHECK (target_chars > 0),
    overlap_chars integer NOT NULL
        CHECK (overlap_chars >= 0 AND overlap_chars < target_chars),
    ingest_job_id text,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'indexing', 'active', 'superseded',
            'failed', 'cancelled', 'deleted', 'purged'
        )),
    error_kind text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    activated_at timestamptz,
    deleted_at timestamptz,
    purged_at timestamptz,
    UNIQUE (scope_id, id),
    UNIQUE (scope_id, kb_id, document_id, id),
    UNIQUE (scope_id, document_id, version),
    FOREIGN KEY (scope_id, kb_id, document_id)
        REFERENCES kb_documents(scope_id, kb_id, id)
);
```

建完 versions 后再增加：

```sql
ALTER TABLE kb_documents
ADD CONSTRAINT fk_kb_documents_desired_version
FOREIGN KEY (scope_id, kb_id, id, desired_version_id)
REFERENCES kb_document_versions(scope_id, kb_id, document_id, id);

ALTER TABLE kb_documents
ADD CONSTRAINT fk_kb_documents_active_version
FOREIGN KEY (scope_id, kb_id, id, active_version_id)
REFERENCES kb_document_versions(scope_id, kb_id, document_id, id);
```

`content` 在 pending/active/superseded/failed/cancelled 时保留；purge 后置 `NULL`。
每个 version 持久化 `mime_type`、`chunking_version`、`target_chars` 与
`overlap_chars`。ingest 重启后只需 payload 中的 KB/document/version IDs，即可从
version 读取原文与完整 chunking 输入、从 `mime_type` 严格派生 `source_type`，并从
KB 读取 pinned embedding model/dim；不依赖进程内 settings snapshot。

### 6.4 `kb_chunks`

```sql
CREATE TABLE kb_chunks (
    id text PRIMARY KEY,
    scope_id text NOT NULL,
    kb_id text NOT NULL,
    document_id text NOT NULL,
    document_version_id text NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    text text NOT NULL,
    char_start integer NOT NULL CHECK (char_start >= 0),
    char_end integer NOT NULL CHECK (char_end >= char_start),
    content_hash text NOT NULL,
    heading_path jsonb NOT NULL DEFAULT '[]'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    model text NOT NULL,
    dim integer NOT NULL CHECK (dim > 0),
    embedding vector NOT NULL,
    fts tsvector GENERATED ALWAYS AS
        (to_tsvector('simple', text)) STORED,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (scope_id, id),
    UNIQUE (scope_id, document_version_id, ordinal),
    FOREIGN KEY (
        scope_id, kb_id, document_id, document_version_id
    ) REFERENCES kb_document_versions(
        scope_id, kb_id, document_id, id
    )
);
```

这些 composite FK 不只隔离 scope，也保证 document/version/chunk 不会在同一 scope
内被拼接到不同 KB。

### 6.5 `knowledge_idempotency`

所有 mutating Knowledge endpoint 共用持久化 request ledger：

```sql
CREATE TABLE knowledge_idempotency (
    id text PRIMARY KEY,
    scope_id text NOT NULL,
    operation text NOT NULL CHECK (operation IN (
        'create_base', 'delete_base', 'create_document',
        'update_document', 'reindex_document', 'delete_document'
    )),
    idempotency_key text NOT NULL,
    request_fingerprint text NOT NULL
        CHECK (char_length(request_fingerprint) = 64),
    resource_kind text NOT NULL
        CHECK (resource_kind IN ('base', 'document')),
    resource_id text NOT NULL,
    document_version_id text,
    job_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (scope_id, operation, idempotency_key)
);
```

- `operation` 只允许 `create_base`、`delete_base`、`create_document`、
  `update_document`、`reindex_document`、`delete_document`；
- `request_fingerprint` 是 method、normalized path IDs 和 validated canonical body 的
  SHA-256；不保存 raw content；
- 相同 `(scope, operation, key)` 且 fingerprint 相同，返回已记录的
  resource/version/job IDs；
- key 相同但 fingerprint 不同，返回 `409 idempotency_key_reused`；
- resource 与 ledger row 在同一 transaction 创建；
- job 可在该 transaction commit 后创建；若 crash，retry 根据 ledger 找回 resource，
  再用 deterministic job key 补建同一个 job 并回填 `job_id`；
- 并发首次请求以 transaction advisory lock
  `(scope, operation, idempotency_key)` 串行化。

表本身应用与其他 Knowledge 表相同的 scope filter、`app.scope_id` 和 fail-closed RLS。

### 6.6 Indexes

```sql
CREATE INDEX ix_kb_scope_status
ON knowledge_bases (scope_id, status);

CREATE UNIQUE INDEX uq_kb_scope_active_name
ON knowledge_bases (scope_id, name)
WHERE status = 'active';

CREATE INDEX ix_kb_documents_scope_kb_status
ON kb_documents (scope_id, kb_id, status);

CREATE INDEX ix_kb_versions_scope_document_status
ON kb_document_versions (scope_id, document_id, status);

CREATE INDEX ix_kb_versions_scope_document_fingerprint
ON kb_document_versions (scope_id, document_id, index_fingerprint);

CREATE INDEX ix_kb_chunks_scope_collection
ON kb_chunks (scope_id, kb_id, model, dim);

CREATE INDEX ix_kb_chunks_fts
ON kb_chunks USING gin (fts);

CREATE INDEX ix_kb_chunks_trgm
ON kb_chunks USING gin (text gin_trgm_ops);
```

首版不建固定维度 ANN index：

- `vector` 允许不同 pinned model/dim；
- 数据量先以单机小型 KB 为目标；
- query 严格过滤 `(model, dim)`；
- 后续基于实际规模选择 per-collection HNSW/partition。

## 7. ID、大小与输入限制

IDs 使用：

```text
kb_<uuid>
doc_<uuid>
kbv_<uuid>
kbc_<uuid>
kbi_<uuid>
```

限制：

```python
knowledge_document_max_bytes = 1_048_576      # 1 MiB UTF-8
knowledge_title_max_chars = 300
knowledge_description_max_chars = 2_000
knowledge_search_query_max_chars = 2_000
knowledge_search_k_max = 10
knowledge_chunk_target_chars = 1_600
knowledge_chunk_overlap_chars = 200
knowledge_tool_output_max_chars = 8_000
```

输入：

- 必须是 valid UTF-8 string；
- 拒绝 NUL、unpaired surrogate；
- normalize CRLF → LF；
- 空白原文拒绝；
- JSON/API errors 不回显原文。

## 8. Deterministic chunking

### 8.1 统一 normalized text

```text
CRLF/CR -> LF
remove UTF-8 BOM
trim trailing whitespace per line
collapse >3 blank lines to 2
strip document ends
```

`char_start/char_end` 指向 normalized text。

### 8.2 Markdown

Markdown chunker：

1. 解析 ATX headings `#..######`；
2. heading 更新 `heading_path`；
3. 按段落、list block、fenced code block 划分 units；
4. fenced code 不在中间拆分，除非单 block 超过 hard limit；
5. pack units 到 target chars；
6. overlap 尽量从前一 chunk 最后的完整 unit 开始；
7. 超长 unit 按字符 hard split。

### 8.3 Plain text

- blank-line paragraph split；
- pack + overlap；
- 超长段落 hard split。

### 8.4 Hard-split coverage contract

- 每个 emitted span 都是 normalized text 的 exact contiguous substring；content 不改写，
  `char_start/char_end` 始终指向原 normalized text；
- hard-split emitted span 长度不超过 `target_chars`；唯一例外仍是保持完整、且长度不超过
  `4 * target_chars` 的 unsplit fenced code block；
- whitespace-only candidate window 不生成 embedding chunk，也不与相邻 window 合并；
- 每个 non-whitespace character 必须被至少一个 chunk 覆盖；任意未覆盖 gap 必须只包含
  Unicode whitespace；
- chunk 保持确定性顺序、允许 overlap、且永不为空或 whitespace-only，但不保证构成原文的
  contiguous partition。Eval 与 citation 必须使用持久化的 exact text/hash/offsets，不能通过
  拼接 chunks 重建原文，也不能假设 whitespace characters 全覆盖。

### 8.5 版本

```text
chunking_version = "keel-char-v1"
```

`index_fingerprint`：

```text
SHA-256(canonical compact sorted JSON {
  "content_sha256": content_sha256,
  "source_type": source_type,
  "chunking_version": chunking_version,
  "embedding_model": embedding_model,
  "embedding_dim": embedding_dim,
  "target_chars": target_chars,
  "overlap_chars": overlap_chars
})
```

`index_fingerprint` 不是永久唯一键。只有当 fingerprint 等于当前
`desired_version_id` 或 `active_version_id` 指向的 version，且该 version 状态为
`pending`、`indexing` 或 `active` 时，才幂等复用当前 version 并补建缺失 job。

若相同 fingerprint 只存在于历史 `superseded`、`failed`、`cancelled` 或 `purged`
version，必须创建单调递增的新 version。这样支持回退到旧内容，也允许在已有 active
version 时用 reindex 恢复失败/取消的较新 indexing attempt。

## 9. Document create/update/reindex

### 9.1 Create

`POST /v1/knowledge-bases/{kb_id}/documents`

1. require `Idempotency-Key`；
2. validate scope/KB/status/input；
3. normalize + hash；
4. advisory-lock + transaction 创建 idempotency ledger、document 与 version 1；
5. `desired_version_id = version.id`；
6. commit；
7. `jobs.enqueue_once(kind="knowledge.ingest", key=f"ingest:{version.id}")`；
8. best-effort arq enqueue；
9. transaction update `version.ingest_job_id` 与 ledger `job_id`；
10. 返回 `202` document/version/job。

若进程在 version commit 后、job create 前 crash：

- HTTP response 未完成；
- client 以同一 `Idempotency-Key` retry；
- endpoint 从 ledger 找回同一 version；
- 补建同一个 `ingest:<version.id>` job。

### 9.2 Update

`PUT /v1/knowledge-bases/{kb_id}/documents/{document_id}`

- 新增 version N+1；
- transaction lock document row 后分配 version number、做 current fingerprint
  dedupe 并更新 desired pointer；
- 更新 title/source metadata；
- `desired_version_id` 指向新 version；
- 旧 `active_version_id` 继续可检索；
- 新 ingest 成功前不切换。
- 若 normalized request 与当前 pending/indexing/active desired/active version 的
  fingerprint 相同，更新 document metadata 后复用该 version；历史同 fingerprint
  version 不阻止创建 N+1。

### 9.3 Reindex

`POST /.../{document_id}/reindex`

- 从 active version 的原文创建新 version；
- source format 只从 active version 的 `mime_type` 严格派生：
  `text/plain -> text`、`text/markdown -> markdown`；其他 MIME 拒绝。不得使用可被
  pending/failed update 改写的 document-level `source_type`；
- 若没有 active version，返回 `409 no_active_version`；首次 ingest 失败后的恢复使用
  update 重新提交原文；
- 使用当前 chunking settings 与 KB pinned model/dim；
- 当前 desired/active version 已有相同 fingerprint 且状态为
  pending/indexing/active 时幂等返回并补建缺失 job；
- 只有历史 failed/cancelled/superseded version 匹配时仍创建 N+1；
- 不更改文档内容；
- 仍通过 `knowledge.ingest` handler。

首版不需要独立 `knowledge.reindex` handler；reindex endpoint 只是创建新 version +
enqueue `knowledge.ingest`。减少重复执行逻辑。

## 10. Durable job kinds

### 10.0 Durable job lifecycle extension

Knowledge ingest 的 partial chunks/domain status 不能被通用 job terminal transition
绕过。migration 同时扩展已有 `jobs`：

```sql
ALTER TABLE jobs
ADD COLUMN cancel_mode text NOT NULL DEFAULT 'immediate'
CHECK (cancel_mode IN ('immediate', 'cooperative', 'disabled'));

ALTER TABLE jobs
ADD COLUMN terminal_intent text
    CHECK (terminal_intent IN ('failed', 'cancelled')),
ADD COLUMN terminal_intent_at timestamptz,
ADD CONSTRAINT ck_jobs_terminal_intent_timestamp
    CHECK (
        (terminal_intent IS NULL AND terminal_intent_at IS NULL)
        OR (terminal_intent IS NOT NULL AND terminal_intent_at IS NOT NULL)
    );
```

语义：

- `immediate`：保持现有行为；queued job 可直接 cancelled，running job cooperative；
- `cooperative`：queued/running 都只写 `cancel_requested_at`；queued job 立即变为 due，
  由 worker claim 后执行 domain cancel hook，再进入 cancelled；
- `disabled`：`request_cancel` 返回 `409 job_not_cancellable`。

`JobRecord`、`enqueue_once()` 与 Jobs API additive 暴露 `cancel_mode`；现有调用默认
`immediate`，行为不变。

`terminal_intent` 是 hook 与 job terminal transition 之间的持久化协议：

- worker 持有有效 lease 时先原子 reserve `failed` 或 `cancelled` intent，再执行
  对应 hook；
- failure intent 同时写入 bounded `error_kind/error_message`；
- intent 一旦写入不可切换；running failed-intent job 的后续 cancel 返回
  `409 job_finalizing`，已 terminal job 仍幂等返回当前 record；
- claim/normal dispatch 不得 reclaim intent row；
- lease 在 hook 中过期时，原 worker 用 fresh clock finalization 失去 lease；dispatcher
  之后只重放同一种 hook 并完成同一 intent；
- max-attempt lease expiry 通过 row lock 原子 reserve exhaustion intent：只有
  expiry 前已持久化的 cancel request 才选择 cancelled，否则选择 failed；
- hook/domain commit 后、job terminal commit 前 crash 只会重复同一种幂等 hook，
  terminal event injection 仍由原有 transaction exactly once 完成。

`JobDefinition` 增加可选 hooks：

```python
JobCancelledHook = Callable[[JobRecord], Awaitable[None]]
JobFailedHook = Callable[[JobRecord, JobError], Awaitable[None]]


@dataclass(frozen=True)
class JobDefinition:
    ...
    on_cancelled: JobCancelledHook | None = None
    on_failed: JobFailedHook | None = None
```

- worker reserve terminal intent 后、`finalize_terminal` **之前**调用对应 hook；
- lease-expiry recovery 先读取/原子 reserve immutable intent，再调用唯一对应 hook，
  最后 `finalize_exhausted`；
- hooks 必须 idempotent、scope-bound、不得记录 payload/raw content；
- hook 成功后 worker crash：下一次重复 hook，再做 terminal transition；
- hook 失败：job 保持 running + reserved intent，后续 dispatcher 重试；不得先
  terminal 再 best-effort cleanup；
- `JobContext` additive 暴露 `max_attempts`，便于 handler/error tests，但 Knowledge
  correctness 依赖 terminal hooks，而不是仅依赖 handler catch。

### 10.1 `knowledge.ingest`

Payload：

```python
class KnowledgeIngestPayload(BaseModel):
    kb_id: str
    document_id: str
    document_version_id: str
```

Handler：

1. load same-scope KB/doc/version；
2. lock/check lifecycle：KB/document deleted 或 version deleted/purged 时 cleanup 后幂等
   退出；active/superseded/failed/cancelled version 不得被降级回 indexing；
3. 若尚未 active 且 `document.desired_version_id != version.id`，清理 chunks、标记
   superseded 后幂等退出；
4. mark version `indexing`；
5. normalize/chunk；
6. progress `chunking`；
7. embed batches；
8. per batch checkpoint；
9. upsert chunks by `(scope, version, ordinal)`；
10. remove extra stale ordinals from prior failed attempt；
11. activation transaction。

Terminal replay：

- version 已 active/superseded：返回对应幂等 `JobResult`；
- version 已 failed：重新抛出 stored bounded permanent error，使尚未完成的 job
  terminal transition 继续走 `on_failed`；
- version 已 cancelled 且 job 仍有 cancel request：pre-handler checkpoint 再次走
  `on_cancelled`；
- 这保证 hook/domain commit 后、job terminal commit 前 crash 可安全重放。

每次 chunk batch write 都在短 transaction 中锁 document row，并再次确认
KB/document 未删除且 `version.status == "indexing"`；否则不写入并退出当前
execution。embedding provider call 不持锁。delete/purge 也锁同一 document row，
因此已完成 terminal cleanup/purge 后 zombie execution 不可能重新写入 chunk。

Activation：

```text
lock document + version
if kb.deleted or document.deleted:
  delete this version's chunks
  clear raw content
  version -> purged, no activation
elif version.status != indexing:
  preserve authoritative terminal/active status, no activation
elif document.desired_version_id != version.id:
  delete chunks for this never-active attempt
  version -> superseded
else:
  old active -> superseded
  new version -> active
  document.active_version_id = version.id
  document.status = active
  clear document error
```

Retry：

- whole handler restarts；
- chunk upsert idempotent；
- embeddings may repeat；
- activation transaction idempotent。

Cancellation：

- checkpoint 每 embedding batch；
- enqueue 使用 `cancel_mode="cooperative"`；
- `on_cancelled` 在 domain cleanup transaction 中：
  - version `cancelled`；
  - delete chunks for incomplete version；
  - old active version remains，document 继续 active；没有 old active 时 document
    进入 failed 并写 bounded cancellation status；
  - 若 document/version 已 deleted/purged，保持 delete precedence；
  - hook 返回后 worker 才使 job terminal `cancelled`。

Failure：

- retryable error：job requeue；version 可保持 indexing；
- `on_failed` 删除 incomplete chunks，并把 version 标为 `failed`；document
  `failed` only when no active version；
- expected permanent failure、final retryable failure、unexpected terminal failure 和
  lease-expiry exhaustion 都经过同一个 hook；
- active old version 永不被破坏。

所有 handler transition 都遵守 `purged > deleted > cancelled/failed/superseded >
indexing` 的 precedence，并在 document lock 下重读当前状态；ingest cleanup/failure
不得把 delete 已写入的 tombstone 降级成旧状态。

### 10.2 `knowledge.delete`

Document delete API transaction 先立即隐藏：

- lock document row；
- document `status=deleted`；
- `active_version_id=NULL`；
- `desired_version_id=NULL`；
- `deleted_at=now()`。

KB delete API transaction：

- lock KB row；
- KB `status=deleted, deleted_at=now()`；
- partial unique name constraint 随即释放；
- search 因 KB 非 active 立即返回空；
- 后续 create/update/reindex 在 service 层拒绝。

然后 enqueue：

```python
class KnowledgeDeletePayload(BaseModel):
    kb_id: str
    document_id: str | None = None
```

Handler：

- document delete：lock document row；delete all chunks；所有 versions
  `content=NULL,status=purged,purged_at`；
- KB delete：逐 document 使用相同 lock + purge transaction，并把 document 标为
  deleted；
- 保留 KB/document/version IDs、KB name、document title、hash、version、
  timestamps、bounded job/error status；
- purge 清空 KB description、document `source_uri` 与所有 version raw content；
- no chunk/content remains。

Purge 是幂等操作。tombstone 后可 best-effort request-cancel 对应非 terminal ingest
jobs 以节省资源，但 correctness 不依赖 cancellation；batch-write lock 与 activation
cleanup 必须独立关闭所有 ingest/delete interleaving。

`knowledge.delete` 使用 `cancel_mode="disabled"` 和
`max_attempts=2_147_483_647`：删除一旦同步 tombstone 就不能通过 generic Jobs API
取消，DB/provider transient failure 以 capped backoff 持续重试。若最终进入理论上的
attempt exhaustion，`on_failed` 仍先执行同一个幂等 purge，确保 raw content/chunks
cleanup 不被 job terminal transition 绕过。

## 11. Knowledge Store contracts

主要接口：

```python
class KnowledgeStore:
    async def create_base(...)
    async def create_base_idempotent(command, idempotency, ...)
    async def list_bases(...)
    async def get_base(...)
    async def create_document_version(...)
    async def create_document_version_idempotent(command, idempotency, ...)
    async def update_document_version_idempotent(command, idempotency, ...)
    async def reindex_document(...)
    async def reindex_document_idempotent(command, idempotency, ...)
    async def get_document(...)
    async def list_documents(...)
    async def mark_indexing(...)
    async def replace_version_chunks(...)
    async def activate_version(...)
    async def mark_version_failed(...)
    async def mark_version_cancelled(...)
    async def tombstone_document(...)
    async def tombstone_document_idempotent(command, idempotency, ...)
    async def tombstone_base(...)
    async def tombstone_base_idempotent(command, idempotency, ...)
    async def purge_document(...)
    async def purge_base(...)
    async def get_idempotent_request(...)
    async def attach_idempotent_job(...)
```

Store：

- scope-bound constructor；
- every transaction `_SET_SCOPE`；
- explicit scope filters；
- global-ID foreign-scope collision → not found / domain error；
- payload/title/content safety；
- raw content 不写日志。
- idempotency key reuse 必须比较 request fingerprint，不能把不同请求 replay 成旧
  resource。

## 12. Hybrid retrieval

### 12.1 Query scope

Search requires explicit `kb_id`：

```python
async def search(
    kb_id: str,
    query: str,
    *,
    k: int = 5,
) -> tuple[list[KnowledgeHit], KnowledgeSearchStatus]
```

只查：

- KB active；
- document status active；
- `kb_documents.active_version_id = kb_chunks.document_version_id`；
- chunk `(model, dim)` 与 KB pinned values 相同。

### 12.2 Lexical arm

```sql
GREATEST(
  similarity(chunk.text, :query),
  ts_rank(chunk.fts, plainto_tsquery('simple', :query))
)
```

CJK 依赖 trigram。

### 12.3 Semantic arm

- embed query；
- server embedder model/dim 必须与 KB pinned values 相同；
- filter KB model/dim；
- cosine distance order；
- exact KNN first slice。

### 12.4 RRF

复用 `rrf_fuse()`。现有 helper 接受 integer IDs；search 在一次请求内为 candidate
chunk IDs 建立稳定的 local integer map，fuse 后再映射回 chunk IDs，不把 UUID/text
ID 直接传给 helper。

```text
candidates = max(k * 3, 20)
RRF k = existing default
```

### 12.5 Degradation

```python
mode: Literal["hybrid", "lexical", "lexical-degraded"]
semantic_error: str | None
```

- embedding 未配置：`lexical`
- embedding runtime error 或 configured model/dim 与 KB 不匹配：
  `lexical-degraded`
- 不吞掉 lexical result；
- `semantic_error` 只使用 bounded public error kind，不包含 provider message；
- log 只含 scope/kb/model/dim/error type。

## 13. Citations

```python
class KnowledgeCitation(BaseModel):
    id: str                  # cite_1 per result list
    kb_id: str
    document_id: str
    document_version_id: str
    chunk_id: str
    title: str
    source_uri: str | None
    ordinal: int
    char_start: int
    char_end: int
    label: str               # "Guide.md#chunk-3"


class KnowledgeHit(BaseModel):
    snippet: str
    rank: int
    citation: KnowledgeCitation
    heading_path: list[str]
```

Citation 指向 immutable version/chunk。document 删除后：

- search 不再返回；
- 历史 event 中 citation metadata 仍可显示；
- 首版不提供独立 citation-resolve endpoint；document API 只返回 tombstone metadata，
  不返回已删除 content。

## 14. `ToolResult` citation extension

Additive extension：

```python
class Citation(BaseModel):
    id: str
    label: str
    source: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ToolResult(BaseModel):
    ...
    citations: list[Citation] = Field(default_factory=list)
```

Loop `tool.result` event payload 增加：

```json
"citations": [...]
```

Provider model-facing `output`：

```text
[1] Guide.md#chunk-3
<snippet>

[2] FAQ.md#chunk-1
<snippet>
```

UI 使用 event citations 渲染，不从 model text 反解析。

## 15. `kb_search` tool

```python
name = "kb_search"
writes = False
```

Input：

```json
{
  "type": "object",
  "properties": {
    "kb_id": {"type": "string"},
    "query": {"type": "string"},
    "k": {"type": "integer", "minimum": 1, "maximum": 10}
  },
  "required": ["kb_id", "query"]
}
```

Result：

- `ok=True`
- `output=bounded numbered snippets`
- `display=optional Markdown`
- `citations=structured`
- `taint=ContentTaint.tainted` **always**

Web agent：

- engine/embedder available 时注册 `kb_search`；
- permission 为 allow（read-only）；
- 当前 web agent 没有 outbound connector tool；`kb_search` 自身仍始终产生 taint；
- 任一同时注册 outbound connector 的 agent configuration 必须把 base permission
  engine 包装为 `ConfusedDeputyEngine`；
- taint 进入 durable event 后，后续 **下一批/下一轮** connector outbound 升级为
  ask。同一并行 tool batch 不作为安全边界，因此安全测试必须顺序执行。

## 16. API

新增 router：`/v1/knowledge-bases`。

### 16.1 RBAC

| Endpoint | Role |
|---|---|
| list/get/search/status | viewer |
| create/update/reindex/delete | operator |
| hard purge | 不提供 public API |

### 16.2 Endpoints

```text
POST   /v1/knowledge-bases
GET    /v1/knowledge-bases
GET    /v1/knowledge-bases/{kb_id}
DELETE /v1/knowledge-bases/{kb_id}

GET    /v1/knowledge-bases/{kb_id}/documents
POST   /v1/knowledge-bases/{kb_id}/documents
GET    /v1/knowledge-bases/{kb_id}/documents/{document_id}
PUT    /v1/knowledge-bases/{kb_id}/documents/{document_id}
POST   /v1/knowledge-bases/{kb_id}/documents/{document_id}/reindex
DELETE /v1/knowledge-bases/{kb_id}/documents/{document_id}

GET    /v1/knowledge-bases/{kb_id}/search?q=&k=
```

### 16.3 Idempotency

以下 endpoint 要求：

```http
Idempotency-Key: <1..512 UTF-8 bytes>
```

- create KB
- create document
- update document
- reindex
- delete

相同 operation + key + fingerprint 返回同一 resource/version/job IDs。相同
operation + key 携带不同 path/body 返回 `409 idempotency_key_reused`。

不接受 request body `idempotency_key`。

### 16.4 Requests

Create KB 不接受 model/dim：

```json
{
  "name": "Product docs",
  "description": "Keel product documentation"
}
```

Response 中回显 server snapshot 的 pinned `embedding_model` 和 `embedding_dim`。

Document create/update：

```json
{
  "title": "Guide.md",
  "source_type": "markdown",
  "content": "# Guide\n...",
  "source_uri": null,
  "target_session_id": null
}
```

Response `202`：

```json
{
  "document": {...},
  "version": {...},
  "job": {...}
}
```

Job 详情继续使用 `/v1/jobs/{job_id}`。

## 17. Minimal React UI

新增 sidebar：

```text
工作区
  Knowledge
```

Route：

```text
/knowledge
```

### 17.1 页面

左栏：

- KB list；
- create KB。

中栏：

- selected KB documents；
- title/status/version/job progress；
- add text/Markdown；
- update/reindex/delete。

右/下方：

- search box；
- result snippets；
- citations title/chunk/range；
- retrieval mode badge。

### 17.2 首版交互

- JSON text textarea，不做 binary upload；
- poll `/v1/jobs/{id}`；
- job terminal 后 invalidate document/KB query；
- delete optimistic hide；
- error 显示 bounded public error；
- no chat picker。

## 18. Settings

新增：

```python
knowledge_document_max_bytes: int = 1_048_576
knowledge_title_max_chars: int = 300
knowledge_description_max_chars: int = 2_000
knowledge_search_query_max_chars: int = 2_000
knowledge_search_k_max: int = 10
knowledge_chunk_target_chars: int = 1_600
knowledge_chunk_overlap_chars: int = 200
knowledge_embedding_batch_size: int = 32
knowledge_tool_output_max_chars: int = 8_000
```

Validation：

- overlap < target；
- limits positive；
- embedding batch > 0。

## 19. Observability

Spans：

```text
knowledge.ingest
knowledge.search
knowledge.delete
```

Allowed attributes：

- scope
- kb/document/version/job IDs
- source type
- content bytes（数字）
- chunk count
- embedding model/dim
- retrieval mode
- duration
- error kind/type

禁止：

- raw content
- chunk text
- query text
- citation snippet
- provider/token secret

## 20. Knowledge Evals

新增独立 replay suite，不把 dataset 强塞进 Memory case union。

### 20.1 Dataset

```text
evals/datasets/knowledge/v1.jsonl
```

Case 类型：

- English semantic paraphrase；
- Chinese semantic/CJK lexical；
- Markdown heading citation；
- multi-document ranking；
- no-answer；
- embedding failure → lexical-degraded；
- deleted document freshness；
- prompt-injection text remains tainted。

### 20.2 Determinism

- repo JSONL source of truth；
- deterministic normalized text、chunk ordinals、offsets、labels 与 ranking inputs；
- hard-split chunks 的 text/offsets 是 exact locators，但 chunks 可在 Unicode
  whitespace-only 区间留 gap；golden/citation assertions 不要求 contiguous full-text
  coverage；
- production UUID IDs 不作为 cassette/golden assertion；citation 断言使用
  `content_sha256 + version + ordinal + char_start/char_end`；
- embedding cassette 复用现有 format；
- replay 不回退 live；
- isolated `keel_test`/`keel_eval`；
- per-case scope cleanup。

### 20.3 Metrics/gates

```text
Recall@5 >= 0.80
MRR >= 0.70
Citation precision = 1.00
Deleted-doc leakage = 0
Taint pass rate = 1.00
Degraded-mode pass rate = 1.00
Weighted overall >= 0.80
```

Exit codes 与 Memory Evals 一致：

- 0 pass
- 1 quality gate
- 2 infra/dataset/cassette

## 21. Security

- all KB text tainted；
- API/store scope-bound + RLS；
- no cross-scope grants；
- job payload allow-listed/Pydantic；
- no path/import/callable in payload；
- source_uri metadata only，首版不 fetch；
- query/result logs redacted；
- citation metadata 不含完整 content；
- deleted raw content/chunks 被 purge；
- outbound-enabled agent 在已持久化 `kb_search` taint 后的下一批/下一轮 action
  requires confused-deputy approval。

## 22. Testing

### Unit

- normalization/chunk determinism；
- Markdown headings/code blocks；
- overlap/offsets；
- hash/fingerprint；
- request/header validation；
- citation model；
- tool formatting/bounds/taint；
- job payload validation；
- cancel mode validation and existing-job default compatibility；
- terminal hook ordering/error propagation；
- activation stale desired-version logic；
- settings。

### Postgres integration

- migration/index/RLS；
- composite FK blocks cross-scope references；
- composite FK blocks same-scope cross-KB document/version/chunk references；
- create/update idempotency；
- idempotency key/body conflict → 409，crash-after-resource-commit retry 补建 job；
- failed/cancelled reindex 与历史-content revert 创建新 version；
- no-active reindex → 409；first-ingest failure can recover through update；
- old active remains on failure；
- newer desired version prevents stale activation；
- chunk upsert retry；
- delete immediately hides；
- purge removes content/chunks, keeps tombstone；
- ingest/delete interleaving cannot recreate chunks after purge；
- search active-only；
- model/dim filter；
- ingest model/dim mismatch fails without mislabeled vectors；
- lexical/hybrid/degraded；
- CJK。

### Job acceptance

- real arq ingest；
- progress/checkpoint；
- duplicate delivery；
- crash/retry；
- cancel cleanup；
- cooperative queued/running cancellation and disabled cancellation semantics；
- lifecycle hook failure leaves job non-terminal and retries；
- queued-after-retry cancel still runs domain cleanup hook；
- cancelled lease exhaustion finalizes cancelled, not failed；
- lease-expiry failure exhaustion runs terminal hook before job finalization；
- zombie execution cannot write/activate after failed/cancelled hook；
- attempt exhaustion；
- result injection exactly once；
- delete purge idempotency。

### API/RBAC

- viewer reads/search；
- viewer mutation denied；
- operator CRUD；
- required Idempotency-Key；
- same key with different request fingerprint → 409；
- malformed/cross-scope IDs → 404；
- document limit；
- `knowledge.delete` cancel → 409；job responses expose cancel mode；
- no hard-purge or generic job create/retry endpoints。

### Web

- create/select KB；
- add/update/reindex/delete doc；
- status/progress；
- search/citation rendering；
- mode badge；
- error/empty states。

### E2E safety

1. configure an agent with `kb_search`, `email_send`, and
   `ConfusedDeputyEngine`；
2. doc contains prompt injection；
3. one model/tool batch calls `kb_search`；
4. tool result event is durably tainted and cited；
5. a later model/tool batch attempts outbound `email_send`；
6. confused-deputy engine produces approval；
7. no auto-send。

## 23. Rollout

1. migration/store/chunker；
2. search/citations/tool；
3. knowledge job handlers + worker registry；
4. REST API；
5. React management page；
6. knowledge eval dataset/cassette；
7. live isolated ingest/search/delete；
8. `docs/STATUS.md` update。

Migration 后默认没有 KB，不影响现有 memory/search。

## 24. 完成标准

- text/Markdown create/update/reindex/delete 全链路；
- active version atomic switch；
- old version survives failed update；
- durable ingest/delete jobs；
- hybrid/degraded search；
- citation-bearing API/tool；
- all results tainted；
- minimal React management UI；
- knowledge replay evals green；
- real provider/embedder smoke；
- full tests/Ruff/format/mypy green；
- merged `main` 后下一步转 Event Upcasters + Retention/Erasure。
