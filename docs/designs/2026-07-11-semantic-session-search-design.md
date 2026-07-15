# Semantic Session Search 设计文档

> **状态：** brainstorming 已完成，待用户复核后进入 implementation plan。
>
> **目标：** 把历史会话搜索从 lexical-only 升级为统一的混合检索：
> Postgres trigram/FTS + Ollama `bge-m3` semantic KNN + RRF，并同时服务于
> agent 的 `session_search` 工具和 Sessions 页面的搜索 API。
>
> **相关设计：** `ARCHITECTURE.md` §8.1–8.3、ADR-0007、
> `2026-07-09-memory-retrieval-foundation-design.md`。

---

## 1. 背景与现状

刚完成的记忆基础切片已经提供：

- Core memory：`memory_blocks` 每轮注入 + 自编辑工具。
- Archival memory：`archival_insert` + lexical/semantic RRF 检索。
- 本地 embedding：Ollama `bge-m3`，1024 维，`(model,dim)` 固定。
- Recall memory 工具：`session_search` 已接入 web assistant。

但历史会话搜索仍是 lexical-only：

- `keel_core.search.session_search()` 仅按 `pg_trgm similarity` 排历史消息。
- `keel_core.search.search_sessions()` 仅用 trigram + FTS，按最佳消息聚合 session。
- `events` 是 append-only source of truth，没有 message embedding 投影。

因此，同义改写、跨语言表达或没有共同关键词的搜索仍无法召回相关会话。

---

## 2. 范围

### 2.1 本切片交付

- 新增 scope-bound `message_embeddings` 投影表。
- 只索引完整、非空的 `user` + `assistant` `message.token` 事件。
- Web run 完成后，在独立后台任务中增量索引该 session。
- 搜索前最多自动补齐 500 条缺失 embedding，作为崩溃/历史数据的安全网。
- 用同一套 message-level 排名管线升级：
  - agent 的 `session_search` 工具；
  - `GET /v1/sessions/search`（Sessions 页面）。
- Semantic 不可用时显式降级为 lexical，而不是让搜索整体失败。
- 保持旧调用方式兼容：没有 embedder 时仍可 lexical-only 搜索。

### 2.2 不在本切片

- RAG / Knowledge Base 文档摄取与切块。
- Memory consolidation。
- IM / digest session 的自动 embedding。
- Reranker、temporal decay。
- HNSW / IVFFlat ANN 索引优化。
- 新的前端控件或搜索模式开关。
- Embedding 模型切换后的旧投影清理 / 批量 re-embed job。

---

## 3. 核心架构选择

采用独立 `message_embeddings` 投影表，而不是修改 `events` 或复用 `archival`。

### 3.1 为什么不直接修改 `events`

`events` 是 append-only source of truth。Embedding 是可重建、模型相关的派生数据：

- 需要异步生成和重试；
- 切换模型时需要生成新版本；
- 不应持续更新不可变事件；
- 不应让 Ollama 故障影响 durable event admission。

因此 embedding 必须位于独立投影层。

### 3.2 为什么不复用 `archival`

Archival memory 是 agent 主动保存、精选的长期 passage；recall memory 是完整会话历史。
两者在保留、删除、来源、UI 分组和未来 consolidation 中语义不同，不能混为一表。

---

## 4. 数据模型

新增 Alembic migration：`0007_message_embeddings.py`。

```sql
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
);

CREATE INDEX ix_message_embeddings_scope_session
    ON message_embeddings (scope_id, session_id);
CREATE INDEX ix_message_embeddings_scope_model_dim
    ON message_embeddings (scope_id, model, dim);
```

表启用 RLS，策略与其它 scoped 表一致：

```sql
USING (scope_id = current_setting('app.scope_id', true))
WITH CHECK (scope_id = current_setting('app.scope_id', true))
```

设计约束：

- `event_id` 外键 + `ON DELETE CASCADE`：删除源事件时不会留下孤儿向量。
- 主键包含 `(event_id, model, dim)`：同一消息可并存多个模型版本，支持未来迁移。
- `content` 是可重建快照，避免检索后再次读取 JSON payload；仍受同 scope 的 RLS 保护。
- 本切片使用 exact KNN；不添加 ANN 索引。

测试 fixture 的清理列表必须加入 `message_embeddings`，并与 `events` 一起 TRUNCATE。

---

## 5. 模块边界

新增 `packages/keel-core/src/keel_core/recall.py`，避免继续扩大同时承担 archival
和 API 映射职责的 `search.py`。

### 5.1 数据类型

```python
RecallMode = Literal["hybrid", "lexical", "lexical-degraded"]

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
class BackfillResult:
    indexed: int
    remaining: bool
```

### 5.2 `MessageEmbeddingIndexer`

```python
class MessageEmbeddingIndexer:
    def __init__(
        self,
        engine: AsyncEngine,
        scope_id: ScopeId,
        embedder: Embedder,
        *,
        batch_size: int = 64,
    ) -> None: ...

    async def index_session(self, session_id: SessionId) -> int: ...

    async def backfill_scope(self, *, limit: int = 500) -> BackfillResult: ...
```

职责：

- 查询当前 `(model,dim)` 尚未投影的完整 user/assistant 消息。
- 排除：
  - `partial=true`；
  - system 消息；
  - tool call/result；
  - 空文本。
- 每批最多 `batch_size` 条。
- Embedding API 调用发生在事务外；每批写入发生在一个事务中。
- 插入使用 `ON CONFLICT DO NOTHING`，允许 run-end 索引和 search catch-up 并发。
- `index_session()` 索引指定 session 的全部缺口。
- `backfill_scope(limit)` 最多读取 `limit + 1` 条：
  - 只索引前 `limit` 条；
  - 多出一条即表示 `remaining=True`；
  - 后续搜索继续补齐。

---

## 6. 索引生命周期

### 6.1 Run 结束后的增量索引

`AgentRuntime` 在有 engine + embedder 时创建 `MessageEmbeddingIndexer`。

`AgentRuntime._run()` 的 agent run 完成（包括 error stop）后，在 `finally` 中调用
`_schedule_session_index(session_id)`：

- 创建独立 `asyncio.Task`；
- 不阻塞 SSE 和用户响应；
- 不计入 `_runs`，因此不会延长 run 的 active/interrupt 状态；
- 存入 `_index_tasks` 集合，done callback 移除引用；
- task wrapper 捕获异常并 `logger.exception`，不把索引失败伪装成 agent run 失败。

这次扫描会覆盖本轮已经 durable 的 user + assistant 完整消息。

### 6.2 搜索时自动补漏

每次有 embedder 的 semantic search 在执行排名前调用：

```python
await indexer.backfill_scope(limit=session_embedding_catchup_limit)
```

正常情况下，run-end 索引已完成，补漏为 0；以下情况由它恢复：

- 功能上线前的已有会话；
- server 在 run.ended 和索引之间崩溃；
- Ollama 短暂故障；
- 两个并发索引任务中的一个失败。

每次最多补 500 条，避免第一次搜索在大型历史库上无限阻塞。Lexical arm 始终覆盖
全部历史；若仍有未索引消息，semantic arm 是部分覆盖，日志记录 `remaining=True`。

### 6.3 Shutdown

新增 `AgentRuntime.aclose()`：

- cancel 所有未完成 `_index_tasks`；
- `asyncio.gather(..., return_exceptions=True)` 等待清理；
- `app.py` lifespan 在释放 Redis / Postgres engine 前调用它。

未完成的行由下次搜索 catch-up 自动恢复。

---

## 7. 混合排名

新增低层函数：

```python
async def rank_session_messages(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int,
    embedder: Embedder | None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> tuple[list[RankedMessage], RecallStatus]: ...
```

### 7.1 Lexical arm

从 `events` 查询 complete user/assistant `message.token`：

- `GREATEST(similarity(text, query), ts_rank(...))`；
- 使用已有 `ix_events_msg_trgm`；
- 返回 event IDs；
- 候选量：`max(k * 8, 80)`。

### 7.2 Semantic arm

当 embedder 可用：

- 先执行 scope catch-up；
- embed query；
- 只查询当前 `model` + `dim`；
- `ORDER BY embedding <=> query_vector`；
- 返回 event IDs，候选量同 lexical arm。

### 7.3 RRF

使用已有 `rrf_fuse([lexical_ids, semantic_ids])`：

- 按 fused event ID 顺序取 `RankedMessage`；
- 返回前 `k` 条；
- 不加入 reranker 或时间衰减。

### 7.4 Search mode

- 没有配置 embedder：`mode="lexical"`。
- Semantic 成功：`mode="hybrid"`。
- 配置了 embedder，但 embedding/query 失败：`mode="lexical-degraded"`，并保留错误文本供
  API/tool 显式呈现。

Catch-up 写入失败会记录日志；若 query embedding 和已有 semantic rows 仍可用，则仍可
返回 hybrid。只有 semantic ranking 本身不可用时才降级。

---

## 8. Agent 工具与 Sessions API

### 8.1 Backward-compatible wrappers

`keel_core.search` 保留现有 list-returning API：

```python
async def session_search(
    engine, scope_id, query, *, k=5, embedder=None,
    batch_size=64, catchup_limit=500
) -> list[SearchHit]: ...

async def search_sessions(
    engine, scope_id, query, *, k=20, embedder=None,
    batch_size=64, catchup_limit=500
) -> list[SessionSearchHit]: ...
```

同时新增带状态的内部/高级接口：

```python
async def hybrid_session_search(...) -> tuple[list[SearchHit], RecallStatus]: ...
async def hybrid_search_sessions(...) -> tuple[list[SessionSearchHit], RecallStatus]: ...
```

旧测试和没有 embedder 的调用仍 lexical-only。

### 8.2 Message-level 工具结果

`SessionSearchTool` 接收 optional embedder + batch/catch-up 配置，使用
`hybrid_session_search()`。

- `hybrid` / `lexical`：保持现有逐行内容输出。
- `lexical-degraded`：输出前缀：
  `"[semantic unavailable; lexical results only]"`。

本切片不处理已存在 backlog 中的空结果 `"no matches"` 文案。

### 8.3 Session-level API

`hybrid_search_sessions()`：

1. 取得 over-fetched fused messages；
2. 按 fused 顺序保留每个 session 第一次出现的消息；
3. 该消息作为 snippet；
4. join `sessions`，补 title / updated_at / message count；
5. 截取前 `k` 个 session。

`AgentRuntime` 新增只读属性：

```python
@property
def embedder(self) -> Embedder | None: ...

@property
def session_embedding_batch_size(self) -> int: ...

@property
def session_embedding_catchup_limit(self) -> int: ...
```

`GET /v1/sessions/search` 从 runtime 读取这些值，调用 `hybrid_search_sessions()`，
并设置：

- `X-Keel-Search-Mode: hybrid`
- `X-Keel-Search-Mode: lexical`
- `X-Keel-Search-Mode: lexical-degraded`

响应 body 保持现有 list shape，前端无需修改。

如果测试 app 没有 runtime，则 embedder 为 `None`，保持 lexical-only。

---

## 9. 配置

`Settings` 新增：

```python
session_embedding_batch_size: int = 64
session_embedding_catchup_limit: int = 500
```

`app.py` 把它们传入 `AgentRuntime`。运行时继续复用已存在：

- `embedding_model`
- `embedding_dim`
- `embedding_send_dimensions`

不新增第二套 embedder 配置。

---

## 10. 错误处理与可观测性

- Post-run indexing task：
  - 捕获异常；
  - `logger.exception` 包含 scope/session/model；
  - 不改变 agent run 的结果。
- Search-time catch-up：
  - 失败时记录错误；
  - 仍尝试使用已有 semantic rows；
  - semantic ranking 不可用时显式 lexical-degraded。
- API 用 header 暴露 mode，保持 body 兼容。
- Tool 用文本前缀暴露 degraded mode。
- 日志记录：
  - indexed count；
  - session / scope；
  - model / dim；
  - catch-up 是否达到 limit 且仍有 remaining。

不吞掉异常、不静默伪装成完整 hybrid success。

---

## 11. 安全与数据治理

- 所有 SQL 同时：
  - 设置 `app.scope_id`；
  - 显式过滤 `scope_id`。
- `message_embeddings` 启用 RLS。
- 只复制完整 user/assistant 文本，不复制 system/tool output。
- `event_id` FK cascade 确保删除源事件时删除投影。
- 多模型行按 `(model,dim)` 隔离，拒绝跨模型 KNN。
- 本切片只由 web runtime 主动索引；不扩大 IM/digest 的数据面。
- Projection 是可重建数据，不成为新的 source of truth。

---

## 12. 测试计划

### 12.1 Unit

- `AgentRuntime` run 完成后调度 index task。
- Index task 不进入 `_runs`，不影响 interrupt/active-run 状态。
- Index task failure 被记录，不让 run 失败。
- `SessionSearchTool` 在 degraded 时添加明确前缀。
- API 使用 runtime embedder 并设置正确 mode header。
- 没有 runtime/embedder 的旧路径保持 lexical。
- `AgentRuntime.aclose()` 清理 indexing tasks。

### 12.2 Postgres integration（`keel_test`）

- Migration/table/index/RLS 存在。
- Scope A 无法读取 scope B 的投影。
- `index_session()`：
  - 只索引 complete user/assistant；
  - 排除 system/partial/tool/empty；
  - 正确保存 model/dim/content。
- 重跑索引幂等。
- 两个并发 `index_session()` 最终每个 `(event_id,model,dim)` 只有一行。
- Model/dim pinning：新模型不读取旧模型向量。
- `backfill_scope(limit)`：
  - 最多索引 limit；
  - 正确报告 `remaining`；
  - 后续调用继续补齐。
- 自定义 scripted embedder 制造“文本无共同词但向量相近”的 semantic-only 命中。
- Message-level 与 session-level 搜索使用同一 fused 顺序。
- 删除 event 后 projection 自动 cascade。

### 12.3 Regression

- 现有 `embedder=None` lexical tests 全部通过。
- Sessions API/frontend 测试保持 body shape 不变。
- Full backend suite、ruff、mypy。

---

## 13. Live verification

1. 删除/清空 `web:local` 的当前模型投影。
2. 调用 Sessions search，确认自动 catch-up，header 为 `hybrid`。
3. 新建对话写入一个中英文事实，run 结束后确认 user+assistant 两条 complete message 被索引。
4. 用几乎没有 lexical overlap 的中文改写搜索，命中正确 session/snippet。
5. 让 agent 调 `session_search`，确认命中同一相关消息。
6. 停止 Ollama：
   - API header 为 `lexical-degraded`；
   - tool 输出含降级前缀；
   - lexical 结果仍正常；
   - server 不崩溃。
7. 重启 Ollama，再次搜索：
   - 缺失 embedding 自动补齐；
   - header 恢复 `hybrid`。

---

## 14. 完成标准

- 两个入口（agent tool + Sessions API）均使用统一 hybrid ranking。
- 正常聊天路径不等待 embedding。
- 历史数据和崩溃缺口能搜索时自动恢复。
- Semantic 故障显式降级，不影响 lexical search。
- 跨 scope、角色过滤、模型固定、删除 cascade 均有集成测试。
- Full test suite、ruff、mypy 通过。
