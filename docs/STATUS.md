# Keel 当前状态与里程碑

> **快照日期：** 2026-07-16
> **代码基线：** `feat/rag-knowledge-base@cd153f6`（RAG/KB verified baseline）
> **路线来源：** [`PRD.md`](./PRD.md) §11、[`ARCHITECTURE.md`](./ARCHITECTURE.md) §19、
> [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md)

本文档是 Keel 的 living status。PRD/Architecture/Implementation Plan 定义目标与边界；
本文档记录已经落地的能力、尚未满足的 exit criteria，以及当前执行顺序。每个 epic 合并
到 `main` 后都应更新这里。

## 1. 一句话结论

Keel 当前处于 **M3（Knowledge & quality）后段**：

- M0 已完成。
- M1 的主要产品能力已经可用，但仍有少量正式 KPI/exit-evidence 债务。
- M2 已完成调度、durable background jobs、审批、RBAC、基础 provider failover 与
  Telegram 等切片；N-worker scale-out、per-task routing 和 WeCom 尚未完成。
- M3 的 **Memory、Knowledge 与 Quality 主线**已经完成；Plugin SDK/hooks、
  event upcasters、retention/erasure 与 Tauri Desktop 尚未完成。

## 2. 当前可工作的产品能力

### Agent runtime 与 surfaces

- 持久化 agent loop、streaming、tool calling、interrupt、approval/resume。
- LiteLLM provider gateway，同时支持 chat-completions 与 Responses API 路径。
- CLI、React Web/SSE、QQ/OneBot 与 Telegram gateway。
- 文件、shell、web 等工具经过 permission/sandbox 边界。

### Scope、connectors 与 autonomy

- `scope_id` + Postgres RLS 的数据隔离。
- Gmail OAuth、真实 inbox read/send、token revoke/purge。
- Durable approvals、RBAC/API keys、admin overview。
- Cron/interval/one-shot schedules、daily digest、daily memory consolidation。
- Durable background jobs：
  - Postgres/RLS lifecycle source of truth；
  - at-least-once arq delivery + DB lease/reclaim；
  - durable progress、cooperative cancellation、bounded retry；
  - crash/duplicate-delivery recovery；
  - terminal assistant result injection exactly once；
  - list/detail/cancel API + RBAC。

### Memory、search 与 quality

- Core memory blocks：每轮加载、版本/history、自编辑工具。
- Archival memory：pgvector + lexical/semantic hybrid retrieval。
- Session recall：lexical + semantic + RRF，显式
  `hybrid / lexical / lexical-degraded`。
- Memory consolidation：
  - constrained agent；
  - Archival 高置信自动写；
  - Core 只生成待人工批准 proposal；
  - lease/cursor、证据约束、stale CAS、retry dedupe。
- Memory Evals：
  - 12-case JSONL dataset；
  - Consolidation / Recall / Safety 三个 suites；
  - provider + embedding cassettes；
  - live/record 与 fail-closed replay；
  - JSON/JUnit/terminal reports、optional judge、optional Langfuse。
- RAG / Knowledge Base：
  - scope-bound KB/document/version/chunk lifecycle；
  - text/Markdown create、update、reindex、immediate-hide delete + durable purge；
  - deterministic chunking + pinned embedding model/dim；
  - lexical/semantic hybrid retrieval、stable structured citations；
  - always-tainted `kb_search`，normal/resume event paths preserve citations + taint；
  - idempotent REST API、RBAC、React Knowledge management/search page；
  - durable `knowledge.ingest` / `knowledge.delete` jobs with ownership fencing、
    active/desired rollback safety and zombie-write prevention。

## 3. 当前验证基线

- Python test suite：**1036 passed / 1 skipped**（1037 collected）。
- React/Vitest：**49 passed**。
- Ruff lint + format：通过。
- mypy strict：通过。
- Memory golden replay：
  - **12/12 cases PASS**；
  - **7/7 gates PASS**；
  - weighted overall：**0.982**；
  - replay 不回退到 live provider。
- Integration/eval destructive fixtures 只允许显式 `keel_test`/`keel_eval`。
- Durable Jobs acceptance：真实 Postgres + Redis/arq，覆盖丢投递、重复投递、retry、
  worker crash/reclaim、attempt exhaustion、cancel 与 exactly-once injection。
- Knowledge replay：
  - **8/8 cases PASS**；
  - **7/7 gates PASS**；
  - Recall@5 / MRR / citation precision / taint / degraded / deletion leakage 全部 **1.000**；
  - weighted overall：**1.000**；
  - replay 不回退 live embedding。
- Knowledge isolated live smoke：真实 Postgres + Redis/arq，覆盖 create → ingest →
  cited/tainted search → update while old active remains → activate new version →
  immediate-hide delete → durable purge。

## 4. Milestone 状态

| Milestone | 状态 | 已完成 | 主要剩余 |
|---|---|---|---|
| **M0 Foundations** | 完成 | monorepo、compose、CI、contracts、migrations、S1-S5 | 无阻塞项 |
| **M1 Core that talks** | 功能性完成 | loop、providers、tools、memory、search、CLI/Web/IM、connectors、skills/MCP、observability | 通用 task-suite/KPI 证据、SDK 使用体验收尾 |
| **M2 Autonomy & scale** | 部分完成 | schedules、durable background jobs、digest、durable approvals、RBAC、admin overview、Telegram、基础 failover | N-worker demo、per-task routing、WeCom |
| **M3 Knowledge & quality** | 进行中 | retrieval foundation、memory consolidation、memory evals、RAG/KB | event upcasters、retention/erasure、Plugin SDK/hooks、Tauri Desktop |
| **M4 Hardening** | 未正式开始 | 已有部分 security/DB safety 基础 | security review、perf、backup/DR、multi-tenant groundwork、docs/examples |

## 5. M3 已完成与未完成边界

### 已完成：Memory/Knowledge/Quality

1. Core + Archival + Recall 的生产链。
2. Semantic session search 与 embedding degradation。
3. Proposal-first Memory Consolidation。
4. 可在 CI 重放的 deterministic Memory Evals。
5. 完整 RAG/KB vertical slice：
   - KB/document/version/chunk lifecycle；
   - durable ingest/delete；
   - hybrid retrieval + citations + taint；
   - REST/RBAC + React management/search；
   - deterministic Knowledge Evals。

### 尚未完成：Data lifecycle/Extensibility/Desktop

1. **Event/data lifecycle**
   - event upcaster registry；
   - retention policy；
   - scope/session erasure；
   - memory/vector/KB purge 与审计。
2. **Plugin SDK + hooks**
   - manifest；
   - lifecycle hooks；
   - validation、hot-load、rollback；
   - SDK examples。
3. **Tauri Desktop**
   - 复用稳定 Web/API；
   - 不在第一版引入 LocalDaemon/local-file execution。

## 6. 执行顺序

### 1. Durable background jobs（已完成）

RAG ingestion 所依赖的通用执行底座已经落地：

- durable job row 与 scope/RLS；
- enqueue + lease/claim；
- queued/running/succeeded/failed/cancelled 状态机；
- progress；
- cooperative cancellation；
- bounded retry；
- result injection 回目标 session，且只能注入一次；
- list/detail/cancel API；
- crash/retry/idempotency acceptance tests。

第一版没有引入 N-worker benchmark、完整 Jobs UI 或假的 production job kind；第一个真实
consumer 将是 RAG document ingest/reindex。

### 2. RAG/KB vertical slice（已完成）

text/Markdown 首版已经完成：

`create KB -> ingest -> chunk -> embed -> hybrid retrieve -> cited tool result -> update/delete/reindex`

后续 connector ingestion 将复用当前 version/job/citation/taint 边界，优先接
Google Drive/OneDrive/Notion 等来源。

### 3. Event upcasters + retention/erasure（下一步）

在知识数据继续增长前，完成 schema evolution 与完整数据删除边界。

### 4. Plugin SDK + lifecycle hooks

复用现有 Skills/MCP/ImportGuard 基础，增加 manifest validation、hot-load 与 rollback。

### 5. Tauri Desktop shell

待 API、data lifecycle 与 plugin boundary 稳定后封装现有 Web shell。

### 6. M4 hardening

Security review、performance、backup/restore drill、multi-tenant groundwork 与发布文档。

## 7. 当前下一步

立即进入 **Event Upcasters + Retention/Erasure**，先稳定事件演进与完整删除边界，
再扩大 connector ingestion 与知识数据规模。
