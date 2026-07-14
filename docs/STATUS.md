# Keel 当前状态与里程碑

> **快照日期：** 2026-07-14  
> **代码基线：** `main@e1d9fd1`  
> **路线来源：** [`PRD.md`](./PRD.md) §11、[`ARCHITECTURE.md`](./ARCHITECTURE.md) §19、
> [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md)

本文档是 Keel 的 living status。PRD/Architecture/Implementation Plan 定义目标与边界；
本文档记录已经落地的能力、尚未满足的 exit criteria，以及当前执行顺序。每个 epic 合并
到 `main` 后都应更新这里。

## 1. 一句话结论

Keel 当前处于 **M3（Knowledge & quality）中段**：

- M0 已完成。
- M1 的主要产品能力已经可用，但仍有少量正式 KPI/exit-evidence 债务。
- M2 已完成调度、审批、RBAC、基础 provider failover 与 Telegram 等切片，但通用
  background jobs、N-worker scale-out、per-task routing 和 WeCom 尚未完成。
- M3 的 **Memory/Quality 主线**已经完成；完整 RAG/KB、Plugin SDK/hooks、
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

## 3. 当前验证基线

- Python test suite：**542 passed / 1 skipped**。
- Ruff lint + format：通过。
- mypy strict：通过。
- Memory golden replay：
  - **12/12 cases PASS**；
  - **7/7 gates PASS**；
  - weighted overall：**0.982**；
  - replay 不回退到 live provider。
- Integration/eval destructive fixtures 只允许显式 `keel_test`/`keel_eval`。

## 4. Milestone 状态

| Milestone | 状态 | 已完成 | 主要剩余 |
|---|---|---|---|
| **M0 Foundations** | 完成 | monorepo、compose、CI、contracts、migrations、S1-S5 | 无阻塞项 |
| **M1 Core that talks** | 功能性完成 | loop、providers、tools、memory、search、CLI/Web/IM、connectors、skills/MCP、observability | 通用 task-suite/KPI 证据、SDK 使用体验收尾 |
| **M2 Autonomy & scale** | 部分完成 | schedules、digest、durable approvals、RBAC、admin overview、Telegram、基础 failover | durable background jobs、progress/cancel/result injection、N-worker demo、per-task routing、WeCom |
| **M3 Knowledge & quality** | 进行中 | retrieval foundation、memory consolidation、memory evals | RAG/KB、event upcasters、retention/erasure、Plugin SDK/hooks、Tauri Desktop |
| **M4 Hardening** | 未正式开始 | 已有部分 security/DB safety 基础 | security review、perf、backup/DR、multi-tenant groundwork、docs/examples |

## 5. M3 已完成与未完成边界

### 已完成：Memory/Quality

1. Core + Archival + Recall 的生产链。
2. Semantic session search 与 embedding degradation。
3. Proposal-first Memory Consolidation。
4. 可在 CI 重放的 deterministic Memory Evals。

### 尚未完成：Knowledge/Extensibility/Desktop

1. **RAG/Knowledge Base**
   - knowledge base / document / chunk lifecycle；
   - ingest、reindex、delete；
   - chunk/embed；
   - hybrid retrieval + citations；
   - retrieval-as-tool；
   - KB eval dataset。
2. **Event/data lifecycle**
   - event upcaster registry；
   - retention policy；
   - scope/session erasure；
   - memory/vector/KB purge 与审计。
3. **Plugin SDK + hooks**
   - manifest；
   - lifecycle hooks；
   - validation、hot-load、rollback；
   - SDK examples。
4. **Tauri Desktop**
   - 复用稳定 Web/API；
   - 不在第一版引入 LocalDaemon/local-file execution。

## 6. 执行顺序

### 1. Durable background jobs（M2 prerequisite）

先补 RAG ingestion 所依赖的通用执行底座：

- durable job row 与 scope/RLS；
- enqueue + lease/claim；
- queued/running/succeeded/failed/cancelled 状态机；
- progress；
- cooperative cancellation；
- bounded retry；
- result injection 回目标 session，且只能注入一次；
- list/detail/cancel API；
- crash/retry/idempotency acceptance tests。

第一版不同时追求 N-worker benchmark、完整 Jobs UI 或所有 job kinds；第一个真实 consumer
将是 RAG document ingest/reindex。

### 2. RAG/KB vertical slice

先支持 text/Markdown 文档，完成：

`create KB -> ingest -> chunk -> embed -> hybrid retrieve -> cited tool result -> update/delete/reindex`

然后再接 Google Drive/OneDrive/Notion 等 connector 来源。

### 3. Event upcasters + retention/erasure

在知识数据继续增长前，完成 schema evolution 与完整数据删除边界。

### 4. Plugin SDK + lifecycle hooks

复用现有 Skills/MCP/ImportGuard 基础，增加 manifest validation、hot-load 与 rollback。

### 5. Tauri Desktop shell

待 API、data lifecycle 与 plugin boundary 稳定后封装现有 Web shell。

### 6. M4 hardening

Security review、performance、backup/restore drill、multi-tenant groundwork 与发布文档。

## 7. 当前下一步

立即进入 **Durable Background Jobs 设计**。设计通过后按 TDD 实现，再开始 RAG/KB。

