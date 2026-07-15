# 记忆 + 语义检索基础切片 — 设计文档（Design Spec）

> **状态：** 已完成 brainstorming，待用户复核 → 之后进入 writing-plans。
> **目标（一句话）：** 让 web 助手获得可用的「记忆三件套」——常驻可自编辑的**核心记忆（core memory）**、可语义存取的**归档记忆（archival memory）**、以及对历史会话的**词法召回（lexical recall）**。
> **相关：** ADR-0007（嵌入与重排）、ADR-0009（作用域隔离）、ARCHITECTURE §8（四层记忆）、[核心记忆机制详解（中文）](./2026-07-09-core-memory-notes-zh.md)。
> **技术栈：** Python（keel-core / keel-server）、Postgres + pgvector、LiteLLM、Ollama（`bge-m3`）、arq worker、Docker Compose。

---

## 1. 目标与范围

**本切片交付：**
- 核心记忆：每轮把 scope 的记忆块注入系统提示 + 三个自编辑工具（`memory_append` / `memory_replace` / `memory_rethink`）。
- 归档记忆：`archival_insert`（新）+ `archival_search`（已存在）+ 真实嵌入模型（Ollama `bge-m3`）。
- 词法召回：把已存在的 `session_search`（trigram）工具接入助手。
- 仅面向 **web 聊天助手**；随切片附带 Ollama 容器 + 相关配置项。

**不在本切片（后续 M3 独立切片）：**
- 语义 **session 搜索**（需要给每条历史消息做嵌入的新基础设施）。
- 后台**记忆固化 / 整理（consolidation）**。
- **RAG / 知识库**文档摄取（切块）。
- **评测（evals）**、重排器（reranker）、桌面壳、web 端 **Memory 页面**（侧栏那个禁用项）。
- IM（QQ/Telegram）/ digest agent **不**获得自编辑记忆工具（不可信面，防投毒）。

---

## 2. 现状盘点（为什么这是"接线"而非"从零造"）

**已存在、直接复用：**
- `PostgresMemoryStore`（`keel_core/memory.py`）：记忆块 + 版本化历史，`get/set/history`。
- `ArchivalStore`（`keel_core/search.py`）：**完整**的混合检索——`add()` 嵌入并入库；`search()` 融合词法（trigram+FTS）⊕ 语义（pgvector KNN），用 RRF。
- `archival` 表（迁移 `0004_archival.py`，含 pgvector 列 + `model`/`dim` 固定）。
- `Embedder` 协议、`FakeEmbedder`、`LiteLLMEmbedder`、`rrf_fuse`（`keel_core/embeddings.py`）。
- `ArchivalSearchTool`、`SessionSearchTool`（`keel_core/search.py`）。

**缺失 / 未接线：**
- ❌ 任何地方都**没有实例化 `Embedder`** → 语义功能实机不可用。
- ❌ 检索工具已导出，但**未注册进任何 agent 的 toolset**。
- ❌ 核心记忆块**没有被注入**到 loop 的提示（`_build_request` 无记忆步骤）；**没有自编辑工具**。
- ❌ `session_search` 语义臂仍为 lexical-only（本切片保持词法，语义版延后）。

---

## 3. 架构总览

**组件（→=新增，✎=修改，✓=原样复用）：**

| 组件 | 变更 | 说明 |
|---|---|---|
| `PostgresMemoryStore` | ✎ | 新增 `blocks() -> dict[str,str]`（列出 scope 全部块，供注入） |
| `format_core_memory` | → | 块 → `<core_memory>` 系统文本（含空默认块渲染） |
| `MemoryAppendTool/ReplaceTool/RethinkTool` | → | 三个自编辑工具 |
| `loop._build_request` / `run` / `resume` | ✎ | 新增 `system_context` 可选回调，每轮注入 |
| `LiteLLMEmbedder` | ✎ | `dimensions` 改为条件发送 + 向量长度断言 |
| `ArchivalInsertTool` | → | `archival_insert`（唯一缺失的工具） |
| `ArchivalSearchTool` / `SessionSearchTool` | ✓ | 接入 toolset |
| `AgentRuntime` | ✎ | 造 embedder + 记忆/归档工具，注册，提供 `system_context` 闭包 |
| Docker Compose | ✎ | 新增 `ollama` + `ollama-pull` 服务；server/worker 加 `OLLAMA_API_BASE` |
| `Settings` | ✎ | 4 个新配置项（见 §7） |

**每轮数据流：**
```
run() 每次迭代：
  └─ _build_request(agent, store, session, registry, system_context)
       ├─ messages = project_messages(events)                  # 既有
       └─ 若 system_context: messages.insert(0, {"role":"system","content": 现读现拼的核心记忆})
  └─ 模型流式输出 → 可能调用 memory_append/replace/rethink（写 memory_blocks）
                  → 可能调用 archival_insert / archival_search / session_search
  └─ 下一轮 _build_request 重读 blocks() → 上一轮的编辑本轮可见（方案 A）
```

---

## 4. 核心记忆（详见中文详解文档，这里给落地要点）

**数据模型：** 复用 `memory_blocks`（`scope_id, key, value, version`）+ `memory_block_versions` 历史。**不改 schema**。

**默认块：** `persona`（助手自我设定）、`human`（用户持久事实）；**空块也渲染**成 `<persona></persona>` 以提示模型填写。

**注入 seam（loop 保持记忆无关）：**
```python
# keel_core/loop.py
SystemContextFn = Callable[[], Awaitable[str]]

async def _build_request(agent, store, session_id, registry,
                         system_context: SystemContextFn | None = None) -> ProviderRequest:
    events = [e async for e in store.read(session_id)]
    messages = project_messages(events)
    if system_context is not None:
        text = await system_context()
        if text:
            messages.insert(0, {"role": "system", "content": text})   # 已核对：content 而非 text
    return ProviderRequest(model=agent.model, messages=messages, tools=registry.schemas())
```
`run()` 与 `resume()` 均新增 `system_context` 参数并透传到每次 `_build_request`。

**格式化（`keel_core/memory.py`）：**
```python
def format_core_memory(blocks: dict[str, str], *, defaults=("persona", "human")) -> str:
    """块 → <core_memory> 系统文本；defaults 中的块即使缺失也渲染为空标签。"""
```
渲染示例：
```
<core_memory>
可编辑的长期记忆，始终可见。请用 memory_* 工具保持它准确；它会跨你所有会话持久化。
<persona>...</persona>
<human>...</human>
</core_memory>
```

**自编辑三工具**（`writes=True`，但 own-scope 无副作用 → 权限 `allow`，不审批）：

| 工具 | 签名 | 行为 | 出错 |
|---|---|---|---|
| `memory_append` | `(block, content)` | 追加（缺则建块） | 超上限报错，提示改用 `rethink` |
| `memory_replace` | `(block, old, new)` | 换首处 `old`→`new` | **`old` 未找到 → 报错** |
| `memory_rethink` | `(block, content)` | 整块覆盖 | 超上限报错 |
每个返回新版本号。上限 `memory_block_max_chars`（默认 2000）。

**生效时序（方案 A）：** 本轮 `append` 写 DB → 下一轮 `_build_request` 重读 → 本轮可见；换新会话按 **scope** 读取，记忆依旧在。

---

## 5. 归档记忆 + 嵌入模型

**`LiteLLMEmbedder` 修改：**
```python
class LiteLLMEmbedder:
    def __init__(self, model: str, dim: int, *, send_dimensions: bool = False) -> None: ...
    async def embed(self, texts):
        kwargs = {"model": self.model, "input": list(texts)}
        if self.send_dimensions:            # 仅 OpenAI text-embedding-3 需要；Ollama 会拒绝
            kwargs["dimensions"] = self.dim
        resp = await aembedding(**kwargs)
        vecs = [list(i["embedding"]) for i in resp.data]
        assert all(len(v) == self.dim for v in vecs)   # 长度断言：尽早发现 model/dim 配错
        return vecs
```

**归档工具：**

| 工具 | 状态 | 后端 | 权限 |
|---|---|---|---|
| `archival_insert(content)` | **新** `ArchivalInsertTool(engine, embedder)` | `ArchivalStore.add()` | `allow`（写，own-scope，安全） |
| `archival_search(query, k)` | 已存在 | `ArchivalStore.search()`（RRF 混合） | `allow`（读） |
| `session_search(query, k)` | 已存在（词法） | `session_search()` | `allow`（读） |

**`(model, dim)` 固定：** 每行归档存 `model`+`dim`，检索按其过滤（已实现）。本切片固定 `bge-m3`/`1024`；换模型是**重嵌入迁移**（ADR-0007），不在范围。

**优雅降级：** Ollama 宕机时 `archival_*` 返回错误结果（聊天不受影响）；**核心记忆不依赖嵌入**（纯 DB），照常工作。memory/lite 无引擎档位：记忆/归档工具直接不注册（与现有 DB 依赖功能同规则）。

---

## 6. Ollama 部署（Docker，自包含）

```yaml
# docker-compose.yml（profiles: dev, full）
ollama:
  image: ollama/ollama:latest
  volumes: ["ollama:/root/.ollama"]     # 模型缓存持久化
  healthcheck:
    test: ["CMD-SHELL", "ollama list >/dev/null 2>&1 || exit 1"]
    interval: 10s; timeout: 5s; retries: 20; start_period: 10s
ollama-pull:                            # 一次性拉模型，仿 keel-migrate
  image: ollama/ollama:latest
  environment: { OLLAMA_HOST: "http://ollama:11434" }
  entrypoint: ["/bin/sh","-c","ollama pull bge-m3"]
  depends_on: { ollama: { condition: service_healthy } }
  restart: "no"
```
`keel-server` / `keel-worker`：加 `OLLAMA_API_BASE=http://ollama:11434`（litellm 原生读取），并 `depends_on: ollama-pull (service_completed_successfully)`，确保首个 embed 前模型已就绪。卷 `ollama:` 加入顶层 `volumes`。

---

## 7. 配置项（`Settings`，`KEEL_` 前缀）

| 设置 | 默认 | 用途 |
|---|---|---|
| `embedding_model` | `"ollama/bge-m3"` | LiteLLM 模型 id；**空 ⇒ 关闭归档**（核心记忆仍开） |
| `embedding_dim` | `1024` | `(model,dim)` 固定 + 向量长度断言 |
| `embedding_send_dimensions` | `False` | 是否发送 OpenAI `dimensions`（Ollama 为 False） |
| `memory_block_max_chars` | `2000` | 核心记忆块字数上限 |

`OLLAMA_API_BASE` 由 compose 注入（litellm 原生），非 keel 设置。

---

## 8. Runtime 接线（`AgentRuntime`，集成枢纽）

```
engine 存在？
 ├─ 若 embedding_model 非空：embedder = LiteLLMEmbedder(model, dim, send_dimensions=…)
 ├─ memory_tools   = [memory_append, memory_replace, memory_rethink]        (engine + cap)
 ├─ recall_tools   = [session_search]                                       (engine)
 ├─ archival_tools = [archival_insert, archival_search]                     (engine + embedder，仅当 embedder 存在)
 ├─ registry  = ToolRegistry(_build_tools(workspace) + 上述工具)
 ├─ toolset  += 上述工具名
 └─ system_context = self._core_memory_context   # 读 blocks() → format_core_memory
```
- 新增 `_build_memory_tools(engine, embedder, *, cap) -> list[ToolProto]`；`_build_tools(workspace)` 仍只管文件/shell 工具；仅在 `engine` 存在时拼接记忆工具。
- 工具在调用时从 `ToolContext.scope_id` 取 scope（与 `ArchivalSearchTool` 现有做法一致），scope 安全。
- `run(..., system_context=self._core_memory_context)`。

**权限（`_web_permissions()`）：** 为 `memory_append/replace/rethink`、`archival_insert/search`、`session_search` 加 `allow` 规则。它们 own-scope 无外部副作用，**不审批**；`write/edit/shell` 规则不变（仍受控）。

---

## 9. 安全边界
- 仅 web 助手拿自编辑工具；IM/digest（不可信/特化）不给，防记忆投毒。
- 字数上限防上下文膨胀；版本化历史可审计/回滚。
- 归档 `(model,dim)` 固定，拒绝跨模型 KNN。

---

## 10. 测试策略

**单元（`FakeEmbedder` + 脚本化 provider）：**
- `format_core_memory`：格式 + 空默认块渲染。
- 三工具：`append`（建块/追加/超限报错）、`replace`（`old` 缺失报错）、`rethink`（整块覆盖）、版本自增。
- 注入 + 时序：脚本化 provider 捕获请求 → `messages[0]` 是 `<core_memory>`；本轮 `append` → 下一轮请求可见。
- `LiteLLMEmbedder`：`send_dimensions=False` 不发 `dimensions`；长度不符触发断言。
- `ArchivalInsertTool` 用 `FakeEmbedder` 插入。

**集成（`keel_test` 库，真 pgvector + `FakeEmbedder`）：**
- `PostgresMemoryStore.blocks()` 返回全部块；`append/replace/rethink` 落库 + 版本。
- `ArchivalStore` add→search 往返（真 KNN ⊕ trigram ⊕ RRF）。

**实机验证：**
1. Ollama 起 + 拉 `bge-m3` → `/api/tags` 可见；一次 embed 返回 **1024 维**向量。
2. 归档：`archival_insert` 一条事实 → `archival_search` 语义命中。
3. **核心记忆端到端**：对话"记住我叫大中" → agent 调 `memory_append` → **开新会话** → 问"我叫什么" → 从注入的核心记忆答出。
4. 降级：停 Ollama → 聊天照常、`archival_*` 优雅报错、核心记忆不受影响。

---

## 11. 关键接口签名汇总（供 writing-plans 交接）

```python
# keel_core/memory.py
class PostgresMemoryStore:
    async def blocks(self) -> dict[str, str]: ...
def format_core_memory(blocks: dict[str, str], *, defaults=("persona", "human")) -> str: ...
class MemoryAppendTool:   # name="memory_append";   writes=True
class MemoryReplaceTool:  # name="memory_replace";  writes=True
class MemoryRethinkTool:  # name="memory_rethink";  writes=True
#   __init__(engine: AsyncEngine, *, max_chars: int); run(args, ctx) 用 ctx.scope_id

# keel_core/loop.py
SystemContextFn = Callable[[], Awaitable[str]]
async def run(..., system_context: SystemContextFn | None = None): ...
async def resume(..., system_context: SystemContextFn | None = None): ...

# keel_core/embeddings.py
class LiteLLMEmbedder:
    def __init__(self, model: str, dim: int, *, send_dimensions: bool = False) -> None: ...

# keel_core/search.py
class ArchivalInsertTool:  # name="archival_insert"; writes=True
    def __init__(self, engine: AsyncEngine, embedder: Embedder) -> None: ...

# keel_server/runtime.py
def _build_memory_tools(engine, embedder, *, cap: int) -> list[ToolProto]: ...
# AgentRuntime: 造 embedder/工具/注册 + system_context 闭包 + run(system_context=…)

# keel_core/config.py — Settings
embedding_model: str = "ollama/bge-m3"
embedding_dim: int = 1024
embedding_send_dimensions: bool = False
memory_block_max_chars: int = 2000
```
