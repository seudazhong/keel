# 核心记忆（Core Memory）机制详解 — 供审阅

> 本文档只聚焦「记忆基础切片」里**最关键的一环：核心记忆（core memory）的注入与自编辑机制**（对应设计的 Section 2），用中文把原理讲透，方便你 double check。归档记忆（archival）、嵌入模型（Ollama bge-m3）、接线细节会在正式 spec 里覆盖，这里不展开。
>
> 状态：设计草稿（brainstorming 阶段），尚未落地代码。全部 5 个小节都确认后，会写一份完整的正式 spec 再进入实现。

---

## 1. 要解决的问题

现在的 agent **没有跨轮次 / 跨会话的长期记忆**：每次对话只能看到当前 session 的事件日志（`project_messages` 把事件折叠成 messages）。关掉这个会话、或换一个新会话，它就"失忆"了——不记得你叫什么、你的偏好、它自己该用什么口吻。

**核心记忆**就是解决这个问题的第一层：一块**始终常驻在上下文里、且 agent 自己能编辑**的可写记忆。它是 MemGPT / Letta 的经典设计，分两类角色：

- **常驻**：每一轮请求都把它拼进系统提示，所以模型"永远看得见"。
- **自编辑**：模型通过工具（`memory_append` 等）主动更新它——比如你说"我叫大中"，它就把这条写进 `human` 块。
- **跨会话**：记忆按 **scope（用户/作用域）** 存储，不是按 session，所以换个会话它依然记得。

---

## 2. 数据模型（已存在，复用）

核心记忆存在已有的 `memory_blocks` 表里（`packages/keel-core/src/keel_core/memory.py` 的 `PostgresMemoryStore`）：

| 列 | 含义 |
|---|---|
| `scope_id` | 作用域（谁的记忆），受 RLS 行级隔离 |
| `key` | 块名，如 `persona` / `human` |
| `value` | 块内容（纯文本） |
| `version` | 版本号，每次 `set` 自增 |

配套还有 `memory_block_versions` 历史表：**每次写入都归档一份旧值**，天然支持审计与回滚。

`PostgresMemoryStore` 现有方法：`get(key)`、`set(key, value)→version`、`history(key)`。

**本切片新增一个方法**：
```python
async def blocks(self) -> dict[str, str]:
    """返回本 scope 的所有记忆块 {key: value}，用于每轮注入。"""
```

> 说明：存储层早就写好了，本切片**不新建表、不改 schema**，只加一个"列出全部块"的读方法。

---

## 3. 默认块：`persona` 与 `human`

参考 MemGPT，默认有两个块，首次写入时惰性创建：

- **`persona`** — agent 的自我设定 / 行为准则。例："我叫 Keel，回答简洁、默认中文、遇到外发动作先确认。"
- **`human`** — 关于用户的持久事实。例："用户叫大中，在做 Keel 项目，偏好中文、喜欢严谨的技术解释。"

**即使块为空也会显示**（渲染成 `<persona></persona>`），这样模型知道"有这么个块可以填"，会主动往里写。用户也可以让 agent 创建额外的自定义块（动态命名）。

---

## 4. 注入机制（方案 A：每轮重读）

### 4.1 在哪里注入

Agent 主循环 `run()` 每迭代一轮，都会调用 `_build_request()` 重新构造发给模型的请求（因为要带上新产生的工具结果）。我们就在这里注入核心记忆——**每轮都读一次最新的块**，所以永远是最新值。

### 4.2 保持 `loop.py` 与记忆解耦（seam 设计）

`loop.py` 是通用引擎，不该直接依赖"记忆"这个概念。所以我们不在 loop 里 import `PostgresMemoryStore`，而是给它开一个**回调口子**：

```python
# run(...) 和 _build_request(...) 各多一个可选参数：
system_context: Callable[[], Awaitable[str]] | None = None
```

`_build_request` 改动（核心就这几行）：
```python
async def _build_request(agent, store, session_id, registry, system_context=None):
    events = [e async for e in store.read(session_id)]
    messages = project_messages(events)
    if system_context is not None:
        text = await system_context()          # 每轮现读现拼
        if text:
            messages.insert(0, {"role": "system", "content": text})  # 放最前面
    return ProviderRequest(model=agent.model, messages=messages, tools=registry.schemas())
```

- 注意 message 格式是 `{"role", "content"}`（OpenAI/LiteLLM 格式，已和 `project_messages` 核对一致）。
- Web 助手本来没有其它系统消息，所以核心记忆就是**第 0 条消息**。

**谁来提供这个回调？** 由 `AgentRuntime`（web 助手的宿主）提供一个闭包：
```python
async def _core_memory_context() -> str:
    store = PostgresMemoryStore(engine, scope)
    return _format_core_memory(await store.blocks())
```
于是 loop 不知道记忆是什么，只知道"每轮调一下这个函数拿一段系统文本"。

### 4.3 注入格式

每轮拼进去的系统消息长这样：
```
<core_memory>
可编辑的长期记忆，始终可见。请用 memory_* 工具保持它准确；它会跨你所有会话持久化。
<persona>我叫 Keel，回答简洁，默认中文……</persona>
<human>用户叫大中，在做 Keel 项目……</human>
</core_memory>
```
空块渲染成 `<human></human>`，提示模型去补充。

---

## 5. 自编辑三件套（工具）

模型通过三个工具改写核心记忆。它们都是"写"工具（`writes=True`），但**只动自己 scope 的记忆、没有任何外部副作用**，所以权限判定为 `allow`——**不触发审批**（对比一下：发邮件那种外发工具才需要审批）。

| 工具 | 签名 | 行为 | 出错情况 |
|---|---|---|---|
| `memory_append` | `(block, content)` | 往 `block` 追加一行 `content`；块不存在就创建 | 超出字数上限则报错，提示改用 `rethink` |
| `memory_replace` | `(block, old, new)` | 把块里第一处 `old` 换成 `new` | **`old` 找不到就报错**（MemGPT 语义，防止"静默没改成") |
| `memory_rethink` | `(block, content)` | 整块覆盖重写（用于压缩 / 纠正） | 超出上限报错 |

- 每个工具返回**新的版本号**。
- **字数上限** `memory_block_max_chars`（默认 **2000**）：核心记忆是要塞进每一轮上下文的，必须有界，否则 token 会膨胀。`append` 超限时不写入、直接返回错误，引导模型用 `rethink` 先做摘要。

---

## 6. 编辑生效时序（关键点，请重点 double check）

方案 A 的核心行为：**这一轮写入的记忆，下一轮就能读到**。走一遍：

1. 第 N 轮：模型看到 `<human></human>` 是空的，你说"我叫大中"。
2. 模型调用 `memory_append("human", "用户叫大中")` → 写进 DB，`human` 版本变 1。
3. 该工具结果回填，进入第 N+1 轮：`_build_request` **重新读** `blocks()`，`human` 已是"用户叫大中"，拼进系统消息。
4. 模型这一轮就"看见"自己刚记下的东西，可以据此回答。

**换个新会话**时：新 session 的第 0 轮 `_build_request` 同样读 `blocks()`（按 scope，不是 session），于是"用户叫大中"依旧在——这就是跨会话记忆。

> 这正是 MemGPT/Letta 的行为，也是我强烈推荐方案 A 的原因：DB 是唯一真相源，永远最新，loop 保持无状态。

---

## 7. 跨会话一致性 & 为什么不用别的方案

- **核心记忆是按 scope 存的**（一个用户的所有会话共享），而事件日志是**按 session** 存的。所以记忆不能塞进事件日志（那样跨会话就丢了）——这也是**方案 C（记忆当事件）被否掉**的根因。
- **方案 B（每次 run 缓存一次）**：省下的那一次小查询微不足道，却引入了"会话间脏读"——A 会话改了记忆，B 会话正在跑的 run 直到下次才看得到。对"长期记忆"来说这是不该有的 bug。
- **方案 A**：每轮一条按 `scope_id` 命中索引的小查询，代价可忽略，换来永远一致、永远最新、实现最简单。

---

## 8. 安全边界

- **只有 web 助手拿到自编辑工具。** IM（QQ/Telegram）是**不可信面**——外部消息若能直接写记忆，就可能"投毒"记忆（prompt injection 的延伸）。所以 IM/digest agent **不给** `memory_*` 工具。
- **字数上限**防止上下文无限膨胀。
- **版本化历史**让每次改动可审计、可回滚（`memory_block_versions` 已具备）。

---

## 9. 测试计划

- **单元测试**
  - `_format_core_memory`：块 → 系统文本的格式（含空块渲染）。
  - 三个工具：`append` 追加+建块+超限报错；`replace` 找不到 `old` 报错；`rethink` 整块覆盖；版本自增。
  - 注入时序：用一个"脚本化 provider"捕获请求，验证 messages[0] 是 `<core_memory>` 系统消息；且"本轮 append → 下一轮请求里能看到新值"。
- **集成测试**（隔离库 `keel_test`）
  - `PostgresMemoryStore.blocks()` 返回全部块。
- **实机验证**
  - 对话里说"记住我叫 X"→ 观察 agent 调 `memory_append` → **开一个新会话**，问"我叫什么"→ 它从注入的核心记忆里答出 X。

---

## 10. 明确不在本切片（后续 M3 独立切片）

- 语义 **session 搜索**（需要给每条历史消息做嵌入的新基础设施）。
- 后台**记忆固化 / 整理（consolidation）**——由一个受约束的 agent 定期抽取/合并/裁剪。
- **RAG / 知识库**文档摄取（切块）。
- **评测（evals）**、重排器（reranker）、桌面壳。

---

## 11. 请你重点 double check 的点

1. 默认块就 `persona` + `human` 两个够不够？要不要再加一个 `context`（当前任务/项目上下文）？
2. 字数上限默认 **2000 字符/块**是否合适（越大越占 token，越小越常触发 rethink）。
3. 三件套 `append / replace / rethink` 是否够用，还是只要 `append`+`replace`（去掉 rethink 更精简）？
4. 是否认同"IM/digest 不给自编辑工具"这一安全边界。
