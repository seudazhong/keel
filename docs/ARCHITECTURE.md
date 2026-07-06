# Keel — Architecture & Design

> **Status:** Draft v1.0 · **Companion to:** [`PRD.md`](./PRD.md) · **Decisions:** [`adr/`](./adr)
> This document fixes the **technology selection** and the **system architecture** for Keel. It is written to be directly buildable and is grounded in the project's field manual (*How to Develop an AI Agent*); manual principles are cited as **[P#]** and patterns as **[pattern]**.

---

## 1. Design goals & guiding principles

Keel is a **general-purpose agent runtime** with a *narrow-waist* core and many surfaces. The architecture is driven by five commitments:

1. **One core, many surfaces [P2].** `keel-core` is a pure library with no UI/transport dependencies. CLI, web, and IM are clients of one protocol.
2. **One tool interface [P3].** Built-ins, MCP tools, skills, and sub-agents all present to the loop as "a callable with a JSON schema."
3. **Data plane vs. control plane [P4].** The bytes sent to the model are stable and cache-friendly; correctness lives in our runtime code.
4. **Durable & observable by construction [P1, P7].** Event-sourced sessions, durable prompt admission, and trace/observation/score on every run.
5. **Fail closed on trust, degrade on ops [P5].** Permission/security errors deny; provider/tool errors recover.

Non-negotiable invariants (acceptance-tested): bounded loops with named termination; persist-before-first-model-call; stop-reason-gated tool execution; byte-stable prompt prefix; two-level sandbox; shared budget across the delegation tree; import ≠ trust.

---

## 2. Technology selection

Each decision lists the choice, why, and the main alternative rejected. Deeper rationale lives in the ADRs.

### 2.1 Summary stack

| Layer | Choice | Key reason |
|---|---|---|
| Core & backend language | **Python 3.12+ (asyncio)** | richest AI ecosystem (LLM/MCP/embeddings), strong async, fast iteration; Letta/AstrBot/Dify precedent |
| API framework | **FastAPI (ASGI)** | async, WebSocket + SSE, Pydantic v2, OpenAPI codegen |
| Provider plumbing | **LiteLLM** wrapped by our `ProviderGateway` | 100+ providers, streaming, cost, fallback out of the box — *borrow the plumbing, own the policy* |
| Agent loop | **Custom async core** (two-loop) | requirements exceed vanilla frameworks (gateway, budgets, steering, cron) |
| Datastore | **PostgreSQL 16 + pgvector** | one store for relational + vector + FTS; hybrid search; Letta precedent |
| ORM / migrations | **SQLAlchemy 2.0 async + Alembic** | mature async ORM, versioned migrations |
| Cache/queue/bus/lock | **Redis 7** | job queue, pub/sub event fan-out, cache, distributed locks, rate limits |
| Background jobs | **arq** (async Redis queue) | async-native, lightweight; matches asyncio core |
| Scheduler | **Custom leader-elected cron** (Postgres jobstore + Redis lock) | precise at-most-once semantics [pattern] |
| Object store | **MinIO (S3 API)** / local FS in `dev` | tool-output spill, artifacts, uploads |
| Sandbox | **Dedicated executor container** (least-cap) + per-command policy | two-level sandbox [P5] |
| Web frontend | **React + Vite + TypeScript + Tailwind + shadcn/ui** | streaming UI, ecosystem, component richness |
| CLI | **Python + Typer + Rich/Textual** (thin API client) | shares the protocol, not the logic [P2] |
| IM adapters | **OneBot v11 (QQ), Telegram, WeCom** | compliant/available channels; unified adapter model |
| Observability | **OpenTelemetry + Langfuse + Prometheus** | trace/observation/score [P7]; metrics; prompt/eval mgmt |
| Auth | **OAuth2/OIDC (Authlib) + hashed API keys** | humans + machines |
| Packaging | **uv workspace + Docker + docker compose** | reproducible builds, one-command deploy |

### 2.2 Rationale highlights (alternatives rejected)
- **Python vs. TypeScript backend** → Python. TS (OpenClaw/OpenCode) is excellent but Python wins on embedding/RAG/MCP/agent tooling parity and keeps a single backend language. The **web** frontend is still TS/React. *(ADR-0001)*
- **Custom loop vs. LangGraph/Pydantic-AI** → custom core, but we *borrow* provider plumbing (LiteLLM) and may embed a durable graph for complex sub-flows later. The gateway/budget/steering/cron requirements make a purpose-built loop cheaper than bending a framework. *(ADR-0003)*
- **Postgres+pgvector vs. SQLite / dedicated vector DB** → Postgres for the containerised multi-service target (workers need a shared store). SQLite powers a `lite` single-node mode. A dedicated vector DB (Qdrant/Milvus) is an optional backend, not the default. *(ADR-0002)*
- **arq vs. Celery** → arq (async-native, small). Celery is heavier and sync-first. *(ADR-0006)*
- **MinIO always vs. profile-gated** → profile-gated; `dev`/`lite` use a volume. *(ADR-0002)*

---

## 3. System architecture (C4)

### 3.1 Context (level 1)
```
        ┌─────────┐   ┌─────────┐   ┌──────────────┐
        │  CLI    │   │ Web app │   │ IM (QQ/TG/…)  │   ← humans & channels
        └────┬────┘   └────┬────┘   └──────┬───────┘
             │  HTTP/SSE/WS │  HTTP/SSE/WS  │ adapter
             └──────────────┴───────┬───────┘
                                    ▼
                         ┌──────────────────────┐
                         │        KEEL           │  ← the platform (this system)
                         │  core · tools · memory │
                         │  scheduler · gateway   │
                         └───────────┬───────────┘
             ┌───────────────────────┼───────────────────────┐
             ▼                       ▼                       ▼
      LLM providers            MCP servers             Tools' targets
   (OpenAI/Anthropic/…)     (local & remote)      (filesystem, shell, web)
```

### 3.2 Containers (level 2)
```
                         ┌───────────────────────────── clients ─────────────────────────────┐
                         │  keel-cli   │   keel-web (nginx+React)   │  adapters (onebot/tg/…)  │
                         └──────┬──────────────┬───────────────────────────┬──────────────────┘
                                │  HTTP/SSE/WS  │                            │ internal API/Redis
                                ▼               ▼                            ▼
   ┌───────────────────────────────────────────────────────────────────────────────────────┐
   │  keel-server (FastAPI)  — API, auth, admin, SSE/WS hub, gateway host, run admission     │
   └───────┬───────────────────────────────────────────────────────────┬───────────────────┘
           │ enqueue (arq)                       publish/subscribe (Redis pub/sub)          │
           ▼                                                                                 ▼
   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐        (all services import keel-core)
   │  keel-worker ×N  │   │  keel-scheduler  │   │  keel-sandbox ×N  │
   │  runs agent turns│   │  cron leader     │   │  shell/code exec  │
   │  & background jobs│   │  (Redis lock)    │   │  least-privilege  │
   └───┬───────┬──────┘   └────────┬─────────┘   └────────┬─────────┘
       │       │                   │                      │
       ▼       ▼                   ▼                      ▼
   ┌────────┐ ┌────────┐  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
   │Postgres│ │ Redis  │  │ MinIO (S3)   │   │ Langfuse     │   │ OTel + Prom  │
   │+pgvector│ │queue/  │  │ blobs/spill  │   │ traces/evals │   │ metrics      │
   └────────┘ │lock/bus│  └──────────────┘   └──────────────┘   └──────────────┘
              └────────┘
```
- **keel-server** is stateless and horizontally scalable; it *admits* runs (durable) and streams events, but heavy execution happens in **keel-worker** (also stateless, scale-out). **keel-scheduler** is a singleton-by-election. **keel-sandbox** isolates dangerous tool execution.

### 3.3 Components inside `keel-core` (level 3)
```
keel-core
├─ loop/          AgentRuntime: turn state machine, two nested loops, stop-reason gate,
│                 interrupt/steer, budgets, guardrails, named termination
├─ context/       PromptAssembler (layered, cache-stable), Compactor (threshold+verify), epochs
├─ tools/         ToolRegistry, four-gate policy, AsyncToolExecutor (parallel-safe), output bounding
├─ providers/     ProviderGateway over LiteLLM: routing, failover, streaming→event normalization
├─ memory/        Blocks (core), Recall (sessions), Archival (pgvector), HybridSearch, consolidation
├─ state/         EventStore (append-only) + Projectors → read models; durable prompt admission
├─ agents/        AgentSpec, sub-agent delegation (as tool), shared budget, roles
├─ skills/        SKILL.md loader, progressive disclosure (inline/fork)
├─ mcp/           MCP client (stdio + HTTP/SSE), tool import, allow-list
├─ discovery/     tool_search (agentic discovery) over registry+MCP+skills
├─ permissions/   PermissionEngine (allow/ask/deny), approval bus protocol
├─ observability/ tracer (OTel), scores, cost accounting
└─ config/        layered settings, secrets boundary, agent config
```

---

## 4. The agent runtime (`keel-core/loop`)

Implements the manual's **two-loop** core [pattern].

### 4.1 Turn lifecycle
```
run(session, input):
  ADMIT input durably (state.admit) ──────────────► event: PromptAdmitted   [P: persist-before-call]
  acquire session lane (Redis lock)  ──────────────► one active run per session
  outer loop (while calls < max_iter and budget.remaining > 0 or grace):
    prologue: restore/build system prompt · prefetch memory ONCE · preflight compact
    assemble request: per-call COPY of history · inject memory into tail · cache breakpoints
    inner loop (retry/failover):
        stream = provider.stream(request)        # normalized events
        classify failures → recover (compress / refresh creds / fallback model)
    consume reply via STOP-REASON GATE:
        stopReason == "toolUse" → execute tools (parallel/serial) → append results → continue
        else                     → persist final → RETURN(reason="completed")
    drain steer · check interrupt · decrement budget
  on exit → RETURN(reason ∈ {max_iterations, budget, interrupted, halted, error})
```
- **Streaming:** every step emits typed events onto a per-run channel → Redis pub/sub → SSE/WS to any surface. Event vocabulary in §9.2.
- **Interrupt/steer:** `interrupt()` sets a flag checked at loop top; `steer(text)` is drained before the next model call at a safe boundary. Queue modes `steer|followup|interrupt`.
- **Guardrails:** no-progress halt (warn→halt), dedup identical tool calls, cap `delegate` fan-out, per-turn bounded recovery counters.

### 4.2 Concurrency & lanes
- Everything is `async`. **Per-session lane** = a Redis lock so one session runs one turn at a time; **different sessions run concurrently** across workers [pattern: serialize-in/parallelize-across]. Global concurrency caps per worker; sub-agent pool bounded.

---

## 5. Provider layer (`keel-core/providers`)

`ProviderGateway` wraps **LiteLLM** and adds the *policy* the manual insists we own:
- **Normalization:** provider chunks → one internal event stream (`text-delta · reasoning-delta · tool-call · tool-result · finish`).
- **Routing:** strategy chain (explicit override → task classifier → default); model *slots* (`main/fast/reasoning/vision/embed/rerank`).
- **Failover:** classify (`rate_limit/overloaded/auth/context_overflow/server_error/timeout/…`) → bounded recovery; credential pools; process-wide rate-limit guard in Redis; failover reconciles identity.
- **Caching:** compute a stable `prompt_cache_key`; pass provider cache controls (e.g., Anthropic `cache_control`).
- Embedding + rerank go through the same gateway for memory/RAG.
- **Local models:** an optional `ollama` container gives a zero-API-key first run.

---

## 6. Tools (`keel-core/tools`)

### 6.1 Tool contract
```python
class Tool(Protocol):
    name: str; description: str
    input_schema: type[BaseModel]          # validated before execute
    read_only: bool; concurrency_safe: bool; destructive: bool
    defer: bool = False; always_load: bool = False   # discovery
    async def execute(self, args, ctx) -> ToolResult  # {llm_content, display, artifacts, is_error, terminate}
```
- **Four gates [P3]:** registered → visible (availability/`defer`) → allowed (permission policy) → executable (call-time approval). Denied tools are stripped from the prompt *before* assembly.
- **Two outputs:** `llm_content` (terse, model-facing) vs `display` (rich, user-facing).
- **Output bounding:** cap ~2000 lines / 50 KB; spill full text to MinIO/FS; return the path + how to inspect.

### 6.2 Async executor (parallel-safe)
```
partition(tool_calls):
   parallel  = read_only & concurrency_safe & non-overlapping paths
   sequential= writes / overlapping paths / executionMode==sequential
run parallel via asyncio.gather (bounded semaphore); run sequential in order;
emit results in ASSISTANT SOURCE ORDER (deterministic transcript)
```

### 6.3 Built-in toolbox (v1)
| Group | Tools |
|---|---|
| Files | `read`, `write`, `edit` (str/patch), `ls`, `glob`, `grep` (ripgrep) |
| Exec | `bash`, `powershell` (sandboxed, timeout, truncation), `background_process` |
| Web | `web_fetch` (→clean md), `web_scrape` (css/xpath, optional JS render), `web_search` |
| Agentic | `task`/`delegate` (sub-agent), `todo`, `ask_user`, `tool_search`, `skill`, `memory_*` |
| Data | `http_request`, `sql_query` (P1) |

- Shell/web-scrape/JS-render tools run in **keel-sandbox** (least privilege). Web tools use an **SSRF-safe** fetcher (block loopback/link-local/private ranges).

---

## 7. Context engineering (`keel-core/context`)

- **Layered prompt [P4]:** stable prefix (identity, tool descriptions, skills index) → context files (agent/persona) → session-stable (env/date/model) → **cache boundary** → mutable tail (transcript, injected memory, current input).
- **Byte-stable prefix [pattern]:** sorted-key tool-call JSON, deterministic tool IDs, whitespace-normalized; memory recall injected into the *current message*, never the prefix; a `prompt_cache_key` derived from the prefix hash (Redis-cached).
- **Compaction [pattern]:** threshold-triggered (config, e.g. 0.7 of window); ladder = snip duplicates → truncate old tool outputs → summarize middle (protect head + recent) → verify (reject if token count inflated) → align tool-call/result boundaries. Compaction is a first-class event; session identity stays stable.

---

## 8. Memory & search (`keel-core/memory`, `keel-core/state`)

### 8.1 Tiers
- **Core memory** — editable **blocks** (`label/value/limit/read_only/version`) compiled into the prompt; self-editing tools (`memory_append/replace/rethink`) + `block_history` versioning.
- **Recall memory** — the full message history (event store), retrievable via `session_search`.
- **Archival memory** — a pgvector store of `passages` (embedding + tags + timestamp) with `archival_search`.
- **Knowledge bases (P1)** — ingested docs → chunks → embeddings; retrieval-as-tool or pre-injection.

### 8.2 Hybrid search (sessions & memory)
```
lexical  = Postgres FTS: tsvector (websearch) + pg_trgm (CJK / substring)
semantic = pgvector: cosine KNN over embeddings
fuse     = RRF(lexical_rank, semantic_rank) · temporal_decay · optional rerank
```
Session search and archival search share this pipeline. CJK handled via trigram + multilingual embeddings.

### 8.3 State / event sourcing [pattern]
- **Append-only `events`** are the source of truth; **projectors** fold them into read models (`messages`, `parts`, `session` rollups, `todos`). Gives resume, replay, live streaming, and audit from one primitive.
- **Durable prompt admission:** `session_input` row written before execution; a coordinator promotes it; crash → pending & retryable.

---

## 9. Data model & protocol

### 9.1 Core tables (Postgres; abbreviated)
```
users(id, email, role, ...)                      api_keys(id, hash, scopes, ...)
agents(id, name, persona, model_slots, tools_policy, memory_config, ...)
sessions(id, agent_id, key, title, status, token/cost rollups, created_at, ...)
events(id, session_id, seq, type, payload jsonb, ts)          -- append-only, (session_id,seq) unique
messages(id, session_id, seq, role, ...)  parts(id, message_id, kind, content jsonb, ...)  -- projections
memory_blocks(id, agent_id, label, value, limit, read_only, version)  block_history(...)
passages(id, scope, text, embedding vector, tags, ts)         -- pgvector (archival/KB)
kb_docs(...) kb_chunks(id, doc_id, text, embedding vector, ...)
skills(id, name, description, path, frontmatter jsonb)
mcp_servers(id, name, transport, url/cmd, allow_list, auth_ref)
tools_cache(agent_id, name, schema jsonb, source)             -- discovery cache
schedules(id, agent_id, spec, next_run_at, status, payload)   jobs(id, type, status, progress, result_ref, ...)
permissions(id, scope, resource, effect)   audit_log(id, actor, action, target, ts)
connections(id, kind=provider, config jsonb, secret_ref)      config(key, value, scope)
```
- **Session search index:** `messages` maintains a `tsvector` (+ `pg_trgm` index); `passages`/`kb_chunks` use `ivfflat/hnsw` vector indexes.
- **Traces/scores** are emitted to **Langfuse** (source of truth for eval); a thin local mirror for admin dashboards is optional.

### 9.2 API & event protocol
- **REST** (OpenAPI-generated SDK): CRUD for agents, sessions, messages, memory, skills, mcp, schedules, connections, config, users.
- **Run stream (SSE)** `GET /sessions/{id}/events?after=` — replayable per-session; **WebSocket** `/sessions/{id}/ws` for bidirectional control (steer/interrupt/approval).
- **Event vocabulary (typed):**
```
run.started · turn.started · message.delta · thinking.delta ·
tool.call · tool.progress · tool.result · approval.requested · approval.resolved ·
memory.updated · turn.ended · run.ended{reason} · error · lifecycle{phase}
```
- **Global bus** `GET /events` (instance-wide: sessions, jobs, health) for admin/IM fan-out via Redis pub/sub.

---

## 10. Multi-agent (`keel-core/agents`)
- **Sub-agent = tool** (`task`): parent calls it; child runs an isolated loop (fresh context, reduced toolset, own workspace); parent gets the final summary [pattern].
- **Shared budget** across the whole delegation tree (Redis-tracked); bounded `max_depth`; **leaf** vs **orchestrator** roles (leaf can't delegate). Foreground (blocking) and background (job) delegation; child cost rolls up.
- **Topologies:** supervisor + handoff (`transfer_to_<agent>`) v1; round-robin/groups P1. Coordination via shared task lists / optional shared memory blocks (kept small).

---

## 11. Scheduling & jobs (`keel-scheduler`, `keel-worker`)
- **`schedules`** table (cron/interval/one-shot/ISO). The scheduler is **leader-elected** (Redis lock); every tick it selects due rows, **advances `next_run_at` before enqueueing** (at-most-once even on crash) [pattern], and pushes a job to **arq**.
- **Workers** consume agent-run jobs and background jobs, report progress via events, support cancellation, and inject results back into the target session. Overrun → hard interrupt.

---

## 12. Surfaces

### 12.1 CLI (`keel-cli`)
Typer + Rich/Textual. Thin client of the API: interactive TUI (streaming, approvals, `/slash`), one-shot (`keel "…"`), headless (`--json`). An **embedded mode** (import `keel-core` directly, SQLite `lite`) for offline single-user use.

### 12.2 Web app (`web/`)
React + Vite + TS + Tailwind + shadcn/ui + TanStack Query + Zustand. Chat with streaming + tool/step timeline; approvals; session list & **search**; memory/skills/mcp/schedule/connection admin; run **traces** (embeds Langfuse or a local view). Served by nginx, proxying `keel-server`. A **Tauri** desktop shell (P2) reuses the same web bundle.

### 12.3 IM gateway (`adapters/`, hosted by `keel-server` or standalone `keel-gateway`)
- **Adapter interface** normalizes every platform to a unified `InboundEvent` + `MessageChain` and renders replies back [pattern: normalize-early].
- v1: **OneBot v11 (QQ)** (connect to NapCat/Lagrange over WS), **Telegram** (aiogram). P1: **WeCom (企业微信)** official API (compliant WeChat family), **Discord/Slack**; personal-WeChat bridges are opt-in/best-effort with ToS warnings.
- Per-chat **UMO-style session key** (`platform:type:id`) drives config/persona/memory/rate-limit/provider; **wake rules** (@mention/prefix/keyword), whitelist, per-chat rate limits; **untrusted input → constrained safe toolset** [P5/P6].

---

## 13. Security architecture
- **Permission engine [P5]:** rules → `allow/ask/deny`; last-match wins; `deny > ask > allow`; default ask; layered (global → agent → session → sandbox). Denied tools stripped pre-prompt.
- **Approval protocol:** correlation-ID request/response over the event bus; identical on CLI/web/IM; modes `plan/default/auto` (auto requires sandbox).
- **Two-level sandbox [P5]:** process/container isolation (`keel-sandbox`: read-only root, tmpfs, dropped caps, network-deny default, workspace bind, CPU/mem limits) + per-command policy. Advanced: per-session ephemeral containers or gVisor.
- **Egress/paths:** SSRF-safe fetch; workspace-only file access; deny `.git`/`.env`/secrets; artifact path-traversal guard.
- **Secrets:** `connections`/`config` secret refs resolved from env/Docker secrets/keystore; encrypted at rest; redacted in logs & telemetry; `.env` is secrets-only.
- **Trust-gating:** untrusted IM/web content confined to a safe toolset; project skills/MCP gated on trust; **import ≠ trust** (MCP allow-list) [P6].
- **AuthN/Z:** OAuth2/OIDC + hashed API keys; RBAC roles; audit log of tool actions & approvals.

---

## 14. Observability [P7]
- **OpenTelemetry** spans across services → OTel Collector; **Langfuse** for LLM **trace → observation (span/generation/tool/retriever/agent) → score**, prompt versioning, datasets/evals.
- **Cost/token accounting** per run/session/agent (incl. cache-read) stored on `sessions` and mirrored to Langfuse.
- **Prometheus** metrics (latency, queue depth, tool durations, error rates) + optional Grafana; **structlog** JSON logs; health/readiness probes.
- **Durable-first:** telemetry is emitted async and never blocks the loop; failures degrade, not crash.

---

## 15. Deployment (`deploy/`)

### 15.1 Compose services & profiles
```
services (full): keel-server, keel-worker(×N), keel-scheduler, keel-sandbox,
                 keel-web(nginx), postgres(pgvector), redis, minio,
                 langfuse(+clickhouse), otel-collector, prometheus, [ollama], [adapters/*]
profiles:
  dev   → server, worker, web, postgres, redis           (fast inner loop)
  full  → dev + scheduler, sandbox, minio, langfuse, otel, prometheus, adapters
  lite  → single 'keel' container (embedded core + SQLite + local FS)  # CLI/offline
```
- **One command:** `docker compose --profile full up -d`. First-run bootstrap (Alembic migrate, seed default agent + admin) is idempotent.
- **Config:** `.env` + mounted `deploy/config/`; secrets via env/Docker secrets. Images carry no secrets. Every service exposes health/readiness; graceful drain on shutdown.
- **Scale-out:** `keel-worker` scales horizontally; `keel-server` behind a load balancer; single elected `keel-scheduler`.

### 15.2 Repository layout (uv monorepo)
```
keel/
  docs/                      PRD, ARCHITECTURE, ADRs, diagrams
  packages/
    keel-core/               agent runtime library (no transport)
    keel-server/             FastAPI: API/auth/admin/SSE-WS/gateway host
    keel-worker/             arq worker (agent runs + jobs)
    keel-scheduler/          leader-elected cron
    keel-sandbox/            sandbox executor image + agent
    keel-cli/                Typer CLI client
    keel-sdk/                Python SDK for custom tools/agents
  adapters/                  onebot/, telegram/, wecom/, ...
  web/                       React app
  migrations/                Alembic
  deploy/{docker,compose,config}/
  tests/                     unit · integration · e2e · eval
  pyproject.toml             uv workspace
```

---

## 16. Key flows (sequences)

**A. Interactive run (any surface)**
```
surface → POST /sessions/{id}/messages           # admit input (durable) → 202 + run_id
server  → enqueue run(arq) ; surface opens SSE /sessions/{id}/events
worker  → keel-core.run(): loop → provider stream → tools(sandbox) → memory → events(Redis)
server  → relays events to SSE/WS ; approvals round-trip over WS
worker  → run.ended{reason} ; projections + trace(Langfuse) + cost updated
```

**B. Scheduled job**
```
scheduler(leader) tick → select due → advance next_run_at → enqueue run(arq)
worker → runs agent in target session → events → result posted to session/channel
```

**C. IM message (QQ)**
```
OneBot → adapter → normalize → InboundEvent(session_key) → wake rules/rate limit/safe toolset
      → server admit → worker run → events → adapter renders MessageChain → OneBot reply
```

---

## 17. Cross-cutting concerns
- **Config:** Pydantic Settings; precedence defaults → files → env → DB overrides; typed & validated at boot.
- **i18n:** EN + 简体中文 for UI and system prompts; CJK-safe search (trigram + multilingual embeddings).
- **Testing [NFR-12]:** provider record/replay for deterministic tests; unit (core), integration (services+DB), e2e (compose), eval harness (task-suite + Langfuse datasets).
- **Extensibility (footprint ladder [P8]):** capability enters as skill → tool (SDK) → plugin → MCP before touching the core; plugins load with manifest validation + rollback.

---

## 18. ADR index
| ADR | Decision |
|---|---|
| [0001](./adr/0001-language-and-runtime.md) | Python core; TS web; thin clients |
| [0002](./adr/0002-datastore.md) | Postgres+pgvector primary; Redis; MinIO; SQLite `lite` |
| [0003](./adr/0003-agent-runtime.md) | Custom two-loop core; LiteLLM provider plumbing |
| [0004](./adr/0004-frontend.md) | React + Vite + Tailwind + shadcn/ui |
| [0005](./adr/0005-sandbox.md) | Dedicated least-privilege sandbox container + per-command policy |
| [0006](./adr/0006-scheduler-and-queue.md) | arq queue + custom leader-elected at-most-once scheduler |
| [0007](./adr/0007-embeddings-and-rerank.md) | Local `bge-m3` embeddings default; hosted via gateway; rerank optional |
| [0008](./adr/0008-deployment-profiles-and-first-run.md) | `lite`/`dev`/`full`/`demo` profiles; MinIO & Ollama gating; first-run |

---

## 19. Phasing (maps to PRD milestones)
- **M0 Foundations:** monorepo skeleton, `keel-core` interfaces, compose `dev`, CI, migrations.
- **M1 MVP:** loop + providers + tools(sandbox,permissions) + sessions+search + memory + CLI + web + QQ + skills + MCP + discovery + tracing.
- **M2 Autonomy & scale:** scheduler + jobs + worker scale-out + failover/routing + admin dashboards + RBAC + more adapters.
- **M3 Knowledge & quality:** RAG/KB + consolidation + evals + plugin SDK + desktop shell.
- **M4 Hardening:** security review, perf, multi-tenant groundwork, docs/examples.
