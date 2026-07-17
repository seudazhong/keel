# Keel — Architecture & Design

> **Status:** Living target architecture · **Companion to:** [`PRD.md`](./PRD.md) · **Decisions:** [`adr/`](./adr)
> **Current fidelity:** this document describes the intended end state unless the
> implementation-status section says otherwise. See [`STATUS.md`](./STATUS.md) for verified
> evidence and [`ROADMAP.md`](./ROADMAP.md) for remediation order.

---

## 0. Implementation status and fidelity

Keel's core runtime/data work is materially ahead of its product and deployment topology.
The target architecture below remains useful, but these substitutions and gaps are current:

| Area | Current implementation | Target / remediation |
|---|---|---|
| Scope/identity | One hard-coded `web:local` scope; no users, organizations, or persisted Agents CRUD. | Local Agent profiles in M3.2, then user identity, single-organization-v1 membership, private personal Agents, and explicit team grants in [M3.6](./ROADMAP.md#m36--multi-user-identity-access-and-durable-run-topology). |
| RLS | Scoped rows and RLS policies exist, but the runtime role owns the DB/schema and can bypass RLS. | Non-owner runtime role plus audited fail-closed isolation ([M3.3](./ROADMAP.md#m33--cloud-safety-foundation)). |
| Execution | Server/worker tool wiring uses authenticated `keel-sandbox` RPC and fails closed without it; the CLI exposes shell only for a verified sanitized workspace. The sandbox container is not yet deployed by Compose. | Deploy and exercise the isolated executor with enforced mounts/egress (ADR-0005, M3.3). |
| Runs/approvals | Interactive execution and some approvals are server-local/in-memory; durable unattended approvals and jobs exist. | Worker-owned durable interactive topology and restart-safe cross-surface approvals (M3.3/M3.6). |
| Server/worker/scheduler | Server runs interactive turns; worker runs arq jobs and cron scheduling. `keel-scheduler` is a stub, not a separately elected service. | Durable worker ownership in M3.6; elected scheduler and scale-out validation in M3.8. |
| Auth/secrets | Optional plaintext configured API keys; empty config means implicit admin. OAuth state is process-local. | Hashed/scoped machine credentials and durable connector OAuth state in M3.3; human identity/OIDC in M3.6. |
| Gateways/outbound | OneBot/Telegram webhook handlers are unauthenticated; outbound idempotency is process-local. | Authenticated/replay-safe webhooks and durable outbound idempotency (M3.3). |
| Permissions | Main CLI profile is fail-closed, but APIs permit construction paths where an omitted default can become allow-all. | Explicit non-allow default as an invariant in every policy constructor (M3.3). |
| Events/data lifecycle | Event rows have versions; no upcaster registry, retention policy, or complete erasure path. | M3.4 event evolution, then M3.5 retention/erasure. |
| Web delivery | Compose `keel-web` builds and serves the React app through nginx with API/SSE proxying and SPA fallback. | Production profile hardening and accurate full-stack delivery in M3.8. |
| Observability/SDK/CI | Deterministic evals and basic tracing exist; full OTel/metrics/SLOs, generated SDK/version diff, and documented production CI gates do not. | Event/API compatibility in M3.4 and production delivery gates in M3.8. |

Architecture statements using present tense below should be read as **target contracts** unless
this table or [Status](./STATUS.md) confirms current fidelity. ADRs record decisions; they do
not by themselves prove implementation.

---

## 1. Design goals & guiding principles

Keel is a **general-purpose agent runtime** with a *narrow-waist* core and many surfaces. Its **primary product form is a server-side, connected conversational assistant** (team/IM + personal) — see **§1.1** and **[ADR-0009](./adr/0009-product-form-and-primary-use-cases.md)**. The architecture is driven by five commitments:

1. **One core, many surfaces [P2].** `keel-core` is a pure library with no UI/transport dependencies. CLI, web, and IM are clients of one protocol.
2. **One tool interface [P3].** Built-ins, MCP tools, skills, and sub-agents all present to the loop as "a callable with a JSON schema."
3. **Data plane vs. control plane [P4].** The bytes sent to the model are stable and cache-friendly; correctness lives in our runtime code.
4. **Durable & observable by construction [P1, P7].** Event-sourced sessions, durable prompt admission, and trace/observation/score on every run.
5. **Fail closed on trust, degrade on ops [P5].** Permission/security errors deny; provider/tool errors recover.

Non-negotiable invariants (acceptance-tested): bounded loops with named termination; persist-before-first-model-call; stop-reason-gated tool execution; byte-stable prompt prefix; two-level sandbox; shared budget across the delegation tree; import ≠ trust; **per-scope data isolation** [ADR-0009].

### 1.1 Product form & primary use cases [ADR-0009]
The core is general-purpose, but v1 is shaped by two primary use cases:
- **Team / IM assistant** — a shared assistant in group chats + a team web app (Q&A, look-ups, drafting). *Server-side; untrusted input.*
- **Personal connected assistant** — works with **my** email/calendar/docs/knowledge. That data lives in **cloud accounts via OAuth**, so it is *also server-side*; only "my local files" needs local execution.

Consequences that shape this whole document:
1. **Form = server-primary.** Tool execution sits behind a **pluggable `ExecutionEnvironment`** (§6.4); v1 ships the sandbox-container backend, and a **`LocalDaemon`** backend (local files / desktop) is **deferred**. The "server vs personal-machine" question is thus a *backend* choice, not an architecture fork.
2. **An agent is a scoped, persisted entity** (§10): a *group agent* and a *personal agent* are one abstraction with different scope/memory/connectors/trust.
3. **Connectors are a first-class subsystem** (§6.5): email/calendar/docs/IM via OAuth, scoped per agent.
4. **Center of gravity = connectors + memory + retrieval + messaging**; file/shell/code are secondary. **Web + IM are the primary surfaces**; CLI is admin/power-user.
5. **Data isolation is core**, and the headline threat is **cross-scope data exfiltration / confused deputy** (§13), ranked above sandbox escape.

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
- **Target topology:** `keel-server` is stateless and horizontally scalable; it admits runs
  and streams events while `keel-worker` owns execution, `keel-scheduler` is
  singleton-by-election, and `keel-sandbox` isolates dangerous tools. **Current fidelity:**
  server/worker execution is wired through authenticated sandbox RPC and fails closed, but
  the sandbox container is not deployed by Compose; worker cron still performs scheduling.

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
├─ connectors/    email · calendar · contacts · docs · IM — OAuth, per-scope, surfaced as scoped tools
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
- Embedding + rerank go through the same gateway for memory/RAG; embedding collections are `(model, dim)`-pinned (§8.2, ADR-0007).
- **Rate limiting [G10]:** token-bucket limiters keyed by `{provider-credential}`, `{session}`, and `{chat}` in Redis; the provider-side guard reconciles with `429`/`Retry-After` headers; IM per-chat limits reuse the same primitive.
- **Cost control [G7]:** the gateway **reserves** an estimated token/cost budget in Redis *before* a call (hard-cap on reserved) and **reconciles** to authoritative provider usage *after* the run into Postgres/Langfuse; both *reserved* and *settled* cost are surfaced (the shared budget in §10 spends the reservation).
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
**Emphasis [ADR-0009]:** for the primary use cases the headline tools are **connectors + retrieval + messaging**; file/shell/code are secondary (kept, not the star).
| Group | Tools |
|---|---|
| Connectors | `email_*`, `calendar_*`, `contacts_*`, `docs_*`, `kb_search` — via the Connectors subsystem (§6.5), **scoped per agent** |
| Files | `read`, `write`, `edit` (str/patch), `ls`, `glob`, `grep` (ripgrep) |
| Exec | `bash`, `powershell` (sandboxed, timeout, truncation), `background_process` |
| Web | `web_fetch` (→clean md), `web_scrape` (css/xpath, optional JS render), `web_search` |
| Agentic | `task`/`delegate` (sub-agent), `todo`, `ask_user`, `tool_search`, `skill`, `memory_*` |
| Data | `http_request`, `sql_query` (P1) |

- Shell/web-scrape/JS-render tools run in **keel-sandbox** (least privilege). Web tools use an **SSRF-safe** fetcher (block loopback/link-local/private ranges).

### 6.4 Execution environments (pluggable) [ADR-0009]
Tool execution is behind an **`ExecutionEnvironment`** interface (à la Hermes `BaseEnvironment`), so *where* a `bash`/file tool runs is a swappable backend, not baked into the loop:
```
ToolExecutor ──▶ ExecutionEnvironment
                 ├─ SandboxContainer  → server-side keel-sandbox (v1 default; safest)
                 ├─ LocalDaemon       → a light executor the user runs on their machine (deferred; unlocks local-files/desktop)
                 └─ InProcess         → lite mode (reduced isolation)
```
This is what demotes the "server vs personal-machine" fork to a backend choice. v1 ships `SandboxContainer` (+ `InProcess` for `lite`); `LocalDaemon` is a later milestone with its **own** fail-closed permission gate (untrusted input must never drive a local executor).

### 6.5 Connectors (`keel-core/connectors`) [ADR-0009]
The first-class integration subsystem for personal data + messaging — the headline capability for the primary use cases.
- **Kinds (v1 targets):** email (Gmail / MS Graph), calendar, contacts, docs/notes (Google Drive / OneDrive / Notion), knowledge bases (RAG, §8), and IM (§12.3). Implemented natively or via curated MCP servers, presented to the loop as **scoped tools** (`email_*`, `calendar_*`, …).
- **OAuth token management:** per-user, per-connector OAuth with refresh; tokens stored via the secrets envelope (§13, G9); a connector is **granted to a specific agent scope** — never ambient.
- **Per-scope data isolation:** a connector attached to a *personal* agent is invisible to a *group* agent (§10, §13). Retrieval from a connector is logged/auditable.
- **Safety:** connector content is **untrusted input** (an email can carry injection); it is scanned (§13, G6) and cannot silently trigger destructive tools or cross-scope reads.

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

**Embedding pinning [G8, ADR-0007]:** every `passage`/`kb_chunk` records its `embedding_model` + `dim`; a collection is pinned to a single `(model, dim)`, and **cross-model KNN is refused**. Switching the embedding model is a **re-embed migration job**, never a silent config change.

### 8.3 State / event sourcing [pattern]
- **Append-only `events`** are the source of truth; **projectors** fold them into read models (`messages`, `parts`, `session` rollups, `todos`). Gives resume, replay, live streaming, and audit from one primitive.
- **Durable prompt admission:** `session_input` row written before execution; a coordinator promotes it; crash → pending & retryable.
- **Event schema evolution [G3]:** typed, append-only upcasters convert persisted
  events before replay or projection; unknown types, malformed historical payloads,
  missing transitions, and future versions fail closed. Rebuilds can dry-run and
  resume from checkpoints, with tombstone hooks for future lifecycle policy. See
  [event/API versioning](./EVENT-VERSIONING.md).

---

## 9. Data model & protocol

### 9.1 Core tables (Postgres; abbreviated)
```
users(id, email, role, ...)                      api_keys(id, hash, scopes, ...)
agents(id, name, persona, model_slots, tools_policy, memory_config, ...)
sessions(id, agent_id, key, title, status, token/cost rollups, created_at, ...)
events(id, session_id, seq, type, version, payload jsonb, ts)          -- append-only, (session_id,seq) unique
messages(id, session_id, seq, role, ...)  parts(id, message_id, kind, content jsonb, ...)  -- projections
memory_blocks(id, agent_id, label, value, limit, read_only, version)  block_history(...)
passages(id, scope, text, embedding vector, embedding_model, dim, tags, ts)         -- pgvector (archival/KB)
kb_docs(...) kb_chunks(id, doc_id, text, embedding vector, embedding_model, dim, ...)
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
- **REST** (OpenAPI-generated SDK) under a **`/v1`** prefix; **additive-only** evolution with a documented deprecation window; CI regenerates the SDK and diffs it to catch breaking changes [G14]. CRUD for agents, sessions, messages, memory, skills, mcp, schedules, connections, config, users.
- **Run stream (SSE)** `GET /sessions/{id}/events?after=` — replayable per-session; **WebSocket** `/sessions/{id}/ws` for bidirectional control (steer/interrupt/approval).
- **Event vocabulary (typed):**
```
run.started · turn.started · message.delta · thinking.delta ·
tool.call · tool.progress · tool.result · approval.requested · approval.resolved ·
memory.updated · turn.ended · run.ended{reason} · error · lifecycle{phase}
```
- **Global bus** `GET /events` (instance-wide: sessions, jobs, health) for admin/IM fan-out via Redis pub/sub.

---

## 10. Agents, scope & multi-agent (`keel-core/agents`)
- **Target [ADR-0009]:** an Agent is a scoped, persisted entity with persona, memory,
  toolset, connectors, permission boundary, provider, and trust level. Personal and team
  Agents share the abstraction but differ in grants. **Current fidelity:** the product uses
  the fixed `web:local` scope and has no users/Agents CRUD; current RLS is not a hard boundary
  because the runtime DB owner can bypass it.
- **Sub-agent = tool** (`task`): parent calls it; child runs an isolated loop (fresh context, reduced toolset, own workspace); parent gets the final summary [pattern].
- **Shared budget** across the whole delegation tree (Redis-tracked); bounded `max_depth`; **leaf** vs **orchestrator** roles (leaf can't delegate). Foreground (blocking) and background (job) delegation; child cost rolls up.
- **Topologies:** supervisor + handoff (`transfer_to_<agent>`) v1; round-robin/groups P1. Coordination via shared task lists / optional shared memory blocks (kept small). Handoffs are bounded by a **max-handoff cap** and **A→B→A cycle detection** in the delegation tree, alongside `max_depth` [G13].

---

## 11. Scheduling & jobs (`keel-scheduler`, `keel-worker`)
- **Current:** schedules are persisted and worker cron uses compare-and-set advancement before
  enqueue. Durable background jobs provide DB leases/reclaim, retries, progress,
  cancellation, and exactly-once terminal result injection.
- **Not yet target-complete:** `keel-scheduler` is not a separately elected service, and
  interactive Web runs are not worker-owned. Durable-run and scale-out gates are in M3.6/M3.8.

---

## 12. Surfaces

**Priority [ADR-0009]:** **Web + IM are the primary surfaces**; the **CLI** is an admin/power-user surface (and the `lite` embedded mode); a **desktop shell / local-file access** is deferred behind the `LocalDaemon` execution backend (§6.4).

### 12.1 CLI (`keel-cli`)
Typer + Rich/Textual. Thin client of the API: interactive TUI (streaming, approvals, `/slash`), one-shot (`keel "…"`), headless (`--json`). An **embedded mode** (import `keel-core` directly, SQLite `lite`) for offline single-user use.

### 12.2 Web app (`web/`)
The React + Vite + TypeScript app currently provides chat, sessions, connectors, Knowledge,
Jobs, Memory proposals, schedules, approvals, overview, and settings. Compose `keel-web`
packages the production build behind nginx with API/SSE proxying and SPA fallback. Memory
block/Agent/admin governance completeness, responsive/i18n/a11y work, production profile
hardening, and Tauri remain targets.

### 12.3 IM gateway (`adapters/`)
- **Topology [G11]:** adapters are **hosted in `keel-server`** by default (all profiles, as the container diagram shows); a standalone **`keel-gateway`** container is an **opt-in scale-out split** for high-volume channels.
- **Adapter interface** normalizes every platform to a unified `InboundEvent` + `MessageChain` and renders replies back [pattern: normalize-early].
- v1: **OneBot v11 (QQ)** (connect to NapCat/Lagrange over WS), **Telegram** (aiogram). P1: **WeCom (企业微信)** official API (compliant WeChat family), **Discord/Slack**; personal-WeChat bridges are opt-in/best-effort with ToS warnings.
- Per-chat **UMO-style session key** (`platform:type:id`) drives config/persona/memory/rate-limit/provider; **wake rules** (@mention/prefix/keyword), whitelist, per-chat rate limits; **untrusted input → constrained safe toolset** [P5/P6].

---

## 13. Security architecture
- **Permission target [P5]:** rules resolve to `allow/ask/deny` with an explicit fail-closed
  default. The main CLI profile asks for mutations, but every construction path has not yet
  been hardened against an omitted/permissive default.
- **Approval target [G5]:** one durable pending store and TTL applies identically across
  CLI/Web/IM. Durable unattended approvals exist; interactive approval/run state is still
  process-local.
- **Sandbox target [P5]:** the typed environment, authenticated/replay-protected RPC boundary,
  sanitized-workspace admission, and fail-closed service wiring exist. Production still must
  deploy the executor container with the documented mount and egress invariants.
- **Egress/paths:** SSRF-safe fetch; workspace-only file access; deny `.git`/`.env`/secrets; artifact path-traversal guard.
- **Secrets [G9]:** app-level **envelope encryption** — a per-record data key encrypts each secret and is wrapped by a **master key** sourced from env/Docker secret (v1), pluggable to Vault/KMS; keys never ship in images and a rotation procedure is documented. `connections`/`config` hold secret *refs*; values are redacted in logs & telemetry; `.env` is secrets-only.
- **Trust-gating:** untrusted IM/web content confined to a safe toolset; project skills/MCP gated on trust; **import ≠ trust** (MCP allow-list) [P6].
- **Scope isolation & confused-deputy defense [ADR-0009]:** per-agent/scope data boundaries — a group/untrusted agent can never access a personal agent's connectors, memory, or tokens. **Cross-scope data exfiltration** (injection coercing an agent to leak private data) is the **headline threat** for the primary use cases, ranked *above* sandbox escape. Personal-agent connectors require explicit per-connector grants; a personal agent's outbound actions (send email, post) pass approval and are audited; co-hosting a public group bot with a private personal agent is allowed only under hard scope isolation (separate instances recommended for the most sensitive use).
- **Injection scanning [G6]:** tool/skill/MCP **descriptions and imported instructions** are scanned for prompt-injection at import/discovery time; a hit **quarantines** the item (excluded from the prompt) pending review.
- **AuthN/Z target:** OAuth2/OIDC for humans, hashed/scoped API credentials, RBAC, and
  complete audit. Current API-key configuration is optional plaintext and empty means
  implicit admin.
- **Data governance target [G4]:** retention windows, trace/telemetry PII redaction,
  documented data map, and complete erasure across events/projections/vectors/Knowledge/
  tokens/artifacts. These are M3.5 work, not current capability.

---

## 14. Observability [P7]
- **OpenTelemetry** spans across services → OTel Collector; **Langfuse** for LLM **trace → observation (span/generation/tool/retriever/agent) → score**, prompt versioning, datasets/evals.
- **Cost/token accounting** per run/session/agent (incl. cache-read) stored on `sessions` and mirrored to Langfuse.
- **Prometheus** metrics (latency, queue depth, tool durations, error rates) + optional Grafana; **structlog** JSON logs; health/readiness probes.
- **Current fidelity:** deterministic Memory/Knowledge evals and basic tracing/cost fields
  exist. Full cross-service OTel, Prometheus/SLO coverage, reconciled usage, and production
  dashboards remain below this target.

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
- **Current Compose:** `docker compose --profile dev up -d --build` starts Postgres, Redis,
  migration, server, worker, Ollama, and a static web stub. The `full` profile currently
  selects the same implemented services; MinIO, Langfuse, OTel, Prometheus, separate
  scheduler, and sandbox services are aspirational.
- **Current bootstrap:** Alembic migration is idempotent; users/default Agents/admin are not
  seeded because those product models do not yet exist.
- **Target scale-out:** worker/server scale-out and elected scheduler require the durable
  topology and production-delivery gates in M3.6/M3.8.

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
- **Backup / DR [G15]:** the append-only event store is the replay source of truth; `pg_dump` + WAL archiving for Postgres and a mirror for MinIO; an M4 runbook covers restore, exercised by a restore drill in e2e.

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
| [0009](./adr/0009-product-form-and-primary-use-cases.md) | Server-primary connected assistant; agent = scoped entity; Connectors first-class; pluggable execution |
| [0010](./adr/0010-durable-background-jobs.md) | Durable jobs: at-least-once delivery + DB leases; schedules keep at-most-once triggers |

---

## 19. Phasing

The original M0–M4 phasing is retained in
[`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) as history. Active execution is defined
only by [`ROADMAP.md`](./ROADMAP.md): cloud safety, event evolution, retention/erasure,
multi-user identity/Agents and durable topology, connector/product experience, then
production delivery/scale. Plugin SDK and Desktop follow those gates.

---

## 20. Design-review convergence
This target spec incorporates the decisions and risks recorded in the historical
[`DESIGN-REVIEW.md`](./DESIGN-REVIEW.md). Implementation is intentionally not inferred from
that convergence: the fidelity table in §0 and [Status](./STATUS.md) identify what is real,
while [Roadmap](./ROADMAP.md) owns remediation and sequencing.
