# Keel — Product Requirements Document (PRD)

> **Status:** Draft v1.0 · **Owner:** Platform team · **Last updated:** 2026-07-06
> **Related:** [`ARCHITECTURE.md`](./ARCHITECTURE.md) · [`adr/`](./adr)

---

## 1. Overview

**Keel** is a general-purpose, self-hostable **AI agent platform**. A single frontend-agnostic *agent core* is exposed through many surfaces — a **CLI**, a **web app**, and **IM gateways** (QQ, WeChat, Telegram, …) — and is equipped with a robust toolbox (files, shell, web), parallel/async tool execution, multi-agent collaboration, durable memory, session persistence and search, scheduled autonomy, skills, MCP, agentic tool discovery, and first-class observability. Every component is containerised and the whole system deploys with a single `docker compose up`.

The name reflects the design thesis (borrowed from the project's own field manual, *How to Develop an AI Agent*): **the stable "keel" is the core runtime; everything else — surfaces, tools, providers — bolts onto it through narrow, well-defined seams.**

### 1.1 One-line pitch
> Run one capable AI agent anywhere you talk to it — terminal, browser, or chat app — with the memory, tools, safety and observability of a production system, self-hosted in Docker.

### 1.2 Why now / motivation
Teams and individuals increasingly want an autonomous assistant that (a) they **control and self-host** (privacy, cost, data residency), (b) meets them on **their** surface (CLI for devs, web for teams, IM for everyone), (c) **remembers** across sessions, (d) can **act** (files, shell, web, custom tools) safely, and (e) can be **operated** (observed, evaluated, scheduled). Existing OSS projects each solve a slice; Keel unifies them into one coherent, deployable platform built on proven patterns.

---

## 2. Goals & Non-Goals

### 2.0 Primary use cases & product form [ADR-0009]
The core is general-purpose, but v1 is anchored on two primary use cases, which fix the product form:
- **UC-A Team / IM assistant** — a shared assistant in group chats + a team web app (server-side; untrusted input).
- **UC-B Personal connected assistant** — works with **my** email/calendar/docs/knowledge via cloud **OAuth connectors** (also server-side; only local files need a local executor).

Form: **server-primary**, with tool execution behind a **pluggable execution environment** (sandbox now; a local executor deferred); **an agent is a scoped, persisted entity** (group vs personal = same abstraction, different scope); **Connectors are a first-class capability**; **Web + IM are the primary surfaces**, CLI is admin/power-user. See [ADR-0009](./adr/0009-product-form-and-primary-use-cases.md).

### 2.1 Goals
- **G1 — One core, many surfaces.** A single agent runtime serving CLI, web, and IM identically; adding a surface never forks agent logic.
- **G2 — Capable & safe action.** A strong built-in toolbox with a permission engine and sandboxing so tools can touch the real world without undue risk.
- **G3 — Durable & searchable memory.** Long-term memory + full session persistence + fast session/semantic search.
- **G4 — Autonomy.** Scheduled tasks and background jobs that run reliably (at-most-once) without a human present.
- **G5 — Extensible.** Skills, MCP, plugins, and agentic tool discovery let capability grow at the edges, not the core.
- **G6 — Operable.** Built-in observability (traces, cost, evals) and a "one-command" containerised deployment.
- **G7 — Multi-provider.** Work with any major LLM provider, with failover and per-task model routing.

### 2.2 Non-Goals (v1)
- **NG1** — Not a no-code visual workflow builder (à la Dify/n8n). Keel is agent-first; visual flow authoring is out of scope for v1.
- **NG2** — Not a hosted multi-tenant SaaS. v1 targets **self-hosted single-tenant / small-team**; hard multi-tenant isolation & billing are later.
- **NG3** — Not training/fine-tuning models. Keel consumes models via APIs / local inference servers.
- **NG4** — Not a native mobile app in v1 (the web app is responsive/PWA; native shells later).
- **NG5** — WeChat support is **best-effort** and explicitly bounded by platform ToS (see §6.1, Risks §12).

---

## 3. Target users & personas

| Persona | Description | Primary surface | Key needs |
|---|---|---|---|
| **Dev Dana** | Software engineer automating dev/ops tasks | CLI, web | shell/file tools, sandboxing, sub-agents, MCP, low latency |
| **Ops Omar** | Platform/SRE running the agent as a service | web (admin), CLI | scheduling, observability, deployment, RBAC, cost control |
| **Team Tia** | Non-dev team member using it as an assistant | web, IM | conversation, memory, skills, approvals |
| **Individual Ivy** | Uses a personal assistant over her own email/calendar/docs | web, IM (DM) | connectors (OAuth), private memory, proactivity, data isolation |
| **Community Chen** | Runs a bot for a QQ/WeChat group | IM gateway | multi-platform adapters, per-chat sessions, rate limits, safety |
| **Builder Bao** | Extends Keel with custom tools/skills/plugins | CLI, SDK | clean extension APIs, MCP, hot-reload, docs |

---

## 4. Key use cases / journeys

*(Primary: #2 team, #3 group bot, #7 personal. Secondary: #1 dev, #5 research, #6 skills.)*

1. **Dev automation (CLI).** Dana runs `keel "find and fix the flaky test in payments"`; the agent reads files, runs the suite in a sandbox, edits code, and reports a diff — asking approval before writing.
2. **Team assistant (web).** Tia asks the web app to "summarise this week's incidents and draft a postmortem"; the agent recalls prior sessions, pulls linked docs via MCP, and streams a draft.
3. **Group bot (IM).** In a QQ group, members @-mention the bot; it maintains a per-group session, answers with tools, and respects per-group rate limits and a constrained "safe" toolset because input is untrusted.
4. **Scheduled digest (autonomy).** Omar schedules "every weekday 09:00, compile overnight alerts and post to the ops channel"; the scheduler runs it at-most-once and the run is fully traced.
5. **Long-horizon research (multi-agent).** A lead agent decomposes a research task, delegates to isolated sub-agents (shared budget), and synthesises results.
6. **Skill authoring (extensibility).** Bao drops a `skills/postmortem/SKILL.md`; the agent discovers it, shows its name/description in-context, and loads the body on demand.
7. **Personal connected assistant (UC-B).** Ivy's personal agent — scoped to her, with her Gmail/Calendar granted — triages overnight mail, drafts replies for approval, and each morning posts a digest; its connectors and memory are invisible to any group agent.

---

## 5. Product principles (design north-star)
Adopted verbatim from the field manual and treated as **acceptance constraints**:
- **P1 The loop is bounded & terminates gracefully.** Every run caps iterations, budgets tokens, and exits for a named reason.
- **P2 The UI never talks to the model; the core never depends on a UI.** All surfaces are clients of one protocol.
- **P3 One tool interface.** Built-ins, MCP tools, and sub-agents present identically to the loop.
- **P4 Data plane vs. control plane.** Byte-stable, cache-friendly prompt; correctness lives in the runtime (our code).
- **P5 Fail closed on trust, degrade on ops.** Security/permission errors deny; provider/tool errors recover.
- **P6 Import ≠ trust.** MCP tools and plugins are allow-listed and sandboxed.
- **P7 Observable by construction.** Every run emits trace → observations → scores with cost/tokens.
- **P8 Footprint ladder.** New capability enters as a skill/tool/plugin/MCP before it ever touches the core.

---

## 6. Functional requirements

Priorities: **P0** = MVP (must), **P1** = fast-follow (should), **P2** = later (could).

### 6.1 Surfaces & interaction (FR-S)
| ID | Requirement | Priority |
|---|---|---|
| FR-S1 | **CLI** client (interactive TUI + one-shot + headless JSON), a thin client of the core API; supports streaming, approvals, `/slash` commands. | P0 |
| FR-S2 | **Web app**: chat UI with streaming, tool/step timeline, approvals, session list & search, memory & settings admin, run traces. | P0 |
| FR-S3 | **IM gateway** with a pluggable adapter model normalising every platform to one event + message-chain type. | P0 |
| FR-S4 | IM adapters: **QQ (OneBot v11)** and **Telegram** at v1; **Discord/Slack** P1; **WeChat** best-effort P1 (ToS-bounded). | P0/P1 |
| FR-S5 | Per-conversation session keys (`platform:type:id`) driving config, persona, memory, rate-limits, provider. | P0 |
| FR-S6 | Wake rules for group chats (@-mention / prefix / keyword), whitelist, and per-chat rate limits. | P0 |
| FR-S7 | Optional **desktop shell** (Tauri) wrapping the web app for a native experience. | P2 |

### 6.2 Tools & execution (FR-T)
| ID | Requirement | Priority |
|---|---|---|
| FR-T1 | Built-in **file tools**: read, write, edit (string/patch), list, glob, grep (ripgrep). | P0 |
| FR-T2 | Built-in **shell execution**: `bash` (Linux containers) and `powershell`/`pwsh`, with timeout, output truncation, and sandbox. | P0 |
| FR-T3 | Built-in **web tools**: `web_fetch` (HTML→clean text/markdown), `web_scrape` (CSS/xpath extract, JS-render option), `web_search`. | P0 |
| FR-T4 | **Parallel/async tool execution**: read-only/independent tools run concurrently; writes/overlapping paths serialise; deterministic emitted order. | P0 |
| FR-T5 | **Output bounding**: cap model-facing output (~2000 lines / 50 KB), spill full result to a retained file, return the path. | P0 |
| FR-T6 | Tool **schema validation** (Pydantic/JSON-Schema) before execution; separate model-facing vs user-facing output. | P0 |
| FR-T7 | Additional tools: `todo`, `ask_user`/`question`, `task` (sub-agent), `memory_*`, `skill`, `http_request`, `sql_query` (P1). | P0/P1 |
| FR-T8 | **Guardrails**: loop/no-progress detection, dedup, per-turn bounded recovery counters. | P0 |

### 6.2.1 Connectors & personal data (FR-N) [ADR-0009]
| ID | Requirement | Priority |
|---|---|---|
| FR-N1 | **Connectors subsystem**: email, calendar, contacts, docs/notes, knowledge, IM — via OAuth (or curated MCP), surfaced to the loop as **agent-scoped tools**. | P0 |
| FR-N2 | **OAuth token management**: per-user/per-connector auth + refresh; tokens envelope-encrypted; a connector is **granted to a specific agent scope**, never ambient. | P0 |
| FR-N3 | **Per-scope data isolation**: a personal agent's connectors/memory/tokens are invisible to group/other agents; cross-scope reads are denied and audited. | P0 |
| FR-N4 | **Outbound-action control**: a personal agent's send/post actions require approval and are audited. | P0 |
| FR-N5 | **Pluggable execution environment** (sandbox now; **`LocalDaemon`** for local files deferred), with its own fail-closed permission gate. | P1 |

### 6.3 Agent core & loop (FR-C)
| ID | Requirement | Priority |
|---|---|---|
| FR-C1 | Two-loop core (outer tool loop + inner retry/failover), stop-reason-gated tool execution, named termination. | P0 |
| FR-C2 | **Streaming** of typed events (token, thinking, tool-call, tool-result, turn events, lifecycle). | P0 |
| FR-C3 | **Interrupt** and **steer** channels applied at safe turn boundaries; queue modes (steer/followup/interrupt). | P0 |
| FR-C4 | **Context engineering**: layered system prompt, prompt caching (byte-stable prefix), threshold compaction with verify. | P0 |
| FR-C5 | **Durable prompt admission**: user input persisted before the first model call; crash-recoverable, resumable. | P0 |

### 6.4 Multi-agent (FR-M)
| ID | Requirement | Priority |
|---|---|---|
| FR-M1 | **Sub-agents as tools** (`task`/`delegate`), isolated context + reduced toolset + own workspace. | P0 |
| FR-M2 | **Shared budget** across the delegation tree; bounded depth; leaf vs. orchestrator roles. | P0 |
| FR-M3 | Orchestration topologies: supervisor and handoff (`transfer_to_<agent>`); round-robin P1. | P1 |
| FR-M4 | Foreground and **background** delegation (jobs), with cost rolled up to the parent. | P1 |

### 6.5 Memory & persistence (FR-D)
| ID | Requirement | Priority |
|---|---|---|
| FR-D1 | **Session persistence**: every message/part/event stored durably (event-sourced); resume any session. | P0 |
| FR-D2 | **Session search**: full-text + semantic (hybrid) search across a user's session history. | P0 |
| FR-D3 | **Long-term memory**: editable memory blocks (in-context) + retrievable knowledge store (hybrid search, recency decay). | P0 |
| FR-D4 | **Self-editing memory tools** and memory versioning (history/undo). | P1 |
| FR-D5 | **RAG / knowledge bases**: ingest docs → chunk → embed → hybrid retrieve; retrieval-as-tool option. | P1 |
| FR-D6 | Background **memory consolidation** (extract/merge/prune) via a constrained agent. | P2 |

### 6.6 Autonomy & scheduling (FR-A)
| ID | Requirement | Priority |
|---|---|---|
| FR-A1 | **Scheduled tasks**: cron / interval / one-shot / ISO-datetime, per-agent and per-session. | P0 |
| FR-A2 | **At-most-once** execution (advance cursor before run; leader election; hard interrupt on overrun). | P0 |
| FR-A3 | **Background jobs** with progress, cancellation, and result injection back into a session. | P1 |
| FR-A4 | Proactive triggers (webhook, event) initiating agent runs. | P2 |

### 6.7 Skills, MCP & extensibility (FR-E)
| ID | Requirement | Priority |
|---|---|---|
| FR-E1 | **Skills**: `SKILL.md` + YAML frontmatter, progressive disclosure (name/desc in prompt, body on demand), `inline` vs `fork`. | P0 |
| FR-E2 | **MCP client**: stdio + Streamable-HTTP/SSE, OAuth for remote; tools/resources/instructions imported; allow-listed per agent. | P0 |
| FR-E3 | **Agentic tool discovery**: when tools are numerous, advertise name+hint; a `tool_search` tool loads full schemas on demand. | P0 |
| FR-E4 | **Plugins**: manifest + lifecycle hooks (session/prompt/model/tool/compaction/subagent), validated & rollback-safe loading. | P1 |
| FR-E5 | **Layered config** (defaults → global → project → env → runtime) with clear precedence; secrets separate. | P0 |
| FR-E6 | A **build/SDK** path for custom tools/agents (Python decorator API; footprint ladder). | P1 |

### 6.8 Providers & models (FR-P)
| ID | Requirement | Priority |
|---|---|---|
| FR-P1 | **Multi-provider** LLM support (OpenAI, Anthropic, Google, Azure, Bedrock, OpenRouter, local/Ollama, …) behind one interface. | P0 |
| FR-P2 | **Streaming**, tool-calling, structured output, token counting normalised across providers. | P0 |
| FR-P3 | **Failover** with failure classification + credential pools + rate-limit guards; per-task **model routing**. | P1 |
| FR-P4 | Embedding + rerank providers for memory/RAG. | P0 |

### 6.9 Safety, permissions & HITL (FR-X)
| ID | Requirement | Priority |
|---|---|---|
| FR-X1 | **Permission engine**: allow/ask/deny per tool/resource; last-match wins; `deny > ask > allow`; default ask. | P0 |
| FR-X2 | **Approval protocol** (bus-mediated, correlation-ID) surfaced identically on CLI/web/IM; modes plan/default/auto. | P0 |
| FR-X3 | **Two-level sandbox**: process/container isolation + per-command sandbox; network-off/read-only/least-cap defaults. | P0 |
| FR-X4 | **Path & egress restrictions** (workspace-only, deny `.git`/`.env`, SSRF-safe fetch). | P0 |
| FR-X5 | **Secrets**: separated from config, encrypted at rest, redacted in logs/telemetry. | P0 |
| FR-X6 | **Trust-gating**: untrusted IM input runs a constrained safe toolset; project features gated on trust. | P0 |

### 6.10 Observability & evaluation (FR-O)
| ID | Requirement | Priority |
|---|---|---|
| FR-O1 | **Tracing**: every run emits trace → observations (span/generation/tool/retriever/agent) → scores. | P0 |
| FR-O2 | **Cost/token accounting** per run/session/agent, incl. cache-read tokens. | P0 |
| FR-O3 | **OpenTelemetry** export + bundled **Langfuse** for traces/prompt-versions/evals/datasets. | P0/P1 |
| FR-O4 | **Evaluation**: datasets, experiments, LLM-as-judge + human annotation as scores. | P2 |
| FR-O5 | Admin **dashboards**: sessions, runs, cost, health, queue depth. | P1 |

### 6.11 Administration (FR-ADM)
| ID | Requirement | Priority |
|---|---|---|
| FR-ADM1 | Authn/authz for web/admin (local users + OAuth/OIDC) and machine API keys. | P0 |
| FR-ADM2 | RBAC roles (owner/admin/member/viewer) at v1 granularity. | P1 |
| FR-ADM3 | Agent management: create/configure agents (persona, **scope**, tools, **connectors**, model, permissions, memory) as scoped entities. | P0 |
| FR-ADM4 | Config, secrets, connection (MCP/provider) management UI + API. | P1 |

---

## 7. Non-functional requirements (NFR)

| ID | Category | Requirement |
|---|---|---|
| NFR-1 | **Portability** | Every component containerised; `docker compose up` brings up a working stack; single-node dev profile & scale-out profile. |
| NFR-2 | **Performance** | First token < 2 s p50 on a warm path; tool round-trip overhead < 150 ms p50 (excl. tool work); streaming end-to-end. |
| NFR-3 | **Scalability** | Stateless API + horizontally scalable workers over shared Postgres/Redis; one elected scheduler leader. |
| NFR-4 | **Reliability** | Crash-safe (durable admission, event sourcing); at-most-once scheduling; graceful shutdown/drain. |
| NFR-5 | **Security** | Sandbox by default; secrets encrypted; least privilege; SSRF protection; audit log of tool actions & approvals. |
| NFR-6 | **Observability** | 100% of runs traced; structured logs; health/readiness probes; metrics (Prometheus). |
| NFR-7 | **Extensibility** | New tool/skill/MCP server added without core changes or redeploy of the core image (hot-load where feasible). |
| NFR-8 | **Data** | Postgres for state/memory (+pgvector); Redis for queue/cache/lock/pubsub; object store (S3/MinIO) for blobs/artifacts. |
| NFR-9 | **i18n** | English + 简体中文 UI and prompts; UTF-8/CJK-safe search (trigram + vector). |
| NFR-10 | **Cost control** | Per-agent/session budgets; model routing; cost visible in UI; hard caps. |
| NFR-11 | **Licensing** | Permissive OSS (Apache-2.0); dependencies vetted for compatible licenses. |
| NFR-12 | **Testability** | Deterministic record/replay of provider calls; unit + integration + e2e; eval harness. |
| NFR-13 | **Privacy & retention** | Per-agent/session retention windows; PII redaction in traces/telemetry; right-to-erasure (event tombstone + projection rebuild + vector purge); documented data map. |

---

## 8. Deployment requirements
- **DR-1** One-command bring-up: `docker compose up -d` starts core API, worker(s), scheduler, web, Postgres(+pgvector), Redis, object store, and observability (Langfuse).
- **DR-2** Profiles: `dev` (minimal: core+worker+web+postgres+redis), `full` (adds Langfuse/ClickHouse, MinIO, IM adapters), `lite` (single container, SQLite, for local/CLI-only).
- **DR-3** Config via `.env` + mounted `config/`; secrets via env/Docker secrets; no secrets baked into images.
- **DR-4** First-run bootstrap (DB migrate, seed default agent, create admin) idempotent.
- **DR-5** Health/readiness endpoints for every service; graceful shutdown.

---

## 9. Scope

### 9.1 MVP (Milestone 1) — "the connected assistant that talks"
Core agent loop, provider layer (multi-provider), **connectors (email/calendar/docs via OAuth) + per-scope data isolation**, file/shell/web tools with sandbox + permissions (behind a pluggable execution environment), parallel execution, session persistence + search, long-term memory (blocks + hybrid store), **web app + one IM adapter (QQ/OneBot) + CLI (admin)**, skills, MCP client, agentic tool discovery, tracing (OTel + Langfuse), docker-compose (`dev`/`full`). **Local files/desktop deferred** behind the `LocalDaemon` backend.

### 9.2 In scope (later milestones)
Multi-agent orchestration polish, background jobs, scheduler UI, RAG/KB, memory consolidation, plugins/SDK, more IM adapters (WeCom/WeChat/Discord/Slack), more connectors, **local-file execution (`LocalDaemon`)**, RBAC, evals, desktop shell.

### 9.3 Out of scope (v1)
Visual no-code workflow builder; hosted multi-tenant SaaS + billing; model training; native mobile apps.

---

## 10. Success metrics (KPIs)
- **Adoption:** time-to-first-successful-run after `docker compose up` < **10 minutes**.
- **Reliability:** ≥ **99%** of scheduled jobs run at-most-once and complete or fail cleanly; **0** lost user turns on crash (durable admission).
- **Capability:** MVP passes an internal task-suite (file edit, test run, web research, multi-step) at ≥ **80%** success.
- **Safety:** **100%** of mutating tool calls pass the permission gate; **0** sandbox escapes in the security test-suite.
- **Observability:** **100%** of runs produce a complete trace with cost.
- **Extensibility:** a new tool via SDK and a new MCP server each added in < **30 minutes** by following docs.

---

## 11. Milestones / release plan

| Milestone | Theme | Key deliverables |
|---|---|---|
| **M0** | Foundations | Repo, docs (this PRD + architecture), skeleton services, CI, compose `dev`. |
| **M1 (MVP)** | Core that talks | Agent loop, providers, tools+sandbox+permissions, sessions+search, memory, CLI+web, QQ adapter, skills, MCP, tool discovery, tracing. |
| **M2** | Autonomy & scale | Scheduler + background jobs, worker scale-out, failover/routing, admin dashboards, RBAC, more IM adapters. |
| **M3** | Knowledge & quality | RAG/KB, memory consolidation, evals/datasets, plugin SDK, desktop shell. |
| **M4** | Hardening | Security review, performance, multi-tenant groundwork, docs & examples. |

---

## 12. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| **WeChat/QQ ToS & account bans** | Legal/operational | Treat personal-account bridges as best-effort/opt-in; prefer official/bot channels (OneBot, WeCom, Telegram); clear warnings & isolation. |
| **Prompt injection → tool abuse** (untrusted IM/web content) | Security | Constrained safe toolset for untrusted input; sandbox; permission gate; egress control; injection scanning of tool descriptions. |
| **Shell/code execution escapes** | Security | Two-level sandbox, network-off defaults, least-cap containers, path allow-lists; optional gVisor. |
| **Cost runaway** (loops, fan-out) | Cost | Bounded loops, shared budgets, hard caps, cost dashboards, alerts. |
| **Provider instability** | Reliability | Failover + credential pools + rate-limit guards + routing. |
| **Scope creep** (becomes a workflow platform) | Delivery | Firm non-goals; footprint-ladder governance. |
| **CJK search quality** | UX | Hybrid trigram + vector; rerank; tested on Chinese corpora. |
| **Docker resource footprint** (Langfuse/ClickHouse heavy) | Adoption | `lite`/`dev` profiles without heavy deps; observability optional. |

---

## 13. Open questions
1. Default embedding/rerank stack — hosted (OpenAI) vs local (bge/mxbai via a small inference container) as the out-of-box default?
2. Web app framework — React/Vite vs SvelteKit (see architecture ADR-0004).
3. Object store default — bundle MinIO always, or only in `full` profile?
4. Should the `lite` single-binary/SQLite mode be a first-class target or dev-only?
5. Do we ship a default local model (Ollama) container for zero-API-key first-run?

**Resolutions (from the design review):** Q1 → local `bge-m3` default (ADR-0007); Q2 → React + Vite (ADR-0004); Q3 → MinIO profile-gated (ADR-0008); Q4 → `lite` is first-class & CI-tested (ADR-0008); Q5 → Ollama via the `demo` overlay, off by default (ADR-0008). See [`DESIGN-REVIEW.md`](./DESIGN-REVIEW.md) §4 and [`ARCHITECTURE.md`](./ARCHITECTURE.md) §20.
