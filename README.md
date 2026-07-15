<!-- Keel — general-purpose AI agent platform -->
<h1 align="center">⛵ Keel</h1>
<p align="center"><em>One capable AI agent, everywhere you talk to it — CLI, web, and chat apps — self-hosted in Docker.</em></p>

---

**Keel** is a general-purpose, self-hostable **AI agent platform**. A single frontend-agnostic *agent core* is exposed through a **CLI**, a **web app**, and **IM gateways** (QQ, Telegram, WeCom…), equipped with a real toolbox (files, shell, web), parallel/async tool execution, multi-agent collaboration, durable memory, session persistence & search, scheduled autonomy, skills, MCP, agentic tool discovery, and first-class observability — deployable with a single `docker compose up`.

The design follows the project's own field manual, *How to Develop an AI Agent*: **a stable "keel" (the core runtime) with narrow seams onto which surfaces, tools, and providers bolt.**

## Status
🚧 **M3 — Knowledge & quality (in progress).** M0 is complete; the main M1 product
surfaces are usable; M2 autonomy foundations are partially complete; and the M3
Memory/Quality track (semantic recall, consolidation, deterministic evals) is complete.
The next execution slice is durable background jobs followed by RAG/Knowledge Base.
See the living [`docs/STATUS.md`](./docs/STATUS.md) snapshot.

## Documentation
| Doc | What |
|---|---|
| [`docs/PRD.md`](./docs/PRD.md) | Product Requirements — vision, personas, requirements, scope, milestones, risks |
| [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) | Technology selection & system design — C4 views, components, data model, protocol, deployment |
| [`docs/adr/`](./docs/adr) | Architecture Decision Records (language, datastore, runtime, frontend, sandbox, scheduler, embeddings, deployment, **product form**) |
| [`docs/DESIGN-REVIEW.md`](./docs/DESIGN-REVIEW.md) | Critical design review — gaps, risks, resolved open questions, invariant acceptance checklist |
| [`docs/IMPLEMENTATION-PLAN.md`](./docs/IMPLEMENTATION-PLAN.md) | Phased plan (M0–M4) — workstreams, sequencing, exit criteria |
| [`docs/STATUS.md`](./docs/STATUS.md) | Living implementation status — completed capabilities, milestone gaps, current execution order |
| [`docs/INVARIANTS.md`](./docs/INVARIANTS.md) | The ten non-negotiable invariant acceptance specs (merge-blocking gates) |
| [`docs/designs/`](./docs/designs) | Reviewed feature designs and product/UX specifications |
| [`docs/plans/`](./docs/plans) | Executable implementation plans and validation checklists |
| [`docs/diagrams/`](./docs/diagrams) | Mermaid architecture diagrams — C4, agent loop, sequences, ER, deployment |

## What Keel will do (highlights)
Primary form (see [ADR-0009](./docs/adr/0009-product-form-and-primary-use-cases.md)): a **server-side, connected conversational assistant** — for teams/IM and for personal use — where an *agent is a scoped entity* (a group agent and your personal agent are the same thing with different scope).
- **One core, many surfaces** — **web app + IM gateway** (QQ/Telegram/WeCom…) primary · CLI for admin/power-use
- **Connectors** — email · calendar · docs · knowledge via OAuth, **scoped per agent**, with per-scope data isolation
- **Real tools** — web fetch/scrape/search, file ops, `bash`/`powershell` — sandboxed, permissioned, parallel (execution environment is pluggable)
- **Multi-agent** — sub-agents as tools, shared budgets, isolation
- **Memory** — editable memory blocks + session persistence + hybrid (full-text + semantic) session search
- **Autonomy** — cron/interval scheduled tasks with at-most-once guarantees
- **Extensible** — Skills, MCP, agentic tool discovery, plugin SDK
- **Observable** — trace/observation/score, cost/token accounting (OpenTelemetry + Langfuse)
- **Deployable** — every component containerised; `docker compose up` for the whole stack

## Planned stack
Python 3.12 (asyncio) · FastAPI · LiteLLM · PostgreSQL + pgvector · Redis · arq · MinIO · React + Vite + Tailwind · OpenTelemetry + Langfuse · Docker Compose. See [ADRs](./docs/adr) for the rationale.

## Quickstart (planned)
```bash
git clone <repo> keel && cd keel
cp .env.example .env            # add provider API keys (or use the bundled Ollama for zero-key)
docker compose --profile full up -d
# open the web app, or:  keel "summarise today's changes in ./repo"
```

## License
Apache-2.0 (planned).
