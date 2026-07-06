# Keel — Design Review

> **Status:** Draft v1.0 · **Date:** 2026-07-06 · **Reviews:** [`PRD.md`](./PRD.md), [`ARCHITECTURE.md`](./ARCHITECTURE.md), [`adr/`](./adr)
> Scope: a critical read of the product + architecture specs before implementation. It records what is strong, what is missing or under-specified, resolves the PRD's open questions, and checks the non-negotiable invariants. Findings feed [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) and two new ADRs (0007, 0008).

---

## 1. Verdict

The design is **coherent, buildable, and faithful to the field manual**. The narrow-waist core (one runtime, many surfaces), one-tool-interface, event-sourced state with durable admission, the two-loop runtime, the at-most-once scheduler, and the permission-engine + two-level sandbox are the right load-bearing choices, and each major technology decision is backed by an ADR with rejected alternatives.

**Recommendation:** proceed to **M0** after closing three open questions (below) and folding the medium-severity gaps in §3 into the backlog. None of the gaps invalidate the architecture; they are refinements that are cheaper to decide now than to retrofit (event-schema versioning and privacy/retention especially).

---

## 2. Strengths (kept, do not re-litigate)

- **P2 narrow waist** — `keel-core` as a pure library with surfaces as protocol clients is the single most important decision and is consistently applied (CLI/web/IM all thin).
- **P3 one tool interface + four gates** — built-ins, MCP, skills, and sub-agents presenting identically keeps the loop simple and the footprint ladder (P8) real.
- **Event sourcing + durable admission** — append-only `events` with projectors gives resume, replay, live streaming, and audit from one primitive; persist-before-first-model-call removes the classic "lost turn on crash" failure.
- **At-most-once scheduler** — advance-cursor-before-enqueue + leader lock is the correct, testable pattern.
- **Security posture** — fail-closed permissions (`deny > ask > allow`, default ask), two-level sandbox, SSRF-safe egress, import ≠ trust, trust-gated untrusted IM input.
- **Provider seam** — *borrow the plumbing (LiteLLM), own the policy (`ProviderGateway`)* keeps routing/failover/caching correctness in our code while avoiding undifferentiated adapter work.
- **Observability by construction** — trace → observation → score on every run, cost incl. cache-read tokens.

---

## 3. Gaps & recommendations

Severity: **H** = decide before M1 code, **M** = design now / build in the milestone that first needs it, **L** = track.

| # | Area | Gap | Recommendation | Sev |
|---|---|---|---|---|
| G1 | **Diagrams** | `docs/diagrams/` is referenced in the repo layout but empty; ARCHITECTURE only has ASCII. | Provide Mermaid C4 (context/container/component), agent-loop, key sequences, ER, and deployment. *(Done — see [`diagrams/`](./diagrams).)* | L |
| G2 | **Open questions** | PRD §13 Q1/Q4/Q5 are not actually decided anywhere. | Close via **ADR-0007** (embeddings/rerank) and **ADR-0008** (profiles, `lite`, ollama). See §4. | H |
| G3 | **Event schema evolution** | Event-sourced system with no event **version** or upcasting story; schema drift will break replay/projection rebuilds. | Add `events.version` (per `type`); an **upcaster registry** that migrates old payloads on read; projections rebuildable from v0. Contract-test old→new. | H |
| G4 | **Privacy / retention / erasure** | Memory + full session persistence store user content and PII; no retention, redaction, or right-to-erasure policy (self-host incl. EU). | Per-agent/session **retention** config; **PII redaction** in traces/telemetry (already redact secrets — extend); **erasure** via event tombstone + projection rebuild + vector purge; document a data map. | M |
| G5 | **Approval-bus reliability** | Approvals ride Redis pub/sub with a correlation ID; if no surface is subscribed (offline/crash) the request can hang. | Persist approvals as `events` (durable pending store); **TTL → fail-closed deny**; resume shows pending approvals; `auto` mode requires sandbox. | H |
| G6 | **Prompt-injection scanning** | PRD lists it as a risk mitigation, but the architecture never places the control. | Scan tool/skill/MCP **descriptions & imported instructions** at import/discovery time; quarantine on hit; keep untrusted input on the safe toolset. | M |
| G7 | **Cost-accounting truth** | Shared budget is tracked fast in Redis; authoritative provider cost arrives async — reconciliation/drift is unspecified. | **Reserve** in Redis pre-call → **reconcile** to Postgres/Langfuse post-run from normalized usage; expose both "reserved" and "settled" cost; hard-cap on reserved. | M |
| G8 | **Embedding dimension pinning** | pgvector columns are fixed-dim; switching embedding models (local ↔ hosted) silently breaks similarity. | Pin `(model_id, dim)` per collection; store model id on every `passage`/chunk; **refuse cross-model KNN**; re-embed job on model change. (Ties to ADR-0007.) | H |
| G9 | **Secrets at rest — key source** | "Encrypted at rest" is stated but the key source/KMS is undecided. | App-level **envelope encryption**; data-key wrapped by a master key from **env/Docker secret** (v1), pluggable to Vault/KMS; never in images; rotate procedure documented. | M |
| G10 | **Rate-limit design** | Redis rate limiting is named but not specified (scope/algorithm). | **Token-bucket** keyed by `{provider-cred}`, `{session}`, `{chat}`; provider-side guard reconciled with 429 headers; per-chat limits for IM. | L |
| G11 | **Gateway topology** | §12.3 mentions a standalone `keel-gateway`, but the container diagram hosts adapters in `keel-server`. | Pick one default (adapters hosted **in `keel-server`** for `full`; `keel-gateway` is an opt-in scale-out split) and make the diagrams agree. | L |
| G12 | **Cross-platform exec** | `powershell` built-in vs a Linux least-cap sandbox is acknowledged but under-specified. | v1: **`pwsh` inside the Linux sandbox**; a Windows-container executor is opt-in and documented; tool advertises availability per target. | L |
| G13 | **Multi-agent handoff safety** | `transfer_to_<agent>` can cycle; shared budget caps cost but not loops. | Cap **max handoffs** and detect A→B→A cycles in the delegation tree alongside `max_depth`. | L |
| G14 | **API/SDK versioning** | Generated OpenAPI SDK, but no version/compat policy. | `/v1` prefix; **additive-only** evolution; deprecation window; regenerate SDK in CI and diff. | L |
| G15 | **Backup / DR runbook** | No backup/restore or disaster-recovery guidance for Postgres/MinIO/event store. | M4 ops runbook: `pg_dump` + WAL, MinIO mirror, event-store as the replay source of truth; restore drill in e2e. | L |

---

## 4. Open questions — resolution

| PRD Q | Question | Resolution | Where |
|---|---|---|---|
| Q1 | Embedding/rerank default: hosted vs local? | **Local `bge-m3`** (multilingual, strong CJK) as the zero-key default via a small embed service; hosted (OpenAI `text-embedding-3`) routed through the gateway's `embed` slot; **rerank `bge-reranker-v2-m3` optional, off by default** (RRF suffices for MVP). | **ADR-0007** |
| Q2 | Web framework React vs SvelteKit? | **React + Vite** (already decided). | ADR-0004 |
| Q3 | Object store: MinIO always vs profile-gated? | **Profile-gated** — MinIO in `full`; a volume/FS in `dev`/`lite`. | ADR-0002 / **ADR-0008** |
| Q4 | Is `lite` (single-binary/SQLite) first-class or dev-only? | **First-class, CI-tested** target for CLI/offline/single-user — but explicitly **not** the scale target (in-process scheduler, reduced isolation; caveats documented). | **ADR-0008** |
| Q5 | Ship a default local model (Ollama) for zero-key first run? | **Optional**, shipped via a `demo`/`full` overlay as the documented **zero-key path**; **not on by default** in `dev` (keep the inner loop light). | **ADR-0008** |

---

## 5. Invariant acceptance checklist

The architecture names non-negotiable invariants. Each must have an owner component and an acceptance test *before* the milestone that relies on it ships.

| Invariant | Enforced in | Acceptance test (record/replay where possible) |
|---|---|---|
| Bounded loop, **named termination** | `loop/` | Force `max_iterations`/budget/interrupt/halt; assert `run.ended{reason}` for each. |
| **Persist-before-first-model-call** | `state/` admission | Kill the worker between admit and first call; on restart the run resumes with **0 lost turns**. |
| **Stop-reason-gated** tool execution | `loop/` | Replay a stream with `stop_reason != toolUse` + trailing tool JSON; assert no tool runs. |
| **Byte-stable prompt prefix** | `context/` | Snapshot prefix bytes across turns/agents; assert identical prefix → `prompt_cache_key` stable; memory never in prefix. |
| Parallel-safe executor, **deterministic order** | `tools/` | Mixed read/write calls with overlapping paths; assert writes serialize, results emit in source order. |
| **Two-level sandbox** | `keel-sandbox` + `permissions/` | Attempt egress/`.git`/`.env`/path-escape; assert deny; network-off by default. |
| **Shared budget** across delegation tree | `agents/` + Redis | Fan-out sub-agents; assert tree cost ≤ cap and children can't exceed the parent's remaining. |
| **Import ≠ trust** | `mcp/`, `skills/`, `discovery/` | Register a malicious MCP tool description; assert allow-list gate + injection scan (G6) quarantine. |
| **At-most-once** schedule | `keel-scheduler` | Crash mid-tick after cursor advance; assert the job runs **0 or 1** times, never twice. |

---

## 6. Suggested doc changes (additive)

- Add **ADR-0007** and **ADR-0008**; extend the ADR index in `ARCHITECTURE.md §18` and the README ADR line. *(Done.)*
- Populate **`docs/diagrams/`**. *(Done.)*
- In a future pass, fold G3/G5/G8 into `ARCHITECTURE.md` (§8/§9/§13) once agreed, so the spec and this review converge.

---

## 7. Summary

Green-light the architecture. Close Q1/Q4/Q5 (ADR-0007/0008), treat **G3, G5, G8** as must-do-in-M1 design tasks (they are expensive to retrofit), and schedule **G4, G6, G7, G9** into the milestone that first needs them. The invariant checklist in §5 is the acceptance backbone for M0/M1.
