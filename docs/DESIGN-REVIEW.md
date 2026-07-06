# Keel — Design Review

> **Status:** Draft v1.0 · **Date:** 2026-07-06 · **Reviews:** [`PRD.md`](./PRD.md), [`ARCHITECTURE.md`](./ARCHITECTURE.md), [`adr/`](./adr)
> Scope: a critical read of the product + architecture specs before implementation. It records what is strong, what is missing or under-specified, resolves the PRD's open questions, and checks the non-negotiable invariants. Findings feed [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) and two new ADRs (0007, 0008).

---

## 1. Verdict

The design is **coherent, buildable, and faithful to the field manual**. The narrow-waist core (one runtime, many surfaces), one-tool-interface, event-sourced state with durable admission, the two-loop runtime, the at-most-once scheduler, and the permission-engine + two-level sandbox are the right load-bearing choices, and each major technology decision is backed by an ADR with rejected alternatives.

**Recommendation:** proceed to **M0** after closing three open questions (below) and folding the medium-severity gaps in §3 into the backlog. None of the gaps invalidate the architecture; they are refinements that are cheaper to decide now than to retrofit (event-schema versioning and privacy/retention especially).

> **Addendum (post-pivot).** §1–§3 were written **before** [ADR-0009](./adr/0009-product-form-and-primary-use-cases.md) (server-primary connected assistant). That pivot added the largest capability and security surface in the design — the **Connectors** subsystem, **OAuth token** management, **per-scope data isolation**, and a new *headline* threat (**cross-scope exfiltration / confused-deputy**, ranked above sandbox escape) — none of which this review had critiqued. **§3A** reviews them and adds one invariant to **§5**. The verdict is unchanged (proceed to M0), but **G16 (scope-isolation enforcement) and G17 (confused-deputy) join G3/G8 as must-do-in-M1 design tasks.**

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

## 3A. Addendum — ADR-0009 pivot review (connectors · scope · confused-deputy)

The pivot to a **server-primary connected assistant** ([ADR-0009](./adr/0009-product-form-and-primary-use-cases.md)) landed after §1–§3 were written. It elevates personal-data **Connectors** over **OAuth**, makes **an agent a scoped entity**, and declares a new *headline* threat — **cross-scope data exfiltration / confused-deputy**, ranked *above* sandbox escape. None of that was in the original review. This addendum critiques it in the §3 format and on the same severity scale (**H** = decide before M1 code · **M** = design now / build when first needed · **L** = track).

| # | Area | Gap | Recommendation | Sev |
|---|---|---|---|---|
| G16 | **Scope-isolation enforcement point** | ADR-0009 makes per-scope isolation a headline goal but the architecture never names *where* it is enforced; ad-hoc `WHERE scope_id = ?` in each query is one forgotten filter away from a cross-scope leak. | Make `scope_id` a **mandatory column** on every scoped row (sessions, events, memory, connectors, tokens, passages) and route **all** data access through one **`ScopeGuard`** repository layer that injects the filter; add **Postgres RLS** as defense-in-depth; **deny-by-default** and **audit** every cross-scope access attempt. This is the enforcement home for the new §5 invariant. | **H** |
| G17 | **Confused-deputy: content-trust ≠ scope-trust** | The safe-toolset control (FR-X6 / G6) gates *untrusted surfaces* (group input). But a **trusted personal agent** holding connectors routinely ingests **untrusted content** — an email body, a fetched web page, a shared doc — the classic injection→exfiltration path the current model does not stop. | Separate **trust of the scope** from **trust of the content**: **taint-tag** tool/connector outputs (email, web, external docs) as untrusted and propagate the taint through context; **gate outbound & cross-connector actions** whose plan is influenced by tainted content (require approval / drop to the safe toolset). Ties to G6 injection scanning and G5 approvals. | **H** |
| G18 | **OAuth token lifecycle & scope binding** | Tokens are said to be envelope-encrypted and granted to a scope, but refresh failure, revocation, least-scope grants, and token fate on **scope deletion** are unspecified. | Store tokens encrypted (extends **G9**) keyed by `(scope_id, connector_id)`; request **least OAuth scopes**; **fail-closed** on refresh/revoke; on scope deletion **revoke + purge** tokens (ties to G4 erasure); audit every token use. | **M** |
| G19 | **Connector ↔ one-tool-interface (P3)** | A first-class Connectors subsystem risks a **parallel path** into the loop, violating P3 (one tool interface). | Keep connectors surfaced to the loop **as scoped tools** (as FR-N1 already states); the subsystem owns only **auth / lifecycle / scoping / provenance**, never a second tool contract. Connector tools carry **scope + taint** metadata. | **L** |
| G20 | **Outbound-action idempotency & audit** | FR-N4 requires approval + audit for outbound actions (email send, calendar invite, IM post), but **retry-safety** is unspecified — a worker retry could double-send. | Outbound connector actions carry an **idempotency key** (at-most-once send), are **always audited**, and are approval-gated by scoped policy; reconcile against provider message IDs. Mirrors the scheduler's at-most-once discipline. | **M** |

**Co-hosting caveat (ADR-0009 §6).** Hard per-scope isolation is precisely what lets a public group bot and a private personal agent share one instance. The **acceptance test for that claim** is the new §5 invariant below; until it is green, **recommend separate instances** for the most sensitive personal use (as ADR-0009 already advises).

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
| **Per-scope data isolation** [ADR-0009] | `state/` + `connectors/` via `ScopeGuard` (+ Postgres RLS) | Co-host a group agent and a personal agent in one instance/DB; assert the group agent **cannot** read the personal agent's connectors, tokens, memory, or sessions; every cross-scope attempt is **denied *and* audited**. Extends to the **confused-deputy** case: tainted content (email/web) cannot drive an unapproved outbound/cross-connector action (G17). |

---

## 6. Suggested doc changes (additive)

- Add **ADR-0007** and **ADR-0008**; extend the ADR index in `ARCHITECTURE.md §18` and the README ADR line. *(Done.)*
- Populate **`docs/diagrams/`**. *(Done.)*
- In a future pass, fold G3/G5/G8 into `ARCHITECTURE.md` (§8/§9/§13) once agreed, so the spec and this review converge.
- Fold the **§3A pivot gaps** — G16 (scope-guard), G17 (taint / confused-deputy), G18 (token lifecycle), G20 (outbound idempotency) — into `ARCHITECTURE.md` (§6.5 connectors, §8/§9 data model, §13 security), and thread the new invariant into `IMPLEMENTATION-PLAN.md`. *(Threaded into the plan — M0 seam + spike S5, M1 scope/connectors steps; the ARCHITECTURE fold-in is tracked for the next spec pass.)*

---

## 7. Summary

Green-light the architecture. Close Q1/Q4/Q5 (ADR-0007/0008), treat **G3, G5, G8** — and, from the **§3A pivot addendum, G16 (scope-isolation enforcement) and G17 (confused-deputy)** — as must-do-in-M1 design tasks (all expensive to retrofit), and schedule **G4, G6, G7, G9, G18, G20** into the milestone that first needs them. The §5 invariant checklist — now including **per-scope data isolation** — is the acceptance backbone for M0/M1.
