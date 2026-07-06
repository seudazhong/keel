# Keel — Invariant Acceptance Specs

> **Source:** [`DESIGN-REVIEW.md`](./DESIGN-REVIEW.md) §5. These ten invariants are
> non-negotiable. Each is a **merge-blocking gate** for the milestone that
> introduces it ([`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) §5). This
> document is the spec M1 codes against; the executable registry is
> [`tests/invariants/test_invariants.py`](../tests/invariants/test_invariants.py).

**Status legend:** **proven** = acceptance test green in M0 (via a spike) · **M1** =
spec frozen here; the enforcement + its acceptance test land in M1.

| # | Invariant | Enforced in | Status | Acceptance test |
|---|---|---|---|---|
| I1 | Bounded loop, **named termination** | `keel_core/loop` | **proven (loop α)** | Force `max_iterations`/budget/interrupt/halt/error/completed; assert `run.ended{reason}` for each. |
| I2 | **Persist-before-first-model-call** | `keel_core/state` admission | **proven** | `admit` appends the user event before `run` calls the provider; with the durable `PostgresEventStore` a fresh store over the same DB replays the full log (0 lost turns). |
| I3 | **Stop-reason-gated** tool execution | `keel_core/loop` | **proven (loop α)** | A stream with `finish_reason != tool_use` + a trailing tool call runs **no tool**. |
| I4 | **Byte-stable prompt prefix** | `keel_core/context` | **proven (S1)** | Prefix bytes identical across turns/agents → stable `cache_key`; memory never in prefix. |
| I5 | Parallel-safe executor, **deterministic order** | `keel_core/tools` | **proven** | Mixed read/write calls with overlapping resources: writes serialize, independent reads run concurrently, results emit in source order. |
| I6 | **Two-level sandbox** | `keel_sandbox` + `permissions/` | **proven (S3, policy)** | Egress / `.git` / `.env` / path-escape denied; network off by default. |
| I7 | **Shared budget** across delegation tree | `keel_core/agents` | M1 | Fan-out sub-agents; tree cost ≤ cap; a child can't exceed the parent's remaining. |
| I8 | **Import ≠ trust** | `mcp/`, `skills/`, `discovery/` | M1 | Register a malicious tool description; assert allow-list gate + injection-scan quarantine. |
| I9 | **At-most-once** schedule | `keel_scheduler` | **proven (S2)** | Crash mid-tick after cursor advance; the job runs **0 or 1** times, never twice. |
| I10 | **Per-scope data isolation** | `keel_core/scope` + Postgres RLS | **proven (S5)** | Co-hosted group vs personal: cross-scope read **denied and audited**. |

## Notes per invariant

- **I1 — bounded loop / named termination.** *(proven — loop α)* Every run caps iterations, budgets tokens, and exits for exactly one named `StopReason` (`keel_core.types.StopReason`), emitted as `run.ended{reason}`. Test: [`tests/unit/test_loop.py`](../tests/unit/test_loop.py).
- **I2 — durable admission.** *(proven)* `keel_core.loop.admit` persists the user input as an event **before** `run` makes the first model call. With the durable `PostgresEventStore`, a fresh store over the same DB replays the full log — **0 lost turns**. Tests: [`tests/unit/test_loop.py`](../tests/unit/test_loop.py), [`tests/integration/test_state_postgres.py`](../tests/integration/test_state_postgres.py).
- **I3 — stop-reason gate.** *(proven — loop α)* Tools execute **only** when the provider's `finish_reason == tool_use`; a trailing tool call after any other finish reason must not trigger execution. Test: [`tests/unit/test_loop.py`](../tests/unit/test_loop.py).
- **I4 — byte-stable prefix.** *(proven — S1)* The cache-friendly prompt prefix depends only on stable inputs; volatile content (history, memory) lives in the suffix and never perturbs `prompt_cache_key`. Test: [`tests/unit/test_spike_s1_prompt_cache.py`](../tests/unit/test_spike_s1_prompt_cache.py).
- **I5 — deterministic parallel executor.** *(proven)* Independent read-only tools run concurrently; writes to overlapping resources serialize; results always emit in source order. Test: [`tests/unit/test_tools_executor.py`](../tests/unit/test_tools_executor.py).
- **I6 — two-level sandbox.** *(proven — S3, policy layer)* Network is off by default; egress to loopback/link-local/private/SSRF targets is denied; file access is workspace-only and blocks `.git`/`.env`/path-escape. Container enforcement wraps this in M1. Test: [`tests/unit/test_spike_s3_sandbox_policy.py`](../tests/unit/test_spike_s3_sandbox_policy.py).
- **I7 — shared budget.** A delegation tree shares one budget; children cannot exceed the parent's remaining allowance; depth and handoff cycles are capped (G13).
- **I8 — import ≠ trust.** MCP/skill/plugin descriptions and imported instructions are allow-listed and injection-scanned (G6) at import/discovery time; hits are quarantined.
- **I9 — at-most-once schedule.** *(proven — S2)* The cursor advances (atomic CAS claim) **before** enqueue, so a crash between claim and enqueue yields 0/1 runs and two leaders enqueue exactly once. Test: [`tests/unit/test_spike_s2_at_most_once.py`](../tests/unit/test_spike_s2_at_most_once.py).
- **I10 — per-scope isolation.** *(proven — S5)* Application-layer `ScopeGuard` denies + audits cross-scope access; Postgres RLS is defense-in-depth. Tests: [`tests/unit/test_spike_s5_scope_guard.py`](../tests/unit/test_spike_s5_scope_guard.py), [`tests/integration/test_spike_s5_rls.py`](../tests/integration/test_spike_s5_rls.py). **Confused-deputy extension (G17)** *(proven — WS-G)*: a `ConfusedDeputyEngine` taints external connector content and escalates any influenced **outbound** action to `ask`, so tainted input can never trigger an unapproved send; connector OAuth tokens are envelope-encrypted and scope-bound (`connector_tokens`, RLS). Tests: [`tests/unit/test_connectors.py`](../tests/unit/test_connectors.py), [`tests/integration/test_tokens_postgres.py`](../tests/integration/test_tokens_postgres.py).
