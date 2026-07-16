# Keel — Invariant Acceptance Specs

> **Source:** [`DESIGN-REVIEW.md`](./DESIGN-REVIEW.md) §5. These ten invariants are
> non-negotiable. Each is a **merge-blocking gate** for the milestone that
> introduces or hardens it. The original milestone mapping is preserved in the historical
> [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md); active gates are in
> [`ROADMAP.md`](./ROADMAP.md). This document is the acceptance spec; the executable registry is
> [`tests/invariants/test_invariants.py`](../tests/invariants/test_invariants.py).

> **Fidelity warning:** a green policy/spike test is not proof that the deployed topology
> satisfies the invariant. In particular, current shell execution is in-process (I6), and
> the runtime DB owner can bypass RLS (I10). See [`STATUS.md`](./STATUS.md).

**Status legend:** **proven** = acceptance test green in M0 (via a spike) · **M1** =
spec frozen here; the enforcement + its acceptance test land in M1.

| # | Invariant | Enforced in | Status | Acceptance test |
|---|---|---|---|---|
| I1 | Bounded loop, **named termination** | `keel_core/loop` | **proven (loop α)** | Force `max_iterations`/budget/interrupt/halt/error/completed; assert `run.ended{reason}` for each. |
| I2 | **Persist-before-first-model-call** | `keel_core/state` admission | **proven** | `admit` appends the user event before `run` calls the provider; with the durable `PostgresEventStore` a fresh store over the same DB replays the full log (0 lost turns). |
| I3 | **Stop-reason-gated** tool execution | `keel_core/loop` | **proven (loop α)** | A stream with `finish_reason != tool_use` + a trailing tool call runs **no tool**. |
| I4 | **Byte-stable prompt prefix** | `keel_core/context` | **proven (S1)** | Prefix bytes identical across turns/agents → stable `cache_key`; memory never in prefix. |
| I5 | Parallel-safe executor, **deterministic order** | `keel_core/tools` | **proven** | Mixed read/write calls with overlapping resources: writes serialize, independent reads run concurrently, results emit in source order. |
| I6 | **Two-level sandbox** | `keel_sandbox` + `permissions/` | **policy proven; deployment open** | Egress / `.git` / `.env` / path-escape denied; network off by default; execution must not occur in the API process. |
| I7 | **Shared budget** across delegation tree | `keel_core/agents` | M1 | Fan-out sub-agents; tree cost ≤ cap; a child can't exceed the parent's remaining. |
| I8 | **Import ≠ trust** | `mcp/`, `skills/`, `discovery/` | **proven** | Register a malicious tool description; assert allow-list gate + injection-scan quarantine. |
| I9 | **At-most-once** schedule | `keel_scheduler` | **proven (S2)** | Crash mid-tick after cursor advance; the job runs **0 or 1** times, never twice. |
| I10 | **Per-scope data isolation** | `keel_core/scope` + Postgres RLS | **query/RLS tests proven; owner-bypass open** | Co-hosted group vs personal: cross-scope read is denied and audited using a non-bypass runtime role. |

## Notes per invariant

- **I1 — bounded loop / named termination.** *(proven — loop α)* Every run caps iterations, budgets tokens, and exits for exactly one named `StopReason` (`keel_core.types.StopReason`), emitted as `run.ended{reason}`. Test: [`tests/unit/test_loop.py`](../tests/unit/test_loop.py).
- **I2 — durable admission.** *(proven)* `keel_core.loop.admit` persists the user input as an event **before** `run` makes the first model call. With the durable `PostgresEventStore`, a fresh store over the same DB replays the full log — **0 lost turns**. Tests: [`tests/unit/test_loop.py`](../tests/unit/test_loop.py), [`tests/integration/test_state_postgres.py`](../tests/integration/test_state_postgres.py).
- **I3 — stop-reason gate.** *(proven — loop α)* Tools execute **only** when the provider's `finish_reason == tool_use`; a trailing tool call after any other finish reason must not trigger execution. Test: [`tests/unit/test_loop.py`](../tests/unit/test_loop.py).
- **I4 — byte-stable prefix.** *(proven — S1)* The cache-friendly prompt prefix depends only on stable inputs; volatile content (history, memory) lives in the suffix and never perturbs `prompt_cache_key`. Test: [`tests/unit/test_spike_s1_prompt_cache.py`](../tests/unit/test_spike_s1_prompt_cache.py).
- **I5 — deterministic parallel executor.** *(proven)* Independent read-only tools run concurrently; writes to overlapping resources serialize; results always emit in source order. Test: [`tests/unit/test_tools_executor.py`](../tests/unit/test_tools_executor.py).
- **I6 — two-level sandbox.** *(policy layer proven; deployed topology open)* Policy tests
  cover loopback/link-local/private/SSRF and workspace path restrictions, but current
  `ShellTool` execution remains in the server/CLI process. M3.3 must add the isolated
  executor and escape/egress acceptance evidence. Test:
  [`tests/unit/test_spike_s3_sandbox_policy.py`](../tests/unit/test_spike_s3_sandbox_policy.py).
- **I7 — shared budget.** A delegation tree shares one budget; children cannot exceed the parent's remaining allowance; depth and handoff cycles are capped (G13).
- **I8 — import ≠ trust.** *(proven — WS-G)* MCP/skill/plugin descriptions and imported instructions are allow-listed and injection-scanned (G6) at import/discovery time; hits are quarantined, never registered. Skills use progressive disclosure (descriptions for discovery; instructions only on activation). Tests: [`tests/unit/test_extensibility.py`](../tests/unit/test_extensibility.py), gate in [`tests/invariants/test_invariants.py`](../tests/invariants/test_invariants.py).
- **I9 — at-most-once schedule.** *(proven — S2)* The cursor advances (atomic CAS claim) **before** enqueue, so a crash between claim and enqueue yields 0/1 runs and two leaders enqueue exactly once. Promoted to a durable service by the `PostgresClaimStore` (CAS on `schedules.next_run_at`) + `due_tick`. Tests: [`tests/unit/test_spike_s2_at_most_once.py`](../tests/unit/test_spike_s2_at_most_once.py), [`tests/unit/test_scheduler_store.py`](../tests/unit/test_scheduler_store.py), [`tests/integration/test_scheduler_approvals_postgres.py`](../tests/integration/test_scheduler_approvals_postgres.py).
- **I10 — per-scope isolation.** *(query/RLS tests proven; deployed owner-bypass open)*
  Application filters and RLS tests deny cross-scope access, but the current runtime DB role
  owns the schema and can bypass RLS. M3.3 requires a non-bypass runtime role and audited
  adversarial proof. Tests: [`tests/unit/test_spike_s5_scope_guard.py`](../tests/unit/test_spike_s5_scope_guard.py),
  [`tests/integration/test_spike_s5_rls.py`](../tests/integration/test_spike_s5_rls.py).
  The confused-deputy extension taints connector content and escalates influenced outbound
  actions to approval; durable outbound idempotency is still open.

## Beyond the ten (M2 slice)

- **G5 — durable cross-surface approval.** *(proven — autonomy slice v1)* An approval raised by an **unattended** (scheduled) run is a durable `approvals` row; the run **suspends to the event log** (the checkpoint) and later **resumes** on grant/deny/expire, executing the gated outbound action **exactly once** and failing **closed** on timeout — surviving process death. Tests: [`tests/unit/test_loop_suspend_resume.py`](../tests/unit/test_loop_suspend_resume.py), [`tests/unit/test_worker_tasks.py`](../tests/unit/test_worker_tasks.py), [`tests/integration/test_scheduler_approvals_postgres.py`](../tests/integration/test_scheduler_approvals_postgres.py); gate `test_g5_durable_approval_survives_a_fresh_process` in [`tests/invariants/test_invariants.py`](../tests/invariants/test_invariants.py). Design: [`designs/2026-07-07-keel-autonomy-slice-design.md`](designs/2026-07-07-keel-autonomy-slice-design.md).
