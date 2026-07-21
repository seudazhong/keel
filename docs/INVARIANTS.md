# Keel invariant acceptance specifications

Invariants are correctness and safety properties, not feature descriptions. A green unit test may
prove only one layer; deployment and product acceptance are listed separately.

The original executable registry remains in
[`tests/invariants/test_invariants.py`](../tests/invariants/test_invariants.py).

## Existing invariant registry

| ID | Invariant | Current status | Acceptance evidence |
|---|---|---|---|
| I1 | Bounded loop with named termination | **Proven** | Force completion, budget, iteration, interrupt, halt, and error exits; each records one named reason. |
| I2 | Persist before first model call | **Proven** | User input/event is committed before provider invocation and survives a fresh store/process. |
| I3 | Stop-reason-gated tools | **Proven** | A trailing tool call with a non-tool finish reason executes nothing. |
| I4 | Byte-stable prompt prefix | **Proven** | Stable inputs produce identical prefix bytes/cache key; mutable history and memory remain outside it. |
| I5 | Parallel-safe deterministic tool order | **Proven** | Conflicting writes serialize; independent reads may overlap; transcript results retain source order. |
| I6 | Two-level execution boundary | **Partial** | Path/egress policy and deployed authenticated out-of-process file execution are proven. Shell remains disabled because per-run command isolation is not proven. |
| I7 | Shared budget across a delegation tree | **Open** | The executable registry intentionally skips it. Child runs must not ship before immutable inherited authority and one shared budget exist. |
| I8 | Import is not trust | **Proven at import layer** | Unlisted or injection-bearing MCP/skill metadata is rejected or quarantined. |
| I9 | Legacy schedule occurrence executes zero or one times | **Proven, insufficient for product Routines** | Cursor claim prevents duplicate enqueue, but a crash after claim may lose the occurrence. See correction gate C3. |
| I10 | Per-scope data isolation | **Infrastructure proven; product authority partial** | Non-owner RLS, scope guards, and cross-scope tests pass. Full two-user/Agent/resource authorization journeys remain a release gate. |

## Corrections and additional platform gates

These gates capture design corrections discovered after the original ten invariants.

### C1 — Explicit policy, never implicit allow-all

Every executable tool path supplies a permission engine. Omitting policy must fail construction or
deny, never substitute an allow-all engine.

**Current gap:** the low-level loop defaults to allow-all when `permissions` is omitted, even though
the main interactive paths build explicit policies.

**Acceptance:** enumerate every runtime construction path; omit policy; assert no tool can execute.

### C2 — Authority intersection

An operation is allowed only by the intersection of:

```text
actor membership
  x Agent access
  x Agent authority
  x Routine policy
  x resource grant
  x deployment capability
```

RLS/scope equality is defense in depth and never sufficient authority.

**Acceptance:** two users, private Agents, a team Agent, explicit Agent-access edges, private/team
Connections, Knowledge, and Projects; every unauthorized Agent discovery, session read, or
resource use/manage attempt is denied without existence disclosure.

### C3 — No lost accepted Routine occurrence

Once a due occurrence is durably accepted, it is eventually terminal or explicitly cancelled.
Duplicate delivery may not duplicate the occurrence.

**Current gap:** I9's advance-before-enqueue contract permits a crash to lose the occurrence.

**Acceptance:** crash at every transaction/enqueue boundary; after recovery there is exactly one
occurrence row and zero or one active owner, never silent disappearance.

### C4 — Ambiguous effects reconcile before retry

An external mutation that may have succeeded before a timeout enters `unknown`, not `failed`.
Retries are blocked until provider reconciliation proves whether the effect exists.

**Acceptance:** inject success-before-response-loss for email/calendar/comment/PR effects; recovery
produces one external effect and one confirmed local record.

### C5 — Approval binds an exact effect

An approval covers an immutable action hash or candidate revision plus actor, Agent, resource,
budget/policy context, and expiry. Any changed input requires a new approval.

**Current status:** controlled patch writeback implements the strongest form through a bundle hash.
Connector effects use durable approvals/idempotency but do not yet share one generic effect record.

### C6 — Scope is partition, not authority

`scope_id` is derived from organization and Agent and used for storage isolation. Clients cannot
gain access by naming a scope, and product APIs authorize resources explicitly.

**Acceptance:** spoofed organization/Agent/scope headers, foreign run/session ids, and outbox pointer
scope mismatches fail closed and do not reveal foreign existence.

### C7 — Control-plane credentials never enter untrusted execution

Database, Redis, connector refresh, provider, and GitHub writeback credentials stay outside the
sandbox. A guest receives only an explicitly admitted short-lived capability, if any.

**Current status:** Compose sandbox and GitHub patch writeback satisfy this for file-only patch
generation. Future shell/opaque-agent execution must re-prove it.

### C8 — Durable configuration snapshot

Every run records the immutable Agent version, Routine policy, model, budget, grants/resources, and
effect policy used at admission. Later configuration changes do not rewrite an in-flight run.

**Current status:** durable runs capture several admission fields, but the persisted Agent/Routine
model is not yet complete enough to satisfy the full invariant.

## Domain-specific acceptance

### Memory

- Model-learned core/profile changes are proposal-first with provenance; direct mutation tools are
  disabled for normal product Agents.
- Untrusted IM/content cannot write durable memory.
- Erased source content cannot be resurrected through consolidation or rebuild.

### Knowledge

- Every result includes source identity and citation.
- External Knowledge remains tainted.
- Embedding model/dimension mismatch is rejected.
- Delete hides content before asynchronous physical purge and rebuild cannot resurrect it.

### Connections and effects

- Credentials are encrypted, scoped, refresh/revoke failures are explicit, and secret values never
  enter metadata/logs.
- Webhooks authenticate before replay claim and route to exactly one bound scope.
- An Agent/Routine can use only selected and granted provider resources.

### Projects, review, and patches

- Worktrees never expose writable authoritative Git state.
- Review findings cite bounded evidence in the reviewed diff.
- Patch approval binds the immutable candidate.
- Trusted writeback can create only the controlled branch and Draft PR.
- Shell/build/test claims require actual qualified execution evidence.

## Merge and release policy

- A code-level invariant blocks the change that introduces or weakens it.
- A deployment invariant blocks enabling the capability in that trust profile.
- A product invariant blocks the release claim until a real end-to-end scenario passes.
- [Status](./STATUS.md) records what is proven now; [Roadmap](./ROADMAP.md) owns the remaining gate.
