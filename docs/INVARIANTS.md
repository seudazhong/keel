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

**Current status:** proven. The low-level `run()` entry point requires a permission engine, production
construction paths pass an explicit policy, and allow-all remains available only through an explicit
trusted-CLI/test choice.

**Acceptance evidence:** the signature regression test rejects an omitted policy, Linux mypy checks
every package call site, and loop tests select their policy explicitly.

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

User-owned objects that are not Agent-owned use their explicit User/Organization partition instead:
Mailbox/mail/Notification rows require the authenticated User context, and ToDos/reminders require
both User and Organization context. Supplying an Agent scope can never substitute for either.

**Acceptance:** spoofed organization/Agent/scope headers, foreign run/session ids, and outbox pointer
scope mismatches fail closed and do not reveal foreign existence. Foreign `app.user_id` access and
User/Organization mismatches on mail, Notifications, and ToDos are denied under the runtime database
principal.

### C7 — Control-plane credentials never enter untrusted execution

Database, Redis, connector refresh, provider, and GitHub writeback credentials stay outside the
sandbox. A guest receives only an explicitly admitted short-lived capability, if any.

**Current status:** Compose sandbox and GitHub patch writeback satisfy this for file-only patch
generation. Future shell/opaque-agent execution must re-prove it.

### C8 — Durable configuration snapshot

Every run records the immutable Agent version, Routine policy, model, budget, grants/resources, and
effect policy used at admission. Later configuration changes do not rewrite an in-flight run.

**Current status:** durable runs persist a typed, schema-versioned `AgentConfigSnapshot`
(`keel_core.agent_config_snapshot`) at admission — the persisted Agent's optimistic version,
name, persona, selected model, `max_iterations`/`token_budget`, a permission-profile identifier,
the admitted tool-name set, a (currently minimal) memory-policy object, and non-secret active
resource-grant descriptors (type/id/capability only). The snapshot's canonical JSON + content
hash are folded into the admission fingerprint, so a retried admission whose Agent
version/persona/model/budget/snapshot changed is rejected as a conflict rather than silently
repaired (`keel_core.runs.admission_fingerprint`). The worker (`keel_worker.runs`) reconstructs a
claimed run's name/persona/model/bounded config from this frozen record — never from the Agent's
later-mutated fields — while still re-authorizing current Agent visibility/revocation at claim
time (a revocation denies execution; a persona/model edit does not rewrite an admitted run). A
snapshotted tool no longer available is dropped, never substituted (`AgentConfigSnapshot.
restrict_tools`); a legacy pre-R1B row (empty `snapshot_hash`) falls back to the prior
live-Agent-lookup behavior. Routine policy and a richer effect-policy object are not yet part of
the snapshot — the persisted Agent/Routine model this invariant ultimately depends on remains
extensible for later PRs.

### C9 — Mail receipt is content, not authority

A valid provider webhook proves that AgentMail delivered an event. It does not prove that the
address in `From` is an authenticated Keel User or that the sender may exercise the mailbox owner's
authority.

**Acceptance:** verify raw-body signatures before parsing, reject stale/replayed deliveries, keep
mail tainted, and prove that sender spoofing, forwarded instructions, links, and attachments cannot
directly authorize a connector, mail, ToDo, memory, or code effect.

### C10 — Automatic email is a structural exception

Per-message approval may be skipped only for a versioned template addressed to the active verified
delivery endpoint of the same User. The enforcement boundary accepts a template ID and typed
parameters, not caller/model-supplied recipients, HTML, attachments, or arbitrary body text.

**Acceptance:** mutate every recipient/content/attachment/reply field and assert the operation
requires a new exact approval; retry/crash tests produce one logical send or an `unknown` Effect
that reconciles before retry.

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

### Keel Mailboxes, notifications, and ToDos

- If a User has enabled Mail and has any active mailbox, exactly one is Primary; Purpose Mailboxes
  remain private to the same User and cannot become team resources through routing configuration.
- One User cannot discover, read, send from, or route through another User's Keel Mailbox.
- Mailbox provider and webhook credentials never enter model context, browser state, or sandbox
  execution.
- Inbound mail is durably deduplicated and remains untrusted even when its sender address matches the
  mailbox owner's verified delivery address.
- ToDos are owned by the authenticated User, not by a model-supplied owner, scope, Agent, or email
  sender.
- Active ToDo mutation tools are unavailable to untrusted email/IM contexts; those contexts can only
  create bounded proposals.
- Editing, completing, cancelling, or archiving a ToDo atomically cancels or replaces obsolete
  reminder occurrences.
- A templated notification to the verified user endpoint may send automatically; every other email
  requires approval bound to the exact draft.

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
