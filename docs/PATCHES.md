# Controlled patch proposals

> **Status:** Living subsystem reference  
> **Product maturity:** backend foundation; no public API, SDK, or React surface

Keel's patch subsystem creates a bounded, immutable code-change candidate and requires human
approval before a trusted GitHub writeback can create a branch and Draft PR.

## Implemented

- schema:
  - `0019_patch_proposals`
  - `0021_patch_proposal_outbox`
  - `0022_patch_generation_requests`
- proposal state machine, optimistic versions, expiry, budgets, and cost accounting;
- atomic proposal + durable generation-request admission;
- global proposal outbox with fenced claims and reconciliation;
- `patch.generate` and `patch.writeback` durable job contracts;
- a real loop-driven file-editing author using per-run sandbox namespaces;
- authenticated snapshot upload/export/delete between project storage and sandbox;
- immutable patch bundle/artifact handling;
- approval bound to the candidate bundle hash;
- GitHub writeback with re-authorization, base checks, idempotent ledger, branch creation, and
  Draft PR only;
- worker capability registration and recovery for stranded generation/writeback.

## Lifecycle

```text
request
  -> generating
  -> ready
  -> approval_pending
  -> approved
  -> writing
  -> completed

terminal alternatives: failed | rejected | cancelled | expired
```

The proposal outbox is the global cross-scope pointer used by the worker reconciler. Per-Agent
run/job/approval stores remain scope-bound. A pointer whose scope does not match the proposal's
canonical `(org, Agent)` scope fails closed.

## Trust properties

- Project and Agent authority are rechecked at generation and writeback.
- The sandbox never receives a GitHub installation token.
- The authoritative repository is not mounted writable into the sandbox.
- Approval identifies the immutable candidate bundle, not a mutable worktree.
- Writeback may create only the controlled branch and Draft PR; it cannot merge, force-push, or
  update the default branch.
- A worker without GitHub capability does not claim `patch.writeback`.

## Current limitations

- no server Patch router;
- no typed SDK methods;
- no React proposal/diff/approval surface;
- patch capability is enabled in worker settings by default even though it is not product-exposed;
- the author can use isolated file tools but shell/build/test execution is disabled;
- therefore a generated candidate is not evidence that tests passed;
- Compose uses one shared sandbox service with directory namespaces, not one process-isolated
  executor per patch run.

## Productization gates

The active plan is [Roadmap R3](./ROADMAP.md#r3--controlled-code-workflows):

1. public API/SDK and UI;
2. durable transcript/evidence;
3. exact-revision review and approval UX;
4. truthful validation state;
5. separate patch worker capability pool;
6. Draft PR end-to-end acceptance;
7. stronger per-run command sandbox before any build/test or general coding-agent claim.
