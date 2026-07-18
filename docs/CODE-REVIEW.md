# Read-only Code Review (MVP, WS-R)

Keel's read-only, managed-code **review** capability: given an authorized project change set
(a branch, a single commit, or a pull request), it materializes an **isolated, disposable**
worktree from the control-plane-owned Git storage, reviews the change through the shared
provider seam, **verifies every finding's evidence** against the reviewed diff, and stores an
immutable, content-addressed JSON + Markdown report. It reuses the existing project/GitHub
integration, coding storage, durable runs/jobs, identity/authz, provider path, and
observability — no new database migration.

Code: `keel_core.review` (`models`, `diff`, `prompts`, `engine`, `evidence`, `report`,
`service`, `coordinator`, `jobs`, `audit`, `errors`); REST API `keel_server.api.reviews`;
durable job adapter `keel_worker.review`. It stores state in the existing `runs`,
`project_runs`, and coding artifact/worktree stores (no `0018` migration).

## MVP boundaries — what this is, and is NOT

**This phase reviews. It does not change anything.**

* ✅ Reviews a branch/commit/PR diff and produces findings with cited, verified evidence.
* ✅ Stores immutable JSON + Markdown report artifacts (content-addressed, retained).
* ✅ Surfaces status/report/artifacts over a typed REST API + SDK.
* ❌ **No** file writes, edits, or patches — the review agent is given **no tools at all**.
* ❌ **No** `git push`, branch creation, or any remote write.
* ❌ **No** GitHub PR comments, reviews, checks, or any outbound GitHub write.
* ❌ **No** shell, secrets, deploy tools, or outbound connectors.

**A human must validate every finding before acting on it.** Findings are advisory model
output; even verified findings only guarantee the cited file/line/snippet exists in the
reviewed change — not that the issue is real.

## How it works

1. **Request** (`POST /v1/projects/{id}/reviews`, control plane): the actor/Agent is
   authorized for read+run (the `use` capability) on the project. A durable **run**
   (`surface="review"`, idempotent by `Idempotency-Key`) is created and associated to the
   project (`project_runs`); a restart-safe `review.run` **job** is enqueued (duplicate =
   no-op via the job idempotency key).
2. **Materialize**: the worker claims the run, re-authorizes (revocation fails closed), renews
   the run **lease** throughout execution (a lost lease aborts all effects without a terminal
   write — contention is never a terminal success), and materializes an isolated clone of the
   change set — **no remote, no alternates**, never a writable mount of the authoritative
   repository.
2b. **Resolve refs (exact SHAs)**: the change set is resolved to **exact commit SHAs**, never a
   symbolic ref used loosely. A `pull_request` number is resolved on the **control plane** via
   the GitHub App to its exact base/head SHAs and bound repository (installation + project
   binding verified; base repo must match the project's repo), with the JIT token kept out of
   the sandbox/worktree/logs — a PR **number is never used as a Git ref**. A `branch` review with
   an omitted base derives the **merge-base** against the project default branch (the branch's
   own changes). A `commit` review defaults to the commit's first parent. If GitHub is
   unavailable for a PR, resolution **fails explicitly**.
3. **Diff**: a **bounded** `base..head` unified diff is computed inside the worktree. Oversized
   diffs fail closed; large data is never silently truncated into the model.
4. **Review**: the diff is sent through the shared `ProviderGateway` (the *same* provider path
   the agent loop uses — no second, policy-bypassing route) as **untrusted data**, under an
   **authorized model** (from the configured allowlist — never an arbitrary caller string) and an
   **always-enforced, never-unlimited budget** (token budget, per-turn output cap, cost ceiling,
   and a bounded provider-attempt count that includes the single structured-output repair). The
   system prompt states that repository files, commit messages, and PR text are data to review,
   never instructions. Structured JSON output requires an exact `summary`/`findings`/
   `limitations` shape; an empty object / refusal / missing summary is a contract failure with a
   single bounded repair; a provider transport/timeout/rate-limit failure is **retryable** (the
   run is not terminalized until a permanent error or attempts are exhausted).
5. **Verify evidence**: every finding's `file_path` must exist and belong to the reviewed scope,
   its `line_start..line_end` must be a **small, bounded span** that overlaps the reviewed diff
   (computed arithmetically — never by materializing the range), and its `snippet` must appear at
   the **exact cited file and line window** (not anywhere globally, and never in a *different*
   file). Fabricated files/snippets are **rejected**; moved or out-of-scope evidence is
   **downgraded** (kept at low confidence, annotated).
6. **Store**: the report is rendered to canonical JSON + Markdown and stored as immutable,
   content-addressed artifacts under the **shared** project coding storage with an **explicit
   retention TTL** (never indefinite). The run's `result_ref` points at the JSON report's content
   hash. The worktree is always disposed (idempotently).
7. **Read** (`GET .../reviews`, `.../{review_id}`, `.../report`, `.../report.md`): read-only,
   read-authorized status and report access. Status/report routes require the review to be
   **associated with the route's project** (`project_runs`) — a review of another project is a
   404. Pending/failed/list projections are truthful after restart because the immutable request
   metadata (source/base/head/model/project) is durably recorded on the run's event log; a
   projection never fabricates default values.

## Security properties

* **Read-only by construction** — the review agent has **no tools**, so it cannot read, write,
  or execute anything; it only reasons over the provided diff. This is stronger than a
  deny-listed toolset.
* **Prompt-injection resistant** — repository/PR/commit content is fenced, untrusted data;
  system instructions explicitly forbid repo content from changing policy or granting tools.
  An injected "enable the shell tool" in a README cannot enable a tool that does not exist.
* **Token isolation** — the GitHub App JIT installation token used to fetch PR metadata/diff
  lives entirely on the control plane; it is never handed to the review service, the worktree,
  or the model.
* **No source/secret leakage** — reports carry only reviewed diff content and findings, never a
  token, a raw provider log, or a system prompt. Audit details forbid sensitive keys.
* **Fail closed** — authorization, evidence verification, provider-contract violations, and
  bound violations raise rather than degrade silently.

## Shared storage & deployment

Review artifacts are written by the **worker** and read by the **server** report APIs, so both
processes MUST resolve the **same** project/coding storage root. Set `KEEL_PROJECT_STORAGE_ROOT`
to a shared volume path mounted identically into both:

* **Docker Compose** mounts a named `projectdata` volume at `/var/lib/keel/projects` into both
  `keel-server` and `keel-worker`.
* **Kubernetes / multi-host**: back that path with an **RWX** volume (e.g. NFS/EFS/Azure Files)
  so the server and worker pods share it, *or* run an external Git service integration. There is
  **no** container-local `.keel/projects` default in cloud: when the root is unset (or unwritable)
  outside a local/dev environment, startup fails closed and managed projects + review are
  disabled (readiness reflects it) rather than silently splitting server/worker storage. A
  container-local default is accepted only for single-host local development.

## Model & budget policy

The review model comes from an **authorized allowlist** (`KEEL_REVIEW_MODEL_ALLOWLIST`, with
`KEEL_DEFAULT_MODEL` always permitted), never an arbitrary caller string — a request for an
un-allowlisted model is rejected (`422`). Every review runs under an **explicit, never-unlimited
budget**: `KEEL_REVIEW_TOKEN_BUDGET`, `KEEL_REVIEW_OUTPUT_MAX_TOKENS` (a hard per-turn output
cap passed to the provider), `KEEL_REVIEW_COST_CEILING_USD`, and `KEEL_REVIEW_MAX_PROVIDER_ATTEMPTS`
(total provider turns including the repair). Token/cost usage is propagated onto the durable run.

## Durability, idempotency, and lifecycle

* **Idempotent** — a duplicate request (same `Idempotency-Key`) returns the same run and
  re-drives dispatch (enqueue is idempotent and runs on every request, so a lost enqueue can
  never strand the run); a retried job on an already-terminal run is a no-op; re-running a
  reclaimed review re-produces identical content-addressed artifacts.
* **Restart-safe & retryable** — the worker claims/reconciles/retries the `review.run` job on the
  existing jobs substrate; the run lease is heartbeated during execution and a crash reclaims it
  and starts clean. Transient provider failures (transport/timeout/rate limit) retry; a permanent
  error or authorization revocation terminalizes safely.
* **Explicit retention & erasure** — report artifacts are stored `retained` with a concrete
  `retained_until` TTL (`KEEL_REVIEW_REPORT_RETENTION_DAYS`), never indefinitely. Review worktrees
  and report artifacts live under the shared project coding storage, so project/scope erasure
  purges them via the coding-artifact cleaner wired into the erasure coordinator (idempotently);
  run metadata is purged with the `runs` table.

## Known limitations (this MVP)

* **PR ref fetch.** PR resolution obtains exact base/head SHAs and verifies the installation +
  project binding on the control plane (token isolated). The optional `ensure_refs` hook that
  performs a token-authenticated fetch of PR refs into the authoritative repo is a seam left for
  the deployment to wire; when a resolved SHA is not yet present in the materialized repo the
  review fails explicitly (a PR number is never used as a Git ref regardless).
* Findings are advisory model output. Even a verified finding only proves the cited
  file/line/snippet exists in the reviewed change — a human validates whether the issue is real.

