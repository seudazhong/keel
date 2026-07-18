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
2. **Materialize**: the worker claims the run, re-authorizes (revocation fails closed), and
   materializes an isolated clone of the change set — **no remote, no alternates**, never a
   writable mount of the authoritative repository.
3. **Diff**: a **bounded** `base..head` unified diff is computed inside the worktree. Oversized
   diffs fail closed; large data is never silently truncated into the model.
4. **Review**: the diff is sent through the shared `ProviderGateway` (the *same* provider path
   the agent loop uses — no second, policy-bypassing route) as **untrusted data**. The system
   prompt states that repository files, commit messages, and PR text are data to review, never
   instructions, and that nothing in the diff can grant a tool, change the output contract, or
   alter policy. Structured JSON output is validated with a bounded repair loop; a
   provider/tool failure or unrepairable malformed response fails closed.
5. **Verify evidence**: every finding's `file_path` must exist and its `line_start..line_end`
   must belong to the reviewed diff, and its `snippet` must appear in the diff or file.
   Fabricated files/snippets are **rejected**; real files/lines outside the reviewed diff are
   **downgraded** (kept at low confidence, annotated). This is the enforcement home of "no
   invented file/line evidence".
6. **Store**: the report is rendered to canonical JSON + Markdown and stored as immutable,
   content-addressed artifacts under the project's coding storage. The run's `result_ref`
   points at the JSON report's content hash. The worktree is always disposed (idempotently).
7. **Read** (`GET .../reviews`, `.../{review_id}`, `.../report`, `.../report.md`): read-only,
   read-authorized status and report access. No route performs a write.

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

## Durability, idempotency, and lifecycle

* **Idempotent** — a duplicate request (same `Idempotency-Key`) returns the same run; a retried
  job on an already-terminal run is a no-op; re-running a reclaimed review re-produces identical
  content-addressed artifacts.
* **Restart-safe** — the worker claims/reconciles/retries the `review.run` job on the existing
  jobs substrate; a crash mid-review reclaims the run's lease and starts clean.
* **Erasure** — review worktrees and report artifacts live under the project's coding storage
  handle, so project/scope erasure purges them via the existing coding-storage purge; the run
  metadata is purged with the `runs` table. The worktree reaper/purge is idempotent.

## Observability

Traces/audit cover request → start → complete/fail with org/project/run/review and
source/base/head/model, plus cost/tokens accrued onto the durable run. Audit sinks refuse
sensitive detail keys.

## Known limitations (this MVP)

* **Local diff is fully wired; PR fetch is scoped.** Branch and single-commit reviews compute a
  bounded `base..head` diff entirely from the authoritative repo materialized into an isolated
  worktree. `pull_request` is accepted and validated as a source, but resolving a PR to its
  base/head SHAs still requires those refs to be present in the materialized repo — the
  control-plane GitHub client does not yet expose PR metadata/diff fetch. The token-isolation
  and control-plane-only boundaries are already in place for when that fetch lands; the review
  service and worktree never receive the JIT App token regardless.
* Findings are advisory model output. Even a verified finding only proves the cited
  file/line/snippet exists in the reviewed change — a human validates whether the issue is real.

