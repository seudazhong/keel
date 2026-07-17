# Managed Projects & GitHub Synchronization (M3.7)

Keel's durable managed-project foundation: org-owned **projects** with control-plane-owned
Git storage, run-scoped **worktrees**, a **repo sync ledger**, per-org **quotas**, and a
**GitHub App** binding (installations, repositories, webhook deliveries, sync state). It reuses
the identity actor/authorization model and the generic **resource grants** for project Agent
authority rather than duplicating a bespoke authority table.

Code: `keel_core.projects` (`models`, `store`, `service`, `storage`, `audit`, `jobs`, and
`github/` — `auth`, `client`, `webhooks`, `urls`); REST API `keel_server.api.projects`;
durable job adapter `keel_worker.projects`. Schema: migration `0015_projects_github`.

## Model

* **Project** — an org-owned managed project (`prj_…`). Blank/local or GitHub-sourced,
  archivable / soft-deletable, with a `default_branch`, `visibility` (`private`/`internal`),
  optimistic `version`, and a durable **active Git storage handle** owned by the control plane.
* **Project worktree** — an ephemeral, run-scoped materialized worktree (`pwt_…`) bound to a
  durable run id. Materialized as an isolated clone with no remote/alternates, so the sandbox
  never receives writable access to the authoritative repository.
* **Project run** — the project↔durable-run association (`prn_…`); a run belongs to exactly
  one project.
* **Repo sync ledger** — an append-only record (`syn_…`) of every import / fetch /
  webhook-driven sync, for durable idempotent reconciliation and audit.
* **Project quota** — per-org limits (max projects / active worktrees / repository bytes).
* **GitHub installation** — the App installation↔org binding (`ghi_…`). Global (like
  `oidc_identities`) so the HMAC-authenticated webhook path can resolve a delivery's
  installation to its org before any tenant context exists. A live installation binds to
  exactly one org.
* **GitHub repository** — a repository visible through an installation (`ghr_…`), optionally
  linked to a project. Pinned to its installation's org by a composite FK.
* **GitHub sync state** — a per-repository durable sync cursor (`ghs_…`).
* **Webhook delivery** — one row per `X-GitHub-Delivery` id (global); the primary key makes a
  replayed delivery a durable no-op.

## Authorization

Project operations compose the identity principal / resource / capability model
(`keel_core.identity.authz.AuthorizationService`). Over a project (via the acting user's org
capabilities):

| Capability | Operations |
| ---------- | ---------- |
| `read`   | list / get / status / list runs / list grants |
| `use`    | materialize/reclaim a worktree, associate a run |
| `write`  | create / import / update / request sync |
| `manage` | archive / delete / purge / grant / manage installations |

An Agent driven by an actor never exceeds the **intersection** of the actor's org
capabilities and the Agent's explicit `resource_grants` on the project (`resource_type =
"project"`), a confused-deputy defense. Grants reuse the generic identity grant model.

## Isolation & confinement

* Tenant-owned tables (`projects`, `project_worktrees`, `project_runs`, `repo_sync_ledger`,
  `project_quotas`, `github_repositories`, `github_sync_state`) carry an `app.org_id` RLS
  policy + `FORCE ROW LEVEL SECURITY`, keyed by the operating org (ADR-0009).
* Composite foreign keys `(project_id, org_id) -> projects(id, org_id)` make a cross-org
  project/worktree/run/repository link structurally impossible.
* Repositories reference `github_installations(installation_id, org_id)` so a repo can never
  bind to an installation in a different org, and a webhook payload naming a repo id owned by a
  different installation/org is ignored (never processed).
* GitHub tokens are **control-plane only**: minted just-in-time, cached briefly in-process,
  never persisted and never logged, and never handed to the sandbox. Clone/fetch use safe
  argument arrays over an allow-listed, normalized HTTPS URL (no credentials in the URL).

## GitHub App

* Config: `KEEL_GITHUB_APP_ID`, `KEEL_GITHUB_PRIVATE_KEY_REF` (a reference — `env:NAME`,
  `file:PATH`, or a path — never the PEM inline), `KEEL_GITHUB_WEBHOOK_SECRET`,
  `KEEL_GITHUB_API_BASE_URL`, `KEEL_GITHUB_WEB_BASE_URL`, `KEEL_GITHUB_ALLOWED_HOSTS`,
  `KEEL_GITHUB_TOKEN_CACHE_SECONDS`. The integration is disabled unless `KEEL_GITHUB_APP_ID` is
  set.
* Authentication: a short-lived RS256 **App JWT** signs installation-token requests;
  least-scope **installation access tokens** are minted just in time and briefly cached.
* Webhooks: `POST /v1/projects/github/webhook` verifies `X-Hub-Signature-256` (HMAC over the
  raw body) in constant time, enforces delivery-id **replay protection** (durable ledger PK),
  an **event allowlist**, and the installation→org binding, before durable idempotent
  processing. It never uses the actor/org header path.
* SSRF defense: every clone/API URL is normalized and checked against
  `KEEL_GITHUB_ALLOWED_HOSTS`; loopback / private / link-local / reserved addresses,
  credential-bearing URLs, non-HTTPS schemes, and non-default ports are rejected, and redirects
  are never followed.
* This phase performs **no** remote write / push / PR creation.

## Durable sync

A GitHub push (or a manual sync request) enqueues one durable `projects.sync` job keyed by
`(project, delivery)` on the existing jobs/outbox substrate. The job is restart-safe and
idempotent: the sync ledger's unique `delivery_id` collapses a replayed or concurrent sync to a
no-op, and a crash mid-fetch retries.

## API

Authenticated `/v1/projects` (actor + `X-Keel-Org`): list / create / import / detail / update /
archive / delete; `sync` (request + status); `worktrees` (list / materialize / reclaim);
`runs` (list / associate); `grants` (list / create / revoke); `github/installations` (list /
link). The GitHub webhook endpoint is authenticated independently by HMAC. Typed SDK models and
methods are in `keel_sdk`.

## Lifecycle

Organization erasure (`keel_erase_organization`) purges all project + GitHub tables with the
org. A project's `purge` reclaims its worktrees and removes the authoritative repository handle.

## Limitations

* Private-repository clone/fetch that requires embedding an installation token in the transport
  is out of scope for this foundation slice (clone/fetch use a credential-free allow-listed
  URL); credentialed private fetch is a follow-up.
* No remote write / push / PR creation yet (metadata + read + sync only).
* Worker registration of the `projects.sync` job (`keel_worker.projects.register_project_jobs`)
  is wired where the worker constructs its `ProjectService`; the server always enqueues durably
  so the dispatcher recovers pending syncs.
