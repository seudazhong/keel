# Managed projects and GitHub synchronization

> **Status:** Living subsystem reference
> **Product maturity:** Project/import UI preview; review backend headless; patch backend not exposed

Projects are organization-owned code resources. Users and Agents receive explicit capabilities;
they are not storage owners.

## Model

| Entity | Purpose |
|---|---|
| Project | Organization-owned managed codebase. |
| Project worktree | Disposable run-bound checkout/materialization. |
| Project run | Association between a durable run and Project. |
| Repo sync entry | Idempotent import/fetch/webhook synchronization ledger. |
| Project quota | Organization storage/worktree limits. |
| GitHub installation/repository | GitHub App binding and repository inventory. |

## Storage

Current deployments use `KEEL_PROJECT_STORAGE_ROOT` as shared POSIX storage mounted by both server
and worker.

It contains active repository state, disposable worktrees, review reports, and patch artifacts.
Metadata and authority live in PostgreSQL.

Important boundaries:

- the sandbox never receives writable authoritative repository storage;
- worktrees are disposable;
- server and worker must see the same storage root;
- multi-host deployment requires a shared RWX-compatible backend;
- object storage is a future artifact/backup layer, not the current active Git store.

## Authorization and isolation

Project operations use actor membership plus explicit Agent resource grants:

| Capability | Examples |
|---|---|
| read | list, inspect, status, reports |
| use | associate runs, review, materialize work |
| write | create/import/update/sync |
| manage | delete/purge/grants/installations |

Tenant tables carry `org_id`, composite foreign keys prevent cross-organization associations, and
RLS provides defense in depth.

## GitHub App

The integration:

- stores App configuration and secret references, not installation tokens;
- mints short-lived installation tokens just in time;
- validates API/clone hosts and rejects private/loopback/link-local addresses;
- verifies webhook HMAC and delivery replay;
- binds installations and repositories to one organization;
- keeps GitHub credentials out of the sandbox.

The current self-hosted setup is operator-heavy because users may need to configure an App and
secret reference. The product target is:

1. deployment-level App registration once;
2. user clicks **Connect GitHub**;
3. GitHub installation returns and binds automatically;
4. Projects selects from authorized repositories.

Normal users should not handle App IDs, PEM paths, webhook secrets, or installation identifiers.

## API and UI

`/v1/projects` provides:

- create/import/list/detail/update/archive/delete;
- sync request/status;
- worktree list/materialize/reclaim;
- run associations;
- grants;
- GitHub installations and webhook.

The React Projects page supports create/import/list/detail/delete preview flows. It does not expose
the complete review or patch lifecycle.

## Read-only review

Review is a durable API/worker flow over an exact change set. Reports are immutable and
evidence-checked. See [Read-only code review](./CODE-REVIEW.md).

No React review surface ships today.

## Controlled patches

The worker-side patch pipeline exists and can generate an immutable file-only candidate, request
approval, and write back a Draft PR through trusted GitHub credentials. See
[Controlled patch proposals](./PATCHES.md).

There is no public Patch API, SDK, or UI.

## Current limitations

- GitHub setup is not a polished self-service flow.
- Active Git/artifact storage is single shared filesystem infrastructure.
- Project quota/product administration is incomplete.
- Read-only review is headless.
- Patch generation cannot run shell/build/test.
- General coding-agent adapters and per-run command sandboxes are future work.
