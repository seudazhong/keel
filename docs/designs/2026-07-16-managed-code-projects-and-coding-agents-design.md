# Managed Code Projects & Pluggable Coding Agents — Historical Reference Architecture

> **Historical notice:** many prerequisites described here are now implemented, while other
> assumptions have been refined by
> [`../adr/0011-product-boundary-and-domain-model.md`](../adr/0011-product-boundary-and-domain-model.md).
> Use [`../PROJECTS.md`](../PROJECTS.md), [`../PATCHES.md`](../PATCHES.md),
> [`../ARCHITECTURE.md`](../ARCHITECTURE.md), and [`../ROADMAP.md`](../ROADMAP.md) for current
> truth and sequencing.

> **Status:** Draft for architecture review (no product code changed)
> **Date:** 2026-07-16
> **Author role:** Senior cloud / platform / security architect
> **Milestone alignment:** Hard-gated behind **M3.3 (Cloud Safety)** and **M3.6 (Multi-user
> Identity + durable run topology)**; net-new surface delivered as a post-M3.6 vertical
> ("Coding Projects") that reuses M3.3 isolation. **Not** an MVP feature.
> **Related canon:**
> [`ARCHITECTURE.md`](../ARCHITECTURE.md) §6.4/§10/§13,
> [`ROADMAP.md`](../ROADMAP.md) M3.3/M3.6/M3.8,
> [`STATUS.md`](../STATUS.md),
> [`INVARIANTS.md`](../INVARIANTS.md) I6/I10,
> [`DESIGN-REVIEW.md`](../DESIGN-REVIEW.md) G16–G20,
> [`adr/0005-sandbox.md`](../adr/0005-sandbox.md),
> [`adr/0009-product-form-and-primary-use-cases.md`](../adr/0009-product-form-and-primary-use-cases.md),
> [`adr/0002-datastore.md`](../adr/0002-datastore.md),
> [`adr/0006-scheduler-and-queue.md`](../adr/0006-scheduler-and-queue.md).

---

## 0. Executive summary & decisions

Keel today is a **server-primary connected assistant** with a mature agent/data engine but
three load-bearing gaps for this feature: shell/file tools run **in-process** (I6 open,
[`tools/shell.py`](../../packages/keel-core/src/keel_core/tools/shell.py):1–56,
[`tools/files.py`](../../packages/keel-core/src/keel_core/tools/files.py)); the runtime DB
role can **bypass RLS** (I10 open, [`STATUS.md`](../STATUS.md) blocker #1); and there is **no
identity/Agents/scope** model beyond the hard-coded `web:local` scope
([`STATUS.md`](../STATUS.md) blocker #4). ADR-0005's two-level sandbox and dedicated
`keel-sandbox` are **decided but unimplemented** ([`keel-sandbox/policy.py`](../../packages/keel-sandbox/src/keel_sandbox/policy.py)
is pure policy, no container). ADR-0009 ranks **cross-scope data exfiltration above sandbox
escape**.

Adding *managed code projects* (import a Git repo, give an agent a writable working tree, run
builds/tests) and *pluggable coding agents* (Copilot CLI, Claude Code, OSS agents) is
**exactly the workload ADR-0005 was written for** — untrusted code + untrusted model actions +
network + secrets — so it **must not ship** on the in-process executor. It is the forcing
function that finally implements I6.

### 0.1 Recommended reference architecture (decisive)

1. **Model coding work as org-owned resources plus explicit principal grants.** Storage
   ownership is `org → project → worktree → run`; **users and Agents are principals**, not
   ownership ancestors, and receive project capabilities such as `read`, `write`, and `run`.
   UI navigation may enter a project from an Agent, but authorization still evaluates that
   principal against the org-owned project. A **run** is bounded and worker-owned (I1/I2).
2. **Keep authoritative Git state separate from working volumes.** Mutable active bare
   repositories live on durable POSIX/block storage with locking and atomic ref semantics, or
   behind a Git-smart service that provides them. **MinIO/S3 stores immutable bundles,
   snapshots, backups, and artifacts**, not a directly mutated bare repository. A worktree is
   an ephemeral per-run checkout; the sandbox never receives writable access to the
   authoritative repository.
3. **Execution goes through a real two-level sandbox** (finally implementing I6): a
   **control plane** (server/worker: admission, policy, secrets, orchestration — *never*
   executes untrusted code) and an **execution plane** (`keel-sandbox`: ephemeral, rootless,
   read-only base, writable workspace tmpfs/overlay, seccomp/AppArmor, dropped caps,
   cgroup quotas, **egress default-deny via an allow-list proxy**). Rootless OCI is acceptable
   for the trusted single-org M3.3 preview; hostile multi-tenant rollout is gated on an
   acceptance-tested stronger backend such as gVisor, Kata, or a microVM.
4. **Keel-mediated tools use an `ExecutionEnvironment`** (ARCHITECTURE.md §6.4; ADR-0009) and fail closed; the
   existing file/shell tools become clients of `SandboxContainer`, while `InProcess` is
   quarantined to `lite`/trusted-dev and forbidden in server/worker builds. An arbitrary vendor
   CLI can also issue direct guest syscalls: those are constrained by the sandbox's run-level
   filesystem/process/network boundary, not falsely represented as individually mediated.
5. **Coding agents are driven by an internal `CodingAgentAdapter` contract.** MCP is an
   optional tool-interoperability seam, not the adapter protocol. Keel owns the sandbox,
   approved Git write-back, budgets, audit, and the controls it can actually enforce; the vendor agent owns
   planning and model calls. A public plugin SDK is later roadmap work.
6. **Three credential strategies, kept strictly separate:** platform-owned LLM credentials,
   BYO LLM API keys, and user OAuth/device-login into third-party agents. Prefer a
   control-plane provider proxy; if a vendor CLI requires guest injection, use only
   short-lived narrow credentials and acknowledge that they have crossed the untrusted
   boundary. We do **not** assume hosted vendor-agent use is permitted; §7.5 is a legal gate.

### 0.2 Anti-patterns rejected up front

- ❌ **Running coding agents / builds in the server or worker process** (violates I6, ADR-0005).
- ❌ **One long-lived shared build container** for multiple tenants (cross-tenant residue).
- ❌ **Mounting the host Docker socket** into the sandbox for "nested Docker" (host takeover).
- ❌ **Network-on-by-default** so `npm/pip/go` "just work" (supply-chain + SSRF + exfil).
- ❌ **Storing repos only as working trees** (no authoritative Git repository; corruption = data loss).
- ❌ **A second tool path** for the coding agent that bypasses the permission/scope/taint gates
  (violates P3/G19/G17).
- ❌ **Claiming opaque vendor CLIs expose every syscall for per-action approval.** In opaque
  mode, enforce run-level filesystem/network limits, inspect the final diff, and gate remote
  writes; use controlled-tool mode when per-action policy is required.
- ❌ **Automatically pushing a successful run.** Remote branch creation and draft-PR creation
  require explicit human approval and trusted control-plane execution.
- ❌ **Treating "the agent is trusted" as "its inputs are trusted"** — repo contents, issues,
  PR descriptions, and dependency READMEs are **tainted** (G17).
- ❌ **Blocking on unverified vendor ToS assumptions.** We design the seam; we gate the
  provider behind a documented legal check.

---

## 1. Goals, non-goals, prerequisites

**Goals.** Safely (a) import/sync a Git repository as a *managed project*, (b) give an
authorized Agent an isolated writable **worktree**, (c) run builds/tests/tools in a sandbox with
default-deny egress, (d) let **pluggable coding agents** operate inside that sandbox under a
uniform lifecycle (approvals, cancellation, checkpoints, observability), and (e) do all of this
with hard multi-tenant isolation where **cross-scope exfiltration is the top threat**.

**Non-goals (v1).** A hosted multi-org SaaS control panel; a general remote-code-execution
service; guaranteeing arbitrary vendor agents are license-compatible; a browser IDE; GPU build
farms; Windows-container executors (deferred; `pwsh`-in-Linux per G12).

**Hard prerequisites (do not start the feature before these land):**

| Prereq | Invariant / blocker | Milestone |
|---|---|---|
| Real isolated execution backend (no in-process shell) | I6 | **M3.3** |
| Non-owner runtime DB role + enforced RLS | I10 | **M3.3** |
| Explicit fail-closed permission defaults everywhere | §13 / blocker #8 | **M3.3** |
| Hashed/scoped machine credentials; no implicit admin | blocker #5 | **M3.3** |
| Durable connector OAuth state + authenticated webhooks | G18 / blocker #6 | **M3.3** |
| Users/org/RBAC + persisted Agents + grants; no `web:local` | blocker #4 | **M3.6** |
| Worker-owned durable interactive runs | blocker #3 | **M3.6** |

Coding projects are a **post-M3.6** vertical. Delivering them earlier would require exposing
untrusted code execution on a preview stack the roadmap explicitly forbids exposing to
untrusted networks ([`ROADMAP.md`](../ROADMAP.md) execution policy).

---

## 2. Domain & identity model (item 1)

### 2.1 Entity hierarchy and scope

Extend the ADR-0009 scope model. A `ScopeId` today is a string like `web:local`,
`user:ivy`, `chat:qq:12345` ([`types.py`](../../packages/keel-core/src/keel_core/types.py),
[`scope.py`](../../packages/keel-core/src/keel_core/scope.py)). Add org-owned **project
resources** and evaluate access using principal/resource/capability tuples:

```
org:acme                      (tenant boundary — hard isolation unit)
└─ project:acme/web           (org-owned managed Git repository)
   └─ worktree:<uuid>         (ephemeral writable checkout for one run/branch)
      └─ run:<uuid>           (bounded, event-sourced execution; worker-owned)

principals: user:ivy, agent:<uuid>
grants:     (principal, project:acme/web, capability=read|write|run|admin)
```

- **`org`** is the top-level tenant and the **hard isolation unit** (§2.5). Single-org-v1
  (M3.6) has one org; the schema is org-ready from day one so multi-org SaaS (post-M3.8) is
  additive, not a migration.
- **`project`** is owned by an org and has its own resource identity
  (`project:<org>/<slug>`). Users and Agents receive explicit capabilities; neither principal
  becomes the project's storage owner. An Agent-centric UI may link to its granted projects,
  but this does not change the database hierarchy.
- **`worktree`** and **`run`** are ephemeral children; their `scope_id` inherits the project's
  and carries the run's `content_taint` (repo contents are tainted, G17).

**Authorization requires a new layer.** The current
`DefaultScopeGuard.enforce(actor_scope, resource_scope)` is an equality/system check
([`scope.py`](../../packages/keel-core/src/keel_core/scope.py):31–39); it is not a hierarchical
grant evaluator and must not be treated as one. Project work requires a fail-closed
**principal/resource/capability authorization service** that resolves org membership and
explicit grants, plus Postgres RLS as defense in depth (I10). `scope_id`/`org_id` remain
mandatory isolation attributes on every new table (G16).
Platform services act as explicit service principals with narrowly scoped capabilities and
RLS-compatible database roles.

### 2.2 New tables (Postgres; extends ARCHITECTURE §9.1)

```sql
orgs(id, name, created_at, deleted_at)
org_members(org_id, user_id, role /* owner|admin|member|viewer */, created_at)

projects(
  id, org_id, scope_id UNIQUE, slug, display_name,
  default_branch, visibility /* private|team */,
  active_git_ref /* pointer to durable Git volume/service */,
  latest_snapshot_ref /* immutable bundle/backup in object storage */,
  source /* github|generic_git|blank */,
  import_source_ref /* github repo id/url, nullable */,
  quota_id, created_at, archived_at, deleted_at
)
project_grants(project_id, principal_type /* user|agent */, principal_id,
               capability /* read|write|run|admin */, granted_by, created_at)

worktrees(
  id, project_id, scope_id, run_id, branch, base_commit,
  volume_ref /* scratch volume/subvol handle */, status /* provisioning|ready|reclaimed */,
  bytes_used, created_at, reclaimed_at
)

coderuns(                    -- specialization of the existing run/session concept
  id, project_id, worktree_id, scope_id, agent_id, session_id,
  adapter /* copilot-cli|claude-code|oss:<name> */,
  status, stop_reason, budget_id, sandbox_instance_id,
  started_at, ended_at
)

repo_syncs(project_id, direction /* import|fetch|approved_write_back */, ref, sha,
           actor_principal_id, approval_id, ts, result)
artifacts(id, coderun_id, scope_id, kind /* log|diff|build|test-report */,
          object_ref, sha256, bytes, retention_class, created_at)
project_quotas(id, max_worktrees, max_bytes, max_run_seconds,
               max_concurrent_runs, egress_bytes_cap, cpu_millis, mem_bytes)
project_secrets(project_id, scope_id, key, secret_ref /* envelope-encrypted, G9 */, created_at)
```

`active_git_ref`, `latest_snapshot_ref`, `volume_ref`, `object_ref`, and `secret_ref` are
**indirection handles**, never raw bytes/paths in rows (matches the connectors "secret ref"
pattern, ARCHITECTURE §9.1).

### 2.3 Git object storage vs working volumes (the key split)

Two physically distinct stores with different durability and lifecycle:

| Layer | Contents | Backing store | Durability | Lifecycle |
|---|---|---|---|---|
| **Active Git repository** | Mutable bare repository: objects, refs, packfiles | Durable POSIX filesystem/block volume with repository locking + atomic ref updates, or a Git-smart service | **Authoritative active state**, backed up and replicated | Lives with the project; serialized maintenance/GC |
| **Git snapshots/backups** | Immutable bundles/snapshots of active Git state | **MinIO/S3** | Durable recovery copy, not directly mutated | Versioned/retained by policy |
| **Working volume (worktree)** | One writable checkout + build outputs (`node_modules`, `target/`, `.venv`) | Ephemeral scratch (overlayfs on tmpfs, or a thin LVM/btrfs/ZFS subvol, or a Firecracker block device) | **Disposable** | Created per run; **reclaimed on run end**; hard TTL |

Rationale: working trees are big, hot, and disposable; Git objects are small, cold, and
precious. Never conflate them. A corrupted or wiped worktree is a no-op recovery
(materialize a new checkout from the active repository); the **remote + active repository**
are the recovery sources, with immutable object-store snapshots for DR
(mirrors ARCHITECTURE §8.3 "event store is the replay source of truth" and G15's
"event-store-as-truth" DR philosophy).

**Recommended provisioning:** the active repository is fetched into a per-project repository
on a durable Git volume, with repository-level locking and atomic ref transactions, or managed
by a Git-smart service. The control plane materializes a checkout or immutable snapshot into a
per-run worktree. The authoritative repository is never mounted writable into a sandbox.
Worktrees may use copy-on-write subvolumes/overlays off a cached checkout to provision quickly.

### 2.4 Quotas, lifecycle, backup, deletion, encryption

- **Quotas** (`project_quotas`, enforced by the control plane before provisioning and by
  cgroups/filesystem quotas at runtime, §3.4): max worktrees, max bytes, max concurrent runs,
  wall-clock per run, egress bytes, CPU-millis, memory. Denials **fail closed** with an
  audited reason. Quota checks reuse the shared-budget discipline (I7) — a project run cannot
  exceed the org's remaining allowance.
- **Lifecycle:** `project.archived` (read-only, active repository/snapshots retained,
  worktrees reclaimed) →
  `project.deleted` (soft, `deleted_at`) → **purge job** (M3.5 erasure discipline): remove
  active repository, immutable mirror bundles, all worktree volumes, artifacts,
  `project_secrets` (revoked + purged),
  coderun events tombstoned, project-specific PAT/vendor credentials revoked, and the project
  binding to its GitHub App installation removed (there is no durable installation access
  token to purge; ties G4/G18).
  Purge is **idempotent and observable** (matches durable jobs, ARCHITECTURE §11).
- **Backup/DR:** immutable Git bundles/snapshots + artifact objects replicated/versioned in
  object storage; `pg_dump` + WAL for metadata (G15). For imported projects, the remote is an
  additional recovery source. Locally created or unpushed refs depend on active-repository
  replication and snapshot RPO; surface that risk to users and **never auto-push on success**.
- **Encryption:** at rest — object store SSE + volume encryption (LUKS/provider KMS);
  `project_secrets` use the existing **envelope encryption** (per-record data key wrapped by a
  master key from env/Docker secret, pluggable to Vault/KMS, G9). In transit — mTLS between
  control plane and sandbox RPC. Secrets are **redacted in logs/telemetry** (G9) and never
  written into worktree files by the platform.
- **Isolation:** every row carries `scope_id`/`org_id`; the new principal/resource/capability
  authorizer plus RLS deny cross-project reads and audit them (I10/G16). Worktree volumes are
  **never shared** across scopes or runs.

### 2.5 Tenancy isolation invariants (new, extend INVARIANTS.md)

- **T1 — Project confinement:** a run for `project:A` can read/write only `project:A`'s
  worktree; no path, mount, or handle reaches another project/org. (Acceptance: adversarial
  run attempts cross-project path/mount access → denied + audited.)
- **T2 — Worktree ephemerality:** a worktree is bound to exactly one run and is reclaimed
  (unmounted + wiped) at run end; a second run never observes a prior run's residue.
- **T3 — Git authority:** wiping all worktrees loses no commit already ingested into the
  durable active repository and no retained proposal bundle. Remote refs and immutable backups
  provide additional recovery; snapshot RPO is explicit.
- **T4 — Egress confinement:** a run reaches only the allow-listed egress set (§3.5); default
  is deny; loopback/link-local/private/metadata are never reachable (extends `EgressPolicy`,
  [`keel-sandbox/policy.py`](../../packages/keel-sandbox/src/keel_sandbox/policy.py):33–55).

---

## 3. Sandbox execution architecture (items 3 & 4)

Implements ADR-0005 / I6 for real. This is the section the whole feature depends on.

### 3.1 Control plane vs execution plane (hard split)

```
┌───────────────────────── CONTROL PLANE (trusted; never runs untrusted code) ─────────────────────────┐
│ keel-server: admission, authz, project/grant CRUD, OAuth, webhook verify, quota gate               │
│ keel-worker: run orchestration, budget (I7), approvals (G5), secrets broker, approved Git write-back│
│ keel-scheduler: leader-elected sync jobs (fetch/snapshot), serialized GC/repack, worktree reclaim   │
└───────────────────────────────────────────────┬────────────────────────────────────────────────────┘
                        exec RPC (mTLS, scoped, capability-token)   │  results/events (SSE→event store)
                                                                    ▼
┌──────────────────────── EXECUTION PLANE (untrusted; keel-sandbox ×N) ────────────────────────────────┐
│ Sandbox host/agent ──▶ ephemeral microVM/container per run:                                          │
│   • read-only base image  • writable /workspace (worktree volume)  • tmpfs /tmp                      │
│   • rootless / non-root uid, no-new-privileges  • dropped caps  • seccomp+AppArmor                   │
│   • cgroup v2 CPU/mem/pids/io limits  • no host Docker socket                                         │
│   • egress via sidecar allow-list proxy (default deny)  • no inbound                                  │
│   • coding-agent guest process runs HERE, mediated by the sandbox agent                              │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

The control plane **dispatches** work over an RPC (matching ARCHITECTURE §3.2 "worker → exec
RPC → keel-sandbox") and **receives normalized events**; it never shares an address space or a
mount namespace with untrusted code (I6 acceptance: "execution must not occur in the API
process").

### 3.2 Ephemeral compute: staged isolation backends

- **Default (single-org preview, M3.3):** **rootless OCI containers** (podman/containerd
  rootless, or Docker with userns-remap) — one **ephemeral container per run**. Lower ops
  cost, adequate for a single trusted org.
- **Hostile multi-tenant tier (post-M3.6):** rollout is gated on a stronger isolation backend
  such as **gVisor, Kata Containers, or a microVM** (including Firecracker where operationally
  appropriate), selected through the execution-backend seam and validated by acceptance
  evidence. Arbitrary attacker-influenced build code must not launch across mutually untrusted
  tenants on the M3.3 shared-kernel preview backend.
- **Never:** a shared long-lived executor, or `InProcess` on the server/worker (see §4).

Provisioning target: cold start ≤ a few seconds via a **pre-warmed pool**, CoW rootfs, and
backend-appropriate snapshots. One run = one fresh instance = **no cross-run residue** (T2).

### 3.3 Filesystem layout inside the sandbox

```
/            read-only base image (toolchains: git, node, python, go, build-essential)
/workspace   read-write  ← the worktree volume (the ONLY durable-ish writable area)
/tmp         read-write tmpfs (size-capped, wiped on exit)
/home/agent  read-write tmpfs (agent scratch, config)
/secrets     read-only tmpfs, injected just-in-time (§3.6), never on the base image
```

Base image is **read-only** (`--read-only` / immutable rootfs); all writes go to the worktree
volume or tmpfs. Toolchains are baked into the base image so runs need **no install-time
egress** for the common path.

### 3.4 Least privilege: caps, seccomp, AppArmor, cgroups, timeouts

- **User:** non-root uid inside a **user namespace** (rootless); `--no-new-privileges`.
- **Capabilities:** drop **all**, add back none by default (a build needs none). Explicitly
  **no** `CAP_SYS_ADMIN`, `CAP_NET_ADMIN`, `CAP_SYS_PTRACE`.
- **seccomp:** default-deny profile; allow the syscall set builds/tests need; block
  `ptrace`, `mount`, `keyctl`, `bpf`, `clone(CLONE_NEWUSER)` beyond the outer userns, etc.
- **AppArmor/SELinux:** confine mounts and paths; deny access outside `/workspace`,`/tmp`.
- **cgroup v2 quotas:** CPU (millis), memory (hard + OOM-kill), `pids.max`, `io` throttling —
  from `project_quotas`. Prevents fork bombs and noisy-neighbour.
- **Timeouts:** every run has a wall-clock cap (bounded loop, I1) enforced by the control
  plane **and** a sandbox-side watchdog; overrun → SIGTERM → SIGKILL → instance destroyed.
- **No inbound**: the sandbox exposes no listening ports to the network; the only channel is
  the control-plane RPC.

### 3.5 Egress: default-deny + allow-list + proxy (the supply-chain control)

Egress is the single biggest risk (dependency install pulls arbitrary code; exfil is the top
threat). Design:

- **Default deny** — the microVM/container has **no route to the internet**. This directly
  extends `EgressPolicy(network_enabled=False)`
  ([`keel-sandbox/policy.py`](../../packages/keel-sandbox/src/keel_sandbox/policy.py):42–47).
- **All guest egress via an authenticated forward proxy sidecar** in the control plane's trust
  domain. The proxy enforces a **per-run allow-list**: configured package registries,
  control-plane provider endpoints, and explicitly approved public Git dependency hosts, and
  nothing else. The authoritative project remote is not exposed to the guest. Loopback/link-local/private/
  cloud-metadata (`169.254.169.254`, `metadata.google.internal`) are **hard-blocked**
  (already modeled by `EgressPolicy._is_blocked`, policy.py:49–55). This is the SSRF control.
- **DNS** is resolved only by the proxy (no direct DNS from the sandbox), preventing
  DNS-rebinding and DNS-exfil.
- **Bytes cap** per run (`egress_bytes_cap`) to bound exfiltration volume; audited.
- **Recommended hardening:** a **private registry mirror/cache** (Artifactory/Verdaccio/
  devpi/Athens) as the *only* allowed package host, so the platform controls the supply chain
  and can scan/pin. Direct public-registry access is opt-in per project and audited.
- **Egress is a run-admission capability**: turning network on is `ask`/`deny`-gated and
  audited (fail-closed default), not a silent flag. Controlled tools may add finer decisions;
  opaque CLIs remain bounded by the admitted allow-list and bytes cap.

### 3.6 Secrets, provider proxy, injection & redaction

- Secrets live envelope-encrypted in `project_secrets` (G9). Prefer a **control-plane
  LLM/provider proxy** so platform and BYO provider credentials remain outside the guest while
  the proxy applies per-run identity, budget, egress, and audit policy.
- If a vendor CLI cannot use the proxy and requires a BYO/vendor token, injecting it means the
  credential has **entered the untrusted execution boundary**. Inject only a short-lived,
  narrowly scoped token just in time to the specific guest process (read-only tmpfs or env),
  never bake it into an image or write it to `/workspace`, revoke it promptly, and disclose
  the residual theft/exfiltration risk. Do not place deploy credentials in opaque CLI guests.
- **Redaction:** sandbox stdout/stderr/artifacts pass a redaction filter before reaching the
  event log, artifacts, or telemetry. Mask known values and flag likely high-entropy tokens,
  while acknowledging that redaction reduces accidental disclosure but cannot make an exposed
  guest credential safe.
- **GitHub repository credentials never enter the sandbox**: approved remote writes are
  performed by the trusted control plane (§3.7).

### 3.7 Git object flow (fetch in, approved write out)

```
import/sync:  control plane → JIT GitHub App installation token → active bare repo (durable Git storage/service)
snapshot:     control plane → immutable Git bundle/snapshot → MinIO/S3
worktree:     control plane materializes per-run checkout → sandbox /workspace
agent edits:  sandbox writes/commits only in its disposable worktree; authoritative repo is not writable/mounted
write-back:   human approves remote branch + draft PR → trusted control plane mints JIT token and performs both
```

The sandbox **never holds the GitHub repository credential**, never receives writable access
to the authoritative repository, and never contacts the authoritative project remote for
sync or write-back. Explicitly allowed public Git dependency fetches carry no project
credential. A successful run only
produces a proposed diff/commit set. No success path automatically pushes: explicit human
approval is required before the trusted control plane creates a remote branch and draft PR.
Default-branch push, force-push, and merge remain forbidden.

### 3.8 Artifacts, logs, cleanup

- **Logs** stream as events (`tool.progress`/`tool.result`, ARCHITECTURE §9.2) into the event
  store; large output **spills to object storage** and returns a ref (existing bounding
  pattern, [`tools/bounding.py`](../../packages/keel-core/src/keel_core/tools/bounding.py),
  ARCHITECTURE §6.1 "output bounding").
- **Artifacts** (diffs, build outputs, test reports) are written to object storage with
  `sha256` + `retention_class` (`artifacts` table), scoped, and purgeable (M3.5).
- Before reclaiming a successful run's worktree, the control plane captures the final diff,
  base SHA, and proposed commits as an immutable proposal bundle/artifact. Later write-back
  approval is bound to that bundle's SHA-256 and consumes exactly that retained proposal; it
  does not depend on a live sandbox volume.
- **Cleanup:** on run end (success, failure, cancel, timeout, crash), the control plane
  **destroys the instance** and **reclaims the worktree volume** (unmount + secure-wipe or
  discard the CoW subvol). A **reaper** in the scheduler sweeps orphaned volumes/instances
  (crash recovery) — fail-safe, idempotent (matches durable-jobs reclaim, ARCHITECTURE §11).

### 3.9 Nested Docker / build tooling (the hard part)

Many builds want Docker. **Do not mount the host Docker socket** (host takeover). Options,
recommended order and subject to the selected isolation backend:

1. **Rootless/daemonless build tools** — `buildah`, `kaniko`, `img`, `buildkit` rootless — run
   **inside** the sandbox with no privileged daemon. **Recommended default** for image builds.
2. **Stronger-isolation backend:** where the selected backend supports it, a rootless nested
   container runtime inside Kata/a microVM can provide fuller Docker semantics. This is a
   capability, not a universal Firecracker requirement.
3. **Sysbox** runtime (runs Docker-in-Docker without privileged/host-socket) as an opt-in.
4. ❌ Never `-v /var/run/docker.sock` or `--privileged`.

Document per-project which build capability is available; advertise it like a tool capability.

---

## 4. Execution mediation and sandbox enforcement (item 4)

### 4.1 Current state and the seam

Today `ShellTool` and the file tools execute in the **server/CLI process**
([`tools/shell.py`](../../packages/keel-core/src/keel_core/tools/shell.py):41 uses
`asyncio.create_subprocess_shell` in-process; `tools/files.py` reads/writes the local FS
directly). Path confinement exists (`_safe_path`, `PathPolicy`) but there is **no process
isolation** — this is I6's open half.

The **already-named** `ExecutionEnvironment` seam (ARCHITECTURE §6.4, ADR-0009) is the
migration vehicle for **Keel-owned tools and controlled-tool adapters**:

```python
class ExecutionEnvironment(Protocol):
    async def read(self, path, ctx) -> Bytes: ...
    async def write(self, path, data, ctx) -> None: ...
    async def list(self, path, ctx) -> list[Entry]: ...
    async def exec(self, argv, *, cwd, env, timeout, network, ctx) -> ExecResult: ...
    async def spawn(self, argv, ...) -> Handle: ...        # background_process
    async def fetch(self, url, ctx) -> Response: ...       # egress via proxy only
```

Backends: **`SandboxContainer`** (RPC to keel-sandbox — the production backend),
**`InProcess`** (lite/trusted-dev only), **`LocalDaemon`** (deferred, own fail-closed gate).

This interface is not a syscall interposer. An arbitrary CLI running in the guest can call
`open`, `execve`, or sockets directly without invoking `env.*`. Two honest enforcement modes
follow:

| Mode | Per-action visibility/approval | Enforceable outer controls |
|---|---|---|
| **Controlled-tool mode** | Yes, for actions the adapter routes through Keel tools | Tool policy + sandbox filesystem/process/network policy + final diff + remote-write approval |
| **Opaque CLI mode** | No general per-file/per-process guarantee; consume only vendor events the CLI actually exposes | Run-level mounts/quotas/egress, no authoritative Git write access, output redaction, final diff review, explicit remote-write approval |

### 4.2 Fail-closed rules

- A Keel-mediated tool that touches the filesystem/process/network and receives **no `ExecutionEnvironment`**
  (or an environment whose policy is unset) **refuses** — it must not silently fall back to
  in-process (mirrors the executor's "ask → fail closed if no approver",
  [`tools/executor.py`](../../packages/keel-core/src/keel_core/tools/executor.py):52–58).
- `InProcess` is **statically forbidden** in `keel-server`/`keel-worker` builds: a config/CI
  guard (extend `tests/invariants/`) asserts server/worker never construct `InProcess`. This
  is the enforcement home for I6's "execution must not occur in the API process".
- Path policy (`PathPolicy`, `_safe_path`) and egress policy (`EgressPolicy`) move **into the
  sandbox boundary** and are enforced **there** as well as in the tool — defense in depth, not
  either/or. The sandbox is the real boundary; the in-tool checks are a fast-fail nicety.
- Every mediated env operation carries `ToolContext` (`scope_id`, `trust`, `content_taint`) so the
  sandbox can enforce scope and tag outputs tainted (repo content is untrusted, G17).
- Opaque CLI syscalls are contained by the guest's mount namespace, seccomp/LSM/cgroup policy,
  and egress proxy. Keel must not emit synthetic per-action approvals for operations it did
  not observe.

### 4.3 Migration strategy (incremental, behind the seam)

1. **Extract** the interface; wrap the *current* in-process implementations as `InProcess`
   (behavior-preserving; `lite`/tests keep working). Tools call `env.*`, not `Path`/subprocess.
2. **Implement** `SandboxContainer` + the keel-sandbox service + RPC (the ADR-0005 container).
3. **Flip** server/worker default to `SandboxContainer`; add the CI guard forbidding
   `InProcess` there. Land the I6 acceptance evidence (escape/egress tests) — this **closes
   M3.3's sandbox gate**.
4. **Add** coding-specific controlled tools and higher-level services once the base sandbox is
   proven; document whether each adapter runs in controlled-tool or opaque CLI mode.

No product code is changed by *this* document; §4.3 is the implementation order for M3.3.

---

## 5. GitHub import/sync model (item 2)

### 5.1 Repository access vs user identity/attribution

- **Recommended: a GitHub App** for **repository access only**. Installation permissions and
  installation access tokens authorize the App against selected repositories; they do not
  authenticate the human to Keel or prove human action attribution. Rationale:
  fine-grained per-repo installation permissions, short-lived installation tokens, org-level
  install/audit, higher rate limits, and **webhooks scoped to the installation**. This fits the
  "least-scope, per-scope-bound, revocable" doctrine (G18) far better than a broad user OAuth
  token.
- **User login and attribution are separate.** OAuth/OIDC authenticates the user to Keel and
  links their GitHub identity where needed for consent and audit. The control plane records
  both the approving Keel principal and the GitHub App installation that performed a remote
  action; App installation identity must not be presented as if it were the human actor.
- **Fine-grained PAT** as a documented BYO fallback for self-hosters who won't install an App
  (stored envelope-encrypted, least scopes).
- Installation tokens are minted **just in time**, short-lived, minimally permissioned for the
  approved repository action, held only in control-plane memory, and discarded after use.
  Persist the App installation id and authorization metadata, **not installation access tokens
  as durable secrets**. Minting fails closed if the installation is revoked or permissions no
  longer suffice. The sandbox never sees these tokens.

### 5.2 Webhooks (authenticated, replay-safe — closes an M3.3 gate)

- Ingest via `keel-server` at an authenticated endpoint: **verify `X-Hub-Signature-256` HMAC**,
  check delivery id for **idempotency**, and reject stale/forged deliveries — directly fixing
  blocker #6 ("gateway webhooks are unauthenticated") for the coding surface.
- Events consumed: `push`, `pull_request`, `installation`/`installation_repositories`
  (grant lifecycle), `check_run`/`check_suite` (if Keel reports status). A webhook **enqueues a
  durable `repo_syncs` job** (at-least-once, deduped by delivery id, matching ARCHITECTURE §11).
- Webhook payloads are **tainted** (G17) — a PR title/body is attacker-controlled and must not
  auto-trigger destructive tools or cross-scope reads.

### 5.3 Branch / PR semantics

- Imports create an active **bare repository** (all authorized refs). Agent work happens on a
  proposed **dedicated branch**
  (`keel/<agent>/<run-short-id>`), never directly on the default branch.
- Run output is a **proposed diff/commit set**, not an automatic push. After explicit human
  approval, the trusted control plane may mint a JIT App installation token, create a remote
  run branch, and open a **draft pull request**. The PR body includes the run id, budget/cost,
  available approval trail, approving principal, and audit-log link.
- Push/merge to the default branch, force-push, and automatic merge are forbidden. Promoting a
  draft PR or merging remains an ordinary human-controlled repository workflow.
- **Conflicts/base movement:** before write-back, a dedicated trusted Git service (hooks,
  filters, and arbitrary checkout execution disabled) verifies the approved base. If the base
  moved incompatibly, write-back **stops and requests human input**; any regenerated/rebased
  proposal receives a new bundle hash and requires fresh approval. It is never silently
  substituted for the approved proposal.
- **Forks:** for read-only analysis of untrusted external repos, mirror the fork read-only and
  **never** grant write/push; treat contents as tainted. Contributing back uses the fork-PR
  flow with the App on the fork.

### 5.4 Audit & supply-chain / SSRF concerns

- Every import/fetch/approved-write-back/token-use is an `audit_log` + `repo_syncs` row (who
  approved, which installation acted, what ref/sha, when, result) — non-repudiable
  (ARCHITECTURE §13 "complete audit").
- **SSRF:** the import URL and any webhook-provided URL are validated against the same
  block-list as `EgressPolicy` (no loopback/link-local/private/metadata); clone/fetch happens
  **only** through the egress proxy to the allow-listed Git host.
- **Supply chain:** dependency resolution during a run goes through the private registry
  mirror (§3.5); lockfiles are respected; the platform can pin/scan. The imported repo's build
  scripts run **only inside the sandbox** with default-deny egress. A malicious `postinstall`
  cannot reach another tenant or unapproved destinations, though allowed registry endpoints
  remain an explicit residual exfiltration surface bounded by proxy policy and bytes caps.

---

## 6. Pluggable coding-agent contract (item 5)

### 6.1 Position: Keel is the host, the agent is a guest

The coding agent (Copilot CLI, Claude Code, an OSS agent) runs **as a guest process inside the
sandbox**, launched and observed by a Keel **adapter**. `CodingAgentAdapter` is an internal
execution-driver abstraction, not a public plugin API and not necessarily an MCP protocol.
Keel owns the outer sandbox, authoritative Git/write-back, budgets, audit, and enforceable
policy. The agent owns planning and its own model calls (subject to §7). Controlled-tool mode
can route actions through Keel's one-tool interface; opaque CLI mode is instead constrained at
the run boundary (§4).

### 6.2 `CodingAgentAdapter` contract

```python
class CodingAgentAdapter(Protocol):
    name: str                          # "copilot-cli" | "claude-code" | "oss:<name>"
    def capabilities(self) -> AgentCapabilities: ...   # edits? runs cmds? mcp? checkpoints? cancel?
    async def start(self, spec: CodeRunSpec, env: ExecutionEnvironment, ctx: ToolContext) -> Session: ...
    def events(self) -> AsyncIterator[AgentEvent]: ...  # normalized → Keel event vocabulary
    async def approve(self, request_id, decision) -> None: ...   # optional vendor-native approval
    async def cancel(self) -> None: ...                 # cooperative → hard kill
    async def checkpoint(self) -> CheckpointRef: ...    # snapshot worktree+state
    async def restore(self, ref: CheckpointRef) -> None: ...
    async def close(self) -> None: ...
```

- **Lifecycle:** `start → (stream events) → [approvals/cancel/checkpoint] → close`. The
  adapter process lives **only** for the run and dies with the sandbox instance (T2). Bounded
  loop / named termination is preserved (I1); admission persists before first model call (I2).
- **Capabilities negotiation:** the adapter declares what it supports; Keel degrades
  gracefully (e.g., no native checkpoint → Keel snapshots the worktree volume itself).
- **Events → Keel vocabulary:** the adapter normalizes the agent's stream into Keel's typed
  events (`tool.call`, `tool.progress`, `tool.result`, `approval.requested`,
  `approval.resolved`, `message.delta`, `turn.ended`, `run.ended{reason}` — ARCHITECTURE §9.2)
  so Web/CLI/IM render coding runs with the **same** UI and the same SSE/WS stream.
- **Approvals:** in controlled-tool mode, proposed Keel tool actions can use the permission
  engine + approval bus (deny>ask>allow, fail-closed default;
  [`permissions.py`](../../packages/keel-core/src/keel_core/permissions.py); G5). Vendor-native
  approval events may be passed through only when their semantics are verified. In opaque CLI
  mode, do not promise per-action interception: approve run admission/capabilities, enforce
  run-level filesystem/network limits, review the final diff, and separately approve any
  remote branch/draft-PR creation.
- **Cancellation:** cooperative first (signal the guest), then hard kill + instance destroy;
  restart-safe because the run is worker-owned and event-sourced (M3.6 topology).
- **Checkpoints:** a checkpoint = worktree volume snapshot (CoW) + a persisted agent-state
  ref + the event-log cursor. Enables pause/resume across process death (reuses the
  suspend/resume machinery,
  [`test_loop_suspend_resume.py`](../../tests/unit/test_loop_suspend_resume.py), G5).
- **Observability:** every run emits OTel spans + Langfuse trace→observation→score and
  cost/token accounting (ARCHITECTURE §14), tagged with `project/worktree/run/adapter`.
- **Budget:** the agent's model calls draw on the **shared budget** (I7); a coding run cannot
  exceed the org/project allowance; cost rolls up (ARCHITECTURE §10).

### 6.3 Model / provider separation

The **adapter** (how we drive Copilot CLI vs Claude Code) is orthogonal to the **model/
provider** (who serves tokens). Keel's `ProviderGateway` (LiteLLM) policy still governs
platform-metered calls. Three provider modes map to §7. The adapter declares whether it can use
the control-plane provider proxy, requires a short-lived injected credential, or must use its
own vendor backend (device login). Keel records the credential/billing path and applies §3.6.

### 6.4 MCP relationship

- Keel may expose **sandbox-mediated tools** (fs, build, test) as MCP servers/tools where an
  agent supports MCP — reusing the existing MCP seam and the
  **import ≠ trust** control (I8/G6): the agent's tool/skill/MCP descriptions are
  allow-listed + injection-scanned at import.
- MCP is a **tool interoperability seam**, not the `CodingAgentAdapter` wire protocol and not a
  guarantee that arbitrary CLI syscalls pass through Keel tools. MCP calls Keel does expose
  remain subject to its authorization/taint/tool policy; non-MCP guest activity remains under
  the sandbox's outer controls.
- A supported adapter registry is initially internal and reviewed with the product. A stable,
  third-party public plugin SDK, compatibility contract, and untrusted plugin distribution
  model are explicitly later roadmap work.

### 6.5 Adapter-specific notes (design seam only; see §7 for legal gates)

- **Copilot CLI / Claude Code:** run their CLI as an opaque guest unless a documented,
  verified controlled-tool mode exists; the adapter translates only events and approvals the
  CLI actually exposes. Credentials use a provider proxy where supported, otherwise the
  short-lived guest-injection risk in §3.6 applies. Vendor ToS governs hosted use (§7.5).
- **OSS agents (e.g., Aider-class, OpenHands-class):** run in-sandbox; typically accept a
  BYO key/endpoint → cleanest fit with platform provider policy; license must be reviewed
  (§7.5) but is generally the least encumbered.

---

## 7. Authentication / subscription strategies (item 6)

Three strategies, **kept strictly separate**, each with its own security/ops/billing/legal
profile. **We do not assert any vendor permits resale/automation; §7.5 lists the questions.**

### 7.1 Platform-owned LLM credentials (Keel-metered)

- Keel holds provider keys; calls go through `ProviderGateway`; cost is **reserved in Redis
  pre-call, reconciled to Postgres/Langfuse post-run** (G7); org/project budgets enforced (I7).
- **Security:** keys live only in the control plane; the guest uses the provider proxy and
  never receives the platform key.
- **Billing:** Keel meters and bills the org; clean chargeback per project/run.
- **Ops:** rate-limit per provider-cred/session/chat (G10); provider fallback via LiteLLM.
- **Legal:** Keel's own provider agreement governs; simplest path.

### 7.2 BYO LLM API key (recommended default for coding)

- The org/user supplies their own provider key, stored in `project_secrets` (envelope-
  encrypted, G9) and bound to the org-owned project plus authorized principal.
- **Security:** prefer the control-plane provider proxy so the key does not enter the guest.
  If a CLI requires direct injection, issue/use the narrowest short-lived credential possible,
  inject it just in time, redact output, revoke promptly, and accept the residual risk that a
  compromised guest can steal it (§3.6). It is never baked into images or written by the
  platform to the worktree.
- **Billing:** the user pays the provider directly; Keel bills only for platform compute.
- **Legal:** cleanest for coding agents that accept a configurable key/endpoint (most OSS).

### 7.3 User OAuth / device-login into third-party coding agents

- The agent (e.g., Copilot CLI, Claude Code) authenticates **as the user** to **its own vendor
  backend** via device-code/OAuth. This vendor subscription token is separate from GitHub App
  repository access and from Keel user login. Keel brokers the flow and stores refresh
  material like a connector token: envelope-encrypted, principal/adapter-bound, fail-closed on
  refresh/revoke, and purged on deletion (G18).
- **Security:** prefer a control-plane vendor proxy/token exchange where the protocol permits.
  If the CLI must receive an access token, mint/inject the shortest-lived, narrowest token
  available; it has then entered the untrusted guest boundary and is exposed to guest
  compromise despite redaction and revocation controls (§3.6).
- **Billing:** the user's **own subscription** with the vendor is consumed; Keel does not meter
  tokens (it may meter compute).
- **Ops:** token expiry/refresh/health surfaced like connector health (M3.7); device-login UX
  in Web.
- **Legal (critical):** running a user's personal coding-agent subscription inside a
  **platform-hosted, automated, multi-tenant** environment may conflict with the vendor's ToS
  (personal-use / seat / anti-automation / anti-resale clauses). **This must be reviewed and
  explicitly permitted before enabling that adapter in a hosted tier.** See §7.5.

### 7.4 Comparison

| Strategy | Secret home | Who pays LLM | Isolation risk | Legal risk |
|---|---|---|---|---|
| Platform key (7.1) | Control plane proxy | Keel (metered) | Low; guest never receives key | Low (own agreement) |
| BYO key (7.2) | `project_secrets`; proxy preferred | User→provider | Low via proxy; **high residual risk if guest-injected** | Low (esp. OSS) |
| User OAuth/device (7.3) | Control plane refresh material; short-lived guest token only if required | User's vendor sub | Medium/high when guest injection is required | **High — ToS review required** |

### 7.5 Open terms/licensing questions (must be answered by legal, not engineering)

1. Do Copilot CLI / Claude Code ToS permit **automated, headless, server-hosted** use?
2. Do they permit **multi-tenant** hosting where Keel drives many users' agents?
3. Are there **anti-resale / per-seat / personal-use-only** clauses that a hosted coding
   product would violate?
4. What are the **OSS licenses** (and any trademark/branding limits) of the OSS agents we wrap
   or redistribute in a base image?
5. Do vendor endpoints/models permit Keel's **prompt-caching/logging/eval** telemetry (Langfuse)?
6. Data-processing/residency terms when a user's private repo passes through Keel's sandbox +
   a third-party agent backend.

**Decision rule:** an adapter is **disabled by default** in any hosted/multi-tenant tier until
its §7.5 questions are answered affirmatively and recorded; self-hosters accept their own ToS
responsibility via an explicit, logged acknowledgement.

---

## 8. Threat model & tenancy invariants (item 7)

### 8.1 Ranked threats (ADR-0009: exfiltration > escape)

1. **Cross-scope / cross-tenant data exfiltration (confused deputy).** A tainted input — a
   malicious file in an imported repo, a PR body, a dependency README, an issue — coerces the
   agent to read another scope's secrets/worktree/memory and leak it (via a proposed commit, an
   outbound HTTP call, or approved draft-PR content). **Top threat.** Controls: principal/resource/capability
   authorization + RLS (I10/G16), taint
   propagation + outbound gating (G17, `ConfusedDeputyEngine`,
   [`connectors.py`](../../packages/keel-core/src/keel_core/connectors.py):91–110),
   default-deny egress + bytes cap (§3.5), approvals for observable controlled-tool actions,
   and mandatory approval for all remote Git write-back (G20).
2. **Supply-chain compromise.** Malicious dependency/build script executes attacker code.
   Controls: sandbox least-privilege (§3.4), default-deny egress + private registry mirror
   (§3.5), read-only base, ephemeral instance (T2).
3. **Sandbox escape → host / other tenants.** Controls: hostile multi-tenant exposure gated
   on an acceptance-tested stronger backend (gVisor/Kata/microVM, §3.2), plus dropped
   caps/seccomp/AppArmor, rootless execution, and no host Docker socket (§3.9).
4. **Credential theft** (LLM key, Git token, vendor OAuth). Controls: keep GitHub App and
   platform provider credentials in the control plane; prefer provider proxies; where guest
   injection is unavoidable use short-lived narrow tokens, redaction, revocation, and explicit
   residual-risk acceptance (§3.6/G9).
5. **Prompt-injection via tool/agent/MCP metadata.** Controls: import≠trust, injection scan,
   quarantine (I8/G6).
6. **Resource abuse / DoS.** Controls: cgroup quotas, timeouts, concurrency caps, budget (I7).

### 8.2 Trust boundaries

`control plane (trusted)` ⟂ `sandbox/guest agent (untrusted)` ⟂ `imported repo content
(tainted)` ⟂ `third-party agent backend (external trust domain)` ⟂ `other tenants (mutually
untrusted)`. GitHub repository and platform provider credentials stay in the control plane.
Any BYO/vendor token injected into a guest has crossed into the untrusted boundary and carries
the residual risk in §3.6. Repo content is always tainted; observable outbound actions are
gated, while opaque CLI egress is constrained at the run-level proxy boundary (G17).

### 8.3 Tenancy invariants (merge-blocking, extend INVARIANTS.md)

I6 (two-level sandbox) and I10 (per-scope isolation) are reused; add **T1–T4** (§2.5). New
acceptance tests live beside the existing invariant registry
([`tests/invariants/test_invariants.py`](../../tests/invariants/test_invariants.py),
INVARIANTS.md), e.g. cross-project path/mount denial,
worktree residue absence, egress allow-list enforcement, and durable Git/proposal recovery.

### 8.4 Deployment topology (Docker / Kubernetes)

- **Docker Compose (`full`, trusted single-org M3.3 preview):** existing services + a **real
  `keel-sandbox`** running rootless containers, a durable Git volume/service, MinIO for
  immutable snapshots/artifacts, and an egress-proxy sidecar. Matches ARCHITECTURE §15's
  `full` profile (currently aspirational for sandbox).
- **Kubernetes (hostile multi-tenant, rollout gated on stronger isolation):**
  - control plane = Deployments (`keel-server` scale-out, `keel-worker ×N`, elected
    `keel-scheduler`), no untrusted code.
  - execution plane = **per-run Jobs/Pods** using an accepted stronger-isolation runtime such
    as gVisor, Kata, or a microVM backend through `runtimeClassName`;
    `automountServiceAccountToken: false`;
    strict `securityContext` (runAsNonRoot, drop ALL caps, seccomp `RuntimeDefault`+custom,
    read-only rootfs); **NetworkPolicy default-deny** egress, only the proxy reachable;
    resource `limits` from `project_quotas`; ephemeral scratch via `emptyDir`/CSI CoW volumes.
  - **namespace-per-org** (or per-tenant node pools) for blast-radius; PodSecurity `restricted`.
  - active Git repositories = durable POSIX/block PVCs with locking/atomic refs or an external
    Git-smart service; S3 stores immutable bundles/snapshots/artifacts. Secrets use the
    envelope broker (not raw K8s Secrets in the sandbox).
- **Data schema/API boundaries:** all new tables carry `scope_id`/`org_id` + RLS (G16), and
  each endpoint invokes the principal/resource/capability authorizer. The API adds
  `/v1/projects`, `/v1/projects/{id}/grants`, `/v1/projects/{id}/worktrees`,
  `/v1/projects/{id}/runs` (+ SSE `/runs/{id}/events`), and an approval-bound write-back
  operation that may create only a remote run branch + draft PR. It is additive-only under
  `/v1` with SDK diff in CI (G14). The control-plane↔sandbox RPC is internal, mTLS, and
  capability-token-bound — never public.

---

## 9. Alternatives, tradeoffs, anti-patterns

| Decision | Recommended | Alternative | Why recommended |
|---|---|---|---|
| Isolation boundary | **Rootless OCI for trusted single-org M3.3 preview; stronger gVisor/Kata/microVM gate for hostile multi-tenant** | shared long-lived executor | Staged rollout without prematurely requiring one universal backend |
| Git storage | **mutable active bare repo on durable Git storage/service + ephemeral worktree; immutable S3 snapshots** | directly mutate S3 mirror; worktree-only | Correct ref locking/atomicity, durability, fast provisioning, recovery (T3) |
| GitHub integration | **GitHub App for repo access; OAuth/OIDC separately for user identity/attribution** | broad user OAuth / durable installation tokens | Least-scope JIT repository access without conflating machine and human identity |
| Egress | **default-deny + allow-list proxy + registry mirror** | network-on for convenience | Supply-chain + exfil is the top threat; convenience loses |
| Nested Docker | **rootless buildah/kaniko; backend-appropriate nested runtime if qualified** | host docker socket / privileged | Socket/privileged = host takeover |
| Coding agent seam | **internal execution-driver adapter; optional MCP tools** | MCP as mandatory adapter protocol; public plugin SDK now | Honest boundary; public SDK waits for a stable contract |
| Enforcement mode | **controlled-tool when per-action policy is required; opaque CLI with run-level controls otherwise** | claim every CLI syscall is mediated | Vendor CLIs can perform direct guest syscalls |
| Credential default (coding) | **control-plane provider proxy; constrained injection only when unavoidable** | claim guest-injected secrets remain trusted | Injection crosses the untrusted boundary; legal risk also remains gated |
| Execution seam rollout | **`ExecutionEnvironment` + forbid `InProcess` on server/worker** | keep in-process with path checks | Path checks ≠ isolation; I6 requires it |

**Anti-patterns to reject:** see §0.2 (host Docker socket, shared long-lived executor,
network-on-by-default, in-process execution, directly mutating object-store Git, writable
authoritative repos in sandboxes, fictitious per-syscall approvals, automatic push, trusting
repo contents, assuming vendor ToS permission).

---

## 10. Incremental implementation order (aligned to milestones)

**Phase A — Land the isolation substrate (M3.3, prerequisite for anything coding).**
1. Extract `ExecutionEnvironment`; wrap current tools as `InProcess` (behavior-preserving).
2. Build `keel-sandbox` container + control-plane RPC (mTLS): read-only base, writable
   `/workspace`, tmpfs, rootless, dropped caps, seccomp/AppArmor, cgroup quotas, watchdog.
3. Implement `SandboxContainer`; flip server/worker default; **CI guard forbids `InProcess`
   on server/worker**; land I6 escape/egress acceptance evidence.
4. Egress proxy sidecar (default-deny + allow-list + block-list from `EgressPolicy`).
5. Non-owner RLS role (I10); explicit fail-closed permission defaults; hashed/scoped machine
   creds; authenticated/replay-safe webhooks. (These are already M3.3 exit gates.)

**Phase B — Identity + durable runs (M3.6).**
6. Users/org/RBAC, persisted Agents, grants, worker-owned durable interactive runs; retire
   `web:local`. (M3.6 exit gates — reused wholesale.)

**Phase C — Managed projects (post-M3.6 vertical, "Coding Projects v1").**
7. `orgs/projects/worktrees/project_grants/project_quotas/project_secrets` tables (+ RLS/G16);
   new principal/resource/capability authorization; T1–T4 acceptance tests.
8. GitHub App repository-access install flow; separate OAuth/OIDC user identity/attribution;
   JIT installation-token minting; active-repository import, signed webhooks → durable sync
   jobs; SSRF-validated clone via proxy.
9. Durable Git storage/service with locking and atomic refs; immutable snapshots to object
   storage; per-run worktree provisioning + reclaim reaper; artifacts, backup, and purge.
10. Branch/PR flow: run produces proposed commits → explicit human approval → trusted control
    plane creates remote branch + draft PR with audit trail; conflict → human pause. Default
    branch push/merge remains forbidden.

**Phase D — Pluggable coding agents.**
11. Internal `CodingAgentAdapter` + event normalization + capability/mode declaration
    (controlled-tool vs opaque CLI) + supported approval/cancel/checkpoint passthrough;
    budget/observability wiring. MCP remains optional tool interop; public plugin SDK deferred.
12. First adapter: an **OSS agent using the provider proxy/BYO credential path** (lowest legal
    risk) end-to-end in the sandbox.
13. Qualify one or more stronger-isolation backends (gVisor/Kata/microVM) before hostile
    multi-tenant exposure; add backend-appropriate nested-build capability.
14. Copilot CLI / Claude Code adapters **gated behind §7.5 legal sign-off**; device-login UX;
    per-run credential/billing attribution.

**Safety gates between phases:** no coding run on `InProcess`; rootless OCI is limited to the
trusted/single-org preview until stronger-isolation acceptance evidence exists; no adapter is
enabled in a hosted tier without §7.5 sign-off; no run success automatically pushes; every
phase ships its acceptance tests (I1/I2/I6/I10/T1–T4) before exposure. M3.1–M3.2 preview
stacks **must not** expose this surface to untrusted networks
([`ROADMAP.md`](../ROADMAP.md)).

---

## 11. Summary

Managed code projects + pluggable coding agents are the workload ADR-0005 anticipated: they
**force** the real two-level sandbox (I6) and lean entirely on the M3.3 safety foundation and
M3.6 identity/durable-run topology. The reference architecture: **org→project→worktree→run**
org-owned resources, with users and Agents authorized as principals through explicit
capabilities; **mutable authoritative Git on durable atomic-ref storage/service**, immutable
object-store snapshots, and disposable worktrees never given writable authoritative access;
a control-plane/execution-plane split with rootless OCI for trusted single-org preview and a
stronger gVisor/Kata/microVM gate for hostile multi-tenancy; `ExecutionEnvironment` for
Keel-mediated tools while opaque CLIs remain honestly governed by run-level limits; internal
agent adapters with optional MCP interoperability; provider proxies preferred over
guest-injected credentials; and explicit human approval before the trusted control plane may
create a remote branch and draft PR. Default-branch push/merge remains forbidden. Cross-scope
exfiltration remains the top threat, ranked above sandbox escape per ADR-0009.
