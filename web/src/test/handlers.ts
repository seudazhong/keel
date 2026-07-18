import { http, HttpResponse } from "msw";
import type { Agent } from "../features/agents/types";
import type { Approval } from "../features/approvals/types";
import type { SseEvent } from "../features/chat/types";
import type { Connector } from "../features/connectors/types";
import type {
  KnowledgeBase,
  KnowledgeDocument,
  KnowledgeVersion,
} from "../features/knowledge/types";
import type { Job } from "../features/jobs/types";
import type { MemoryProposal } from "../features/memory/types";
import type { Project } from "../features/projects/types";
import type { DiffFile, Run, RunApproval } from "../features/projects/runs/types";
import type { Schedule } from "../features/schedules/types";
import type { SessionSummary } from "../features/sessions/types";
import { makeConnectorFixture } from "./connectorFixtures";

export const sampleApproval: Approval = {
  id: "a1",
  run_id: "r1",
  session_id: "digest:web:local",
  tool: "email_send",
  args: { to: "zhangwei@example.com", subject: "Re: 项目同步" },
  call_id: "c1",
  reason: "tainted",
  status: "pending",
  created_at: "2026-07-08T00:00:00Z",
  expires_at: "2026-07-09T00:00:00Z",
};

// Stateful like the real backend: approve/reject removes the row so a refetch reconciles.
let pending: Approval[] = [sampleApproval];

export function resetApprovals(): void {
  pending = [sampleApproval];
}

const defaultConnectors: Connector[] = [
  makeConnectorFixture({
    id: "gmail",
    name: "Gmail",
    icon: "✉️",
    auth_kind: "oauth",
    description: "Read Gmail messages and send approved email.",
    capabilities: ["read", "write"],
    scopes: ["gmail.readonly", "gmail.send"],
    connected: true,
  }),
  makeConnectorFixture({
    id: "oauth-fixture",
    name: "OAuth fixture",
    auth_kind: "oauth",
  }),
  makeConnectorFixture({
    id: "secret-fixture",
    name: "Secret fixture",
    auth_kind: "secret",
  }),
];

let connectors: Connector[] = defaultConnectors.map((c) => ({ ...c }));

export function resetConnectors(): void {
  connectors = defaultConnectors.map((c) => ({ ...c }));
}

export const sampleConnectors = defaultConnectors;

export const sampleSessions: SessionSummary[] = [
  {
    id: "s1",
    title: "安排明天的行程",
    messages: 6,
    created_at: "2026-07-08T00:00:00Z",
    updated_at: "2026-07-08T02:00:00Z",
  },
  {
    id: "digest:web:local",
    title: "It is your scheduled morning run…",
    messages: 4,
    created_at: "2026-07-07T00:00:00Z",
    updated_at: "2026-07-07T05:00:00Z",
  },
];

const now = "2026-07-16T00:00:00Z";

export const sampleKnowledgeBase: KnowledgeBase = {
  id: "kb_01JZZZZZZZZZZZZZZZZZZZZZZZ",
  name: "Product handbook",
  description: "Policies and operating notes",
  embedding_model: "text-embedding-3-small",
  embedding_dim: 1536,
  status: "active",
  created_at: now,
  updated_at: now,
  deleted_at: null,
};

export const sampleKnowledgeDocument: KnowledgeDocument = {
  id: "kbd_01JZZZZZZZZZZZZZZZZZZZZZZZ",
  kb_id: sampleKnowledgeBase.id,
  title: "Refund policy",
  source_type: "markdown",
  source_uri: "https://example.com/handbook/refunds",
  status: "active",
  desired_version_id: "kbv_01JZZZZZZZZZZZZZZZZZZZZZZZ",
  active_version_id: "kbv_01JZZZZZZZZZZZZZZZZZZZZZZZ",
  last_error_kind: null,
  last_error_message: null,
  created_at: now,
  updated_at: now,
  deleted_at: null,
};

const sampleKnowledgeVersion: KnowledgeVersion = {
  id: "kbv_01JZZZZZZZZZZZZZZZZZZZZZZZ",
  kb_id: sampleKnowledgeBase.id,
  document_id: sampleKnowledgeDocument.id,
  version: 1,
  content: "# Refunds\nCustomers may request a refund within 30 days.",
  content_sha256: "a".repeat(64),
  index_fingerprint: "b".repeat(64),
  mime_type: "text/markdown",
  chunking_version: "v1",
  target_chars: 1600,
  overlap_chars: 200,
  ingest_job_id: "job_knowledge_1",
  status: "active",
  error_kind: null,
  error_message: null,
  created_at: now,
  activated_at: now,
  deleted_at: null,
  purged_at: null,
};

function makeJob(id: string, kind = "knowledge.ingest"): Job {
  return {
    id,
    kind,
    status: "succeeded",
    cancel_mode: "cooperative",
    target_session_id: null,
    attempt: 1,
    max_attempts: 3,
    next_attempt_at: now,
    lease_expires_at: null,
    cancel_requested: false,
    progress_current: 3,
    progress_total: 3,
    progress_message: "Indexed document",
    progress_updated_at: now,
    result: null,
    result_message: "Completed",
    error_kind: null,
    error_message: null,
    injected_event_seq: null,
    created_at: now,
    updated_at: now,
    started_at: now,
    finished_at: now,
  };
}

const defaultJobs: Job[] = [
  {
    ...makeJob("job_running_1", "memory.consolidation"),
    status: "running",
    cancel_mode: "cooperative",
    progress_current: 4,
    progress_total: 10,
    progress_message: "Reviewing recent events",
    result: null,
    result_message: null,
    started_at: now,
    finished_at: null,
  },
  {
    ...makeJob("job_failed_1", "knowledge.ingest"),
    status: "failed",
    progress_current: 2,
    progress_total: 3,
    result: null,
    result_message: null,
    error_kind: "embedding_unavailable",
    error_message: "Embedding provider did not respond.",
  },
];

let jobs = new Map(defaultJobs.map((job) => [job.id, { ...job }]));

export const sampleJobs = defaultJobs;

export function resetJobs(): void {
  jobs = new Map(defaultJobs.map((job) => [job.id, { ...job }]));
}

let knowledgeBases: KnowledgeBase[] = [{ ...sampleKnowledgeBase }];
let knowledgeDocuments: KnowledgeDocument[] = [{ ...sampleKnowledgeDocument }];
let knowledgeVersions = new Map<string, KnowledgeVersion[]>([
  [sampleKnowledgeDocument.id, [{ ...sampleKnowledgeVersion }]],
]);
let knowledgeJobs = new Map<string, Job>([["job_knowledge_1", makeJob("job_knowledge_1")]]);
let knowledgeMutation = 1;

export const knowledgeIdempotencyKeys: string[] = [];

export function resetKnowledge(): void {
  knowledgeBases = [{ ...sampleKnowledgeBase }];
  knowledgeDocuments = [{ ...sampleKnowledgeDocument }];
  knowledgeVersions = new Map([
    [sampleKnowledgeDocument.id, [{ ...sampleKnowledgeVersion }]],
  ]);
  knowledgeJobs = new Map([["job_knowledge_1", makeJob("job_knowledge_1")]]);
  knowledgeMutation = 1;
  knowledgeIdempotencyKeys.length = 0;
}

const defaultMemoryProposals: MemoryProposal[] = [
  {
    id: "mp_1",
    block: "user_preferences",
    expected_version: 3,
    proposed_value: "Prefers concise weekly status summaries.",
    reason: "The preference was stated consistently across recent sessions.",
    confidence: 0.92,
    source_event_ids: [101, 118, 125],
    status: "pending",
    created_at: now,
    resolved_at: null,
    resolved_by: null,
  },
  {
    id: "mp_2",
    block: "working_context",
    expected_version: 5,
    proposed_value: "Current project is Keel.",
    reason: "Resolved proposal retained for audit history.",
    confidence: 0.81,
    source_event_ids: [88],
    status: "applied",
    created_at: now,
    resolved_at: now,
    resolved_by: "web",
  },
];

let memoryProposals = defaultMemoryProposals.map((proposal) => ({ ...proposal }));

export const sampleMemoryProposals = defaultMemoryProposals;

export function resetMemory(): void {
  memoryProposals = defaultMemoryProposals.map((proposal) => ({ ...proposal }));
}

function recordIdempotency(request: Request) {
  const key = request.headers.get("Idempotency-Key");
  if (!key) return HttpResponse.json({ detail: "Idempotency-Key required" }, { status: 400 });
  knowledgeIdempotencyKeys.push(key);
  return null;
}

function nextKnowledgeJob(kind = "knowledge.ingest"): Job {
  const job = makeJob(`job_knowledge_${++knowledgeMutation}`, kind);
  knowledgeJobs.set(job.id, job);
  return job;
}

const sampleHistory: SseEvent[] = [
  { type: "message.token", seq: 1, session_id: "s1", run_id: "r1", ts: "", payload: { role: "user", text: "帮我看看明天有没有空" } },
  { type: "tool.call", seq: 2, session_id: "s1", run_id: "r1", ts: "", payload: { tool: "calendar_list", args: {}, call_id: "c1" } },
  { type: "tool.result", seq: 3, session_id: "s1", run_id: "r1", ts: "", payload: { call_id: "c1", ok: true, output: "3 events" } },
  { type: "message.token", seq: 4, session_id: "s1", run_id: "r1", ts: "", payload: { role: "assistant", text: "你明天下午空闲。" } },
  { type: "run.ended", seq: 5, session_id: "s1", run_id: "r1", ts: "", payload: { reason: "completed" } },
];

let currentModel = "github_copilot/claude-sonnet-4.5";

export function resetModel(): void {
  currentModel = "github_copilot/claude-sonnet-4.5";
}

const defaultSchedules: Schedule[] = [
  {
    id: "digest:web:local",
    agent_id: "digest",
    trigger_kind: "interval",
    spec: "86400",
    interval_s: 86400,
    enabled: true,
    next_run_at: "2026-07-09T06:00:00Z",
    last_run_at: "2026-07-08T06:00:00Z",
    last_status: "completed",
  },
];

let schedules: Schedule[] = defaultSchedules.map((s) => ({ ...s }));

export function resetSchedules(): void {
  schedules = defaultSchedules.map((s) => ({ ...s }));
}

const defaultAgents: Agent[] = [
  {
    id: "agent_personal",
    name: "Personal assistant",
    description: "General-purpose day-to-day helper for email, calendar, and notes.",
    model: "github_copilot/claude-sonnet-4.5",
    tools: ["calendar_list", "email_send"],
    active: true,
    created_at: now,
    updated_at: now,
  },
  {
    id: "agent_researcher",
    name: "Researcher",
    description: "Digs through knowledge bases and cites sources.",
    model: "github_copilot/gpt-4o",
    tools: ["knowledge_search"],
    active: false,
    created_at: now,
    updated_at: now,
  },
];

let agents: Agent[] = defaultAgents.map((a) => ({ ...a }));
let agentMutation = 1;

export function resetAgents(): void {
  agents = defaultAgents.map((a) => ({ ...a }));
  agentMutation = 1;
}

export const sampleAgents = defaultAgents;

const defaultProjects: Project[] = [
  {
    id: "proj_keel",
    name: "Keel",
    description: "Personal assistant monorepo used for the coding-agent preview.",
    repository: "https://github.com/example/keel",
    default_branch: "main",
    agent_id: "agent_researcher",
    created_at: now,
    updated_at: now,
  },
  {
    id: "proj_marketing",
    name: "Marketing site",
    description: "Public marketing site content.",
    repository: "https://github.com/example/marketing-site",
    default_branch: "main",
    agent_id: null,
    created_at: now,
    updated_at: now,
  },
];

let projects: Project[] = defaultProjects.map((p) => ({ ...p }));
let projectMutation = 1;

const defaultRuns: Record<string, Run[]> = {
  proj_keel: [
    {
      id: "run_1",
      project_id: "proj_keel",
      status: "succeeded",
      summary: "Add i18n foundation",
      started_at: now,
      finished_at: now,
      steps: [
        { id: "s1", label: "Plan", status: "done", timestamp: now },
        { id: "s2", label: "Edit files", status: "done", timestamp: now },
        { id: "s3", label: "Run tests", status: "done", timestamp: now },
      ],
    },
    {
      id: "run_2",
      project_id: "proj_keel",
      status: "awaiting_approval",
      summary: "Rotate CI credentials",
      started_at: now,
      finished_at: null,
      steps: [
        { id: "s1", label: "Plan", status: "done", timestamp: now },
        { id: "s2", label: "Edit files", status: "running", timestamp: now },
      ],
    },
  ],
  proj_marketing: [
    {
      id: "run_marketing_1",
      project_id: "proj_marketing",
      status: "awaiting_approval",
      summary: "Update pricing page copy",
      started_at: now,
      finished_at: null,
      steps: [{ id: "s1", label: "Plan", status: "done", timestamp: now }],
    },
  ],
};

const defaultDiffs: Record<string, DiffFile[]> = {
  run_1: [
    {
      path: "web/src/lib/i18n/en.ts",
      additions: 5,
      deletions: 0,
      hunks: [
        {
          header: "@@ -0,0 +1,5 @@",
          lines: [
            { type: "add", text: "export const en = {" },
            { type: "add", text: '  "app.title": "Keel",' },
            { type: "add", text: "};" },
          ],
        },
      ],
    },
  ],
  run_2: [],
  run_marketing_1: [
    {
      path: "docs/pricing.md",
      additions: 2,
      deletions: 1,
      hunks: [
        {
          header: "@@ -1,1 +1,2 @@",
          lines: [
            { type: "del", text: "Old price: $9/mo" },
            { type: "add", text: "New price: $12/mo" },
            { type: "add", text: "Annual billing now available." },
          ],
        },
      ],
    },
  ],
};

const defaultRunApprovals: Record<string, RunApproval[]> = {
  run_1: [],
  run_2: [
    {
      id: "ra_1",
      run_id: "run_2",
      tool: "credentials_rotate",
      summary: "Rotate the GitHub token used by CI.",
      status: "pending",
    },
  ],
  run_marketing_1: [
    {
      id: "ra_marketing_1",
      run_id: "run_marketing_1",
      tool: "publish_page",
      summary: "Publish the updated pricing page.",
      status: "pending",
    },
  ],
};

let runs: Record<string, Run[]> = Object.fromEntries(
  Object.entries(defaultRuns).map(([k, v]) => [k, v.map((r) => ({ ...r, steps: [...r.steps] }))]),
);
let diffs: Record<string, DiffFile[]> = Object.fromEntries(
  Object.entries(defaultDiffs).map(([k, v]) => [k, v.map((f) => ({ ...f }))]),
);
let runApprovals: Record<string, RunApproval[]> = Object.fromEntries(
  Object.entries(defaultRunApprovals).map(([k, v]) => [k, v.map((a) => ({ ...a }))]),
);

export function resetProjects(): void {
  projects = defaultProjects.map((p) => ({ ...p }));
  projectMutation = 1;
  runs = Object.fromEntries(
    Object.entries(defaultRuns).map(([k, v]) => [k, v.map((r) => ({ ...r, steps: [...r.steps] }))]),
  );
  diffs = Object.fromEntries(Object.entries(defaultDiffs).map(([k, v]) => [k, v.map((f) => ({ ...f }))]));
  runApprovals = Object.fromEntries(
    Object.entries(defaultRunApprovals).map(([k, v]) => [k, v.map((a) => ({ ...a }))]),
  );
}

export const sampleProjects = defaultProjects;

export const handlers = [
  http.get("/v1/approvals", () => HttpResponse.json(pending)),
  http.get("/v1/connectors", () => HttpResponse.json(connectors)),
  http.delete("/v1/connectors/:id", ({ params }) => {
    connectors = connectors.map((c) =>
      c.id === String(params.id) ? { ...c, connected: false, updated_at: null } : c,
    );
    return HttpResponse.json({ ok: true });
  }),
  http.delete("/v1/connectors/:id/purge", ({ params }) => {
    connectors = connectors.map((c) =>
      c.id === String(params.id) ? { ...c, connected: false, updated_at: null } : c,
    );
    return HttpResponse.json({ ok: true });
  }),
  http.delete("/v1/connectors/:id/local", ({ params }) => {
    connectors = connectors.map((c) =>
      c.id === String(params.id) ? { ...c, connected: false, updated_at: null } : c,
    );
    return HttpResponse.json({ ok: true });
  }),
  http.delete("/v1/connectors/:id/purge/local", ({ params }) => {
    connectors = connectors.map((c) =>
      c.id === String(params.id) ? { ...c, connected: false, updated_at: null } : c,
    );
    return HttpResponse.json({ ok: true });
  }),
  http.post("/v1/connectors/:id/setup", async ({ params }) => {
    connectors = connectors.map((c) =>
      c.id === String(params.id)
        ? { ...c, connected: true, health: "healthy", updated_at: new Date().toISOString() }
        : c,
    );
    return HttpResponse.json({
      ok: true,
      binding_id: `${String(params.id)}-binding`,
      artifacts: [],
    });
  }),
  http.post("/v1/connectors/:id/connect-url", ({ params }) =>
    HttpResponse.json({ url: `https://provider.example/${String(params.id)}/consent` }),
  ),
  http.post("/v1/connectors/:id/sync", ({ params }) =>
    HttpResponse.json({ ok: true, job_id: `${String(params.id)}-job`, status: "queued" }),
  ),
  http.get("/v1/sessions", () => HttpResponse.json(sampleSessions)),
  http.get("/v1/sessions/search", ({ request }) => {
    const q = new URL(request.url).searchParams.get("q") ?? "";
    if (!q.trim()) return HttpResponse.json([]);
    const hits = sampleSessions
      .filter((s) => (s.title ?? "").includes(q))
      .map((s) => ({
        id: s.id,
        title: s.title,
        snippet: s.title ?? "",
        messages: s.messages,
        updated_at: s.updated_at,
      }));
    return HttpResponse.json(hits);
  }),
  http.get("/v1/sessions/:id/history", () => HttpResponse.json(sampleHistory)),
  http.get("/v1/settings/model", () =>
    HttpResponse.json({
      current: currentModel,
      available: [
        "github_copilot/claude-sonnet-4.5",
        "github_copilot/gpt-4o",
        "github_copilot/gpt-5.3-codex",
      ],
    }),
  ),
  http.put("/v1/settings/model", async ({ request }) => {
    const body = (await request.json()) as { model: string };
    currentModel = body.model;
    return HttpResponse.json({ ok: true, current: currentModel });
  }),
  http.post("/v1/approvals/:id/approve", ({ params }) => {
    pending = pending.filter((r) => r.id !== String(params.id));
    return HttpResponse.json({ ok: true });
  }),
  http.post("/v1/approvals/:id/reject", ({ params }) => {
    pending = pending.filter((r) => r.id !== String(params.id));
    return HttpResponse.json({ ok: true });
  }),
  http.get("/v1/schedules", () => HttpResponse.json(schedules)),
  http.post("/v1/schedules/:id/toggle", async ({ params, request }) => {
    const body = (await request.json()) as { enabled: boolean };
    schedules = schedules.map((s) =>
      s.id === String(params.id) ? { ...s, enabled: body.enabled } : s,
    );
    return HttpResponse.json({ ok: true, enabled: body.enabled });
  }),
  http.post("/v1/schedules/:id/run", () => HttpResponse.json({ ok: true })),
  http.get("/v1/admin/overview", () =>
    HttpResponse.json({
      sessions: 7,
      schedules: { total: 3, enabled: 2 },
      approvals: { pending: 1, granted: 4, denied: 0, expired: 1 },
      connectors: 2,
      usage: {
        runs: 12,
        prompt_tokens: 100,
        completion_tokens: 20,
        cache_read_tokens: 50,
        cost_usd: 0.012,
      },
    }),
  ),
  http.get("/v1/jobs", () =>
    HttpResponse.json([...jobs.values(), ...knowledgeJobs.values()]),
  ),
  http.post("/v1/jobs/:jobId/cancel", ({ params }) => {
    const id = String(params.jobId);
    const source = jobs.has(id) ? jobs : knowledgeJobs;
    const job = source.get(id);
    if (!job) return HttpResponse.json({ detail: "job not found" }, { status: 404 });
    if (
      (job.status !== "queued" && job.status !== "running") ||
      job.cancel_mode === "disabled"
    ) {
      return HttpResponse.json(
        { detail: "job does not allow cancellation" },
        { status: 409 },
      );
    }
    const updated: Job = { ...job, cancel_requested: true, updated_at: now };
    source.set(id, updated);
    return HttpResponse.json(updated);
  }),
  http.get("/v1/memory/proposals", () => HttpResponse.json(memoryProposals)),
  http.post("/v1/memory/proposals/:proposalId/approve", ({ params }) => {
    const id = String(params.proposalId);
    const proposal = memoryProposals.find((row) => row.id === id);
    if (!proposal) {
      return HttpResponse.json(
        { ok: false, status: "not_found", version: null },
        { status: 404 },
      );
    }
    if (proposal.status !== "pending") {
      return HttpResponse.json(
        { ok: false, status: "already_resolved", version: null },
        { status: 409 },
      );
    }
    memoryProposals = memoryProposals.map((row) =>
      row.id === id
        ? { ...row, status: "applied", resolved_at: now, resolved_by: "web" }
        : row,
    );
    return HttpResponse.json({
      ok: true,
      status: "applied",
      version: proposal.expected_version + 1,
    });
  }),
  http.post("/v1/memory/proposals/:proposalId/reject", ({ params }) => {
    const id = String(params.proposalId);
    const proposal = memoryProposals.find((row) => row.id === id);
    if (!proposal) {
      return HttpResponse.json(
        { ok: false, status: "not_found", version: null },
        { status: 404 },
      );
    }
    if (proposal.status !== "pending") {
      return HttpResponse.json(
        { ok: false, status: "already_resolved", version: null },
        { status: 409 },
      );
    }
    memoryProposals = memoryProposals.map((row) =>
      row.id === id
        ? { ...row, status: "rejected", resolved_at: now, resolved_by: "web" }
        : row,
    );
    return HttpResponse.json({ ok: true, status: "rejected", version: null });
  }),
  http.post("/v1/memory/consolidation/run", () => HttpResponse.json({ ok: true })),
  http.get("/v1/knowledge-bases", () => HttpResponse.json(knowledgeBases)),
  http.post("/v1/knowledge-bases", async ({ request }) => {
    const missing = recordIdempotency(request);
    if (missing) return missing;
    const body = (await request.json()) as { name: string; description?: string | null };
    const base: KnowledgeBase = {
      ...sampleKnowledgeBase,
      id: `kb_01JNEW${String(knowledgeMutation).padStart(20, "0")}`,
      name: body.name,
      description: body.description ?? null,
    };
    knowledgeBases.push(base);
    return HttpResponse.json(base, { status: 201 });
  }),
  http.delete("/v1/knowledge-bases/:kbId", ({ params, request }) => {
    const missing = recordIdempotency(request);
    if (missing) return missing;
    const kbId = String(params.kbId);
    const base = knowledgeBases.find((row) => row.id === kbId) ?? sampleKnowledgeBase;
    knowledgeBases = knowledgeBases.filter((row) => row.id !== kbId);
    const job = nextKnowledgeJob("knowledge.delete");
    return HttpResponse.json({
      base: { ...base, status: "deleted", deleted_at: now },
      job,
      replayed: false,
    });
  }),
  http.get("/v1/knowledge-bases/:kbId/documents", ({ params }) =>
    HttpResponse.json(
      knowledgeDocuments.filter((document) => document.kb_id === String(params.kbId)),
    ),
  ),
  http.get("/v1/knowledge-bases/:kbId/documents/:documentId", ({ params }) => {
    const document = knowledgeDocuments.find((row) => row.id === String(params.documentId));
    if (!document) return HttpResponse.json({ detail: "not found" }, { status: 404 });
    return HttpResponse.json({
      document,
      versions: knowledgeVersions.get(document.id) ?? [],
    });
  }),
  http.post("/v1/knowledge-bases/:kbId/documents", async ({ params, request }) => {
    const missing = recordIdempotency(request);
    if (missing) return missing;
    const body = (await request.json()) as {
      title: string;
      source_type: "text" | "markdown";
      source_uri?: string | null;
      content: string;
    };
    const suffix = String(++knowledgeMutation).padStart(20, "0");
    const versionId = `kbv_01JNEW${suffix}`;
    const document: KnowledgeDocument = {
      ...sampleKnowledgeDocument,
      id: `kbd_01JNEW${suffix}`,
      kb_id: String(params.kbId),
      title: body.title,
      source_type: body.source_type,
      source_uri: body.source_uri ?? null,
      status: "pending",
      desired_version_id: versionId,
      active_version_id: null,
    };
    const version: KnowledgeVersion = {
      ...sampleKnowledgeVersion,
      id: versionId,
      kb_id: document.kb_id,
      document_id: document.id,
      content: body.content,
      status: "pending",
      activated_at: null,
    };
    knowledgeDocuments.push(document);
    knowledgeVersions.set(document.id, [version]);
    const job = nextKnowledgeJob();
    return HttpResponse.json({ document, version, job, replayed: false }, { status: 202 });
  }),
  http.put(
    "/v1/knowledge-bases/:kbId/documents/:documentId",
    async ({ params, request }) => {
      const missing = recordIdempotency(request);
      if (missing) return missing;
      const body = (await request.json()) as {
        title: string;
        source_type: "text" | "markdown";
        source_uri?: string | null;
        content: string;
      };
      const current = knowledgeDocuments.find((row) => row.id === String(params.documentId));
      if (!current) return HttpResponse.json({ detail: "not found" }, { status: 404 });
      const version: KnowledgeVersion = {
        ...sampleKnowledgeVersion,
        id: `kbv_01JNEW${String(++knowledgeMutation).padStart(20, "0")}`,
        kb_id: String(params.kbId),
        document_id: current.id,
        version: (knowledgeVersions.get(current.id)?.length ?? 0) + 1,
        content: body.content,
        status: "pending",
        activated_at: null,
      };
      const document = {
        ...current,
        title: body.title,
        source_type: body.source_type,
        source_uri: body.source_uri ?? null,
        status: "pending" as const,
        desired_version_id: version.id,
      };
      knowledgeDocuments = knowledgeDocuments.map((row) =>
        row.id === document.id ? document : row,
      );
      knowledgeVersions.set(document.id, [version, ...(knowledgeVersions.get(document.id) ?? [])]);
      const job = nextKnowledgeJob();
      return HttpResponse.json({ document, version, job, replayed: false }, { status: 202 });
    },
  ),
  http.post(
    "/v1/knowledge-bases/:kbId/documents/:documentId/reindex",
    ({ params, request }) => {
      const missing = recordIdempotency(request);
      if (missing) return missing;
      const document = knowledgeDocuments.find((row) => row.id === String(params.documentId));
      if (!document) return HttpResponse.json({ detail: "not found" }, { status: 404 });
      const previous = knowledgeVersions.get(document.id)?.[0] ?? sampleKnowledgeVersion;
      const version = {
        ...previous,
        id: `kbv_01JNEW${String(++knowledgeMutation).padStart(20, "0")}`,
        version: previous.version + 1,
        status: "pending" as const,
        activated_at: null,
      };
      knowledgeVersions.set(document.id, [version, ...(knowledgeVersions.get(document.id) ?? [])]);
      const job = nextKnowledgeJob();
      return HttpResponse.json({ document, version, job, replayed: false }, { status: 202 });
    },
  ),
  http.delete(
    "/v1/knowledge-bases/:kbId/documents/:documentId",
    ({ params, request }) => {
      const missing = recordIdempotency(request);
      if (missing) return missing;
      const document =
        knowledgeDocuments.find((row) => row.id === String(params.documentId)) ??
        sampleKnowledgeDocument;
      knowledgeDocuments = knowledgeDocuments.filter((row) => row.id !== document.id);
      const job = nextKnowledgeJob("knowledge.delete");
      return HttpResponse.json({
        document: { ...document, status: "deleted", deleted_at: now },
        job,
        replayed: false,
      });
    },
  ),
  http.get("/v1/jobs/:jobId", ({ params }) => {
    const id = String(params.jobId);
    const job = jobs.get(id) ?? knowledgeJobs.get(id);
    return job
      ? HttpResponse.json(job)
      : HttpResponse.json({ detail: "not found" }, { status: 404 });
  }),
  http.get("/v1/knowledge-bases/:kbId/search", ({ request }) => {
    const query = new URL(request.url).searchParams.get("q") ?? "";
    return HttpResponse.json({
      hits: query
        ? [
            {
              snippet: "Customers may request a refund within 30 days.",
              rank: 1,
              heading_path: ["Policies", "Refunds"],
              citation: {
                id: "kbc_01JZZZZZZZZZZZZZZZZZZZZZZZ",
                kb_id: sampleKnowledgeBase.id,
                document_id: sampleKnowledgeDocument.id,
                document_version_id: sampleKnowledgeVersion.id,
                chunk_id: "kbch_01JZZZZZZZZZZZZZZZZZZZZZZ",
                title: "Refund policy",
                source_uri: "https://example.com/handbook/refunds",
                ordinal: 2,
                char_start: 120,
                char_end: 184,
                label: "Refund policy § Refunds",
              },
            },
          ]
        : [],
      status: {
        mode: "lexical-degraded",
        semantic_error: "embedding_unavailable",
      },
    });
  }),

  http.get("/v1/agents", () => HttpResponse.json(agents)),
  http.post("/v1/agents", async ({ request }) => {
    const body = (await request.json()) as {
      name: string;
      description: string;
      model: string;
      tools: string[];
    };
    const agent: Agent = {
      id: `agent_new_${++agentMutation}`,
      name: body.name,
      description: body.description,
      model: body.model,
      tools: body.tools,
      active: false,
      created_at: now,
      updated_at: now,
    };
    agents = [...agents, agent];
    return HttpResponse.json(agent, { status: 201 });
  }),
  http.put("/v1/agents/:id", async ({ params, request }) => {
    const id = String(params.id);
    const existing = agents.find((a) => a.id === id);
    if (!existing) return HttpResponse.json({ detail: "not found" }, { status: 404 });
    const body = (await request.json()) as {
      name: string;
      description: string;
      model: string;
      tools: string[];
    };
    const updated: Agent = { ...existing, ...body, updated_at: now };
    agents = agents.map((a) => (a.id === id ? updated : a));
    return HttpResponse.json(updated);
  }),
  http.delete("/v1/agents/:id", ({ params }) => {
    const id = String(params.id);
    agents = agents.filter((a) => a.id !== id);
    return HttpResponse.json({ ok: true });
  }),
  http.post("/v1/agents/:id/activate", ({ params }) => {
    const id = String(params.id);
    const target = agents.find((a) => a.id === id);
    if (!target) return HttpResponse.json({ detail: "not found" }, { status: 404 });
    agents = agents.map((a) => ({ ...a, active: a.id === id, updated_at: now }));
    return HttpResponse.json(agents.find((a) => a.id === id));
  }),

  http.get("/v1/projects", () => HttpResponse.json(projects)),
  http.get("/v1/projects/:id", ({ params }) => {
    const project = projects.find((p) => p.id === String(params.id));
    return project
      ? HttpResponse.json(project)
      : HttpResponse.json({ detail: "not found" }, { status: 404 });
  }),
  http.post("/v1/projects", async ({ request }) => {
    const body = (await request.json()) as {
      name: string;
      repository: string;
      default_branch: string;
    };
    const project: Project = {
      id: `proj_new_${++projectMutation}`,
      name: body.name,
      description: "",
      repository: body.repository,
      default_branch: body.default_branch || "main",
      agent_id: null,
      created_at: now,
      updated_at: now,
    };
    projects = [...projects, project];
    runs[project.id] = [];
    return HttpResponse.json(project, { status: 201 });
  }),
  http.delete("/v1/projects/:id", ({ params }) => {
    const id = String(params.id);
    projects = projects.filter((p) => p.id !== id);
    return HttpResponse.json({ ok: true });
  }),
  http.get("/v1/projects/:id/runs", ({ params }) =>
    HttpResponse.json(runs[String(params.id)] ?? []),
  ),
  http.get("/v1/runs/:id/diff", ({ params }) => HttpResponse.json(diffs[String(params.id)] ?? [])),
  http.get("/v1/runs/:id/approvals", ({ params }) =>
    HttpResponse.json(runApprovals[String(params.id)] ?? []),
  ),
  http.post("/v1/runs/:runId/approvals/:approvalId/approve", ({ params }) => {
    const runId = String(params.runId);
    const approvalId = String(params.approvalId);
    const list = runApprovals[runId] ?? [];
    runApprovals[runId] = list.map((a) =>
      a.id === approvalId ? { ...a, status: "approved" } : a,
    );
    const updated = runApprovals[runId].find((a) => a.id === approvalId);
    return updated
      ? HttpResponse.json(updated)
      : HttpResponse.json({ detail: "not found" }, { status: 404 });
  }),
  http.post("/v1/runs/:runId/approvals/:approvalId/reject", ({ params }) => {
    const runId = String(params.runId);
    const approvalId = String(params.approvalId);
    const list = runApprovals[runId] ?? [];
    runApprovals[runId] = list.map((a) =>
      a.id === approvalId ? { ...a, status: "rejected" } : a,
    );
    const updated = runApprovals[runId].find((a) => a.id === approvalId);
    return updated
      ? HttpResponse.json(updated)
      : HttpResponse.json({ detail: "not found" }, { status: 404 });
  }),
];
