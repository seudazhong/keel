import { http, HttpResponse } from "msw";
import type { Approval } from "../features/approvals/types";
import type { SseEvent } from "../features/chat/types";
import type { Connector } from "../features/connectors/types";
import type {
  Job,
  KnowledgeBase,
  KnowledgeDocument,
  KnowledgeVersion,
} from "../features/knowledge/types";
import type { Schedule } from "../features/schedules/types";
import type { SessionSummary } from "../features/sessions/types";

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
  {
    id: "gmail",
    name: "Gmail",
    icon: "✉️",
    kind: "oauth",
    scopes: ["gmail.readonly", "gmail.send"],
    connected: true,
    updated_at: "2026-07-08T04:19:55Z",
  },
  {
    id: "calendar",
    name: "Google Calendar",
    icon: "📅",
    kind: "oauth",
    scopes: ["calendar.events"],
    connected: false,
    updated_at: null,
  },
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

export const handlers = [
  http.get("/v1/approvals", () => HttpResponse.json(pending)),
  http.get("/v1/connectors", () => HttpResponse.json(connectors)),
  http.delete("/v1/connectors/:id", ({ params }) => {
    connectors = connectors.map((c) =>
      c.id === String(params.id) ? { ...c, connected: false, updated_at: null } : c,
    );
    return HttpResponse.json({ ok: true });
  }),
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
    const job = knowledgeJobs.get(String(params.jobId));
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
];
