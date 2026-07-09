import { http, HttpResponse } from "msw";
import type { Approval } from "../features/approvals/types";
import type { SseEvent } from "../features/chat/types";
import type { Connector } from "../features/connectors/types";
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
];
