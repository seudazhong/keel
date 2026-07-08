import { http, HttpResponse } from "msw";
import type { Approval } from "../features/approvals/types";

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

export const handlers = [
  http.get("/v1/approvals", () => HttpResponse.json([sampleApproval])),
  http.post("/v1/approvals/:id/approve", () => HttpResponse.json({ ok: true })),
  http.post("/v1/approvals/:id/reject", () => HttpResponse.json({ ok: true })),
];
