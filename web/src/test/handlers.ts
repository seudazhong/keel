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

// Stateful like the real backend: approve/reject removes the row so a refetch reconciles.
let pending: Approval[] = [sampleApproval];

export function resetApprovals(): void {
  pending = [sampleApproval];
}

export const handlers = [
  http.get("/v1/approvals", () => HttpResponse.json(pending)),
  http.post("/v1/approvals/:id/approve", ({ params }) => {
    pending = pending.filter((r) => r.id !== String(params.id));
    return HttpResponse.json({ ok: true });
  }),
  http.post("/v1/approvals/:id/reject", ({ params }) => {
    pending = pending.filter((r) => r.id !== String(params.id));
    return HttpResponse.json({ ok: true });
  }),
];
