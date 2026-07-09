import type { SseEvent } from "./types";

export function eventsUrl(sessionId: string, after: number): string {
  return `/v1/sessions/${sessionId}/events?after=${after}`;
}

export async function postMessage(
  sessionId: string,
  content: string,
): Promise<{ session_id: string; run_id: string }> {
  const res = await fetch(`/v1/sessions/${sessionId}/messages`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return (await res.json()) as { session_id: string; run_id: string };
}

export async function resolveApproval(
  approvalId: string,
  decision: "allow" | "deny",
): Promise<void> {
  const res = await fetch(`/v1/approvals/${approvalId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ approval_id: approvalId, decision }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

export async function interruptRun(runId: string): Promise<void> {
  const res = await fetch(`/v1/runs/${runId}/interrupt`, { method: "POST" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

/** Open the SSE stream for a session; caller closes it (on run.ended or unmount). */
export function openEvents(
  sessionId: string,
  after: number,
  onEvent: (ev: SseEvent) => void,
  onError: () => void,
): EventSource {
  const es = new EventSource(eventsUrl(sessionId, after));
  es.onmessage = (e: MessageEvent) => onEvent(JSON.parse(e.data as string) as SseEvent);
  es.onerror = () => onError();
  return es;
}
