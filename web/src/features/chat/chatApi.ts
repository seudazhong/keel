import { authHeaders } from "../auth/authState";
import { openSse, type SseHandle } from "./sseClient";
import type { SseEvent } from "./types";

export function eventsUrl(sessionId: string, after: number): string {
  return `/v1/sessions/${sessionId}/events?after=${after}`;
}

function jsonHeaders(): HeadersInit {
  return { "Content-Type": "application/json", ...authHeaders() };
}

export async function postMessage(
  sessionId: string,
  content: string,
): Promise<{ session_id: string; run_id: string }> {
  const res = await fetch(`/v1/sessions/${sessionId}/messages`, {
    method: "POST",
    headers: jsonHeaders(),
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
    headers: jsonHeaders(),
    body: JSON.stringify({ approval_id: approvalId, decision }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

export async function interruptRun(runId: string): Promise<void> {
  const res = await fetch(`/v1/runs/${runId}/interrupt`, {
    method: "POST",
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

/**
 * Open the authenticated SSE stream for a session; caller closes it (on run.ended or unmount).
 *
 * Uses the `fetch`/`ReadableStream` client (not the header-less native `EventSource`) so the
 * credential + workspace headers reach the server, reconnecting with `Last-Event-ID` and
 * de-duplicating replayed events. Malformed frames are skipped rather than throwing.
 */
export function openEvents(
  sessionId: string,
  after: number,
  onEvent: (ev: SseEvent) => void,
  onError: () => void,
): SseHandle {
  return openSse(eventsUrl(sessionId, after), {
    lastEventId: after > 0 ? String(after) : null,
    onError,
    onMessage: (message) => {
      if (!message.data) return;
      try {
        onEvent(JSON.parse(message.data) as SseEvent);
      } catch {
        // A partial/keep-alive frame that is not JSON: ignore rather than break the stream.
      }
    },
  });
}
