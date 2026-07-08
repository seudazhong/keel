import type { ChatItem, ChatState, SseEvent } from "./types";

export type { ChatItem, ChatState, SseEvent } from "./types";

export const initialChatState: ChatState = { items: [], running: false, lastSeq: 0 };

let _idCounter = 0;
function nextId(): string {
  _idCounter += 1;
  return `i${_idCounter}`;
}

/** Append a user message and mark the run as starting. */
export function userSent(state: ChatState, text: string): ChatState {
  return {
    ...state,
    items: [...state.items, { kind: "user", id: nextId(), text }],
    running: true,
    reason: undefined,
  };
}

/** Fold a streaming assistant token into the trailing bubble (or start one). */
function applyAssistantToken(items: ChatItem[], text: string, partial: boolean): ChatItem[] {
  const last = items[items.length - 1];
  if (last && last.kind === "assistant" && last.streaming) {
    const updated: ChatItem = partial
      ? { ...last, text: last.text + text }
      : { ...last, text, streaming: false };
    return [...items.slice(0, -1), updated];
  }
  return [...items, { kind: "assistant", id: nextId(), text, streaming: partial }];
}

/**
 * Reduce one SSE event into the chat state (ports keel_server/webui.py handleEvent).
 * Only events with a truthy ``seq`` advance the replay cursor; partial deltas (seq 0)
 * accumulate but never move it. ``includeUser`` renders user ``message.token`` events as
 * user bubbles — off for live chat (``userSent`` already added them + the SSE echoes
 * them), on for durable replay (:func:`foldHistory`).
 */
export function applyEvent(state: ChatState, ev: SseEvent, includeUser = false): ChatState {
  const lastSeq = ev.seq ? ev.seq : state.lastSeq;
  const p = ev.payload ?? {};
  switch (ev.type) {
    case "message.token": {
      const text = typeof p.text === "string" ? p.text : "";
      if (p.role === "assistant") {
        return { ...state, lastSeq, items: applyAssistantToken(state.items, text, p.partial === true) };
      }
      if (p.role === "user" && includeUser) {
        return { ...state, lastSeq, items: [...state.items, { kind: "user", id: nextId(), text }] };
      }
      return { ...state, lastSeq };
    }
    case "tool.call": {
      const item: ChatItem = {
        kind: "tool",
        id: nextId(),
        callId: String(p.call_id ?? ""),
        tool: String(p.tool ?? ""),
        args: p.args,
      };
      return { ...state, lastSeq, items: [...state.items, item] };
    }
    case "tool.result": {
      const callId = String(p.call_id ?? "");
      const result = { ok: p.ok === true, output: typeof p.output === "string" ? p.output : "" };
      return {
        ...state,
        lastSeq,
        items: state.items.map((it) =>
          it.kind === "tool" && it.callId === callId ? { ...it, result } : it,
        ),
      };
    }
    case "approval.requested": {
      const item: ChatItem = {
        kind: "approval",
        id: nextId(),
        approvalId: String(p.approval_id ?? ""),
        tool: String(p.tool ?? ""),
        args: p.args && typeof p.args === "object" ? (p.args as Record<string, unknown>) : {},
      };
      return { ...state, lastSeq, items: [...state.items, item] };
    }
    case "approval.resolved": {
      const approvalId = String(p.approval_id ?? "");
      const resolved: "allow" | "deny" = p.approved === true ? "allow" : "deny";
      return {
        ...state,
        lastSeq,
        items: state.items.map((it) =>
          it.kind === "approval" && it.approvalId === approvalId ? { ...it, resolved } : it,
        ),
      };
    }
    case "run.started":
      return { ...state, lastSeq, running: true, reason: undefined };
    case "run.ended":
      return { ...state, lastSeq, running: false, reason: String(p.reason ?? "") };
    case "error":
      return {
        ...state,
        lastSeq,
        items: [
          ...state.items,
          { kind: "meta", id: nextId(), tone: "error", text: `! ${String(p.message ?? "error")}` },
        ],
      };
    default:
      // turn.*, message.thinking, run.suspended/resumed: no visible item in this slice.
      return { ...state, lastSeq };
  }
}

/** Fold a session's durable event log into a thread for read-only replay (renders user
 * messages too, unlike live chat where the composer already shows them). */
export function foldHistory(events: SseEvent[]): ChatState {
  return events.reduce((state, ev) => applyEvent(state, ev, true), initialChatState);
}
