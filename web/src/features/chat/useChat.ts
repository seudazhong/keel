import { useCallback, useEffect, useReducer, useRef } from "react";
import { interruptRun, openEvents, postMessage, resolveApproval } from "./chatApi";
import { applyEvent, initialChatState, userSent } from "./chatReducer";
import type { ChatItem, ChatState, SseEvent } from "./types";

type Action =
  | { kind: "user"; text: string }
  | { kind: "event"; ev: SseEvent }
  | { kind: "approval"; approvalId: string; decision: "allow" | "deny" };

function reduce(state: ChatState, action: Action): ChatState {
  switch (action.kind) {
    case "user":
      return userSent(state, action.text);
    case "event":
      return applyEvent(state, action.ev);
    case "approval":
      // Optimistic local resolution; the SSE approval.resolved event confirms it.
      return {
        ...state,
        items: state.items.map((it) =>
          it.kind === "approval" && it.approvalId === action.approvalId
            ? { ...it, resolved: action.decision }
            : it,
        ),
      };
  }
}

function synthetic(type: string, payload: Record<string, unknown>): SseEvent {
  return { type, seq: 0, session_id: "", run_id: null, ts: "", payload };
}

export function useChat(): {
  items: ChatItem[];
  running: boolean;
  reason?: string;
  usage: ChatState["usage"];
  send: (text: string) => void;
  resolve: (approvalId: string, decision: "allow" | "deny") => void;
  interrupt: () => void;
} {
  const [state, dispatch] = useReducer(reduce, initialChatState);

  const sessionRef = useRef<string>("");
  if (!sessionRef.current) sessionRef.current = crypto.randomUUID();
  const esRef = useRef<EventSource | null>(null);
  const runIdRef = useRef<string | null>(null);
  const lastSeqRef = useRef(0);
  useEffect(() => {
    lastSeqRef.current = state.lastSeq;
  }, [state.lastSeq]);

  const closeStream = useCallback(() => {
    esRef.current?.close();
    esRef.current = null;
  }, []);

  const send = useCallback(
    (text: string) => {
      dispatch({ kind: "user", text });
      void (async () => {
        try {
          const { run_id } = await postMessage(sessionRef.current, text);
          runIdRef.current = run_id;
        } catch {
          dispatch({ kind: "event", ev: synthetic("error", { message: "发送失败" }) });
          dispatch({ kind: "event", ev: synthetic("run.ended", { reason: "error" }) });
          return;
        }
        closeStream();
        esRef.current = openEvents(
          sessionRef.current,
          lastSeqRef.current,
          (ev) => {
            dispatch({ kind: "event", ev });
            if (ev.type === "run.ended") closeStream();
          },
          () => undefined, // browser auto-retries; run.ended closes us cleanly
        );
      })();
    },
    [closeStream],
  );

  const resolve = useCallback((approvalId: string, decision: "allow" | "deny") => {
    dispatch({ kind: "approval", approvalId, decision });
    void resolveApproval(approvalId, decision).catch(() => undefined);
  }, []);

  const interrupt = useCallback(() => {
    const runId = runIdRef.current;
    if (runId) void interruptRun(runId).catch(() => undefined);
  }, []);

  useEffect(() => () => closeStream(), [closeStream]);

  return {
    items: state.items,
    running: state.running,
    reason: state.reason,
    usage: state.usage,
    send,
    resolve,
    interrupt,
  };
}
