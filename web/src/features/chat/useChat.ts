import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { getHistory, interruptRun, openEvents, postMessage, resolveApproval } from "./chatApi";
import type { SseHandle } from "./sseClient";
import { applyEvent, foldHistory, initialChatState, userSent } from "./chatReducer";
import type { ChatItem, ChatState, SseEvent } from "./types";

type Action =
  | { kind: "user"; text: string }
  | { kind: "event"; ev: SseEvent }
  | { kind: "hydrate"; state: ChatState }
  | { kind: "reset" }
  | { kind: "approval"; approvalId: string; decision: "allow" | "deny" };

function reduce(state: ChatState, action: Action): ChatState {
  switch (action.kind) {
    case "user":
      return userSent(state, action.text);
    case "event":
      return applyEvent(state, action.ev);
    case "hydrate":
      return action.state;
    case "reset":
      return initialChatState;
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

export function useChat(sessionId: string): {
  items: ChatItem[];
  running: boolean;
  loading: boolean;
  reason?: string;
  usage: ChatState["usage"];
  send: (text: string) => void;
  resolve: (approvalId: string, decision: "allow" | "deny") => void;
  interrupt: () => void;
} {
  const [state, dispatch] = useReducer(reduce, initialChatState);
  const [loading, setLoading] = useState(true);
  const esRef = useRef<SseHandle | null>(null);
  const postRef = useRef<AbortController | null>(null);
  const generationRef = useRef(0);
  const runIdRef = useRef<string | null>(null);
  const lastSeqRef = useRef(0);
  useEffect(() => {
    lastSeqRef.current = state.lastSeq;
  }, [state.lastSeq]);

  const closeStream = useCallback(() => {
    esRef.current?.close();
    esRef.current = null;
  }, []);

  const openStream = useCallback(
    (after: number, generation = generationRef.current) => {
      closeStream();
      esRef.current = openEvents(
        sessionId,
        after,
        (ev) => {
          if (generation !== generationRef.current) return;
          dispatch({ kind: "event", ev });
          if (ev.run_id) runIdRef.current = ev.run_id;
          if (ev.type === "run.ended") closeStream();
        },
        () => undefined,
      );
    },
    [closeStream, sessionId],
  );

  useEffect(() => {
    const generation = generationRef.current + 1;
    generationRef.current = generation;
    const controller = new AbortController();
    postRef.current?.abort();
    postRef.current = null;
    closeStream();
    runIdRef.current = null;
    lastSeqRef.current = 0;
    dispatch({ kind: "reset" });
    setLoading(true);
    void getHistory(sessionId, controller.signal)
      .then((events) => {
        if (generation !== generationRef.current) return;
        const hydrated = foldHistory(events);
        lastSeqRef.current = hydrated.lastSeq;
        runIdRef.current =
          [...events].reverse().find((event) => event.run_id !== null)?.run_id ?? null;
        dispatch({ kind: "hydrate", state: hydrated });
        if (hydrated.running) openStream(hydrated.lastSeq, generation);
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        if (generation !== generationRef.current) return;
        dispatch({
          kind: "event",
          ev: synthetic("error", { message: "加载会话历史失败" }),
        });
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => {
      controller.abort();
      postRef.current?.abort();
      closeStream();
    };
  }, [closeStream, openStream, sessionId]);

  const send = useCallback(
    (text: string) => {
      const generation = generationRef.current;
      const controller = new AbortController();
      postRef.current?.abort();
      postRef.current = controller;
      dispatch({ kind: "user", text });
      void (async () => {
        try {
          const { run_id } = await postMessage(sessionId, text, controller.signal);
          if (generation !== generationRef.current) return;
          runIdRef.current = run_id;
        } catch {
          if (controller.signal.aborted || generation !== generationRef.current) return;
          dispatch({ kind: "event", ev: synthetic("error", { message: "发送失败" }) });
          dispatch({ kind: "event", ev: synthetic("run.ended", { reason: "error" }) });
          return;
        }
        openStream(lastSeqRef.current, generation);
        if (postRef.current === controller) postRef.current = null;
      })();
    },
    [openStream, sessionId],
  );

  const resolve = useCallback((approvalId: string, decision: "allow" | "deny") => {
    dispatch({ kind: "approval", approvalId, decision });
    void resolveApproval(approvalId, decision).catch(() => undefined);
  }, []);

  const interrupt = useCallback(() => {
    const runId = runIdRef.current;
    if (runId) void interruptRun(runId).catch(() => undefined);
  }, []);

  return {
    items: state.items,
    running: state.running,
    loading,
    reason: state.reason,
    usage: state.usage,
    send,
    resolve,
    interrupt,
  };
}
