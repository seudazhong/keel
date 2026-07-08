export interface SseEvent {
  type: string;
  seq: number;
  session_id: string;
  run_id: string | null;
  ts: string;
  payload: Record<string, unknown>;
}

export type ChatItem =
  | { kind: "user"; id: string; text: string }
  | { kind: "assistant"; id: string; text: string; streaming: boolean }
  | {
      kind: "tool";
      id: string;
      callId: string;
      tool: string;
      args: unknown;
      result?: { ok: boolean; output: string };
    }
  | {
      kind: "approval";
      id: string;
      approvalId: string;
      tool: string;
      args: Record<string, unknown>;
      resolved?: "allow" | "deny";
    }
  | { kind: "meta"; id: string; text: string; tone?: "error" };

export interface ChatState {
  items: ChatItem[];
  running: boolean;
  lastSeq: number;
  reason?: string;
}
