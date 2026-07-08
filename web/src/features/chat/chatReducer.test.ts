import { describe, expect, it } from "vitest";
import { applyEvent, foldHistory, initialChatState, userSent, type SseEvent } from "./chatReducer";

const ev = (type: string, payload: Record<string, unknown>, seq = 0): SseEvent => ({
  type,
  seq,
  session_id: "s",
  run_id: "r",
  ts: "",
  payload,
});

describe("chatReducer", () => {
  it("streams assistant deltas then finalizes", () => {
    let s = userSent(initialChatState, "hi");
    s = applyEvent(s, ev("run.started", {}, 1));
    s = applyEvent(s, ev("message.token", { role: "assistant", text: "He", partial: true }));
    s = applyEvent(s, ev("message.token", { role: "assistant", text: "llo", partial: true }));
    expect(s.items.at(-1)).toMatchObject({ kind: "assistant", text: "Hello", streaming: true });
    s = applyEvent(s, ev("message.token", { role: "assistant", text: "Hello world" }, 5));
    expect(s.items.at(-1)).toMatchObject({
      kind: "assistant",
      text: "Hello world",
      streaming: false,
    });
    expect(s.lastSeq).toBe(5); // only seq>0 advances the cursor
  });

  it("ignores non-assistant message tokens", () => {
    const s = applyEvent(initialChatState, ev("message.token", { role: "user", text: "x" }, 2));
    expect(s.items.filter((i) => i.kind === "assistant")).toHaveLength(0);
  });

  it("pairs tool.call with tool.result by callId", () => {
    let s = applyEvent(
      initialChatState,
      ev("tool.call", { tool: "read", args: { path: "a" }, call_id: "c1" }, 3),
    );
    s = applyEvent(s, ev("tool.result", { call_id: "c1", ok: true, output: "data" }, 4));
    expect(s.items).toContainEqual(
      expect.objectContaining({
        kind: "tool",
        callId: "c1",
        tool: "read",
        result: { ok: true, output: "data" },
      }),
    );
  });

  it("tracks approval request then resolution", () => {
    let s = applyEvent(
      initialChatState,
      ev("approval.requested", { approval_id: "a1", tool: "write", args: { path: "x" }, call_id: "c2" }, 6),
    );
    expect(s.items.at(-1)).toMatchObject({ kind: "approval", approvalId: "a1" });
    expect((s.items.at(-1) as { resolved?: string }).resolved).toBeUndefined();
    s = applyEvent(s, ev("approval.resolved", { approval_id: "a1", approved: true }, 7));
    expect(s.items.find((i) => i.kind === "approval")).toMatchObject({ resolved: "allow" });
  });

  it("ends the run with a reason and clears running", () => {
    let s = { ...initialChatState, running: true };
    s = applyEvent(s, ev("run.ended", { reason: "completed" }, 9));
    expect(s.running).toBe(false);
    expect(s.reason).toBe("completed");
  });

  it("appends error meta items", () => {
    const s = applyEvent(initialChatState, ev("error", { message: "boom" }, 8));
    expect(s.items.at(-1)).toMatchObject({
      kind: "meta",
      tone: "error",
      text: expect.stringContaining("boom"),
    });
  });

  it("foldHistory renders user messages for replay", () => {
    const s = foldHistory([
      ev("message.token", { role: "user", text: "hi" }, 1),
      ev("message.token", { role: "assistant", text: "hello" }, 2),
    ]);
    expect(s.items.map((i) => i.kind)).toEqual(["user", "assistant"]);
    expect(s.items[0]).toMatchObject({ kind: "user", text: "hi" });
  });
});
