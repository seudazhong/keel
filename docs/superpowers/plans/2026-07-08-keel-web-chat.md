# Web Chat Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A streaming Chat page in the React app (`web/`) — the primary product surface (M1.7) — with live token streaming, a tool/step timeline, and inline interactive approvals for mutating tools.

**Architecture:** The backend contract already exists (`packages/keel-server/src/keel_server/api/v1.py`): `POST /v1/sessions/{id}/messages {content}` admits a message + launches an in-process run; `GET /v1/sessions/{id}/events?after={seq}` streams the run as SSE (`text/event-stream`), closing on `run.ended`. A working vanilla-JS reference is `keel_server/webui.py` (`handleEvent`). We port its event→UI mapping into a **pure reducer** (`applyEvent`) that is thoroughly unit-tested, plus a thin hook (`useChat`) that wires `EventSource` → reducer, and presentational components using the existing design system. Interactive (chat) approvals resolve via `POST /v1/approvals/{approval_id} {approval_id, decision}` (distinct from the durable-approval `/approve|/reject` paths).

**Tech Stack:** Vite + React 19 + TS strict + Tailwind v3 + native `EventSource`; Vitest + RTL. No new deps.

## Global Constraints

- oxlint clean (`npm run lint`); `tsc` strict; `vitest run` green. No new dependencies.
- Reuse existing UI primitives (`components/ui/{button,card,badge,banner,chip,skeleton}`) + `Topbar`; design tokens already ported.
- SSE event vocabulary (from `keel_core/events.py`, string values): `run.started`, `run.ended`, `turn.started`, `turn.ended`, `message.token`, `message.thinking`, `tool.call`, `tool.result`, `approval.requested`, `approval.resolved`, `run.suspended`, `run.resumed`, `error`.
- Delta semantics (from `webui.py`): `message.token` with `payload.partial === true` is an **increment** (append); a `message.token` without `partial` carries the **full** text (finalize). Only events with `seq > 0` advance the replay cursor; partial deltas have `seq === 0`.
- Scope: slice 1 = chat core. **Defer** (not in this plan): sessions rail (slice 3), context sidebar (tokens/cost/connectors/memory), and steer/follow-up/interrupt (FR-C3, not exposed by the backend yet).

---

## File Structure (all under `web/src/`)

- `features/chat/types.ts` — `SseEvent`, `ChatItem` union, `ChatState`.
- `features/chat/chatReducer.ts` — `initialChatState`, `userSent`, `applyEvent` (the tested heart).
- `features/chat/chatApi.ts` — `postMessage`, `resolveApproval`, `eventsUrl`, `openEvents`.
- `features/chat/useChat.ts` — hook wiring reducer + api + `EventSource`.
- `features/chat/MessageBubble.tsx`, `ToolStep.tsx`, `InlineApproval.tsx`, `Composer.tsx`, `RunBar.tsx` — presentational.
- `features/chat/ChatPage.tsx` — assembles the page.
- `components/Sidebar.tsx` — enable the Chat nav item (`to: "/chat"`).
- `router.tsx` — add `/chat` route; make it the index.
- Tests: `chatReducer.test.ts`, `chatApi.test.ts`, `chat.components.test.tsx`.

---

### Task 1: Route + nav

**Files:** Modify `web/src/router.tsx`, `web/src/components/Sidebar.tsx`, `web/src/components/AppShell.test.tsx` (if it asserts the index redirect).

**Interfaces:** Produces the `/chat` route rendering `ChatPage`.

- [ ] Add a temporary `ChatPage` stub (`export function ChatPage() { return <div>chat</div>; }`) so the route compiles; real impl in Task 6.
- [ ] `router.tsx`: `{ index: true, element: <Navigate to="/chat" replace /> }` and `{ path: "chat", element: <ChatPage /> }` (keep `/approvals`).
- [ ] `Sidebar.tsx`: give the Chat item `to: "/chat"`.
- [ ] Run `npm run test -- --run` + `npm run lint`; fix any AppShell test asserting the old index target.
- [ ] Commit.

### Task 2: types + reducer (the heart)

**Files:** Create `web/src/features/chat/types.ts`, `web/src/features/chat/chatReducer.ts`, `web/src/features/chat/chatReducer.test.ts`.

**Interfaces — Produces:**
```ts
export interface SseEvent { type: string; seq: number; session_id: string; run_id: string | null; ts: string; payload: Record<string, unknown>; }
export type ChatItem =
  | { kind: "user"; id: string; text: string }
  | { kind: "assistant"; id: string; text: string; streaming: boolean }
  | { kind: "tool"; id: string; callId: string; tool: string; args: unknown; result?: { ok: boolean; output: string } }
  | { kind: "approval"; id: string; approvalId: string; tool: string; args: Record<string, unknown>; resolved?: "allow" | "deny" }
  | { kind: "meta"; id: string; text: string; tone?: "error" };
export interface ChatState { items: ChatItem[]; running: boolean; lastSeq: number; reason?: string; }
export const initialChatState: ChatState;
export function userSent(state: ChatState, text: string): ChatState; // append user item, running=true, reason=undefined
export function applyEvent(state: ChatState, ev: SseEvent): ChatState;
```

- [ ] **Write failing tests** in `chatReducer.test.ts` covering:
```ts
import { describe, expect, it } from "vitest";
import { applyEvent, initialChatState, userSent, type SseEvent } from "./chatReducer";

const ev = (type: string, payload: Record<string, unknown>, seq = 0): SseEvent =>
  ({ type, seq, session_id: "s", run_id: "r", ts: "", payload });

describe("chatReducer", () => {
  it("streams assistant deltas then finalizes", () => {
    let s = userSent(initialChatState, "hi");
    s = applyEvent(s, ev("run.started", {}, 1));
    s = applyEvent(s, ev("message.token", { role: "assistant", text: "He", partial: true }));
    s = applyEvent(s, ev("message.token", { role: "assistant", text: "llo", partial: true }));
    const streaming = s.items.at(-1);
    expect(streaming).toMatchObject({ kind: "assistant", text: "Hello", streaming: true });
    s = applyEvent(s, ev("message.token", { role: "assistant", text: "Hello world" }, 5));
    expect(s.items.at(-1)).toMatchObject({ kind: "assistant", text: "Hello world", streaming: false });
    expect(s.lastSeq).toBe(5); // only seq>0 advances cursor
  });

  it("ignores non-assistant message tokens", () => {
    let s = applyEvent(initialChatState, ev("message.token", { role: "user", text: "x" }, 2));
    expect(s.items.filter((i) => i.kind === "assistant")).toHaveLength(0);
  });

  it("pairs tool.call with tool.result by callId", () => {
    let s = applyEvent(initialChatState, ev("tool.call", { tool: "read", args: { path: "a" }, call_id: "c1" }, 3));
    s = applyEvent(s, ev("tool.result", { call_id: "c1", ok: true, output: "data" }, 4));
    expect(s.items).toContainEqual(expect.objectContaining({ kind: "tool", callId: "c1", tool: "read", result: { ok: true, output: "data" } }));
  });

  it("tracks approval request then resolution", () => {
    let s = applyEvent(initialChatState, ev("approval.requested", { approval_id: "a1", tool: "write", args: { path: "x" }, call_id: "c2" }, 6));
    expect(s.items.at(-1)).toMatchObject({ kind: "approval", approvalId: "a1", resolved: undefined });
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
    expect(s.items.at(-1)).toMatchObject({ kind: "meta", tone: "error", text: expect.stringContaining("boom") });
  });
});
```
- [ ] Run `npm run test -- --run chatReducer` → FAIL (module missing).
- [ ] Implement `types.ts` + `chatReducer.ts`. `applyEvent` mirrors `webui.py handleEvent`: advance `lastSeq` when `ev.seq` truthy; switch on `ev.type`. For `message.token` (assistant only): if `partial`, append to a trailing streaming assistant item (create if the last item is not a streaming assistant); else set the trailing streaming item's text to the full value + `streaming=false` (or push a finalized assistant item). Use a monotonic id counter (module-local) or `crypto.randomUUID()` for `id`.
- [ ] Run tests → PASS. `tsc`/lint clean.
- [ ] Commit.

### Task 3: chatApi

**Files:** Create `web/src/features/chat/chatApi.ts`, `web/src/features/chat/chatApi.test.ts`.

**Interfaces — Produces:**
```ts
export function eventsUrl(sessionId: string, after: number): string; // `/v1/sessions/${id}/events?after=${after}`
export async function postMessage(sessionId: string, content: string): Promise<{ session_id: string; run_id: string }>;
export async function resolveApproval(approvalId: string, decision: "allow" | "deny"): Promise<void>;
export function openEvents(sessionId: string, after: number, onEvent: (ev: SseEvent) => void, onError: () => void): EventSource;
```

- [ ] **Write failing tests** (mock `global.fetch`): `eventsUrl("s", 3) === "/v1/sessions/s/events?after=3"`; `postMessage` POSTs to `/v1/sessions/s/messages` with JSON body `{content}` and returns parsed json; `resolveApproval` POSTs `/v1/approvals/a1` with body `{approval_id:"a1", decision:"allow"}`.
- [ ] Run → FAIL.
- [ ] Implement (`fetch` with `method:"POST"`, `Content-Type: application/json`; `openEvents` returns `new EventSource(eventsUrl(...))` wiring `onmessage`→`onEvent(JSON.parse(e.data))` and `onerror`→`onError`).
- [ ] Run → PASS. lint/tsc clean.
- [ ] Commit.

### Task 4: presentational components

**Files:** Create `MessageBubble.tsx`, `ToolStep.tsx`, `InlineApproval.tsx`, `Composer.tsx`, `RunBar.tsx`, `chat.components.test.tsx`.

**Interfaces — Produces:**
- `MessageBubble({ role: "user" | "assistant"; text: string; streaming?: boolean })`
- `ToolStep({ tool: string; args: unknown; result?: { ok: boolean; output: string } })`
- `InlineApproval({ tool: string; args: Record<string, unknown>; resolved?: "allow" | "deny"; onResolve: (d: "allow" | "deny") => void })`
- `Composer({ disabled: boolean; onSend: (text: string) => void })`
- `RunBar({ running: boolean; reason?: string })`

- [ ] **Write failing tests**: `MessageBubble` renders text; `ToolStep` shows the tool name and, with a result, the output; `InlineApproval` calls `onResolve("allow")` when 批准 is clicked and disables when `resolved`; `Composer` calls `onSend` with the trimmed value on Enter and clears, and does nothing when empty/disabled.
- [ ] Run → FAIL.
- [ ] Implement using the design system (user bubble right-aligned `bg-surface-2`; assistant left; `ToolStep` a mono `→ tool` / `← ok|err: output` block like `webui.py`; `InlineApproval` mirrors `ApprovalCard` with a `Banner tone="warn"` "内容来源可信/受污点" line + 批准/拒绝 `Button`s; `Composer` an `<input>` + `Button`; `RunBar` a pulse + "运行中"/`[reason]`).
- [ ] Run → PASS. lint/tsc clean.
- [ ] Commit.

### Task 5: useChat hook

**Files:** Create `web/src/features/chat/useChat.ts`.

**Interfaces — Produces:** `useChat(): { items: ChatItem[]; running: boolean; reason?: string; send: (text: string) => void; resolve: (approvalId: string, decision: "allow" | "deny") => void }`

- [ ] Implement: `useReducer` over `chatReducer` (dispatch wraps `userSent`/`applyEvent`); a `sessionId` via `useRef(crypto.randomUUID())`; keep the `EventSource` in a ref. `send(text)`: dispatch user, `await postMessage`, then `openEvents(sessionId, lastSeqRef.current, onEvent, onError)`; `onEvent` dispatches applyEvent and, when `ev.type==="run.ended"`, closes the source; `onError` is a no-op (browser retries). `resolve`: optimistically dispatch the approval resolution then `void resolveApproval(...)`. Clean up the `EventSource` on unmount. (No unit test — SSE needs a live server; verified in Task 7. Keep the hook a thin wrapper so the tested reducer/api carry the logic.)
- [ ] `tsc`/lint clean; `npm run test -- --run` still green.
- [ ] Commit.

### Task 6: ChatPage

**Files:** Replace the `ChatPage` stub with the real page; add a render test to `chat.components.test.tsx`.

- [ ] Implement `ChatPage`: `Topbar title="Chat" sub="· 个人助理" right={<Badge tone="violet">{model}</Badge>}` (model literal `github_copilot / claude-sonnet-4.5` for now — wiring real model info is deferred); a scrollable thread mapping `items` to the components by `kind`; `RunBar`; `Composer disabled={running}`. Auto-scroll to bottom on new items (`useEffect` + a ref).
- [ ] Render test: given a component structured to accept seeded items (extract a pure `ChatThread({ items })` and test it), user/assistant/tool/approval items each render.
- [ ] `npm run test -- --run` green; lint/tsc clean; `npm run build`.
- [ ] Commit.

### Task 7: Live verification (Docker server, real Claude)

- [ ] Ensure the Docker stack is up (`keel-server` healthy, runtime wired) and `npm run dev` in `web/`.
- [ ] In the controlled browser open `/chat`: send "What files are in the current directory? Use your tools." → observe streamed assistant tokens + a `ls`/`glob` **tool step** with a result → `run.ended`.
- [ ] Send a prompt that triggers a mutating tool (e.g. "create a file notes.txt with 'hi'") → an **inline approval** appears → click 批准 → the tool executes and the run completes; click 拒绝 on a second attempt → refused.
- [ ] Confirm no console errors; screenshot the thread.
