import { describe, expect, it, vi } from "vitest";
import { openSse, type SseMessage } from "./sseClient";

function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  let i = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i < chunks.length) controller.enqueue(encoder.encode(chunks[i++]));
      else controller.close();
    },
  });
}

function fakeResponse(chunks: string[]): Response {
  return { ok: true, status: 200, body: streamOf(chunks) } as unknown as Response;
}

async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("timeout waiting for condition");
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe("openSse", () => {
  it("parses id/event/data frames across chunk boundaries", async () => {
    const seen: SseMessage[] = [];
    const fetchImpl = vi.fn(async () =>
      fakeResponse(['id: 1\nevent: run.started\ndata: {"a":', '1}\n\nid: 2\ndata: hi\n\n']),
    ) as unknown as typeof fetch;
    const handle = openSse("/u", {
      onMessage: (m) => seen.push(m),
      fetchImpl,
      defaultRetryMs: 100_000,
    });
    await waitFor(() => seen.length >= 2);
    handle.close();
    expect(seen[0]).toMatchObject({ id: "1", event: "run.started", data: '{"a":1}' });
    expect(seen[1]).toMatchObject({ id: "2", event: "message", data: "hi" });
  });

  it("drops replayed events at or below the initial Last-Event-ID (no duplicates)", async () => {
    const seen: SseMessage[] = [];
    const fetchImpl = vi.fn(async () =>
      fakeResponse(["id: 1\ndata: a\n\nid: 2\ndata: b\n\nid: 3\ndata: c\n\n"]),
    ) as unknown as typeof fetch;
    const handle = openSse("/u", {
      onMessage: (m) => seen.push(m),
      lastEventId: "2",
      fetchImpl,
      defaultRetryMs: 100_000,
    });
    await waitFor(() => seen.length >= 1);
    handle.close();
    expect(seen.map((m) => m.id)).toEqual(["3"]);
  });

  it("reconnects with Last-Event-ID after the stream ends", async () => {
    const sentLastEventId: (string | null)[] = [];
    let call = 0;
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      sentLastEventId.push(headers.get("Last-Event-ID"));
      call += 1;
      // First connection delivers id 5 then ends; later connections deliver nothing.
      return call === 1 ? fakeResponse(["id: 5\ndata: x\n\n"]) : fakeResponse([]);
    }) as unknown as typeof fetch;
    const handle = openSse("/u", {
      onMessage: () => undefined,
      fetchImpl,
      defaultRetryMs: 5,
    });
    await waitFor(() => sentLastEventId.length >= 2);
    handle.close();
    expect(sentLastEventId[0]).toBeNull(); // fresh connection: no cursor
    expect(sentLastEventId[1]).toBe("5"); // reconnect resumes from the last dispatched id
  });

  it("honors a server retry hint and stops after close()", async () => {
    const mock = vi.fn(async () => fakeResponse(["retry: 4\ndata: a\n\n"]));
    const fetchImpl = mock as unknown as typeof fetch;
    const handle = openSse("/u", { onMessage: () => undefined, fetchImpl, defaultRetryMs: 5 });
    await waitFor(() => mock.mock.calls.length >= 1);
    handle.close();
    const callsAfterClose = mock.mock.calls.length;
    await new Promise((r) => setTimeout(r, 30));
    // No further reconnect attempts fire once closed.
    expect(mock.mock.calls.length).toBe(callsAfterClose);
  });
});
