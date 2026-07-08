import { afterEach, describe, expect, it, vi } from "vitest";
import { eventsUrl, postMessage, resolveApproval } from "./chatApi";

afterEach(() => vi.restoreAllMocks());

function mockFetch(body: unknown, ok = true) {
  const fn = vi.fn().mockResolvedValue({ ok, status: ok ? 200 : 500, json: async () => body });
  vi.stubGlobal("fetch", fn);
  return fn;
}

describe("chatApi", () => {
  it("builds the events url with the replay cursor", () => {
    expect(eventsUrl("s", 3)).toBe("/v1/sessions/s/events?after=3");
  });

  it("posts a message and returns the run", async () => {
    const fetchMock = mockFetch({ session_id: "s", run_id: "r1" });
    const out = await postMessage("s", "hello");
    expect(out).toEqual({ session_id: "s", run_id: "r1" });
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/sessions/s/messages",
      expect.objectContaining({ method: "POST", body: JSON.stringify({ content: "hello" }) }),
    );
  });

  it("throws on a non-ok message post", async () => {
    mockFetch({}, false);
    await expect(postMessage("s", "x")).rejects.toThrow("HTTP 500");
  });

  it("resolves an interactive approval", async () => {
    const fetchMock = mockFetch({});
    await resolveApproval("a1", "allow");
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/approvals/a1",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ approval_id: "a1", decision: "allow" }),
      }),
    );
  });
});
