import { http, HttpResponse } from "msw";
import { afterEach, expect, test } from "vitest";
import type { Approval } from "../features/approvals/types";
import { emptyAuth, onAuthError, setAuthSnapshot } from "../features/auth/authState";
import { server } from "../test/setup";
import { api } from "./api";

afterEach(() => setAuthSnapshot(emptyAuth));

test("api.get returns the approvals list", async () => {
  const rows = await api.get<Approval[]>("/v1/approvals?status=pending");
  expect(rows).toHaveLength(1);
  expect(rows[0].tool).toBe("email_send");
});

test("api.get throws on a non-2xx response", async () => {
  server.use(
    http.get("/v1/approvals", () =>
      HttpResponse.json({ detail: "datastore unavailable" }, { status: 503 }),
    ),
  );
  await expect(api.get("/v1/approvals")).rejects.toThrow("datastore unavailable");
});

test("api surfaces structured domain error messages", async () => {
  server.use(
    http.post("/v1/things", () =>
      HttpResponse.json(
        {
          detail: {
            code: "knowledge_base_name_conflict",
            message: "An active Knowledge Base already uses this name.",
          },
        },
        { status: 409 },
      ),
    ),
  );
  await expect(api.post("/v1/things", {})).rejects.toThrow(
    "An active Knowledge Base already uses this name.",
  );
});

test("api attaches the credential + workspace headers from the auth snapshot", async () => {
  setAuthSnapshot({ credential: { kind: "bearer", secret: "tok-123" }, org: "acme", agent: "a1" });
  let seen: Record<string, string | null> = {};
  server.use(
    http.get("/v1/approvals", ({ request }) => {
      seen = {
        auth: request.headers.get("authorization"),
        apiKey: request.headers.get("x-api-key"),
        org: request.headers.get("x-keel-org"),
        agent: request.headers.get("x-keel-agent"),
      };
      return HttpResponse.json([]);
    }),
  );
  await api.get("/v1/approvals");
  expect(seen.auth).toBe("Bearer tok-123");
  expect(seen.apiKey).toBeNull();
  expect(seen.org).toBe("acme");
  expect(seen.agent).toBe("a1");
});

test("api sends X-API-Key for an api-key credential and an idempotency key on mutations", async () => {
  setAuthSnapshot({ credential: { kind: "api-key", secret: "vkey" }, org: null, agent: null });
  let apiKey: string | null = null;
  let idem: string | null = null;
  server.use(
    http.post("/v1/things", ({ request }) => {
      apiKey = request.headers.get("x-api-key");
      idem = request.headers.get("idempotency-key");
      return HttpResponse.json({ ok: true });
    }),
  );
  await api.post("/v1/things", { a: 1 });
  expect(apiKey).toBe("vkey");
  expect(idem).toBeTruthy();
});

test("an expected 403 is surfaced without clearing a valid credential", async () => {
  let rejected = false;
  onAuthError(() => {
    rejected = true;
  });
  server.use(
    http.post("/v1/things", () =>
      HttpResponse.json({ detail: "insufficient privilege" }, { status: 403 }),
    ),
  );
  await expect(api.post("/v1/things", {})).rejects.toThrow("insufficient privilege");
  expect(rejected).toBe(false);
  onAuthError(null);
});
