import { http, HttpResponse } from "msw";
import { expect, test } from "vitest";
import type { Approval } from "../features/approvals/types";
import { server } from "../test/setup";
import { api } from "./api";

test("api.get returns the approvals list", async () => {
  const rows = await api.get<Approval[]>("/v1/approvals?status=pending");
  expect(rows).toHaveLength(1);
  expect(rows[0].tool).toBe("email_send");
});

test("api.get throws on a non-2xx response", async () => {
  server.use(http.get("/v1/approvals", () => new HttpResponse(null, { status: 500 })));
  await expect(api.get("/v1/approvals")).rejects.toThrow("HTTP 500");
});
