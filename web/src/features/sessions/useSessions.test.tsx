import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { expect, test } from "vitest";
import { useSessionHistory, useSessions } from "./useSessions";

function makeWrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
}

test("useSessions loads the session list", async () => {
  const { result } = renderHook(() => useSessions(), { wrapper: makeWrapper() });
  await waitFor(() => expect(result.current.data).toBeDefined());
  expect(result.current.data?.some((s) => s.id === "s1")).toBe(true);
});

test("useSessionHistory loads a session's events", async () => {
  const { result } = renderHook(() => useSessionHistory("s1"), { wrapper: makeWrapper() });
  await waitFor(() => expect(result.current.data).toBeDefined());
  expect(result.current.data?.some((e) => e.type === "message.token")).toBe(true);
});
