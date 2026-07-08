import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { expect, test } from "vitest";
import { useConnectors } from "./useConnectors";

function makeWrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
}

test("useConnectors loads the connector list with status", async () => {
  const { result } = renderHook(() => useConnectors(), { wrapper: makeWrapper() });
  await waitFor(() => expect(result.current.data).toBeDefined());
  const gmail = result.current.data?.find((c) => c.id === "gmail");
  expect(gmail?.connected).toBe(true);
  expect(gmail?.scopes).toContain("gmail.send");
});
