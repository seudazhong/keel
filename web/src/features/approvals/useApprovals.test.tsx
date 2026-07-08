import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { expect, test } from "vitest";
import { useApprovals, useResolveApproval } from "./useApprovals";

function makeWrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
}

test("useApprovals loads the pending list", async () => {
  const { result } = renderHook(() => useApprovals(), { wrapper: makeWrapper() });
  await waitFor(() => expect(result.current.data).toHaveLength(1));
  expect(result.current.data?.[0].tool).toBe("email_send");
});

test("useResolveApproval approves and removes the row", async () => {
  const { result } = renderHook(
    () => ({ list: useApprovals(), resolve: useResolveApproval() }),
    { wrapper: makeWrapper() },
  );
  await waitFor(() => expect(result.current.list.data).toHaveLength(1));
  result.current.resolve.mutate({ id: "a1", decision: "approve" });
  await waitFor(() => expect(result.current.list.data).toHaveLength(0));
});
