import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { Approval } from "./types";

const KEY = ["approvals", "pending"] as const;

export function useApprovals() {
  return useQuery({
    queryKey: KEY,
    queryFn: () => api.get<Approval[]>("/v1/approvals?status=pending"),
  });
}

export function useResolveApproval() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, decision }: { id: string; decision: "approve" | "reject" }) =>
      api.post<{ ok: boolean }>(`/v1/approvals/${id}/${decision}`),
    onSuccess: (_data, { id }) => {
      // Optimistically drop the resolved row, then refetch to reconcile with the server.
      qc.setQueryData<Approval[]>(KEY, (rows) => rows?.filter((r) => r.id !== id) ?? []);
      void qc.invalidateQueries({ queryKey: ["approvals"] });
    },
  });
}
