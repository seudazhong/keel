import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { MemoryProposal, ProposalResolution } from "./types";

const memoryKeys = {
  proposals: ["memory", "proposals"] as const,
};

export function useMemoryProposals() {
  return useQuery({
    queryKey: memoryKeys.proposals,
    queryFn: () => api.get<MemoryProposal[]>("/v1/memory/proposals"),
  });
}

function useResolveProposal(action: "approve" | "reject") {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      api.post<ProposalResolution>(
        `/v1/memory/proposals/${encodeURIComponent(id)}/${action}`,
      ),
    onSuccess: () => void qc.invalidateQueries({ queryKey: memoryKeys.proposals }),
  });
}

export function useApproveMemoryProposal() {
  return useResolveProposal("approve");
}

export function useRejectMemoryProposal() {
  return useResolveProposal("reject");
}

export function useRunConsolidation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.post<{ ok: boolean }>("/v1/memory/consolidation/run"),
    onSuccess: () => void qc.invalidateQueries({ queryKey: memoryKeys.proposals }),
  });
}
