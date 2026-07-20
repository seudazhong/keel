import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type {
  MemoryBlock,
  MemoryProposal,
  ProposalResolution,
  QueuedMemoryRun,
} from "./types";

const memoryKeys = {
  blocks: ["memory", "blocks"] as const,
  proposals: ["memory", "proposals"] as const,
};

export function useMemoryBlocks() {
  return useQuery({
    queryKey: memoryKeys.blocks,
    queryFn: () => api.get<MemoryBlock[]>("/v1/memory/blocks"),
  });
}

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
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: memoryKeys.blocks });
      void qc.invalidateQueries({ queryKey: memoryKeys.proposals });
    },
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
    mutationFn: async () => {
      const result = await api.post<{ ok: boolean }>("/v1/memory/consolidation/run");
      return {
        ...result,
        schedule_id: "memory-consolidation",
        queued_at: new Date().toISOString(),
      } satisfies QueuedMemoryRun;
    },
    onSuccess: () => void qc.invalidateQueries({ queryKey: memoryKeys.proposals }),
  });
}
