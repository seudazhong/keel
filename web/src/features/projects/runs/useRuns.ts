import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../lib/api";
import type { DiffFile, Run, RunApproval } from "./types";

export function useRuns(projectId: string | undefined) {
  return useQuery({
    queryKey: ["projects", projectId, "runs"],
    queryFn: () => api.get<Run[]>(`/v1/projects/${projectId}/runs`),
    enabled: Boolean(projectId),
  });
}

export function useRunDiff(runId: string | undefined) {
  return useQuery({
    queryKey: ["runs", runId, "diff"],
    queryFn: () => api.get<DiffFile[]>(`/v1/runs/${runId}/diff`),
    enabled: Boolean(runId),
  });
}

export function useRunApprovals(runId: string | undefined) {
  return useQuery({
    queryKey: ["runs", runId, "approvals"],
    queryFn: () => api.get<RunApproval[]>(`/v1/runs/${runId}/approvals`),
    enabled: Boolean(runId),
  });
}

export function useDecideRunApproval(runId: string | undefined) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ approvalId, decision }: { approvalId: string; decision: "approve" | "reject" }) =>
      api.post<RunApproval>(`/v1/runs/${runId}/approvals/${approvalId}/${decision}`),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["runs", runId, "approvals"] });
    },
  });
}
