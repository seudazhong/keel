import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { Job } from "./types";

export const jobKeys = {
  all: ["jobs"] as const,
  detail: (id: string) => ["jobs", id] as const,
};

export function isActiveJob(job: Job): boolean {
  return job.status === "queued" || job.status === "running";
}

export function canCancelJob(job: Job): boolean {
  return isActiveJob(job) && job.cancel_mode !== "disabled" && !job.cancel_requested;
}

export function useJobs() {
  return useQuery({
    queryKey: jobKeys.all,
    queryFn: () => api.get<Job[]>("/v1/jobs?limit=100"),
    refetchInterval: (query) => (query.state.data?.some(isActiveJob) ? 1_000 : false),
  });
}

export function useJob(id: string | null) {
  return useQuery({
    queryKey: jobKeys.detail(id ?? ""),
    queryFn: () => api.get<Job>(`/v1/jobs/${encodeURIComponent(id ?? "")}`),
    enabled: Boolean(id),
    refetchInterval: (query) => (query.state.data && isActiveJob(query.state.data) ? 1_000 : false),
  });
}

export function useCancelJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.post<Job>(`/v1/jobs/${encodeURIComponent(id)}/cancel`),
    onSuccess: (job) => {
      qc.setQueryData(jobKeys.detail(job.id), job);
      qc.setQueryData<Job[]>(jobKeys.all, (rows) =>
        rows?.map((row) => (row.id === job.id ? job : row)),
      );
      void qc.invalidateQueries({ queryKey: jobKeys.all });
    },
  });
}
