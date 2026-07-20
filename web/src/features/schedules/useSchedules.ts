import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { Schedule } from "./types";

export interface QueuedScheduleRun {
  ok: boolean;
  schedule_id: string;
  queued_at: string;
}

export function useSchedules(pendingRun?: QueuedScheduleRun) {
  return useQuery({
    queryKey: ["schedules"],
    queryFn: () => api.get<Schedule[]>("/v1/schedules"),
    refetchInterval: (query) => {
      if (!pendingRun) return false;
      const queuedAt = new Date(pendingRun.queued_at).getTime();
      const schedule = query.state.data?.find((row) => row.id === pendingRun.schedule_id);
      const lastRunAt = schedule?.last_run_at
        ? new Date(schedule.last_run_at).getTime()
        : Number.NaN;
      return Number.isFinite(lastRunAt) && lastRunAt >= queuedAt ? false : 2_000;
    },
  });
}

export function useToggleSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { id: string; enabled: boolean }) =>
      api.post<{ ok: boolean; enabled: boolean }>(
        `/v1/schedules/${encodeURIComponent(v.id)}/toggle`,
        { enabled: v.enabled },
      ),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["schedules"] });
    },
  });
}

export function useRunSchedule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      const result = await api.post<{ ok: boolean }>(
        `/v1/schedules/${encodeURIComponent(id)}/run`,
      );
      return {
        ...result,
        schedule_id: id,
        queued_at: new Date().toISOString(),
      };
    },
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["schedules"] });
    },
  });
}
