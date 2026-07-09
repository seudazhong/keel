import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { Schedule } from "./types";

export function useSchedules() {
  return useQuery({
    queryKey: ["schedules"],
    queryFn: () => api.get<Schedule[]>("/v1/schedules"),
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
  return useMutation({
    mutationFn: (id: string) =>
      api.post<{ ok: boolean }>(`/v1/schedules/${encodeURIComponent(id)}/run`),
  });
}
