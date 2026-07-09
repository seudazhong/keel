import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";

export interface ModelSettings {
  current: string;
  available: string[];
}

export function useModel() {
  return useQuery({
    queryKey: ["settings", "model"],
    queryFn: () => api.get<ModelSettings>("/v1/settings/model"),
  });
}

export function useSetModel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (model: string) =>
      api.put<{ ok: boolean; current: string }>("/v1/settings/model", { model }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["settings", "model"] });
    },
  });
}
