import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { Connector } from "./types";

export function useConnectors() {
  return useQuery({
    queryKey: ["connectors"],
    queryFn: () => api.get<Connector[]>("/v1/connectors"),
  });
}

export function useRevokeConnector() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.del<{ ok: boolean }>(`/v1/connectors/${id}`),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["connectors"] });
    },
  });
}
