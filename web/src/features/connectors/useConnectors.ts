import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { Connector, ConnectorResource } from "./types";

export function useConnectors() {
  return useQuery({
    queryKey: ["connectors"],
    queryFn: () => api.get<Connector[]>("/v1/connectors"),
  });
}

export function useSetupConnector() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, values }: { id: string; values: Record<string, string> }) =>
      api.post<{ ok: boolean; binding_id: string }>(`/v1/connectors/${id}/setup`, { values }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["connectors"] });
    },
  });
}

export function useRevokeConnector(purge = false) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      api.del<{ ok: boolean }>(`/v1/connectors/${id}${purge ? "/purge" : ""}`),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["connectors"] });
    },
  });
}

export function useSyncConnector() {
  return useMutation({
    mutationFn: (id: string) =>
      api.post<{ ok: boolean; job_id: string; status: string }>(`/v1/connectors/${id}/sync`, {}),
  });
}

export function useConnectorResources(id: string, enabled: boolean) {
  return useQuery({
    queryKey: ["connectors", id, "resources"],
    queryFn: () => api.get<ConnectorResource[]>(`/v1/connectors/${id}/resources`),
    enabled,
  });
}

export function useSelectConnectorResources(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (externalIds: string[]) =>
      api.put<{ ok: boolean; changed: number }>(`/v1/connectors/${id}/resources`, {
        external_ids: externalIds,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["connectors", id, "resources"] });
    },
  });
}
