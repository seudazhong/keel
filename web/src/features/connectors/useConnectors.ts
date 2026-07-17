import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../../lib/api";
import type {
  Connector,
  ConnectorResource,
  ConnectorSetupArtifact,
  ConnectorTargetKind,
} from "./types";

export function useConnectors() {
  return useQuery({
    queryKey: ["connectors"],
    queryFn: () => api.get<Connector[]>("/v1/connectors"),
  });
}

export function useSetupConnector() {
  const qc = useQueryClient();
  const [isPending, setPending] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  async function submit(id: string, values: Record<string, string>) {
    setPending(true);
    setError(null);
    try {
      const result = await api.post<{
        ok: boolean;
        binding_id: string;
        artifacts: ConnectorSetupArtifact[];
      }>(
        `/v1/connectors/${id}/setup`,
        { values },
      );
      void qc.invalidateQueries({ queryKey: ["connectors"] });
      return result;
    } catch (caught) {
      const failure = caught instanceof Error ? caught : new Error("Connector setup failed");
      setError(failure);
      throw failure;
    } finally {
      setPending(false);
    }
  }

  return { submit, isPending, error };
}

export function useConfigureConnectorTargets(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (targets: Partial<Record<ConnectorTargetKind, string>>) =>
      api.put<{ ok: boolean; targets: Partial<Record<ConnectorTargetKind, string>> }>(
        `/v1/connectors/${id}/targets`,
        { targets },
      ),
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
