import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api, ApiError } from "../../lib/api";
import { isAuthError } from "../auth/authState";
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

class PopupBlockedError extends Error {
  constructor() {
    super("The authorization window was blocked. Allow popups for this site and try again.");
    this.name = "PopupBlockedError";
  }
}

export function useConnectUrl(id: string) {
  const mutation = useMutation({
    mutationFn: async () => {
      const { url } = await api.post<{ url: string }>(`/v1/connectors/${id}/connect-url`);
      let opened: Window | null;
      try {
        opened = window.open(url, "_blank", "noopener,noreferrer");
      } catch {
        throw new PopupBlockedError();
      }
      if (opened === null) throw new PopupBlockedError();
    },
  });
  const error =
    mutation.error instanceof ApiError && isAuthError(mutation.error.status)
      ? null
      : mutation.error;
  return { ...mutation, error };
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

export function useRevokeConnector(purge = false, localOnly = false) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => {
      const suffix = localOnly ? (purge ? "/purge/local" : "/local") : purge ? "/purge" : "";
      return api.del<{ ok: boolean }>(`/v1/connectors/${id}${suffix}`);
    },
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
