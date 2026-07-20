import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { Agent, CreateAgentInput, UpdateAgentInput } from "./types";

export function useAgents() {
  return useQuery({
    queryKey: ["agents"],
    queryFn: () => api.get<Agent[]>("/v1/identity/agents"),
  });
}

export function useCreateAgent() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: CreateAgentInput) => api.post<Agent>("/v1/identity/agents", input),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["agents"] });
    },
  });
}

export function useUpdateAgent() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: UpdateAgentInput }) =>
      api.patch<Agent>(`/v1/identity/agents/${encodeURIComponent(id)}`, input),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["agents"] });
    },
  });
}

export function useArchiveAgent() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, version }: { id: string; version: number }) =>
      api.post<Agent>(`/v1/identity/agents/${encodeURIComponent(id)}/archive`, {
        expected_version: version,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["agents"] });
    },
  });
}
