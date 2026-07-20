import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { GitHubInstallation, Project, ProjectImportInput } from "./types";

export function useProjects() {
  return useQuery({
    queryKey: ["projects"],
    queryFn: () => api.get<Project[]>("/v1/projects"),
  });
}

export function useProject(id: string | undefined) {
  return useQuery({
    queryKey: ["projects", id],
    queryFn: () => api.get<Project>(`/v1/projects/${id}`),
    enabled: Boolean(id),
  });
}

export function useImportProject() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: ProjectImportInput) => api.post<Project>("/v1/projects/import", input),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["projects"] });
    },
  });
}

export function useGitHubInstallations() {
  return useQuery({
    queryKey: ["projects", "github-installations"],
    queryFn: () => api.get<GitHubInstallation[]>("/v1/projects/github/installations"),
  });
}

export function useDeleteProject() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, version }: { id: string; version: number }) =>
      api.post<Project>(`/v1/projects/${encodeURIComponent(id)}/delete`, {
        expected_version: version,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["projects"] });
    },
  });
}
