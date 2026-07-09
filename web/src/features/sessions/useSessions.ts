import { useQuery } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { SseEvent } from "../chat/types";
import type { SessionSearchResult, SessionSummary } from "./types";

export function useSessions() {
  return useQuery({
    queryKey: ["sessions"],
    queryFn: () => api.get<SessionSummary[]>("/v1/sessions"),
  });
}

export function useSessionSearch(q: string) {
  return useQuery({
    queryKey: ["sessions", "search", q],
    queryFn: () =>
      api.get<SessionSearchResult[]>(`/v1/sessions/search?q=${encodeURIComponent(q)}`),
    enabled: q.trim().length > 0,
  });
}

export function useSessionHistory(id: string) {
  return useQuery({
    queryKey: ["sessions", id, "history"],
    queryFn: () => api.get<SseEvent[]>(`/v1/sessions/${encodeURIComponent(id)}/history`),
  });
}
