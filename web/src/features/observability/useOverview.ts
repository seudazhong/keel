import { useQuery } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { AdminOverview } from "./types";

export function useOverview() {
  return useQuery({
    queryKey: ["admin", "overview"],
    queryFn: () => api.get<AdminOverview>("/v1/admin/overview"),
  });
}
