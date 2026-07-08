import { useQuery } from "@tanstack/react-query";
import { api } from "../../lib/api";
import type { Connector } from "./types";

export function useConnectors() {
  return useQuery({
    queryKey: ["connectors"],
    queryFn: () => api.get<Connector[]>("/v1/connectors"),
  });
}
