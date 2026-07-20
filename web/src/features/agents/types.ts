export type AgentKind = "personal" | "team";

export interface Agent {
  id: string;
  org_id: string;
  kind: AgentKind;
  owner_user_id: string;
  name: string;
  persona: string;
  status: string;
  version: number;
  created_at: string | null;
  updated_at: string | null;
}

export interface CreateAgentInput {
  kind: AgentKind;
  name: string;
  persona: string;
}

export interface UpdateAgentInput {
  expected_version: number;
  name: string;
  persona: string;
}
