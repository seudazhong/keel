export interface Agent {
  id: string;
  name: string;
  description: string;
  model: string;
  tools: string[];
  active: boolean;
  created_at: string;
  updated_at: string;
}

export interface AgentInput {
  name: string;
  description: string;
  model: string;
  tools: string[];
}
