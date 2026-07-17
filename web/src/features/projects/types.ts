export interface Project {
  id: string;
  name: string;
  description: string;
  repository: string;
  default_branch: string;
  agent_id: string | null;
  created_at: string;
  updated_at: string;
}

export interface ProjectImportInput {
  name: string;
  repository: string;
  default_branch: string;
}
