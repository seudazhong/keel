export interface Project {
  id: string;
  org_id: string;
  slug: string;
  display_name: string;
  source: "blank" | "github";
  visibility: "private" | "internal";
  status: "active" | "archived" | "deleted";
  default_branch: string;
  github_repository_id: number | null;
  version: number;
  created_at: string | null;
  updated_at: string | null;
}

export interface ProjectImportInput {
  slug: string;
  display_name: string;
  installation_id: number;
  repo_full_name: string;
}

export interface GitHubInstallation {
  id: string;
  org_id: string;
  installation_id: number;
  app_id: number;
  account_login: string;
  account_type: string;
  status: string;
}
