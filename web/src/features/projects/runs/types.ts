export type RunStatus = "running" | "succeeded" | "failed" | "awaiting_approval";
export type RunStepStatus = "done" | "running" | "pending" | "failed";

export interface RunStep {
  id: string;
  label: string;
  status: RunStepStatus;
  timestamp: string;
}

export interface Run {
  id: string;
  project_id: string;
  status: RunStatus;
  summary: string;
  started_at: string;
  finished_at: string | null;
  steps: RunStep[];
}

export type DiffLineType = "add" | "del" | "context";

export interface DiffLine {
  type: DiffLineType;
  text: string;
}

export interface DiffHunk {
  header: string;
  lines: DiffLine[];
}

export interface DiffFile {
  path: string;
  additions: number;
  deletions: number;
  hunks: DiffHunk[];
}

export type RunApprovalStatus = "pending" | "approved" | "rejected";

export interface RunApproval {
  id: string;
  run_id: string;
  tool: string;
  summary: string;
  status: RunApprovalStatus;
}
