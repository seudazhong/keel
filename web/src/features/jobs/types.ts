export type JobStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";
export type JobCancelMode = "immediate" | "cooperative" | "disabled";

export interface Job {
  id: string;
  kind: string;
  status: JobStatus;
  cancel_mode: JobCancelMode;
  target_session_id: string | null;
  attempt: number;
  max_attempts: number;
  next_attempt_at: string;
  lease_expires_at: string | null;
  cancel_requested: boolean;
  progress_current: number;
  progress_total: number | null;
  progress_message: string | null;
  progress_updated_at: string | null;
  result: Record<string, unknown> | null;
  result_message: string | null;
  error_kind: string | null;
  error_message: string | null;
  injected_event_seq: number | null;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  finished_at: string | null;
}
