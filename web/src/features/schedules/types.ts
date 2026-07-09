export interface Schedule {
  id: string;
  agent_id: string;
  trigger_kind: string;
  spec: string;
  interval_s: number;
  enabled: boolean;
  next_run_at: string;
  last_run_at: string | null;
  last_status: string | null;
}
