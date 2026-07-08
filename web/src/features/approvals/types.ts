export interface Approval {
  id: string;
  run_id: string;
  session_id: string;
  tool: string;
  args: Record<string, unknown>;
  call_id: string;
  reason: string;
  status: string;
  created_at: string;
  expires_at: string;
}
