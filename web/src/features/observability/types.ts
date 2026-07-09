export interface AdminOverview {
  sessions: number;
  schedules: { total: number; enabled: number };
  approvals: { pending: number; granted: number; denied: number; expired: number };
  connectors: number;
  usage: {
    runs: number;
    prompt_tokens: number;
    completion_tokens: number;
    cache_read_tokens: number;
    cost_usd: number;
  };
}
