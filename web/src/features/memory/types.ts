export type MemoryProposalStatus = "pending" | "applied" | "rejected" | "stale";

export interface MemoryBlock {
  key: string;
  value: string;
  version: number;
}

export interface MemoryProposal {
  id: string;
  block: string;
  expected_version: number;
  proposed_value: string;
  reason: string;
  confidence: number;
  source_event_ids: number[];
  status: MemoryProposalStatus;
  created_at: string;
  resolved_at: string | null;
  resolved_by: string | null;
}

export interface ProposalResolution {
  ok: boolean;
  status: "applied" | "rejected" | "stale" | "not_found" | "already_resolved";
  version: number | null;
}

export interface QueuedMemoryRun {
  ok: boolean;
  schedule_id: string;
  queued_at: string;
}
