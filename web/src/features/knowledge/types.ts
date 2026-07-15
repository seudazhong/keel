export type KnowledgeBaseStatus = "active" | "deleted";
export type KnowledgeDocumentStatus = "pending" | "active" | "failed" | "deleted";
export type KnowledgeVersionStatus =
  | "pending"
  | "indexing"
  | "active"
  | "superseded"
  | "failed"
  | "cancelled"
  | "deleted"
  | "purged";
export type KnowledgeSourceType = "text" | "markdown";
export type KnowledgeSearchMode = "hybrid" | "lexical" | "lexical-degraded";
export type JobStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

export interface KnowledgeBase {
  id: string;
  name: string;
  description: string | null;
  embedding_model: string;
  embedding_dim: number;
  status: KnowledgeBaseStatus;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}

export interface KnowledgeDocument {
  id: string;
  kb_id: string;
  title: string;
  source_type: KnowledgeSourceType;
  source_uri: string | null;
  status: KnowledgeDocumentStatus;
  desired_version_id: string | null;
  active_version_id: string | null;
  last_error_kind: string | null;
  last_error_message: string | null;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}

export interface KnowledgeVersion {
  id: string;
  kb_id: string;
  document_id: string;
  version: number;
  content: string | null;
  content_sha256: string;
  index_fingerprint: string;
  mime_type: string;
  chunking_version: string;
  target_chars: number;
  overlap_chars: number;
  ingest_job_id: string | null;
  status: KnowledgeVersionStatus;
  error_kind: string | null;
  error_message: string | null;
  created_at: string;
  activated_at: string | null;
  deleted_at: string | null;
  purged_at: string | null;
}

export interface KnowledgeDocumentDetail {
  document: KnowledgeDocument;
  versions: KnowledgeVersion[];
}

export interface Job {
  id: string;
  kind: string;
  status: JobStatus;
  cancel_mode: "immediate" | "cooperative" | "disabled";
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

export interface KnowledgeDocumentJobResponse {
  document: KnowledgeDocument;
  version: KnowledgeVersion;
  job: Job;
  replayed: boolean;
}

export interface KnowledgeDocumentDeleteResponse {
  document: KnowledgeDocument;
  job: Job;
  replayed: boolean;
}

export interface KnowledgeBaseDeleteResponse {
  base: KnowledgeBase;
  job: Job;
  replayed: boolean;
}

export interface KnowledgeCitation {
  id: string;
  kb_id: string;
  document_id: string;
  document_version_id: string;
  chunk_id: string;
  title: string;
  source_uri: string | null;
  ordinal: number;
  char_start: number;
  char_end: number;
  label: string;
}

export interface KnowledgeHit {
  snippet: string;
  rank: number;
  citation: KnowledgeCitation;
  heading_path: string[];
}

export interface KnowledgeSearchResponse {
  hits: KnowledgeHit[];
  status: {
    mode: KnowledgeSearchMode;
    semantic_error: string | null;
  };
}

export interface CreateKnowledgeBaseInput {
  name: string;
  description?: string | null;
}

export interface WriteKnowledgeDocumentInput {
  title: string;
  source_type: KnowledgeSourceType;
  content: string;
  source_uri?: string | null;
  target_session_id?: string | null;
}
