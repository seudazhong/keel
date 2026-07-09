export interface SessionSummary {
  id: string;
  title: string | null;
  messages: number;
  created_at: string;
  updated_at: string;
}

export interface SessionSearchResult {
  id: string;
  title: string | null;
  snippet: string;
  messages: number;
  updated_at: string | null;
}
