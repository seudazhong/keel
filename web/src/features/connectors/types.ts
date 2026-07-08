export interface Connector {
  id: string;
  name: string;
  icon: string;
  kind: string;
  scopes: string[];
  connected: boolean;
  updated_at: string | null;
}
