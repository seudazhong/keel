export type ConnectorAuthKind =
  | "oauth"
  | "github_app"
  | "secret"
  | "app_credentials"
  | "webhook"
  | "url";

export type ConnectorCapability = "read" | "write" | "sync" | "webhook" | "resources";

export interface ConnectorSetupField {
  id: string;
  label: string;
  required: boolean;
  secret: boolean;
  input_type: string;
  help_text: string | null;
}

export interface ConnectorBinding {
  id: string;
  status: "configured" | "connected" | "error" | "revoked";
  display_name: string | null;
  external_account_id: string | null;
  external_tenant_id: string | null;
  metadata: Record<string, unknown>;
  last_success_at: string | null;
  error_code: string | null;
  error_summary: string | null;
}

export interface Connector {
  id: string;
  name: string;
  description: string;
  icon: string;
  kind: ConnectorAuthKind;
  auth_kind: ConnectorAuthKind;
  capabilities: ConnectorCapability[];
  scopes: string[];
  setup_fields: ConnectorSetupField[];
  resource_label: string | null;
  enabled: boolean;
  operational: boolean;
  connected: boolean;
  updated_at: string | null;
  binding: ConnectorBinding | null;
  health: "unconfigured" | "healthy" | "degraded" | "error";
}

export interface ConnectorResource {
  id: string;
  external_id: string;
  kind: string;
  display_name: string;
  url: string | null;
  selected: boolean;
  config: Record<string, unknown>;
}
