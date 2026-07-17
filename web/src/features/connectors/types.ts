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

export interface ConnectorAuthAction {
  label: string;
  callback_parameters: { id: string; required: boolean }[];
  requires_setup: boolean;
  help_text: string | null;
}

export type ConnectorTargetKind = "knowledge" | "trigger_session" | "trigger_routine";

export interface ConnectorTargetField {
  kind: ConnectorTargetKind;
  label: string;
  required: boolean;
  help_text: string | null;
}

export interface ConnectorSetupArtifact {
  kind: "instruction" | "url" | "secret";
  label: string;
  value: string;
  secret: boolean;
}

export interface ConnectorBinding {
  id: string;
  status:
    | "unconfigured"
    | "configured"
    | "authorizing"
    | "connected"
    | "degraded"
    | "error"
    | "revoked";
  display_name: string | null;
  external_account_id: string | null;
  external_tenant_id: string | null;
  metadata: Record<string, unknown>;
  last_success_at: string | null;
  error_code: string | null;
  error_summary: string | null;
  renewal_expires_at: string | null;
  next_sync_at: string | null;
  next_renewal_at: string | null;
  targets: Partial<Record<ConnectorTargetKind, string>>;
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
  auth_action: ConnectorAuthAction | null;
  setup_action_label: string;
  default_sync_cadence_seconds: number | null;
  renewal: {
    cadence_seconds: number;
    expiry_behavior: "degraded" | "error" | "revoked";
  } | null;
  resource_label: string | null;
  target_fields: ConnectorTargetField[];
  actions: {
    name: string;
    description: string;
    input_schema: Record<string, unknown>;
    semantics: "read" | "outbound";
    idempotency: "none" | "optional" | "required";
    approval: "none" | "tainted";
  }[];
  available: boolean;
  availability_error: string | null;
  enabled: boolean;
  operational: boolean;
  configured: boolean;
  connected: boolean;
  next_action: {
    kind: "setup" | "authorize";
    label: string;
    instructions: string;
  } | null;
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
