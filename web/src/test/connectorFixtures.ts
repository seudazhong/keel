import type {
  Connector,
  ConnectorAuthKind,
  ConnectorSetupField,
} from "../features/connectors/types";

const defaultFields: Partial<Record<ConnectorAuthKind, ConnectorSetupField[]>> = {
  secret: [
    {
      id: "secret",
      label: "Secret",
      required: true,
      secret: true,
      input_type: "password",
      help_text: null,
    },
  ],
  app_credentials: [
    {
      id: "client_id",
      label: "Client ID",
      required: true,
      secret: false,
      input_type: "text",
      help_text: null,
    },
    {
      id: "client_secret",
      label: "Client secret",
      required: true,
      secret: true,
      input_type: "password",
      help_text: null,
    },
  ],
  url: [
    {
      id: "url",
      label: "URL",
      required: true,
      secret: false,
      input_type: "url",
      help_text: null,
    },
  ],
};

export function makeConnectorFixture(
  overrides: Partial<Connector> & Pick<Connector, "id" | "name" | "auth_kind">,
): Connector {
  const connected = overrides.connected ?? false;
  const enabled = overrides.enabled ?? true;
  return {
    id: overrides.id,
    name: overrides.name,
    description: overrides.description ?? `${overrides.name} fixture`,
    icon: overrides.icon ?? "🔌",
    kind: overrides.auth_kind,
    auth_kind: overrides.auth_kind,
    capabilities: overrides.capabilities ?? ["read"],
    scopes: overrides.scopes ?? [],
    setup_fields: overrides.setup_fields ?? defaultFields[overrides.auth_kind] ?? [],
    resource_label: overrides.resource_label ?? null,
    enabled,
    operational: connected && enabled,
    connected,
    updated_at: overrides.updated_at ?? (connected ? "2026-07-08T04:19:55Z" : null),
    binding:
      overrides.binding ??
      (connected
        ? {
            id: `${overrides.id}-binding`,
            status: "connected",
            display_name: overrides.name,
            external_account_id: null,
            external_tenant_id: null,
            metadata: {},
            last_success_at: "2026-07-08T04:19:55Z",
            error_code: null,
            error_summary: null,
          }
        : null),
    health: overrides.health ?? (connected ? "healthy" : "unconfigured"),
  };
}
