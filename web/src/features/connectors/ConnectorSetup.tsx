import { useState } from "react";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import type { Connector } from "./types";
import { useSetupConnector } from "./useConnectors";

function usesBrowserAuth(connector: Connector): boolean {
  return connector.auth_kind === "oauth" || connector.auth_kind === "github_app";
}

export function ConnectorSetup({ connector, compact = false }: { connector: Connector; compact?: boolean }) {
  const setup = useSetupConnector();
  const [values, setValues] = useState<Record<string, string>>({});
  const [saved, setSaved] = useState(false);

  function connect() {
    window.open(`/v1/connectors/${connector.id}/connect`, "_blank");
  }

  function submit() {
    setup.mutate(
      { id: connector.id, values },
      {
        onSuccess: () => {
          setValues({});
          setSaved(true);
        },
      },
    );
  }

  return (
    <Card className={compact ? "p-3" : "flex flex-col gap-3 p-4"}>
      <div className="flex items-start gap-3">
        <div className="text-2xl">{connector.icon}</div>
        <div className="min-w-0 flex-1">
          <div className="font-semibold">{connector.name}</div>
          <p className="mt-1 text-xs text-text-muted">{connector.description}</p>
        </div>
      </div>

      {usesBrowserAuth(connector) ? (
        <Button className="w-full" onClick={connect}>
          Connect
        </Button>
      ) : (
        <div className="space-y-2">
          {connector.setup_fields.map((field) => (
            <label className="block" key={field.id}>
              <span className="mb-1 block text-xs font-semibold text-text-soft">{field.label}</span>
              <input
                aria-label={field.label}
                className="w-full rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm"
                type={field.secret ? "password" : field.input_type}
                required={field.required}
                value={values[field.id] ?? ""}
                onChange={(event) => {
                  setSaved(false);
                  setValues((current) => ({ ...current, [field.id]: event.target.value }));
                }}
                autoComplete={field.secret ? "new-password" : undefined}
              />
              {field.help_text && <span className="mt-1 block text-xs text-text-muted">{field.help_text}</span>}
            </label>
          ))}
          <Button className="w-full" onClick={submit} disabled={setup.isPending}>
            {setup.isPending ? "Saving…" : connector.auth_kind === "webhook" ? "Create webhook" : "Save"}
          </Button>
          {saved && <p className="text-xs text-green">Saved. Secret values are not displayed.</p>}
          {setup.isError && <p className="text-xs text-red">{setup.error.message}</p>}
        </div>
      )}
    </Card>
  );
}

export function ConnectorSetupList({
  connectors,
  compact = false,
}: {
  connectors: Connector[];
  compact?: boolean;
}) {
  return (
    <div className={compact ? "space-y-2" : "grid grid-cols-1 gap-3 sm:grid-cols-2"}>
      {connectors.map((connector) => (
        <ConnectorSetup connector={connector} compact={compact} key={connector.id} />
      ))}
    </div>
  );
}
