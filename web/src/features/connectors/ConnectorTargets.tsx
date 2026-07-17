import { useEffect, useState } from "react";
import { Button } from "../../components/ui/button";
import type { Connector, ConnectorTargetKind } from "./types";
import { useConfigureConnectorTargets } from "./useConnectors";

export function ConnectorTargets({ connector }: { connector: Connector }) {
  const configure = useConfigureConnectorTargets(connector.id);
  const [values, setValues] = useState<Partial<Record<ConnectorTargetKind, string>>>(
    connector.binding?.targets ?? {},
  );

  useEffect(() => {
    setValues(connector.binding?.targets ?? {});
  }, [connector.binding?.targets]);

  if (!connector.target_fields.length) return null;

  return (
    <div className="mt-3 rounded-sm border border-border p-3">
      <div className="mb-2 text-xs font-semibold text-text-soft">Destinations and triggers</div>
      <div className="space-y-2">
        {connector.target_fields.map((field) => (
          <label className="block" key={field.kind}>
            <span className="mb-1 block text-xs font-semibold text-text-soft">{field.label}</span>
            <input
              aria-label={field.label}
              className="w-full rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm"
              required={field.required}
              value={values[field.kind] ?? ""}
              onChange={(event) =>
                setValues((current) => ({ ...current, [field.kind]: event.target.value }))
              }
            />
            {field.help_text && (
              <span className="mt-1 block text-xs text-text-muted">{field.help_text}</span>
            )}
          </label>
        ))}
      </div>
      <Button
        className="mt-2 px-2.5 py-1 text-xs"
        disabled={configure.isPending}
        onClick={() => configure.mutate(values)}
      >
        Save targets
      </Button>
      {configure.isError && <p className="mt-1 text-xs text-red">{configure.error.message}</p>}
    </div>
  );
}
