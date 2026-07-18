import { useEffect, useState } from "react";
import { Button } from "../../components/ui/button";
import type { Connector } from "./types";
import { useConnectorResources, useSelectConnectorResources } from "./useConnectors";

export function ConnectorResources({ connector }: { connector: Connector }) {
  const resources = useConnectorResources(
    connector.id,
    connector.connected && connector.capabilities.includes("resources"),
  );
  const select = useSelectConnectorResources(connector.id);
  const [selected, setSelected] = useState<string[]>([]);

  useEffect(() => {
    if (resources.data) {
      setSelected(resources.data.filter((item) => item.selected).map((item) => item.external_id));
    }
  }, [resources.data]);

  if (!connector.capabilities.includes("resources")) return null;
  if (resources.isLoading) return <p className="mt-2 text-xs text-text-muted">Loading resources…</p>;
  if (!resources.data?.length) return <p className="mt-2 text-xs text-text-muted">No resources available.</p>;

  return (
    <div className="mt-3 rounded-sm border border-border p-3">
      <div className="mb-2 text-xs font-semibold text-text-soft">
        {connector.resource_label ?? "Resources"}
      </div>
      <div className="space-y-1">
        {resources.data.map((resource) => (
          <label className="flex items-center gap-2 text-sm" key={resource.external_id}>
            <input
              type="checkbox"
              checked={selected.includes(resource.external_id)}
              onChange={(event) =>
                setSelected((current) =>
                  event.target.checked
                    ? [...current, resource.external_id]
                    : current.filter((id) => id !== resource.external_id),
                )
              }
            />
            {resource.display_name}
          </label>
        ))}
      </div>
      <Button className="mt-2 px-2.5 py-1 text-xs" onClick={() => select.mutate(selected)}>
        Save selection
      </Button>
    </div>
  );
}
