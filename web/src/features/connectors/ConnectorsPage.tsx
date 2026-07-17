import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Chip } from "../../components/ui/chip";
import { Skeleton } from "../../components/ui/skeleton";
import { ConnectorResources } from "./ConnectorResources";
import { ConnectorSetupList } from "./ConnectorSetup";
import { ConnectorTargets } from "./ConnectorTargets";
import type { Connector } from "./types";
import { useConnectors, useRevokeConnector, useSyncConnector } from "./useConnectors";

function fmtDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "—" : d.toLocaleString();
}

function healthTone(connector: Connector): "green" | "red" | "amber" {
  if (!connector.enabled) return "amber";
  if (connector.health === "healthy") return "green";
  if (connector.health === "error") return "red";
  return "amber";
}

function ConnectedConnector({ connector }: { connector: Connector }) {
  const revoke = useRevokeConnector();
  const purge = useRevokeConnector(true);
  const forget = useRevokeConnector(false, true);
  const forcePurge = useRevokeConnector(true, true);
  const sync = useSyncConnector();

  return (
    <Card className="p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="font-semibold">
            {connector.icon} {connector.name}
          </div>
          <p className="mt-1 text-xs text-text-muted">{connector.description}</p>
        </div>
        <Badge tone={healthTone(connector)}>
          {connector.enabled ? connector.health : "disabled"}
        </Badge>
      </div>
      <div className="mt-3 flex flex-wrap gap-1.5">
        {connector.scopes.map((scope) => (
          <Chip key={scope}>{scope}</Chip>
        ))}
        {connector.capabilities.map((capability) => (
          <Chip key={capability}>{capability}</Chip>
        ))}
      </div>
      <p className="mt-3 text-xs text-text-muted">Updated {fmtDate(connector.updated_at)}</p>
      {!connector.available && (
        <Banner tone="warn" className="mt-3">
          <span>⚠️</span>
          <div>
            {connector.availability_error ?? "The provider cannot be loaded."} Remote revoke is
            unavailable; local removal remains explicit.
          </div>
        </Banner>
      )}
      <ConnectorTargets connector={connector} />
      <ConnectorResources connector={connector} />
      <div className="mt-3 flex flex-wrap gap-2">
        {connector.capabilities.includes("sync") && (
          <Button onClick={() => sync.mutate(connector.id)} disabled={sync.isPending}>
            Sync now
          </Button>
        )}
        {connector.auth_action && (
          <Button onClick={() => window.open(`/v1/connectors/${connector.id}/connect`, "_blank")}>
            Reconnect
          </Button>
        )}
        <Button variant="danger" onClick={() => revoke.mutate(connector.id)} disabled={revoke.isPending}>
          Disconnect
        </Button>
        <Button variant="danger" onClick={() => purge.mutate(connector.id)} disabled={purge.isPending}>
          Disconnect and purge
        </Button>
        <Button
          variant="danger"
          onClick={() => {
            if (
              window.confirm(
                "Forget local credentials and state without remote revoke? Imported data will remain and its connector mappings will be lost.",
              )
            ) {
              forget.mutate(connector.id);
            }
          }}
          disabled={forget.isPending}
        >
          Forget local state
        </Button>
        <Button
          variant="danger"
          onClick={() => {
            if (
              window.confirm(
                "Force local purge without remote revoke? Imported connector data will be removed.",
              )
            ) {
              forcePurge.mutate(connector.id);
            }
          }}
          disabled={forcePurge.isPending}
        >
          Force local purge
        </Button>
      </div>
      {(revoke.isError || purge.isError || forget.isError || forcePurge.isError || sync.isError) && (
        <p className="mt-2 text-xs text-red">
          {(revoke.error ?? purge.error ?? forget.error ?? forcePurge.error ?? sync.error)?.message}
        </p>
      )}
    </Card>
  );
}

export function ConnectorsPage() {
  const { data, isLoading, isError, refetch } = useConnectors();
  const connected = data?.filter((connector) => connector.connected) ?? [];
  const addable = data?.filter((connector) => !connector.connected) ?? [];

  return (
    <>
      <Topbar
        title="Connectors"
        sub="· Manifest-driven setup with encrypted credentials"
        right={<Badge tone="violet">scope: personal</Badge>}
      />
      <div className="w-full max-w-[900px] p-[22px]">
        {isLoading && <Skeleton className="h-28" />}
        {isError && (
          <Banner tone="danger">
            <span>⚠️</span>
            <div>
              Failed to load connectors.
              <button className="ml-2 underline" onClick={() => void refetch()}>
                Retry
              </button>
            </div>
          </Banner>
        )}
        {data && (
          <>
            <div className="mb-2.5 text-sm font-semibold text-text-soft">
              Connected · {connected.length}
            </div>
            <div className="mb-[22px] space-y-3">
              {connected.length ? (
                connected.map((connector) => (
                  <ConnectedConnector connector={connector} key={connector.id} />
                ))
              ) : (
                <Card className="p-4 text-sm text-text-muted">No connectors are configured.</Card>
              )}
            </div>
            <Banner tone="warn" className="mb-[22px]">
              <span>🛡️</span>
              <div>
                External connector content is tainted. Outbound actions influenced by it require
                approval and use durable idempotency.
              </div>
            </Banner>
            {addable.length > 0 && (
              <>
                <div className="mb-2.5 text-sm font-semibold text-text-soft">Available</div>
                <ConnectorSetupList connectors={addable} />
              </>
            )}
          </>
        )}
      </div>
    </>
  );
}
