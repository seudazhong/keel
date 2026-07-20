import { useRef, useState } from "react";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import type { Connector, ConnectorSetupArtifact } from "./types";
import { useConnectUrl, useSetupConnector } from "./useConnectors";

function safeUrl(value: string): string | null {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:" ? url.toString() : null;
  } catch {
    return null;
  }
}

function Artifacts({ artifacts }: { artifacts: ConnectorSetupArtifact[] }) {
  if (!artifacts.length) return null;
  return (
    <div className="space-y-1 rounded-sm border border-border p-2 text-xs">
      {artifacts.map((artifact) => {
        const href = artifact.kind === "url" ? safeUrl(artifact.value) : null;
        return (
          <div key={`${artifact.kind}:${artifact.label}`}>
            <strong>{artifact.label}:</strong>{" "}
            {href ? (
              <a className="underline" href={href} rel="noreferrer" target="_blank">
                {artifact.value}
              </a>
            ) : (
              <span>{artifact.value}</span>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function ConnectorSetup({ connector, compact = false }: { connector: Connector; compact?: boolean }) {
  const setup = useSetupConnector();
  const connect = useConnectUrl(connector.id);
  const [saved, setSaved] = useState(false);
  const [artifacts, setArtifacts] = useState<ConnectorSetupArtifact[]>([]);
  const form = useRef<HTMLFormElement>(null);
  const secretHost = useRef<HTMLDivElement>(null);

  async function submit() {
    const current = form.current;
    if (!current) return;
    const data = new FormData(current);
    const values = Object.fromEntries(
      connector.setup_fields.map((field) => [field.id, String(data.get(field.id) ?? "")]),
    );
    try {
      const result = await setup.submit(connector.id, values);
      current.reset();
      setSaved(true);
      setArtifacts(result.artifacts.filter((artifact) => !artifact.secret));
      const host = secretHost.current;
      host?.replaceChildren();
      for (const artifact of result.artifacts.filter((item) => item.secret)) {
        const row = document.createElement("div");
        const label = document.createElement("strong");
        const value = document.createElement("code");
        label.textContent = `${artifact.label}: `;
        value.textContent = artifact.value;
        row.append(label, value);
        host?.append(row);
      }
      if (host?.childElementCount) {
        const hide = document.createElement("button");
        hide.type = "button";
        hide.className = "underline";
        hide.textContent = "Hide secret values";
        hide.addEventListener("click", () => host.replaceChildren(), { once: true });
        host.append(hide);
      }
    } catch {
      return;
    }
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

      {!connector.available && (
        <p className="text-xs text-red">{connector.availability_error ?? "Provider unavailable"}</p>
      )}
      {connector.binding?.error_summary && (
        <p role="alert" className="rounded-sm border border-red/30 bg-red/10 p-2 text-xs text-red">
          {connector.binding.error_summary}
        </p>
      )}
      {connector.next_action && (
        <p className="rounded-sm border border-border p-2 text-xs text-text-soft">
          <strong>{connector.next_action.label}:</strong> {connector.next_action.instructions}
        </p>
      )}
      {connector.auth_action && (
        <Button
          className="w-full"
          onClick={() => connect.mutate()}
          disabled={
            connect.isPending ||
            !connector.available ||
            (connector.auth_action.requires_setup && !connector.configured)
          }
        >
          {connect.isPending ? "Opening…" : connector.auth_action.label}
        </Button>
      )}
      {connect.error && <p role="alert" className="text-xs text-red">{connect.error.message}</p>}
      {(!connector.auth_action || connector.setup_fields.length > 0) && (
        <form
          className="space-y-2"
          ref={form}
          onSubmit={(event) => {
            event.preventDefault();
            void submit();
          }}
        >
          {connector.setup_fields.map((field) => (
            <label className="block" key={field.id}>
              <span className="mb-1 block text-xs font-semibold text-text-soft">{field.label}</span>
              <input
                aria-label={field.label}
                className="w-full rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm"
                type={field.secret ? "password" : field.input_type}
                name={field.id}
                required={field.required}
                onChange={() => {
                  setSaved(false);
                  setArtifacts([]);
                  secretHost.current?.replaceChildren();
                }}
                autoComplete={field.secret ? "new-password" : undefined}
              />
              {field.help_text && <span className="mt-1 block text-xs text-text-muted">{field.help_text}</span>}
            </label>
          ))}
          <Button
            className="w-full"
            disabled={setup.isPending || !connector.available}
            type="submit"
          >
            {setup.isPending ? "Saving…" : connector.setup_action_label}
          </Button>
          {saved && (
            <p className="text-xs text-green">
              Saved. Stored credentials are not displayed again.
            </p>
          )}
          <Artifacts artifacts={artifacts} />
          <div className="space-y-1 text-xs" ref={secretHost} />
          {setup.error && <p className="text-xs text-red">{setup.error.message}</p>}
        </form>
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
