import { useState, type FormEvent } from "react";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Chip } from "../../components/ui/chip";
import { Skeleton } from "../../components/ui/skeleton";
import { useTranslation } from "../../lib/i18n";
import { AgentSwitcher } from "./AgentSwitcher";
import type { Agent, AgentInput } from "./types";
import {
  useAgents,
  useCreateAgent,
  useDeleteAgent,
  useSetActiveAgent,
  useUpdateAgent,
} from "./useAgents";

const inputClass =
  "w-full rounded-sm border border-border bg-surface px-3 py-2 text-sm outline-none focus:border-accent";

function parseTools(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function AgentForm({
  initial,
  submitLabel,
  pending,
  onCancel,
  onSubmit,
}: {
  initial?: Agent;
  submitLabel: string;
  pending: boolean;
  onCancel: () => void;
  onSubmit: (input: AgentInput) => void;
}) {
  const { t } = useTranslation();
  const [name, setName] = useState(initial?.name ?? "");
  const [description, setDescription] = useState(initial?.description ?? "");
  const [model, setModel] = useState(initial?.model ?? "");
  const [tools, setTools] = useState(initial?.tools.join(", ") ?? "");

  function handleSubmit(e: FormEvent) {
    e.preventDefault();
    onSubmit({ name: name.trim(), description: description.trim(), model: model.trim(), tools: parseTools(tools) });
  }

  return (
    <form className="flex flex-col gap-3" onSubmit={handleSubmit}>
      <label className="text-xs font-semibold text-text-soft">
        {t("agents.form.name")}
        <input className={inputClass} value={name} onChange={(e) => setName(e.target.value)} required />
      </label>
      <label className="text-xs font-semibold text-text-soft">
        {t("agents.form.description")}
        <input
          className={inputClass}
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />
      </label>
      <label className="text-xs font-semibold text-text-soft">
        {t("agents.form.model")}
        <input className={inputClass} value={model} onChange={(e) => setModel(e.target.value)} />
      </label>
      <label className="text-xs font-semibold text-text-soft">
        {t("agents.form.tools")}
        <input className={inputClass} value={tools} onChange={(e) => setTools(e.target.value)} />
      </label>
      <div className="flex justify-end gap-2">
        <Button type="button" onClick={onCancel}>
          {t("common.cancel")}
        </Button>
        <Button type="submit" variant="primary" disabled={pending}>
          {pending ? t("common.saving") : submitLabel}
        </Button>
      </div>
    </form>
  );
}

export function AgentsPage() {
  const { t } = useTranslation();
  const agents = useAgents();
  const create = useCreateAgent();
  const update = useUpdateAgent();
  const del = useDeleteAgent();
  const setActive = useSetActiveAgent();

  const [creating, setCreating] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);

  return (
    <>
      <Topbar
        title={t("agents.title")}
        sub={t("agents.subtitle")}
        right={<AgentSwitcher />}
      />
      <div className="w-full max-w-[900px] p-[22px]">
        <Banner tone="info" className="mb-4">
          <span>🧪</span>
          <div>{t("agents.preview.banner")}</div>
        </Banner>

        {agents.isLoading && <Skeleton className="h-40" />}

        {agents.isError && (
          <Banner tone="danger" className="mb-4">
            <div>
              {t("agents.loadError")}
              <button className="ml-2 underline" onClick={() => void agents.refetch()}>
                {t("common.retry")}
              </button>
            </div>
          </Banner>
        )}

        {agents.data && agents.data.length === 0 && !creating && (
          <Card className="mb-4 p-6 text-center text-sm text-text-muted">
            {t("agents.list.empty")}
          </Card>
        )}

        {agents.data && agents.data.length > 0 && (
          <div className="mb-4 flex flex-col gap-3">
            {agents.data.map((agent) =>
              editingId === agent.id ? (
                <Card key={agent.id} className="p-4">
                  <AgentForm
                    initial={agent}
                    submitLabel={t("agents.form.update")}
                    pending={update.isPending}
                    onCancel={() => setEditingId(null)}
                    onSubmit={(input) =>
                      update.mutate(
                        { id: agent.id, input },
                        { onSuccess: () => setEditingId(null) },
                      )
                    }
                  />
                </Card>
              ) : (
                <Card key={agent.id} className="p-4">
                  <div className="flex flex-wrap items-start justify-between gap-2">
                    <div>
                      <div className="flex items-center gap-2">
                        <b className="text-sm">{agent.name}</b>
                        {agent.active && <Badge tone="green">{t("agents.active")}</Badge>}
                      </div>
                      <p className="mt-1 text-sm text-text-muted">{agent.description}</p>
                      <p className="mt-1 text-xs text-text-muted">{agent.model}</p>
                      <div className="mt-2 flex flex-wrap gap-1.5">
                        {agent.tools.map((tool) => (
                          <Chip key={tool}>{tool}</Chip>
                        ))}
                      </div>
                    </div>
                    <div className="flex shrink-0 gap-2">
                      {!agent.active && (
                        <Button
                          className="text-xs"
                          disabled={setActive.isPending}
                          onClick={() => setActive.mutate(agent.id)}
                        >
                          {t("agents.setActive")}
                        </Button>
                      )}
                      <Button className="text-xs" onClick={() => setEditingId(agent.id)}>
                        {t("common.edit")}
                      </Button>
                      <Button
                        variant="danger"
                        className="text-xs"
                        disabled={del.isPending}
                        onClick={() => {
                          if (window.confirm(t("agents.confirmDelete"))) del.mutate(agent.id);
                        }}
                      >
                        {t("common.delete")}
                      </Button>
                    </div>
                  </div>
                </Card>
              ),
            )}
          </div>
        )}

        {(create.isError || update.isError || del.isError || setActive.isError) && (
          <Banner tone="danger" className="mb-4">
            {t("agents.mutateError")}
          </Banner>
        )}

        {creating ? (
          <Card className="p-4">
            <AgentForm
              submitLabel={t("agents.form.create")}
              pending={create.isPending}
              onCancel={() => setCreating(false)}
              onSubmit={(input) => create.mutate(input, { onSuccess: () => setCreating(false) })}
            />
          </Card>
        ) : (
          <Button variant="primary" onClick={() => setCreating(true)}>
            + {t("agents.form.create")}
          </Button>
        )}
      </div>
    </>
  );
}
