import { useState, type FormEvent } from "react";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { safeApiErrorMessage } from "../../lib/api";
import { useTranslation } from "../../lib/i18n";
import type { Agent, AgentKind, CreateAgentInput, UpdateAgentInput } from "./types";
import {
  useArchiveAgent,
  useAgents,
  useCreateAgent,
  useUpdateAgent,
} from "./useAgents";

const inputClass =
  "w-full rounded-sm border border-border bg-surface px-3 py-2 text-sm outline-none focus:border-accent";

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
  onSubmit: (input: CreateAgentInput | UpdateAgentInput) => void;
}) {
  const { t } = useTranslation();
  const [name, setName] = useState(initial?.name ?? "");
  const [kind, setKind] = useState<AgentKind>(initial?.kind ?? "personal");
  const [persona, setPersona] = useState(initial?.persona ?? "");

  function handleSubmit(e: FormEvent) {
    e.preventDefault();
    if (initial) {
      onSubmit({
        expected_version: initial.version,
        name: name.trim(),
        persona: persona.trim(),
      });
    } else {
      onSubmit({ kind, name: name.trim(), persona: persona.trim() });
    }
  }

  return (
    <form className="flex flex-col gap-3" onSubmit={handleSubmit}>
      <label className="text-xs font-semibold text-text-soft">
        {t("agents.form.name")}
        <input className={inputClass} value={name} onChange={(e) => setName(e.target.value)} required />
      </label>
      {!initial && (
        <label className="text-xs font-semibold text-text-soft">
          {t("agents.form.kind")}
          <select
            className={inputClass}
            value={kind}
            onChange={(e) => setKind(e.target.value as AgentKind)}
          >
            <option value="personal">{t("agents.kind.personal")}</option>
            <option value="team">{t("agents.kind.team")}</option>
          </select>
        </label>
      )}
      <label className="text-xs font-semibold text-text-soft">
        {t("agents.form.persona")}
        <textarea
          className={`${inputClass} min-h-32 resize-y`}
          value={persona}
          onChange={(e) => setPersona(e.target.value)}
          placeholder={t("agents.form.personaPlaceholder")}
        />
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
  const archive = useArchiveAgent();

  const [creating, setCreating] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);

  return (
    <>
      <Topbar
        title={t("agents.title")}
        sub={t("agents.subtitle")}
      />
      <div className="w-full max-w-[900px] p-[22px]">
        <Banner tone="info" className="mb-4">
          <span>ℹ️</span>
          <div>
            <p>{t("agents.preview.banner")}</p>
            <p className="mt-1">{t("agents.preview.limit")}</p>
          </div>
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
                        { id: agent.id, input: input as UpdateAgentInput },
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
                        <Badge tone={agent.status === "active" ? "green" : "amber"}>
                          {agent.status}
                        </Badge>
                        <Badge>
                          {t(
                            agent.kind === "personal"
                              ? "agents.kind.personal"
                              : "agents.kind.team",
                          )}
                        </Badge>
                      </div>
                      <p className="mt-2 whitespace-pre-wrap text-sm text-text-muted">
                        {agent.persona || t("agents.persona.empty")}
                      </p>
                      <p className="mt-2 text-xs text-text-muted">
                        {agent.id} · v{agent.version}
                      </p>
                    </div>
                    <div className="flex shrink-0 gap-2">
                      <Button className="text-xs" onClick={() => setEditingId(agent.id)}>
                        {t("common.edit")}
                      </Button>
                      <Button
                        variant="danger"
                        className="text-xs"
                        disabled={archive.isPending || agent.status !== "active"}
                        onClick={() => {
                          if (window.confirm(t("agents.confirmArchive"))) {
                            archive.mutate({ id: agent.id, version: agent.version });
                          }
                        }}
                      >
                        {t("agents.archive")}
                      </Button>
                    </div>
                  </div>
                </Card>
              ),
            )}
          </div>
        )}

        {(create.isError || update.isError || archive.isError) && (
          <Banner tone="danger" className="mb-4">
            {safeApiErrorMessage(
              create.error ?? update.error ?? archive.error,
              t("agents.mutateError"),
            )}
          </Banner>
        )}

        {creating ? (
          <Card className="p-4">
            <AgentForm
              submitLabel={t("agents.form.create")}
              pending={create.isPending}
              onCancel={() => setCreating(false)}
              onSubmit={(input) =>
                create.mutate(input as CreateAgentInput, {
                  onSuccess: () => setCreating(false),
                })
              }
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
