import { Badge } from "../../../components/ui/badge";
import { Card } from "../../../components/ui/card";
import { useTranslation } from "../../../lib/i18n";
import type { Run, RunStepStatus } from "./types";

const stepIcon: Record<RunStepStatus, string> = {
  done: "✅",
  running: "🔄",
  pending: "⏳",
  failed: "❌",
};

const statusTone = {
  running: "sky",
  succeeded: "green",
  failed: "red",
  awaiting_approval: "amber",
} as const;

export function RunTimeline({
  runs,
  selectedId,
  onSelect,
}: {
  runs: Run[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  const { t } = useTranslation();

  if (runs.length === 0) {
    return (
      <Card className="p-6 text-center text-sm text-text-muted">{t("runs.timeline.empty")}</Card>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <Card className="overflow-hidden" aria-label={t("runs.timeline.title")}>
        {runs.map((run) => (
          <button
            key={run.id}
            type="button"
            className={`block w-full border-b border-border px-4 py-3 text-left last:border-0 hover:bg-surface-2 ${
              selectedId === run.id ? "bg-accent/10" : ""
            }`}
            aria-pressed={selectedId === run.id}
            onClick={() => onSelect(run.id)}
          >
            <span className="flex items-center justify-between gap-2">
              <b className="truncate text-sm">{run.summary}</b>
              <Badge tone={statusTone[run.status]}>{t(`runs.status.${run.status}`)}</Badge>
            </span>
            <ol className="mt-2 flex flex-wrap gap-2 text-xs text-text-muted">
              {run.steps.map((step) => (
                <li key={step.id} className="inline-flex items-center gap-1">
                  <span aria-hidden="true">{stepIcon[step.status]}</span>
                  {step.label}
                </li>
              ))}
            </ol>
          </button>
        ))}
      </Card>
    </div>
  );
}
