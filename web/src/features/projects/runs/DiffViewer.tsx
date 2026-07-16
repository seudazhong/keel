import { useState } from "react";
import { Badge } from "../../../components/ui/badge";
import { Card } from "../../../components/ui/card";
import { cn } from "../../../lib/cn";
import { useTranslation } from "../../../lib/i18n";
import type { DiffFile, DiffLineType } from "./types";

const lineClass: Record<DiffLineType, string> = {
  add: "bg-green/10 text-green",
  del: "bg-red/10 text-red",
  context: "text-text-muted",
};

const linePrefix: Record<DiffLineType, string> = {
  add: "+",
  del: "-",
  context: " ",
};

function DiffFileCard({ file }: { file: DiffFile }) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);

  return (
    <Card className="overflow-hidden">
      <button
        type="button"
        className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left hover:bg-surface-2"
        aria-expanded={expanded}
        onClick={() => setExpanded((v) => !v)}
      >
        <span className="truncate font-mono text-xs">{file.path}</span>
        <span className="flex shrink-0 items-center gap-2 text-xs">
          <Badge tone="green">{t("runs.diff.additions", { count: file.additions })}</Badge>
          <Badge tone="red">{t("runs.diff.deletions", { count: file.deletions })}</Badge>
          <span className="text-text-muted">
            {expanded ? t("runs.diff.collapse") : t("runs.diff.expand")}
          </span>
        </span>
      </button>
      {expanded && (
        <div className="max-h-72 overflow-auto border-t border-border bg-surface-2 font-mono text-xs">
          {file.hunks.map((hunk, hi) => (
            <div key={hi}>
              <div className="px-3 py-1 text-text-muted">{hunk.header}</div>
              {hunk.lines.map((line, li) => (
                <div
                  key={li}
                  className={cn("whitespace-pre px-3 py-0.5", lineClass[line.type])}
                >
                  {linePrefix[line.type]} {line.text}
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

export function DiffViewer({ files }: { files: DiffFile[] }) {
  const { t } = useTranslation();

  if (files.length === 0) {
    return <Card className="p-6 text-center text-sm text-text-muted">{t("runs.diff.empty")}</Card>;
  }

  return (
    <div className="flex flex-col gap-2" aria-label={t("runs.diff.title")}>
      {files.map((file) => (
        <DiffFileCard key={file.path} file={file} />
      ))}
    </div>
  );
}
