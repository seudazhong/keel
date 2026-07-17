import { useState, type FormEvent } from "react";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { useTranslation } from "../../lib/i18n";
import type { ProjectImportInput } from "./types";

const inputClass =
  "w-full rounded-sm border border-border bg-surface px-3 py-2 text-sm outline-none focus:border-accent";

export function ImportProjectDialog({
  pending,
  onCancel,
  onSubmit,
}: {
  pending: boolean;
  onCancel: () => void;
  onSubmit: (input: ProjectImportInput) => void;
}) {
  const { t } = useTranslation();
  const [name, setName] = useState("");
  const [repository, setRepository] = useState("");
  const [branch, setBranch] = useState("main");

  function handleSubmit(e: FormEvent) {
    e.preventDefault();
    onSubmit({ name: name.trim(), repository: repository.trim(), default_branch: branch.trim() || "main" });
  }

  return (
    <Card className="p-4" role="dialog" aria-label={t("projects.import.title")}>
      <h2 className="mb-3 text-sm font-semibold">{t("projects.import.title")}</h2>
      <form className="flex flex-col gap-3" onSubmit={handleSubmit}>
        <label className="text-xs font-semibold text-text-soft">
          {t("projects.import.name")}
          <input className={inputClass} value={name} onChange={(e) => setName(e.target.value)} required />
        </label>
        <label className="text-xs font-semibold text-text-soft">
          {t("projects.import.repository")}
          <input
            className={inputClass}
            value={repository}
            onChange={(e) => setRepository(e.target.value)}
            required
          />
        </label>
        <label className="text-xs font-semibold text-text-soft">
          {t("projects.import.branch")}
          <input className={inputClass} value={branch} onChange={(e) => setBranch(e.target.value)} />
        </label>
        <div className="flex justify-end gap-2">
          <Button type="button" onClick={onCancel}>
            {t("common.cancel")}
          </Button>
          <Button type="submit" variant="primary" disabled={pending}>
            {pending ? t("common.saving") : t("projects.import.submit")}
          </Button>
        </div>
      </form>
    </Card>
  );
}
