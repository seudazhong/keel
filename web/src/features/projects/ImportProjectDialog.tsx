import { useEffect, useState, type FormEvent } from "react";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { useTranslation } from "../../lib/i18n";
import type { GitHubInstallation, ProjectImportInput } from "./types";

const inputClass =
  "w-full rounded-sm border border-border bg-surface px-3 py-2 text-sm outline-none focus:border-accent";

export function ImportProjectDialog({
  pending,
  installations,
  onCancel,
  onSubmit,
}: {
  pending: boolean;
  installations: GitHubInstallation[];
  onCancel: () => void;
  onSubmit: (input: ProjectImportInput) => void;
}) {
  const { t } = useTranslation();
  const [displayName, setDisplayName] = useState("");
  const [slug, setSlug] = useState("");
  const [slugEdited, setSlugEdited] = useState(false);
  const [repository, setRepository] = useState("");
  const [installationId, setInstallationId] = useState(
    installations[0] ? String(installations[0].installation_id) : "",
  );

  useEffect(() => {
    if (!installationId && installations[0]) {
      setInstallationId(String(installations[0].installation_id));
    }
  }, [installationId, installations]);

  function handleSubmit(e: FormEvent) {
    e.preventDefault();
    onSubmit({
      slug: slug.trim(),
      display_name: displayName.trim(),
      installation_id: Number(installationId),
      repo_full_name: repository.trim(),
    });
  }

  return (
    <Card className="p-4" role="dialog" aria-label={t("projects.import.title")}>
      <h2 className="mb-3 text-sm font-semibold">{t("projects.import.title")}</h2>
      <form className="flex flex-col gap-3" onSubmit={handleSubmit}>
        <label className="text-xs font-semibold text-text-soft">
          {t("projects.import.name")}
          <input
            className={inputClass}
            value={displayName}
            onChange={(e) => {
              const value = e.target.value;
              setDisplayName(value);
              if (!slugEdited) {
                setSlug(
                  value
                    .toLowerCase()
                    .replace(/[^a-z0-9]+/g, "-")
                    .replace(/^-|-$/g, "")
                    .slice(0, 40),
                );
              }
            }}
            required
          />
        </label>
        <label className="text-xs font-semibold text-text-soft">
          {t("projects.import.slug")}
          <input
            className={inputClass}
            value={slug}
            onChange={(e) => {
              setSlugEdited(true);
              setSlug(e.target.value);
            }}
            required
          />
        </label>
        <label className="text-xs font-semibold text-text-soft">
          {t("projects.import.installation")}
          <select
            className={inputClass}
            value={installationId}
            onChange={(e) => setInstallationId(e.target.value)}
            required
          >
            {installations.map((installation) => (
              <option key={installation.id} value={installation.installation_id}>
                {installation.account_login} · #{installation.installation_id}
              </option>
            ))}
          </select>
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
        <div className="flex justify-end gap-2">
          <Button type="button" onClick={onCancel}>
            {t("common.cancel")}
          </Button>
          <Button
            type="submit"
            variant="primary"
            disabled={
              pending ||
              installations.length === 0 ||
              !displayName.trim() ||
              !slug.trim() ||
              !repository.trim()
            }
          >
            {pending ? t("common.saving") : t("projects.import.submit")}
          </Button>
        </div>
      </form>
    </Card>
  );
}
