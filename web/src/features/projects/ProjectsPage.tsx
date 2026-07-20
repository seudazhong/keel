import { useState } from "react";
import { Link } from "react-router-dom";
import { Topbar } from "../../components/Topbar";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { safeApiErrorMessage } from "../../lib/api";
import { useTranslation } from "../../lib/i18n";
import { ImportProjectDialog } from "./ImportProjectDialog";
import { useGitHubInstallations, useImportProject, useProjects } from "./useProjects";

export function ProjectsPage() {
  const { t } = useTranslation();
  const projects = useProjects();
  const installations = useGitHubInstallations();
  const importProject = useImportProject();
  const [importing, setImporting] = useState(false);

  return (
    <>
      <Topbar title={t("projects.title")} sub={t("projects.subtitle")} />
      <div className="w-full max-w-[900px] p-[22px]">
        <Banner tone="info" className="mb-4">
          <span>ℹ️</span>
          <div>{t("projects.preview.banner")}</div>
        </Banner>

        {installations.data?.length === 0 && (
          <Banner tone="warn" className="mb-4">
            {t("projects.installationRequired")}
          </Banner>
        )}

        {projects.isLoading && <Skeleton className="h-40" />}

        {projects.isError && (
          <Banner tone="danger" className="mb-4">
            <div>
              {t("projects.loadError")}
              <button className="ml-2 underline" onClick={() => void projects.refetch()}>
                {t("common.retry")}
              </button>
            </div>
          </Banner>
        )}

        {projects.data && projects.data.length === 0 && !importing && (
          <Card className="mb-4 p-6 text-center text-sm text-text-muted">
            {t("projects.list.empty")}
          </Card>
        )}

        {projects.data && projects.data.length > 0 && (
          <div className="mb-4 grid gap-3 sm:grid-cols-2">
            {projects.data.map((project) => (
              <Link key={project.id} to={`/projects/${encodeURIComponent(project.id)}`}>
                <Card className="h-full p-4 hover:border-accent">
                  <b className="text-sm">{project.display_name}</b>
                  <p className="mt-1 text-sm text-text-muted">
                    {project.source} · {project.status} · {project.default_branch}
                  </p>
                  <p className="mt-2 truncate font-mono text-xs text-text-muted">{project.slug}</p>
                </Card>
              </Link>
            ))}
          </div>
        )}

        {importProject.isError && (
          <Banner tone="danger" className="mb-4">
            {safeApiErrorMessage(importProject.error, t("common.requestFailed"))}
          </Banner>
        )}
        {importProject.isSuccess && (
          <Banner tone="success" className="mb-4">
            {t("projects.import.success", { name: importProject.data.display_name })}
          </Banner>
        )}

        {importing ? (
          <ImportProjectDialog
            pending={importProject.isPending}
            installations={installations.data ?? []}
            onCancel={() => setImporting(false)}
            onSubmit={(input) =>
              importProject.mutate(input, { onSuccess: () => setImporting(false) })
            }
          />
        ) : (
          <Button variant="primary" onClick={() => setImporting(true)}>
            + {t("projects.import.action")}
          </Button>
        )}
      </div>
    </>
  );
}
