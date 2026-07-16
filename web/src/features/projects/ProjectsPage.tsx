import { useState } from "react";
import { Link } from "react-router-dom";
import { Topbar } from "../../components/Topbar";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { useTranslation } from "../../lib/i18n";
import { ImportProjectDialog } from "./ImportProjectDialog";
import { useImportProject, useProjects } from "./useProjects";

export function ProjectsPage() {
  const { t } = useTranslation();
  const projects = useProjects();
  const importProject = useImportProject();
  const [importing, setImporting] = useState(false);

  return (
    <>
      <Topbar title={t("projects.title")} sub={t("projects.subtitle")} />
      <div className="w-full max-w-[900px] p-[22px]">
        <Banner tone="info" className="mb-4">
          <span>🧪</span>
          <div>{t("projects.preview.banner")}</div>
        </Banner>

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
                  <b className="text-sm">{project.name}</b>
                  <p className="mt-1 text-sm text-text-muted">{project.description}</p>
                  <p className="mt-2 truncate font-mono text-xs text-text-muted">
                    {project.repository}
                  </p>
                </Card>
              </Link>
            ))}
          </div>
        )}

        {importProject.isError && (
          <Banner tone="danger" className="mb-4">
            {t("common.requestFailed")}
          </Banner>
        )}

        {importing ? (
          <ImportProjectDialog
            pending={importProject.isPending}
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
