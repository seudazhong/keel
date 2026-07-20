import { useNavigate, useParams } from "react-router-dom";
import { Topbar } from "../../components/Topbar";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { useTranslation } from "../../lib/i18n";
import { useDeleteProject, useProject } from "./useProjects";

export function ProjectDetailPage() {
  const { id } = useParams<{ id: string }>();
  return <ProjectDetailView key={id ?? "unknown"} id={id} />;
}

function ProjectDetailView({ id }: { id: string | undefined }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const project = useProject(id);
  const del = useDeleteProject();

  return (
    <>
      <Topbar
        title={project.data?.display_name ?? t("projects.title")}
        sub={`· ${t("projects.detail.overview")}`}
      />
      <div className="w-full max-w-[1100px] p-[22px]">
        {project.isLoading && <Skeleton className="h-40" />}

        {project.isError && (
          <Banner tone="danger">
            <div>{t("projects.detail.notFound")}</div>
          </Banner>
        )}

        {project.data && (
          <Card className="p-4">
            <dl className="grid gap-3 text-sm sm:grid-cols-2">
              <div>
                <dt className="text-xs text-text-muted">{t("projects.detail.slug")}</dt>
                <dd className="break-all font-mono text-xs">{project.data.slug}</dd>
              </div>
              <div>
                <dt className="text-xs text-text-muted">{t("projects.detail.branch")}</dt>
                <dd>{project.data.default_branch}</dd>
              </div>
              <div>
                <dt className="text-xs text-text-muted">{t("projects.detail.source")}</dt>
                <dd>{project.data.source}</dd>
              </div>
              <div>
                <dt className="text-xs text-text-muted">{t("projects.detail.status")}</dt>
                <dd>{project.data.status}</dd>
              </div>
              <div>
                <dt className="text-xs text-text-muted">{t("projects.detail.repositoryId")}</dt>
                <dd>{project.data.github_repository_id ?? "—"}</dd>
              </div>
              <div>
                <dt className="text-xs text-text-muted">{t("projects.detail.created")}</dt>
                <dd>
                  {project.data.created_at
                    ? new Date(project.data.created_at).toLocaleString()
                    : "—"}
                </dd>
              </div>
            </dl>
            <Banner tone="info" className="mt-4">
              {t("projects.detail.runLimit")}
            </Banner>
            <div className="mt-4">
              <Button
                variant="danger"
                disabled={del.isPending}
                onClick={() => {
                  if (window.confirm(t("projects.detail.confirmDelete"))) {
                    del.mutate(
                      { id: project.data.id, version: project.data.version },
                      { onSuccess: () => navigate("/projects") },
                    );
                  }
                }}
              >
                {t("projects.detail.delete")}
              </Button>
            </div>
          </Card>
        )}
      </div>
    </>
  );
}
