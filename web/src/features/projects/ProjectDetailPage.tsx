import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Topbar } from "../../components/Topbar";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { useTranslation } from "../../lib/i18n";
import { DiffViewer } from "./runs/DiffViewer";
import { RunApprovalPanel } from "./runs/RunApprovalPanel";
import { RunTimeline } from "./runs/RunTimeline";
import { useRunDiff, useRuns } from "./runs/useRuns";
import { useDeleteProject, useProject } from "./useProjects";

type Tab = "overview" | "runs";

function RunDiffSection({ runId }: { runId: string }) {
  const { t } = useTranslation();
  const diff = useRunDiff(runId);
  return (
    <div>
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-text-muted">
        {t("runs.diff.title")}
      </h3>
      {diff.isLoading && <Skeleton className="h-24" />}
      {diff.isError && (
        <Banner tone="danger">
          <div>{t("common.requestFailed")}</div>
        </Banner>
      )}
      {diff.data && <DiffViewer files={diff.data} />}
    </div>
  );
}

export function ProjectDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { t } = useTranslation();
  const navigate = useNavigate();
  const project = useProject(id);
  const del = useDeleteProject();
  const runs = useRuns(id);
  const [tab, setTab] = useState<Tab>("overview");
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedRunId && runs.data?.length) setSelectedRunId(runs.data[0].id);
  }, [runs.data, selectedRunId]);

  return (
    <>
      <Topbar
        title={project.data?.name ?? t("projects.title")}
        sub={`· ${t(tab === "overview" ? "projects.detail.overview" : "projects.detail.runs")}`}
      />
      <div className="w-full max-w-[1100px] p-[22px]">
        {project.isLoading && <Skeleton className="h-40" />}

        {project.isError && (
          <Banner tone="danger">
            <div>{t("projects.detail.notFound")}</div>
          </Banner>
        )}

        {project.data && (
          <>
            <div className="mb-4 flex gap-2 border-b border-border" role="tablist">
              {(["overview", "runs"] as const).map((tabId) => (
                <button
                  key={tabId}
                  type="button"
                  role="tab"
                  aria-selected={tab === tabId}
                  className={`px-3 py-2 text-sm font-medium ${
                    tab === tabId
                      ? "border-b-2 border-accent text-accent"
                      : "text-text-muted hover:text-text-soft"
                  }`}
                  onClick={() => setTab(tabId)}
                >
                  {t(tabId === "overview" ? "projects.detail.overview" : "projects.detail.runs")}
                </button>
              ))}
            </div>

            {tab === "overview" && (
              <Card className="p-4">
                <dl className="grid gap-3 text-sm sm:grid-cols-2">
                  <div>
                    <dt className="text-xs text-text-muted">{t("projects.detail.repository")}</dt>
                    <dd className="break-all font-mono text-xs">{project.data.repository}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-text-muted">{t("projects.detail.branch")}</dt>
                    <dd>{project.data.default_branch}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-text-muted">{t("projects.detail.agent")}</dt>
                    <dd>{project.data.agent_id ?? "—"}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-text-muted">{t("projects.detail.created")}</dt>
                    <dd>{new Date(project.data.created_at).toLocaleString()}</dd>
                  </div>
                </dl>
                <p className="mt-3 text-sm text-text-muted">{project.data.description}</p>
                <div className="mt-4">
                  <Button
                    variant="danger"
                    disabled={del.isPending}
                    onClick={() => {
                      if (window.confirm(t("projects.detail.confirmDelete"))) {
                        del.mutate(project.data.id, { onSuccess: () => navigate("/projects") });
                      }
                    }}
                  >
                    {t("projects.detail.delete")}
                  </Button>
                </div>
              </Card>
            )}

            {tab === "runs" && (
              <div>
                <Banner tone="info" className="mb-4">
                  <span>🧪</span>
                  <div>{t("projects.runs.optionalHint")}</div>
                </Banner>
                {runs.isLoading && <Skeleton className="h-40" />}
                {runs.isError && (
                  <Banner tone="danger">
                    <div>{t("common.requestFailed")}</div>
                  </Banner>
                )}
                {runs.data && (
                  <div className="grid gap-4 lg:grid-cols-[minmax(280px,0.85fr)_minmax(380px,1.15fr)]">
                    <RunTimeline
                      runs={runs.data}
                      selectedId={selectedRunId}
                      onSelect={setSelectedRunId}
                    />
                    <div className="flex flex-col gap-4">
                      {selectedRunId ? (
                        <>
                          <RunDiffSection runId={selectedRunId} />
                          <RunApprovalPanel runId={selectedRunId} />
                        </>
                      ) : (
                        <Card className="p-6 text-center text-sm text-text-muted">
                          {t("runs.select")}
                        </Card>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </>
  );
}
