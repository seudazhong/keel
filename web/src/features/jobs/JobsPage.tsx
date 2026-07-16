import { useEffect, useState } from "react";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import type { Job, JobStatus } from "./types";
import { canCancelJob, useCancelJob, useJob, useJobs } from "./useJobs";

function fmtDate(value: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString();
}

function statusTone(status: JobStatus): "green" | "amber" | "red" | "sky" {
  if (status === "succeeded") return "green";
  if (status === "failed" || status === "cancelled") return "red";
  return status === "running" ? "sky" : "amber";
}

function progressLabel(job: Job): string {
  if (job.progress_total && job.progress_total > 0) {
    return `${job.progress_current} / ${job.progress_total}`;
  }
  return String(job.progress_current);
}

function JobDetail({ job }: { job: Job }) {
  const cancel = useCancelJob();
  const cancellable = canCancelJob(job);
  const progress =
    job.progress_total && job.progress_total > 0
      ? Math.min(100, Math.round((job.progress_current / job.progress_total) * 100))
      : null;

  return (
    <Card className="p-4" aria-label={`Job ${job.id} detail`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="text-base font-semibold">{job.kind}</h2>
            <Badge tone={statusTone(job.status)}>{job.status}</Badge>
            <Badge>{job.cancel_mode} cancel</Badge>
          </div>
          <p className="mt-1 break-all text-xs text-text-muted">{job.id}</p>
        </div>
        <Button
          variant="danger"
          disabled={!cancellable || cancel.isPending}
          title={
            cancellable
              ? "Request cancellation"
              : job.cancel_requested
                ? "Cancellation already requested"
                : "This job cannot be cancelled in its current state"
          }
          onClick={() => cancel.mutate(job.id)}
        >
          {cancel.isPending ? "Cancelling…" : job.cancel_requested ? "Cancel requested" : "Cancel"}
        </Button>
      </div>

      <dl className="mt-4 grid gap-3 text-sm sm:grid-cols-2">
        <div>
          <dt className="text-xs text-text-muted">Progress</dt>
          <dd className="font-semibold">{progressLabel(job)}</dd>
        </div>
        <div>
          <dt className="text-xs text-text-muted">Attempt</dt>
          <dd className="font-semibold">
            {job.attempt} / {job.max_attempts}
          </dd>
        </div>
        <div>
          <dt className="text-xs text-text-muted">Started</dt>
          <dd>{fmtDate(job.started_at)}</dd>
        </div>
        <div>
          <dt className="text-xs text-text-muted">Updated</dt>
          <dd>{fmtDate(job.updated_at)}</dd>
        </div>
      </dl>

      {progress !== null && (
        <div
          className="mt-3 h-2 overflow-hidden rounded-full bg-surface-2"
          role="progressbar"
          aria-label="Job progress"
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={progress}
        >
          <div className="h-full bg-accent" style={{ width: `${progress}%` }} />
        </div>
      )}
      {job.progress_message && <p className="mt-2 text-sm text-text-soft">{job.progress_message}</p>}

      {(job.error_kind || job.error_message) && (
        <Banner tone="danger" className="mt-4">
          <div>
            <b>{job.error_kind ?? "Job failed"}</b>
            {job.error_message && <p className="mt-1">{job.error_message}</p>}
          </div>
        </Banner>
      )}

      {(job.result_message || job.result) && (
        <div className="mt-4 rounded-sm border border-border bg-surface-2 p-3">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-text-muted">Result</h3>
          {job.result_message && <p className="mt-1 text-sm">{job.result_message}</p>}
          {job.result && (
            <pre className="mt-2 max-h-56 overflow-auto whitespace-pre-wrap break-words text-xs text-text-soft">
              {JSON.stringify(job.result, null, 2)}
            </pre>
          )}
        </div>
      )}

      {cancel.isError && (
        <Banner tone="danger" className="mt-4">
          Cancellation failed: {cancel.error.message}
        </Banner>
      )}
    </Card>
  );
}

export function JobsPage() {
  const jobs = useJobs();
  const [selectedId, setSelectedId] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedId && jobs.data?.length) setSelectedId(jobs.data[0].id);
    if (selectedId && jobs.data && !jobs.data.some((job) => job.id === selectedId)) {
      setSelectedId(jobs.data[0]?.id ?? null);
    }
  }, [jobs.data, selectedId]);

  const detail = useJob(selectedId);

  return (
    <>
      <Topbar title="Jobs" sub="· Durable background work" />
      <div className="grid w-full max-w-[1100px] gap-4 p-[22px] lg:grid-cols-[minmax(300px,0.85fr)_minmax(420px,1.15fr)]">
        <section aria-label="Jobs list">
          {jobs.isLoading && <Skeleton className="h-48" />}
          {jobs.isError && (
            <Banner tone="danger">
              <div>
                Could not load jobs.
                <button className="ml-2 underline" onClick={() => void jobs.refetch()}>
                  Retry
                </button>
              </div>
            </Banner>
          )}
          {!jobs.isLoading && !jobs.isError && jobs.data?.length === 0 && (
            <Card className="p-6 text-center text-sm text-text-muted">
              No durable jobs have been created for this scope.
            </Card>
          )}
          {jobs.data && jobs.data.length > 0 && (
            <Card className="overflow-hidden">
              {jobs.data.map((job) => (
                <button
                  key={job.id}
                  className={`block w-full border-b border-border px-4 py-3 text-left last:border-0 hover:bg-surface-2 ${
                    selectedId === job.id ? "bg-accent/10" : ""
                  }`}
                  aria-pressed={selectedId === job.id}
                  onClick={() => setSelectedId(job.id)}
                >
                  <span className="flex items-center justify-between gap-2">
                    <b className="truncate text-sm">{job.kind}</b>
                    <Badge tone={statusTone(job.status)}>{job.status}</Badge>
                  </span>
                  <span className="mt-1 flex justify-between gap-2 text-xs text-text-muted">
                    <span className="truncate">{job.id}</span>
                    <span>{progressLabel(job)}</span>
                  </span>
                </button>
              ))}
            </Card>
          )}
        </section>

        <section aria-label="Selected job">
          {selectedId && detail.isLoading && <Skeleton className="h-72" />}
          {selectedId && detail.isError && (
            <Banner tone="danger">
              <div>
                Could not load job detail.
                <button className="ml-2 underline" onClick={() => void detail.refetch()}>
                  Retry
                </button>
              </div>
            </Banner>
          )}
          {detail.data && <JobDetail job={detail.data} />}
        </section>
      </div>
    </>
  );
}
