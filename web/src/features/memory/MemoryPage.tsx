import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import type { MemoryProposal, MemoryProposalStatus } from "./types";
import {
  useApproveMemoryProposal,
  useMemoryProposals,
  useRejectMemoryProposal,
  useRunConsolidation,
} from "./useMemory";

function fmtDate(value: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString();
}

function statusTone(status: MemoryProposalStatus): "green" | "amber" | "red" {
  if (status === "applied") return "green";
  if (status === "pending") return "amber";
  return "red";
}

function ProposalCard({
  proposal,
  onApprove,
  onReject,
  pending,
}: {
  proposal: MemoryProposal;
  onApprove: () => void;
  onReject: () => void;
  pending: boolean;
}) {
  const canResolve = proposal.status === "pending";
  return (
    <Card className="p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="text-base font-semibold">{proposal.block}</h2>
            <Badge tone={statusTone(proposal.status)}>{proposal.status}</Badge>
            <Badge>{Math.round(proposal.confidence * 100)}% confidence</Badge>
          </div>
          <p className="mt-1 text-xs text-text-muted">
            Expected block version {proposal.expected_version} · {fmtDate(proposal.created_at)}
          </p>
        </div>
        <div className="flex gap-2">
          <Button disabled={!canResolve || pending} onClick={onReject}>
            Reject
          </Button>
          <Button variant="primary" disabled={!canResolve || pending} onClick={onApprove}>
            Approve & apply
          </Button>
        </div>
      </div>

      <div className="mt-4">
        <h3 className="text-xs font-semibold uppercase tracking-wide text-text-muted">
          Proposed value
        </h3>
        <pre className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap break-words rounded-sm border border-border bg-surface-2 p-3 text-sm">
          {proposal.proposed_value}
        </pre>
      </div>

      <div className="mt-4">
        <h3 className="text-xs font-semibold uppercase tracking-wide text-text-muted">Reason</h3>
        <p className="mt-1 text-sm text-text-soft">{proposal.reason}</p>
      </div>

      <div className="mt-4">
        <h3 className="text-xs font-semibold uppercase tracking-wide text-text-muted">Evidence</h3>
        {proposal.source_event_ids.length > 0 ? (
          <p className="mt-1 break-words text-sm text-text-soft">
            Source events: {proposal.source_event_ids.join(", ")}
          </p>
        ) : (
          <p className="mt-1 text-sm text-text-muted">No source event IDs were attached.</p>
        )}
      </div>

      {proposal.resolved_at && (
        <p className="mt-4 text-xs text-text-muted">
          Resolved {fmtDate(proposal.resolved_at)}
          {proposal.resolved_by ? ` by ${proposal.resolved_by}` : ""}
        </p>
      )}
    </Card>
  );
}

export function MemoryPage() {
  const proposals = useMemoryProposals();
  const approve = useApproveMemoryProposal();
  const reject = useRejectMemoryProposal();
  const consolidation = useRunConsolidation();
  const mutationError = approve.error ?? reject.error;

  return (
    <>
      <Topbar
        title="Memory"
        sub="· Consolidation proposals"
        right={
          <Button
            variant="primary"
            disabled={consolidation.isPending}
            onClick={() => consolidation.mutate()}
          >
            {consolidation.isPending ? "Starting…" : "Run consolidation"}
          </Button>
        }
      />
      <div className="w-full max-w-[900px] p-[22px]">
        <Banner tone="info" className="mb-4">
          <span>ℹ️</span>
          <div>
            This page reviews consolidation proposals. A full API for directly listing and editing
            memory blocks is not available yet, so this UI does not simulate block editing.
          </div>
        </Banner>

        {consolidation.isSuccess && (
          <Banner tone="info" className="mb-4">
            Consolidation was queued. New proposals will appear after processing.
          </Banner>
        )}
        {consolidation.isError && (
          <Banner tone="danger" className="mb-4">
            Could not start consolidation: {consolidation.error.message}
          </Banner>
        )}
        {mutationError && (
          <Banner tone="danger" className="mb-4">
            Could not resolve proposal: {mutationError.message}. Refresh before retrying.
          </Banner>
        )}

        {proposals.isLoading && <Skeleton className="h-64" />}
        {proposals.isError && (
          <Banner tone="danger">
            <div>
              Could not load memory proposals.
              <button className="ml-2 underline" onClick={() => void proposals.refetch()}>
                Retry
              </button>
            </div>
          </Banner>
        )}
        {!proposals.isLoading && !proposals.isError && proposals.data?.length === 0 && (
          <Card className="p-6 text-center">
            <p className="text-sm text-text-muted">There are no consolidation proposals to review.</p>
            <Button
              className="mt-4"
              disabled={consolidation.isPending}
              onClick={() => consolidation.mutate()}
            >
              Run consolidation
            </Button>
          </Card>
        )}
        {proposals.data && proposals.data.length > 0 && (
          <div className="flex flex-col gap-4">
            {proposals.data.map((proposal) => (
              <ProposalCard
                key={proposal.id}
                proposal={proposal}
                pending={
                  (approve.isPending && approve.variables === proposal.id) ||
                  (reject.isPending && reject.variables === proposal.id)
                }
                onApprove={() => approve.mutate(proposal.id)}
                onReject={() => reject.mutate(proposal.id)}
              />
            ))}
          </div>
        )}
      </div>
    </>
  );
}
