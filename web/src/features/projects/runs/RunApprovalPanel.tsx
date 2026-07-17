import { Banner } from "../../../components/ui/banner";
import { Button } from "../../../components/ui/button";
import { Card } from "../../../components/ui/card";
import { useTranslation } from "../../../lib/i18n";
import { useDecideRunApproval, useRunApprovals } from "./useRuns";

export function RunApprovalPanel({ runId }: { runId: string }) {
  const { t } = useTranslation();
  const approvals = useRunApprovals(runId);
  const decide = useDecideRunApproval(runId);

  const pending = approvals.data?.filter((a) => a.status === "pending") ?? [];
  const decided = approvals.data?.filter((a) => a.status !== "pending") ?? [];

  return (
    <div className="flex flex-col gap-3" aria-label={t("runs.approvals.title")}>
      {approvals.data && pending.length === 0 && decided.length === 0 && (
        <Card className="p-4 text-center text-sm text-text-muted">
          {t("runs.approvals.empty")}
        </Card>
      )}

      {pending.map((approval) => (
        <Card key={approval.id} className="p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <b className="text-sm">{approval.tool}</b>
              <p className="mt-1 text-sm text-text-muted">{approval.summary}</p>
            </div>
            <div className="flex gap-2">
              <Button
                variant="primary"
                disabled={decide.isPending}
                onClick={() => decide.mutate({ approvalId: approval.id, decision: "approve" })}
              >
                {t("runs.approvals.approve")}
              </Button>
              <Button
                variant="danger"
                disabled={decide.isPending}
                onClick={() => decide.mutate({ approvalId: approval.id, decision: "reject" })}
              >
                {t("runs.approvals.reject")}
              </Button>
            </div>
          </div>
        </Card>
      ))}

      {decided.length > 0 && (
        <Banner tone="info">
          <span>ℹ️</span>
          <span>{t("runs.approvals.decided")}</span>
        </Banner>
      )}
    </div>
  );
}
