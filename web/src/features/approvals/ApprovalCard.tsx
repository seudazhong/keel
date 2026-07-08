import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Chip } from "../../components/ui/chip";
import type { Approval } from "./types";

export function ApprovalCard({
  approval,
  onResolve,
  pending,
}: {
  approval: Approval;
  onResolve: (decision: "approve" | "reject") => void;
  pending: boolean;
}) {
  const to = typeof approval.args.to === "string" ? approval.args.to : "";
  return (
    <Card className="mb-4 border-red/30">
      <div className="flex items-center gap-2.5 border-b border-red/20 bg-red/5 px-4 py-3.5">
        <h3 className="text-sm font-semibold text-red">✉️ {approval.tool} · 外发动作</h3>
        <Badge tone="red">高风险 · 受污点内容影响</Badge>
        <div className="flex-1" />
        <span className="text-xs text-text-muted">
          定时运行 · run {approval.run_id.slice(0, 8)} · 已挂起
        </span>
      </div>
      <div className="flex flex-col gap-3 p-4">
        {approval.reason === "tainted" && (
          <Banner tone="danger">
            <span>🛡️</span>
            <div>
              <b>Confused-deputy 警示：</b>本次运行读入了 <b>tainted</b> 内容，外发需你批准——
              注入内容无法自动促成发送。
            </div>
          </Banner>
        )}
        <div className="grid grid-cols-2 gap-3">
          <div>
            <div className="mb-1 text-xs font-semibold text-text-soft">动作</div>
            <Chip className="w-full justify-start font-mono">{approval.tool}</Chip>
          </div>
          <div>
            <div className="mb-1 text-xs font-semibold text-text-soft">收件人</div>
            <Chip className="w-full justify-start text-red">{to}</Chip>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <Chip>⏸ run 已挂起</Chip>
          <Chip className="text-amber">超时 fail-closed</Chip>
        </div>
        <div className="flex gap-2">
          <Button variant="danger" disabled={pending} onClick={() => onResolve("reject")}>
            拒绝
          </Button>
          <Button
            variant="primary"
            className="ml-auto"
            disabled={pending}
            onClick={() => onResolve("approve")}
          >
            批准
          </Button>
        </div>
      </div>
    </Card>
  );
}
