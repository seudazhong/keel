import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Skeleton } from "../../components/ui/skeleton";
import { ApprovalCard } from "./ApprovalCard";
import { useApprovals, useResolveApproval } from "./useApprovals";

export function ApprovalsPage() {
  const { data, isLoading, isError, refetch } = useApprovals();
  const resolve = useResolveApproval();
  const count = data?.length ?? 0;

  return (
    <>
      <Topbar
        title="Approvals"
        sub="· 无人值守运行的挂起审批（G5，超时 fail-closed）"
        right={count > 0 ? <Badge tone="amber">{count} 待处理</Badge> : undefined}
      />
      <div className="w-full max-w-[900px] p-[22px]">
        <Banner tone="info" className="mb-4">
          <span>🔒</span>
          <div>
            无人值守运行挂起等待的审批——跨重启存活，超时 fail-closed。批准后自动恢复并幂等发送一次。
          </div>
        </Banner>

        {isLoading && (
          <div className="flex flex-col gap-3">
            <Skeleton className="h-28" />
            <Skeleton className="h-28" />
          </div>
        )}

        {isError && (
          <Banner tone="danger">
            <span>⚠️</span>
            <div>
              加载审批失败。
              <button className="ml-2 underline" onClick={() => void refetch()}>
                重试
              </button>
            </div>
          </Banner>
        )}

        {data && count === 0 && <p className="text-text-muted">没有待处理的审批。</p>}

        {data?.map((a) => (
          <ApprovalCard
            key={a.id}
            approval={a}
            pending={resolve.isPending}
            onResolve={(decision) => resolve.mutate({ id: a.id, decision })}
          />
        ))}
      </div>
    </>
  );
}
