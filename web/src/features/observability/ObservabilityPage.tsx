import type { ReactNode } from "react";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { useOverview } from "./useOverview";

function Stat({ label, value, sub }: { label: string; value: string; sub?: ReactNode }) {
  return (
    <Card className="p-4">
      <div className="text-xs uppercase tracking-wide text-text-muted">{label}</div>
      <div className="mt-1 text-2xl font-bold tabular-nums">{value}</div>
      {sub ? <div className="mt-0.5 text-xs text-text-muted">{sub}</div> : null}
    </Card>
  );
}

export function ObservabilityPage() {
  const { data, isLoading, isError, refetch } = useOverview();
  const totalTokens = data ? data.usage.prompt_tokens + data.usage.completion_tokens : 0;

  return (
    <>
      <Topbar
        title="Observability"
        sub="· 作用域内的用量与运行概览"
        right={<Badge tone="violet">scope: personal · web:local</Badge>}
      />
      <div className="w-full max-w-[900px] p-[22px]">
        {isLoading && <Skeleton className="h-28" />}

        {isError && (
          <Banner tone="danger">
            <span>⚠️</span>
            <div>
              加载概览失败。
              <button className="ml-2 underline" onClick={() => void refetch()}>
                重试
              </button>
            </div>
          </Banner>
        )}

        {data && (
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Stat label="会话" value={data.sessions.toLocaleString()} />
            <Stat label="运行" value={data.usage.runs.toLocaleString()} />
            <Stat
              label="启用的计划"
              value={`${data.schedules.enabled}/${data.schedules.total}`}
            />
            <Stat
              label="待审批"
              value={data.approvals.pending.toLocaleString()}
              sub={`已批准 ${data.approvals.granted} · 已拒绝 ${data.approvals.denied}`}
            />
            <Stat label="已连接连接器" value={data.connectors.toLocaleString()} />
            <Stat
              label="Tokens"
              value={totalTokens.toLocaleString()}
              sub={`输入 ${data.usage.prompt_tokens.toLocaleString()} · 输出 ${data.usage.completion_tokens.toLocaleString()}`}
            />
            <Stat label="缓存读取" value={data.usage.cache_read_tokens.toLocaleString()} />
            <Stat label="成本" value={`$${data.usage.cost_usd.toFixed(4)}`} />
          </div>
        )}
      </div>
    </>
  );
}
