import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { useRunSchedule, useSchedules, useToggleSchedule } from "./useSchedules";

function fmtDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "—" : d.toLocaleString();
}

function fmtTrigger(kind: string, intervalS: number, spec: string): string {
  if (kind !== "interval") return spec;
  if (intervalS % 86400 === 0) return `每 ${intervalS / 86400} 天`;
  if (intervalS % 3600 === 0) return `每 ${intervalS / 3600} 小时`;
  if (intervalS % 60 === 0) return `每 ${intervalS / 60} 分钟`;
  return `每 ${intervalS} 秒`;
}

export function SchedulesPage() {
  const { data, isLoading, isError, refetch } = useSchedules();
  const toggle = useToggleSchedule();
  const run = useRunSchedule();

  return (
    <>
      <Topbar
        title="Schedules"
        sub="· 定时与后台任务（interval / cron 触发）"
        right={<Badge tone="violet">scope: personal</Badge>}
      />
      <div className="w-full max-w-[900px] p-[22px]">
        {isLoading && <Skeleton className="h-28" />}

        {isError && (
          <Banner tone="danger">
            <span>⚠️</span>
            <div>
              加载计划任务失败。
              <button className="ml-2 underline" onClick={() => void refetch()}>
                重试
              </button>
            </div>
          </Banner>
        )}

        {data && (
          <Card className="overflow-hidden">
            {data.length === 0 ? (
              <p className="p-4 text-sm text-text-muted">
                还没有计划任务。通过 CLI 或 agent 的 schedule 工具创建后会出现在这里。
              </p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-xs text-text-muted">
                    <th className="px-4 py-2.5 font-medium">Agent</th>
                    <th className="px-4 py-2.5 font-medium">触发</th>
                    <th className="px-4 py-2.5 font-medium">下次运行</th>
                    <th className="px-4 py-2.5 font-medium">上次</th>
                    <th className="px-4 py-2.5 font-medium">状态</th>
                    <th className="px-4 py-2.5 font-medium" />
                  </tr>
                </thead>
                <tbody>
                  {data.map((s) => (
                    <tr key={s.id} className="border-b border-border last:border-0">
                      <td className="px-4 py-3 font-semibold">{s.agent_id}</td>
                      <td className="px-4 py-3 text-text-soft">
                        {fmtTrigger(s.trigger_kind, s.interval_s, s.spec)}
                      </td>
                      <td className="px-4 py-3 text-text-muted">{fmtDate(s.next_run_at)}</td>
                      <td className="px-4 py-3 text-text-muted">
                        {fmtDate(s.last_run_at)}
                        {s.last_status ? ` · ${s.last_status}` : ""}
                      </td>
                      <td className="px-4 py-3">
                        {s.enabled ? (
                          <Badge tone="green">● 已启用</Badge>
                        ) : (
                          <Badge tone="amber">‖ 已暂停</Badge>
                        )}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-right">
                        <Button
                          className="mr-1.5 px-2.5 py-1 text-xs"
                          disabled={toggle.isPending}
                          onClick={() => toggle.mutate({ id: s.id, enabled: !s.enabled })}
                        >
                          {s.enabled ? "暂停" : "启用"}
                        </Button>
                        <Button
                          variant="primary"
                          className="px-2.5 py-1 text-xs"
                          disabled={run.isPending}
                          onClick={() => run.mutate(s.id)}
                        >
                          立即运行
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Card>
        )}
      </div>
    </>
  );
}
