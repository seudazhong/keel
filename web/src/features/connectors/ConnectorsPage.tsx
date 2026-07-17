import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Chip } from "../../components/ui/chip";
import { Skeleton } from "../../components/ui/skeleton";
import { useConnectors, useRevokeConnector } from "./useConnectors";

function fmtDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "—" : d.toLocaleString();
}

export function ConnectorsPage() {
  const { data, isLoading, isError, refetch } = useConnectors();
  const revoke = useRevokeConnector();
  const connected = data?.filter((c) => c.connected) ?? [];
  const addable = data?.filter((c) => !c.connected) ?? [];

  return (
    <>
      <Topbar
        title="Connectors"
        sub="· OAuth 集成，按 (scope, connector) 加密存储 token"
        right={<Badge tone="violet">scope: personal</Badge>}
      />
      <div className="w-full max-w-[900px] p-[22px]">
        {isLoading && <Skeleton className="h-28" />}

        {isError && (
          <Banner tone="danger">
            <span>⚠️</span>
            <div>
              加载连接器失败。
              <button className="ml-2 underline" onClick={() => void refetch()}>
                重试
              </button>
            </div>
          </Banner>
        )}

        {data && (
          <>
            <div className="mb-2.5 text-sm font-semibold text-text-soft">
              已连接 · {connected.length}
            </div>
            <Card className="mb-[22px] overflow-hidden">
              {connected.length === 0 ? (
                <p className="p-4 text-sm text-text-muted">
                  还没有已连接的连接器。可用的 OAuth 连接器支持从本页发起浏览器授权。
                </p>
              ) : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs text-text-muted">
                      <th className="px-4 py-2.5 font-medium">集成</th>
                      <th className="px-4 py-2.5 font-medium">授予范围（最小）</th>
                      <th className="px-4 py-2.5 font-medium">状态</th>
                      <th className="px-4 py-2.5 font-medium">最近更新</th>
                      <th className="px-4 py-2.5 font-medium" />
                    </tr>
                  </thead>
                  <tbody>
                    {connected.map((c) => (
                      <tr key={c.id} className="border-b border-border last:border-0">
                        <td className="px-4 py-3 font-semibold">
                          {c.icon} {c.name}
                        </td>
                        <td className="px-4 py-3">
                          <div className="flex flex-wrap gap-1.5">
                            {c.scopes.map((s) => (
                              <Chip key={s}>{s}</Chip>
                            ))}
                          </div>
                        </td>
                        <td className="px-4 py-3">
                          <Badge tone="green">● 正常</Badge>
                        </td>
                        <td className="px-4 py-3 text-text-muted">{fmtDate(c.updated_at)}</td>
                        <td className="px-4 py-3 text-right">
                          <Button
                            variant="danger"
                            className="px-2.5 py-1 text-xs"
                            disabled={revoke.isPending}
                            onClick={() => revoke.mutate(c.id)}
                          >
                            撤销
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </Card>

            <Banner tone="warn" className="mb-[22px]">
              <span>🛡️</span>
              <div>
                <b>污点（taint）规则：</b>来自连接器的外部内容（邮件正文、网页、文档）会被标记为{" "}
                <b>untrusted</b>；当某个外发动作的计划受污点内容影响时，会自动升级为「需审批」——防止
                confused-deputy 泄露。
              </div>
            </Banner>

            {addable.length > 0 && (
              <>
                <div className="mb-2.5 text-sm font-semibold text-text-soft">可添加</div>
                <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
                  {addable.map((c) => (
                    <Card key={c.id} className="flex flex-col items-center gap-2 p-4 text-center">
                      <div className="text-2xl">{c.icon}</div>
                      <b className="text-sm">{c.name}</b>
                      <Button
                        className="w-full"
                        disabled={c.kind !== "oauth"}
                        title={
                          c.kind === "oauth"
                            ? "在浏览器中授权（新标签页）"
                            : "暂未支持浏览器内连接"
                        }
                        onClick={() => window.open(`/v1/connectors/${c.id}/connect`, "_blank")}
                      >
                        连接
                      </Button>
                    </Card>
                  ))}
                </div>
                <p className="mt-3 text-xs text-text-muted">
                  点击「连接」在新标签页完成 Google 授权；授权后回到本页刷新即可看到已连接。
                </p>
              </>
            )}
          </>
        )}
      </div>
    </>
  );
}
