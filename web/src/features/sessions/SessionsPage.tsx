import { useState } from "react";
import { Link } from "react-router-dom";
import { Topbar } from "../../components/Topbar";
import { Banner } from "../../components/ui/banner";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { useSessions } from "./useSessions";

function fmtDate(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

export function SessionsPage() {
  const { data, isLoading, isError, refetch } = useSessions();
  const [q, setQ] = useState("");
  const needle = q.trim().toLowerCase();
  const filtered = (data ?? []).filter((s) =>
    `${s.title ?? ""} ${s.id}`.toLowerCase().includes(needle),
  );

  return (
    <>
      <Topbar title="Sessions" sub="· 跨 Web / IM 的历史会话" />
      <div className="w-full max-w-[900px] p-[22px]">
        <input
          className="mb-4 w-full rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm outline-none focus:border-accent"
          placeholder="🔍 过滤会话（标题 / id）…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
        <Banner tone="info" className="mb-4">
          <span>🔎</span>
          <div>
            当前为客户端过滤；服务端混合检索（pg_trgm + tsvector ⊕ pgvector KNN，RRF）即将接入。
          </div>
        </Banner>

        {isLoading && <Skeleton className="h-40" />}

        {isError && (
          <Banner tone="danger">
            <span>⚠️</span>
            <div>
              加载会话失败。
              <button className="ml-2 underline" onClick={() => void refetch()}>
                重试
              </button>
            </div>
          </Banner>
        )}

        {data && filtered.length === 0 && <p className="text-text-muted">没有匹配的会话。</p>}

        {data && filtered.length > 0 && (
          <Card className="overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs text-text-muted">
                  <th className="px-4 py-2.5 font-medium">会话</th>
                  <th className="px-4 py-2.5 font-medium">消息</th>
                  <th className="px-4 py-2.5 font-medium">最近</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((s) => (
                  <tr key={s.id} className="border-b border-border last:border-0 hover:bg-surface-2">
                    <td className="px-4 py-3">
                      <Link
                        to={`/sessions/${encodeURIComponent(s.id)}`}
                        className="font-semibold text-accent hover:underline"
                      >
                        {s.title || s.id}
                      </Link>
                      <div className="truncate text-xs text-text-muted">{s.id}</div>
                    </td>
                    <td className="px-4 py-3 text-text-soft">{s.messages}</td>
                    <td className="px-4 py-3 text-text-muted">{fmtDate(s.updated_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        )}
      </div>
    </>
  );
}
