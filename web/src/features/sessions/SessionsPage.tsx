import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Topbar } from "../../components/Topbar";
import { Banner } from "../../components/ui/banner";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { useSessions, useSessionSearch } from "./useSessions";

function fmtDate(iso: string | null): string {
  const d = new Date(iso ?? "");
  return Number.isNaN(d.getTime()) ? "—" : d.toLocaleString();
}

interface Row {
  id: string;
  title: string | null;
  messages: number;
  updated_at: string | null;
  snippet?: string;
}

export function SessionsPage() {
  const [q, setQ] = useState("");
  const [dq, setDq] = useState(""); // debounced query
  useEffect(() => {
    const t = setTimeout(() => setDq(q.trim()), 250);
    return () => clearTimeout(t);
  }, [q]);

  const list = useSessions();
  const search = useSessionSearch(dq);
  const searching = dq.length > 0;

  const rows: Row[] = searching ? (search.data ?? []) : (list.data ?? []);
  const isLoading = searching ? search.isLoading : list.isLoading;
  const isError = searching ? search.isError : list.isError;

  return (
    <>
      <Topbar title="Sessions" sub="· 跨 Web / IM 的历史会话" />
      <div className="w-full max-w-[900px] p-[22px]">
        <input
          aria-label="搜索会话"
          className="mb-4 w-full rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm outline-none focus:border-accent"
          placeholder="🔍 语义 + 词法混合检索：会话消息内容…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
        <Banner tone="info" className="mb-4">
          <span>🔎</span>
          <div>
            服务端语义与词法混合检索已启用；语义服务暂不可用时会安全降级到词法结果。
          </div>
        </Banner>

        {isLoading && <Skeleton className="h-40" />}

        {isError && (
          <Banner tone="danger">
            <span>⚠️</span>
            <div>
              加载会话失败。
              <button
                className="ml-2 underline"
                onClick={() => void (searching ? search.refetch() : list.refetch())}
              >
                重试
              </button>
            </div>
          </Banner>
        )}

        {!isLoading && !isError && rows.length === 0 && (
          <p className="text-text-muted">{searching ? "没有匹配的会话。" : "还没有会话。"}</p>
        )}

        {rows.length > 0 && (
          <Card className="overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs text-text-muted">
                  <th className="px-4 py-2.5 font-medium">会话</th>
                  <th className="px-4 py-2.5 font-medium">消息</th>
                  <th className="px-4 py-2.5 font-medium">最近</th>
                  <th className="px-4 py-2.5 font-medium" />
                </tr>
              </thead>
              <tbody>
                {rows.map((s) => (
                  <tr key={s.id} className="border-b border-border last:border-0 hover:bg-surface-2">
                    <td className="px-4 py-3">
                      <Link
                        to={`/sessions/${encodeURIComponent(s.id)}`}
                        className="font-semibold text-accent hover:underline"
                      >
                        {s.title || s.id}
                      </Link>
                      {s.snippet ? (
                        <div className="truncate text-xs text-text-muted">…{s.snippet}…</div>
                      ) : (
                        <div className="truncate text-xs text-text-muted">{s.id}</div>
                      )}
                    </td>
                    <td className="px-4 py-3 text-text-soft">{s.messages}</td>
                    <td className="px-4 py-3 text-text-muted">{fmtDate(s.updated_at)}</td>
                    <td className="px-4 py-3 text-right">
                      <Link
                        to={`/chat/${encodeURIComponent(s.id)}`}
                        className="rounded-sm bg-accent px-2.5 py-1.5 text-xs font-semibold text-white hover:opacity-90"
                      >
                        Continue
                      </Link>
                    </td>
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
