import { Link, useParams } from "react-router-dom";
import { Topbar } from "../../components/Topbar";
import { Banner } from "../../components/ui/banner";
import { Skeleton } from "../../components/ui/skeleton";
import { foldHistory } from "../chat/chatReducer";
import { ChatThread } from "../chat/ChatThread";
import { useSessionHistory } from "./useSessions";

export function SessionDetailPage() {
  const { id = "" } = useParams();
  const { data, isLoading, isError, refetch } = useSessionHistory(id);
  const state = foldHistory(data ?? []);

  return (
    <div className="flex h-screen flex-col">
      <Topbar
        title="会话回放"
        sub={`· ${id}`}
        right={
          <Link to="/sessions" className="text-sm text-accent hover:underline">
            ← 返回列表
          </Link>
        }
      />
      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="mx-auto flex w-full max-w-[820px] flex-col gap-2.5 p-[22px]">
          {isLoading && <Skeleton className="h-40" />}

          {isError && (
            <Banner tone="danger">
              <span>⚠️</span>
              <div>
                加载会话历史失败。
                <button className="ml-2 underline" onClick={() => void refetch()}>
                  重试
                </button>
              </div>
            </Banner>
          )}

          {data && state.items.length === 0 && (
            <p className="text-text-muted">这个会话还没有可展示的消息。</p>
          )}

          {data && state.items.length > 0 && (
            <ChatThread items={state.items} onResolve={() => undefined} />
          )}
        </div>
      </div>
    </div>
  );
}
