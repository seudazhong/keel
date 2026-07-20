import { useEffect, useRef } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { ChatContext } from "./ChatContext";
import { ChatThread } from "./ChatThread";
import { Composer } from "./Composer";
import { RunBar } from "./RunBar";
import { useChat } from "./useChat";
import { createChatSessionId, rememberActiveChatSession } from "./chatSession";
import { useModel } from "../settings/useModel";

export function ChatPage() {
  const { sessionId = "" } = useParams();
  const navigate = useNavigate();
  const { items, running, loading, reason, usage, send, resolve, interrupt } = useChat(sessionId);
  const model = useModel();
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [items]);

  useEffect(() => {
    if (sessionId) rememberActiveChatSession(sessionId);
  }, [sessionId]);

  return (
    <div className="flex h-screen">
      <div className="flex min-w-0 flex-1 flex-col">
        <Topbar
          title="Chat"
          sub="· 个人助理"
          right={
            <div className="flex items-center gap-2">
              <button
                type="button"
                className="rounded-sm border border-border px-2.5 py-1.5 text-xs font-semibold hover:border-accent"
                onClick={() => navigate(`/chat/${encodeURIComponent(createChatSessionId())}`)}
              >
                New chat
              </button>
              <Link to="/settings" title="切换模型">
                <Badge tone="violet">{model.data?.current ?? "Model unavailable"}</Badge>
              </Link>
            </div>
          }
        />

        <div className="min-h-0 flex-1 overflow-y-auto">
          <div className="mx-auto flex w-full max-w-[820px] flex-col gap-2.5 p-[22px]">
            {!loading && items.length === 0 && (
              <p className="mt-10 text-center text-text-muted">
                给你的个人助理发条消息开始对话。工具调用与外发审批都会实时显示在这里。
              </p>
            )}
            <ChatThread items={items} onResolve={resolve} />
            <div ref={endRef} />
          </div>
        </div>

        <div className="shrink-0">
          <div className="mx-auto w-full max-w-[820px]">
            <RunBar running={running} reason={reason} onInterrupt={interrupt} />
            <Composer disabled={running || loading} onSend={send} />
          </div>
        </div>
      </div>

      <ChatContext items={items} usage={usage} model={model.data?.current} />
    </div>
  );
}
