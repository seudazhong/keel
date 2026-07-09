import { useEffect, useRef } from "react";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { ChatContext } from "./ChatContext";
import { ChatThread } from "./ChatThread";
import { Composer } from "./Composer";
import { RunBar } from "./RunBar";
import { useChat } from "./useChat";

const MODEL = "github_copilot / claude-sonnet-4.5";

export function ChatPage() {
  const { items, running, reason, usage, send, resolve, interrupt } = useChat();
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [items]);

  return (
    <div className="flex h-screen">
      <div className="flex min-w-0 flex-1 flex-col">
        <Topbar title="Chat" sub="· 个人助理" right={<Badge tone="violet">{MODEL}</Badge>} />

        <div className="min-h-0 flex-1 overflow-y-auto">
          <div className="mx-auto flex w-full max-w-[820px] flex-col gap-2.5 p-[22px]">
            {items.length === 0 && (
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
            <Composer disabled={running} onSend={send} />
          </div>
        </div>
      </div>

      <ChatContext items={items} usage={usage} />
    </div>
  );
}
