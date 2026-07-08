import { cn } from "../../lib/cn";
import { InlineApproval } from "./InlineApproval";
import { MessageBubble } from "./MessageBubble";
import { ToolStep } from "./ToolStep";
import type { ChatItem } from "./types";

export function ChatThread({
  items,
  onResolve,
}: {
  items: ChatItem[];
  onResolve: (approvalId: string, decision: "allow" | "deny") => void;
}) {
  return (
    <div className="flex flex-col gap-2.5">
      {items.map((it) => {
        switch (it.kind) {
          case "user":
            return <MessageBubble key={it.id} role="user" text={it.text} />;
          case "assistant":
            return (
              <MessageBubble key={it.id} role="assistant" text={it.text} streaming={it.streaming} />
            );
          case "tool":
            return <ToolStep key={it.id} tool={it.tool} args={it.args} result={it.result} />;
          case "approval":
            return (
              <InlineApproval
                key={it.id}
                tool={it.tool}
                args={it.args}
                resolved={it.resolved}
                onResolve={(d) => onResolve(it.approvalId, d)}
              />
            );
          case "meta":
            return (
              <div
                key={it.id}
                className={cn(
                  "self-stretch font-mono text-xs",
                  it.tone === "error" ? "text-red" : "text-text-muted",
                )}
              >
                {it.text}
              </div>
            );
        }
      })}
    </div>
  );
}
