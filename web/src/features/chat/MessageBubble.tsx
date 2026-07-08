import { cn } from "../../lib/cn";

export function MessageBubble({
  role,
  text,
  streaming,
}: {
  role: "user" | "assistant";
  text: string;
  streaming?: boolean;
}) {
  const isUser = role === "user";
  return (
    <div
      className={cn(
        "max-w-[80%] whitespace-pre-wrap break-words rounded-md px-3 py-2.5 text-sm",
        isUser
          ? "self-end bg-surface-2"
          : "self-start border border-accent/15 bg-accent/5",
      )}
    >
      {text}
      {streaming && <span className="ml-0.5 inline-block animate-pulse">▋</span>}
    </div>
  );
}
