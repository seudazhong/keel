import { useState } from "react";
import { Button } from "../../components/ui/button";

export function Composer({
  disabled,
  onSend,
}: {
  disabled: boolean;
  onSend: (text: string) => void;
}) {
  const [text, setText] = useState("");
  const submit = () => {
    const trimmed = text.trim();
    if (!trimmed || disabled) return;
    onSend(trimmed);
    setText("");
  };
  return (
    <div className="flex gap-2 border-t border-border bg-surface p-3">
      <input
        className="flex-1 rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm outline-none focus:border-accent"
        placeholder="给个人助理发消息…（连接器、记忆、工具都可用）"
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            submit();
          }
        }}
      />
      <Button variant="primary" disabled={disabled} onClick={submit}>
        发送
      </Button>
    </div>
  );
}
