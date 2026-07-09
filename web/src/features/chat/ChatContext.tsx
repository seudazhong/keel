import type { ChatItem, ChatUsage } from "./types";

const MODEL = "github_copilot / claude-sonnet-4.5";

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between py-1 text-sm">
      <span className="text-text-muted">{label}</span>
      <b className="font-semibold">{value}</b>
    </div>
  );
}

export function ChatContext({ items, usage }: { items: ChatItem[]; usage: ChatUsage }) {
  const tools = items.filter((i) => i.kind === "tool");
  const totalTokens = usage.promptTokens + usage.completionTokens;
  const cachePct =
    usage.promptTokens > 0 ? Math.round((usage.cacheReadTokens / usage.promptTokens) * 100) : 0;

  return (
    <aside className="hidden w-64 shrink-0 overflow-y-auto border-l border-border bg-surface p-4 lg:block">
      <div className="mb-1 text-xs font-semibold uppercase tracking-wide text-text-muted">
        本次运行
      </div>
      <Row label="模型" value="claude-sonnet-4.5" />
      <Row label="Tokens" value={totalTokens.toLocaleString()} />
      <Row label="成本" value={`$${usage.costUsd.toFixed(4)}`} />
      <Row
        label="缓存命中"
        value={`${usage.cacheReadTokens.toLocaleString()} (${cachePct}%)`}
      />

      <div className="mb-1 mt-5 text-xs font-semibold uppercase tracking-wide text-text-muted">
        工具调用
      </div>
      <Row label="次数" value={String(tools.length)} />

      <div className="mb-1 mt-5 text-xs font-semibold uppercase tracking-wide text-text-muted">
        连接器 / 记忆
      </div>
      <p className="text-xs text-text-muted">
        本次运行使用的连接器与触及的记忆将在此展示（记忆接线 + consolidation 计划于 M3）。
      </p>
      <p className="mt-4 text-[11px] text-text-muted">模型 {MODEL}</p>
    </aside>
  );
}
