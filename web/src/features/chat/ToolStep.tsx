function preview(args: unknown): string {
  if (args === undefined || args === null) return "";
  try {
    const s = typeof args === "string" ? args : JSON.stringify(args);
    return s.length > 80 ? `${s.slice(0, 80)}…` : s;
  } catch {
    return "";
  }
}

export function ToolStep({
  tool,
  args,
  result,
}: {
  tool: string;
  args: unknown;
  result?: { ok: boolean; output: string };
}) {
  return (
    <div className="self-stretch rounded-sm border border-border bg-surface-2 px-3 py-2 font-mono text-xs text-text-soft">
      <div className="flex items-center gap-1.5">
        <span className="text-accent">→</span>
        <span className="font-semibold">{tool}</span>
        <span className="truncate text-text-muted">{preview(args)}</span>
      </div>
      {result && (
        <div className="mt-1 flex gap-1.5">
          <span className={result.ok ? "text-green" : "text-red"}>←</span>
          <span className="truncate">{result.output || (result.ok ? "ok" : "error")}</span>
        </div>
      )}
    </div>
  );
}
