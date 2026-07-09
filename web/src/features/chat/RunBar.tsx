export function RunBar({
  running,
  reason,
  onInterrupt,
}: {
  running: boolean;
  reason?: string;
  onInterrupt?: () => void;
}) {
  if (!running && !reason) return null;
  return (
    <div className="flex items-center gap-2 border-t border-border bg-surface-2 px-3 py-1.5 text-xs text-text-soft">
      {running ? (
        <>
          <span className="h-2 w-2 animate-pulse rounded-full bg-accent" />
          运行中…
          {onInterrupt && (
            <button
              className="ml-auto rounded-sm border border-red/30 px-2 py-0.5 font-semibold text-red hover:bg-red/10"
              onClick={onInterrupt}
            >
              ⏸ 打断
            </button>
          )}
        </>
      ) : (
        <span className="text-text-muted">[{reason}]</span>
      )}
    </div>
  );
}
