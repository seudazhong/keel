export function RunBar({ running, reason }: { running: boolean; reason?: string }) {
  if (!running && !reason) return null;
  return (
    <div className="flex items-center gap-2 border-t border-border bg-surface-2 px-3 py-1.5 text-xs text-text-soft">
      {running ? (
        <>
          <span className="h-2 w-2 animate-pulse rounded-full bg-accent" />
          运行中…
        </>
      ) : (
        <span className="text-text-muted">[{reason}]</span>
      )}
    </div>
  );
}
