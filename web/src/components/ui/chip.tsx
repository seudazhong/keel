import type { ReactNode } from "react";
import { cn } from "../../lib/cn";

export function Chip({
  className,
  children,
}: {
  className?: string;
  children: ReactNode;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-sm border border-border bg-surface-2",
        "px-2.5 py-1 text-xs text-text-soft",
        className,
      )}
    >
      {children}
    </span>
  );
}
