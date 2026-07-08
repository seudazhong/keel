import type { ReactNode } from "react";
import { cn } from "../../lib/cn";

export function Card({
  className,
  children,
}: {
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={cn("rounded border border-border bg-surface shadow-card", className)}>
      {children}
    </div>
  );
}
