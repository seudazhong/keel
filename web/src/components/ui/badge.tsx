import type { ReactNode } from "react";
import { cn } from "../../lib/cn";

const tones = {
  green: "text-green bg-green/10",
  amber: "text-amber bg-amber/10",
  red: "text-red bg-red/10",
  sky: "text-sky bg-sky/10",
  violet: "text-accent bg-accent/10",
};

export function Badge({
  tone,
  className,
  children,
}: {
  tone?: keyof typeof tones;
  className?: string;
  children: ReactNode;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border border-border px-2 py-0.5",
        "text-xs font-semibold text-text-soft",
        tone && tones[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}
