import type { ReactNode } from "react";
import { cn } from "../../lib/cn";

const tones = {
  warn: "bg-amber/10 border-amber/30 text-amber",
  danger: "bg-red/10 border-red/30 text-red",
  info: "bg-accent/10 border-accent/30 text-accent",
  success: "bg-green/10 border-green/30 text-green",
};

export function Banner({
  tone = "info",
  className,
  children,
}: {
  tone?: keyof typeof tones;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div
      className={cn("flex gap-2.5 rounded-sm border px-3.5 py-3 text-sm", tones[tone], className)}
    >
      {children}
    </div>
  );
}
