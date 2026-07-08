import type { ReactNode } from "react";

export function Topbar({
  title,
  sub,
  right,
}: {
  title: string;
  sub?: string;
  right?: ReactNode;
}) {
  return (
    <div className="sticky top-0 z-10 flex h-14 items-center gap-3.5 border-b border-border bg-surface px-[22px]">
      <h1 className="text-base font-semibold">{title}</h1>
      {sub && <span className="text-[13px] text-text-muted">{sub}</span>}
      <div className="flex-1" />
      {right}
    </div>
  );
}
