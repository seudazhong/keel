import type { ReactNode } from "react";
import { useShell } from "./ShellContext";
import { useTranslation } from "../lib/i18n";

export function Topbar({
  title,
  sub,
  right,
}: {
  title: string;
  sub?: string;
  right?: ReactNode;
}) {
  const { openSidebar } = useShell();
  const { t } = useTranslation();
  return (
    <div className="sticky top-0 z-10 flex h-14 items-center gap-3.5 border-b border-border bg-surface px-4 sm:px-[22px]">
      <button
        type="button"
        className="-ml-1 rounded-sm p-1.5 text-text-soft hover:bg-surface-2 lg:hidden"
        aria-label={t("shell.openMenu")}
        onClick={openSidebar}
      >
        ☰
      </button>
      <h1 className="text-base font-semibold">{title}</h1>
      {sub && <span className="hidden text-[13px] text-text-muted sm:inline">{sub}</span>}
      <div className="flex-1" />
      {right}
    </div>
  );
}
