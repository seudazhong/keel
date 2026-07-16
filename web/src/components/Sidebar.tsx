import { useEffect, useRef } from "react";
import { Link, NavLink } from "react-router-dom";
import { cn } from "../lib/cn";
import { useTranslation, type MessageKey } from "../lib/i18n";
import { LocaleSwitcher } from "./LocaleSwitcher";
import { Badge } from "./ui/badge";

type NavStatus = "ready" | "preview" | "roadmap";

interface NavItem {
  icon: string;
  labelKey: MessageKey;
  to?: string;
  status: NavStatus;
}

interface NavGroup {
  labelKey: MessageKey;
  items: NavItem[];
}

const groups: NavGroup[] = [
  {
    labelKey: "shell.nav.workspace",
    items: [
      { icon: "💬", labelKey: "shell.nav.chat", to: "/chat", status: "ready" },
      { icon: "🗂️", labelKey: "shell.nav.sessions", to: "/sessions", status: "ready" },
      { icon: "🧠", labelKey: "shell.nav.memory", to: "/memory", status: "ready" },
      { icon: "📚", labelKey: "shell.nav.knowledge", to: "/knowledge", status: "ready" },
      { icon: "📁", labelKey: "shell.nav.projects", to: "/projects", status: "preview" },
    ],
  },
  {
    labelKey: "shell.nav.configuration",
    items: [
      { icon: "🤖", labelKey: "shell.nav.agents", to: "/agents", status: "preview" },
      { icon: "🔌", labelKey: "shell.nav.connectors", to: "/connectors", status: "ready" },
      { icon: "✅", labelKey: "shell.nav.approvals", to: "/approvals", status: "ready" },
      { icon: "🧩", labelKey: "shell.nav.extensions", status: "roadmap" },
    ],
  },
  {
    labelKey: "shell.nav.automation",
    items: [
      { icon: "⏰", labelKey: "shell.nav.schedules", to: "/schedules", status: "ready" },
      { icon: "🕸️", labelKey: "shell.nav.multiAgent", status: "roadmap" },
    ],
  },
  {
    labelKey: "shell.nav.operations",
    items: [
      { icon: "📊", labelKey: "shell.nav.observability", to: "/observability", status: "ready" },
      { icon: "🧰", labelKey: "shell.nav.jobs", to: "/jobs", status: "ready" },
      { icon: "⚙️", labelKey: "shell.nav.admin", status: "roadmap" },
    ],
  },
];

const FOCUSABLE = 'a[href], button:not([disabled]), select, input, [tabindex]:not([tabindex="-1"])';

export function Sidebar({
  isOpen = false,
  onClose,
  onboardingComplete = true,
}: {
  isOpen?: boolean;
  onClose?: () => void;
  onboardingComplete?: boolean;
}) {
  const { t } = useTranslation();
  const asideRef = useRef<HTMLElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);

  // Mobile drawer: move focus in, trap Tab within the panel, and let Escape close it.
  useEffect(() => {
    if (!isOpen) return;
    closeButtonRef.current?.focus();

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        onClose?.();
        return;
      }
      if (e.key !== "Tab" || !asideRef.current) return;
      const focusable = Array.from(asideRef.current.querySelectorAll<HTMLElement>(FOCUSABLE));
      if (focusable.length === 0) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    }

    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [isOpen, onClose]);

  return (
    <>
      {isOpen && (
        <div
          className="fixed inset-0 z-30 bg-black/40 lg:hidden"
          onClick={onClose}
          aria-hidden="true"
        />
      )}
      <aside
        ref={asideRef}
        role={isOpen ? "dialog" : undefined}
        aria-modal={isOpen ? true : undefined}
        aria-label={isOpen ? t("shell.menuDialogLabel") : undefined}
        className={cn(
          "fixed inset-y-0 left-0 z-40 flex h-screen w-72 max-w-[85vw] flex-col border-r border-border bg-surface transition-transform duration-200 ease-out",
          "lg:sticky lg:top-0 lg:z-auto lg:inset-auto lg:w-60 lg:max-w-none lg:translate-x-0",
          isOpen ? "translate-x-0" : "-translate-x-full",
        )}
      >
        <div className="flex items-center gap-2.5 border-b border-border px-4 py-4">
          <div className="grid h-7 w-7 place-items-center rounded-sm bg-accent text-sm font-extrabold text-white">
            K
          </div>
          <b className="text-[15px]">{t("shell.brand")}</b>
          <span className="ml-auto rounded-full border border-border px-2 py-0.5 text-[11px] text-text-muted">
            {t("shell.scopeStandard")}
          </span>
          <button
            ref={closeButtonRef}
            type="button"
            className="rounded-sm p-1 text-text-muted hover:bg-surface-2 lg:hidden"
            aria-label={t("shell.closeMenu")}
            onClick={onClose}
          >
            ✕
          </button>
        </div>

        <div className="mx-3 mt-3 flex items-center gap-2.5 rounded-sm border border-border bg-surface-2 px-3 py-2">
          <span className="h-2 w-2 rounded-full bg-accent" />
          <div className="leading-tight">
            <b className="block text-[13px]">{t("shell.workspaceName")}</b>
            <span className="text-[11px] text-text-muted">{t("shell.workspaceHint")}</span>
          </div>
        </div>

        {!onboardingComplete && (
          <Link
            to="/onboarding"
            className="mx-3 mt-2 flex items-center gap-2 rounded-sm border border-accent/30 bg-accent/10 px-3 py-2 text-xs font-semibold text-accent hover:bg-accent/20"
          >
            <span aria-hidden="true">✨</span>
            {t("onboarding.title")}
          </Link>
        )}

        <nav aria-label={t("shell.nav.label")} className="flex flex-1 flex-col gap-0.5 overflow-y-auto p-2">
          {groups.map((g) => (
            <div key={g.labelKey}>
              <div className="px-2.5 pb-1 pt-3 text-[11px] uppercase tracking-wide text-text-muted">
                {t(g.labelKey)}
              </div>
              {g.items.map((it) =>
                it.status !== "roadmap" && it.to ? (
                  <NavLink
                    key={it.labelKey}
                    to={it.to}
                    className={({ isActive }) =>
                      cn(
                        "flex items-center gap-2.5 rounded-sm px-2.5 py-2 text-sm font-medium text-text-soft hover:bg-surface-2",
                        isActive && "bg-accent/10 font-semibold text-accent",
                      )
                    }
                  >
                    <span className="w-4 text-center opacity-80">{it.icon}</span>
                    {t(it.labelKey)}
                    {it.status === "preview" && (
                      <Badge tone="violet" className="ml-auto text-[10px]">
                        {t("common.preview")}
                      </Badge>
                    )}
                  </NavLink>
                ) : (
                  <span
                    key={it.labelKey}
                    aria-disabled="true"
                    title={t("shell.nav.roadmapHint")}
                    className="flex cursor-default items-center gap-2.5 rounded-sm px-2.5 py-2 text-sm font-medium text-text-muted"
                  >
                    <span className="w-4 text-center opacity-50">{it.icon}</span>
                    {t(it.labelKey)}
                    <Badge tone="amber" className="ml-auto text-[10px]">
                      {t("common.roadmap")}
                    </Badge>
                  </span>
                ),
              )}
            </div>
          ))}
        </nav>

        <div className="flex items-center justify-between gap-2 border-t border-border px-3 py-2.5">
          <span className="text-[11px] text-text-muted">{t("shell.locale.label")}</span>
          <LocaleSwitcher />
        </div>
      </aside>
    </>
  );
}
