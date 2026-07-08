import { NavLink } from "react-router-dom";
import { cn } from "../lib/cn";

const groups = [
  {
    label: "工作区",
    items: [
      { icon: "💬", label: "Chat", to: "/chat" },
      { icon: "🗂️", label: "Sessions", to: "/sessions" },
      { icon: "🧠", label: "Memory" },
    ],
  },
  {
    label: "配置",
    items: [
      { icon: "🤖", label: "Agents" },
      { icon: "🔌", label: "Connectors", to: "/connectors" },
      { icon: "✅", label: "Approvals", to: "/approvals" },
      { icon: "🧩", label: "Extensions" },
    ],
  },
  {
    label: "自动化",
    items: [
      { icon: "⏰", label: "Schedules" },
      { icon: "🕸️", label: "Multi-agent" },
    ],
  },
  {
    label: "运维",
    items: [
      { icon: "📊", label: "Observability" },
      { icon: "⚙️", label: "Admin" },
    ],
  },
];

export function Sidebar() {
  return (
    <aside className="sticky top-0 flex h-screen flex-col border-r border-border bg-surface">
      <div className="flex items-center gap-2.5 border-b border-border px-4 py-4">
        <div className="grid h-7 w-7 place-items-center rounded-sm bg-accent text-sm font-extrabold text-white">
          K
        </div>
        <b className="text-[15px]">Keel</b>
        <span className="ml-auto rounded-full border border-border px-2 py-0.5 text-[11px] text-text-muted">
          standard
        </span>
      </div>

      <div className="mx-3 mt-3 flex items-center gap-2.5 rounded-sm border border-border bg-surface-2 px-3 py-2">
        <span className="h-2 w-2 rounded-full bg-accent" />
        <div className="leading-tight">
          <b className="block text-[13px]">个人助理</b>
          <span className="text-[11px] text-text-muted">personal · web:local</span>
        </div>
      </div>

      <nav className="flex flex-col gap-0.5 overflow-y-auto p-2">
        {groups.map((g) => (
          <div key={g.label}>
            <div className="px-2.5 pb-1 pt-3 text-[11px] uppercase tracking-wide text-text-muted">
              {g.label}
            </div>
            {g.items.map((it) =>
              "to" in it && it.to ? (
                <NavLink
                  key={it.label}
                  to={it.to}
                  className={({ isActive }) =>
                    cn(
                      "flex items-center gap-2.5 rounded-sm px-2.5 py-2 text-sm font-medium text-text-soft hover:bg-surface-2",
                      isActive && "bg-accent/10 font-semibold text-accent",
                    )
                  }
                >
                  <span className="w-4 text-center opacity-80">{it.icon}</span>
                  {it.label}
                </NavLink>
              ) : (
                <span
                  key={it.label}
                  aria-disabled="true"
                  title="即将上线"
                  className="flex cursor-default items-center gap-2.5 rounded-sm px-2.5 py-2 text-sm font-medium text-text-muted opacity-60"
                >
                  <span className="w-4 text-center opacity-50">{it.icon}</span>
                  {it.label}
                </span>
              ),
            )}
          </div>
        ))}
      </nav>
    </aside>
  );
}
