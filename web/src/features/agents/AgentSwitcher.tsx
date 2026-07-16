import { Badge } from "../../components/ui/badge";
import { useTranslation } from "../../lib/i18n";
import { useAgents, useSetActiveAgent } from "./useAgents";

/**
 * Compact control for switching which agent persona is active. Intended to be
 * reusable anywhere an agent needs to be picked (currently: AgentsPage).
 */
export function AgentSwitcher() {
  const { t } = useTranslation();
  const agents = useAgents();
  const setActive = useSetActiveAgent();

  if (!agents.data || agents.data.length === 0) return null;

  const active = agents.data.find((a) => a.active) ?? agents.data[0];

  return (
    <div className="flex items-center gap-2">
      <Badge tone="violet">{t("agents.active")}</Badge>
      <select
        aria-label={t("agents.switcher.label")}
        className="rounded-sm border border-border bg-surface-2 px-2 py-1.5 text-sm outline-none focus:border-accent"
        value={active.id}
        disabled={setActive.isPending}
        onChange={(e) => setActive.mutate(e.target.value)}
      >
        {agents.data.map((agent) => (
          <option key={agent.id} value={agent.id}>
            {agent.name}
          </option>
        ))}
      </select>
    </div>
  );
}
