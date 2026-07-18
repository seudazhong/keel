import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { LocaleSwitcher } from "../../components/LocaleSwitcher";
import { Topbar } from "../../components/Topbar";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { useTranslation } from "../../lib/i18n";
import { ConnectorSetupList } from "../connectors/ConnectorSetup";
import { useConnectors } from "../connectors/useConnectors";
import { useOnboarding } from "./useOnboarding";

const STEPS = ["welcome", "locale", "workspace", "connectors", "finish"] as const;
type StepId = (typeof STEPS)[number];

export function OnboardingPage() {
  const { t } = useTranslation();
  const onboarding = useOnboarding();
  const connectors = useConnectors();
  const navigate = useNavigate();
  const [stepIndex, setStepIndex] = useState(0);
  const [workspaceName, setWorkspaceName] = useState(onboarding.config?.workspaceName ?? "");

  const step: StepId = STEPS[stepIndex];
  const isLast = stepIndex === STEPS.length - 1;

  function goBack() {
    setStepIndex((i) => Math.max(i - 1, 0));
  }

  function goNext() {
    if (isLast) {
      onboarding.complete({ workspaceName: workspaceName.trim() || "Personal" });
      navigate("/chat");
      return;
    }
    setStepIndex((i) => Math.min(i + 1, STEPS.length - 1));
  }

  function restart() {
    onboarding.reset();
    setStepIndex(0);
  }

  return (
    <>
      <Topbar title={t("onboarding.title")} />
      <div className="mx-auto w-full max-w-[640px] p-[22px]">
        {onboarding.completed && (
          <Banner tone="info" className="mb-4">
            <div>
              {t("onboarding.done")}{" "}
              <button className="ml-2 underline" onClick={restart}>
                {t("onboarding.restart")}
              </button>
            </div>
          </Banner>
        )}

        <p className="mb-4 text-sm text-text-muted">{t("onboarding.intro")}</p>

        <Card className="p-5">
          <div className="mb-3 text-xs font-semibold uppercase tracking-wide text-text-muted">
            {t("onboarding.step", { current: stepIndex + 1, total: STEPS.length })}
          </div>

          {step === "welcome" && (
            <div>
              <h2 className="text-lg font-semibold">{t("onboarding.step.welcome.title")}</h2>
              <p className="mt-2 text-sm text-text-soft">{t("onboarding.step.welcome.body")}</p>
            </div>
          )}

          {step === "locale" && (
            <div>
              <h2 className="text-lg font-semibold">{t("onboarding.step.locale.title")}</h2>
              <p className="mt-2 text-sm text-text-soft">{t("onboarding.step.locale.body")}</p>
              <div className="mt-3">
                <LocaleSwitcher />
              </div>
            </div>
          )}

          {step === "workspace" && (
            <div>
              <h2 className="text-lg font-semibold">{t("onboarding.step.workspace.title")}</h2>
              <p className="mt-2 text-sm text-text-soft">{t("onboarding.step.workspace.body")}</p>
              <label className="mt-3 block">
                <span className="mb-1 block text-xs font-semibold text-text-soft">
                  {t("onboarding.step.workspace.label")}
                </span>
                <input
                  className="w-full rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm outline-none focus:border-accent"
                  placeholder={t("onboarding.step.workspace.placeholder")}
                  value={workspaceName}
                  onChange={(e) => setWorkspaceName(e.target.value)}
                />
              </label>
            </div>
          )}

          {step === "connectors" && (
            <div>
              <h2 className="text-lg font-semibold">{t("onboarding.step.connectors.title")}</h2>
              <p className="mt-2 text-sm text-text-soft">{t("onboarding.step.connectors.body")}</p>
              <div className="mt-3">
                {connectors.isLoading && <p className="text-sm text-text-muted">Loading connectors…</p>}
                {connectors.isError && (
                  <Banner tone="warn">
                    Connector setup is unavailable. You can continue and configure it later.
                  </Banner>
                )}
                {connectors.data && (
                  <ConnectorSetupList
                    connectors={connectors.data}
                    compact
                  />
                )}
              </div>
            </div>
          )}

          {step === "finish" && (
            <div>
              <h2 className="text-lg font-semibold">{t("onboarding.step.finish.title")}</h2>
              <p className="mt-2 text-sm text-text-soft">{t("onboarding.step.finish.body")}</p>
            </div>
          )}

          <div className="mt-6 flex items-center justify-between">
            <Button onClick={goBack} disabled={stepIndex === 0}>
              {t("common.back")}
            </Button>
            <Button variant="primary" onClick={goNext}>
              {isLast ? t("common.finish") : t("common.next")}
            </Button>
          </div>
        </Card>
      </div>
    </>
  );
}
