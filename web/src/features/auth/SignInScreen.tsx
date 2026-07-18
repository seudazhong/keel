/**
 * Truthful sign-in / workspace-context screen (M3.6 finding 1 + follow-up).
 *
 * Shown whenever local preview isn't currently granting access: either the server has rejected
 * a request for auth, or the caller explicitly signed out. It accepts an API key or a
 * directly-supplied OIDC bearer token (from an external login page / reverse proxy — it does
 * NOT fake an OIDC authorization-code flow) plus the selected organization and Agent, and warns
 * that secrets are held only for this browser tab. The "local preview" choice is hidden once
 * the server has ever reported that auth is required (`auth.cloudAuthRequired`) so it can never
 * be (re)selected in a deployment that truly needs a credential.
 */

import { useState, type FormEvent } from "react";
import { Button } from "../../components/ui/button";
import { useTranslation } from "../../lib/i18n";
import { useAuth } from "./AuthContext";
import type { Credential } from "./authState";

export function SignInScreen() {
  const { t } = useTranslation();
  const auth = useAuth();
  const [kind, setKind] = useState<Credential["kind"]>("api-key");
  const [secret, setSecret] = useState("");
  const [org, setOrg] = useState(auth.org ?? "");
  const [agent, setAgent] = useState(auth.agent ?? "");

  function submit(event: FormEvent) {
    event.preventDefault();
    const trimmed = secret.trim();
    auth.signIn({
      credential: trimmed ? ({ kind, secret: trimmed } as Credential) : null,
      org: org.trim() || null,
      agent: agent.trim() || null,
    });
  }

  const inputClass =
    "w-full rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm outline-none focus:border-accent";

  return (
    <div className="flex min-h-screen items-center justify-center bg-surface p-6">
      <form
        onSubmit={submit}
        aria-labelledby="auth-title"
        className="w-full max-w-md space-y-4 rounded-lg border border-border bg-surface-1 p-6"
      >
        <div>
          <h1 id="auth-title" className="text-lg font-semibold">
            {t("auth.title")}
          </h1>
          <p className="mt-1 text-sm text-muted">{t("auth.subtitle")}</p>
        </div>

        {auth.needsAuth && (
          <p role="alert" className="rounded-sm bg-danger/10 px-3 py-2 text-sm text-danger">
            {t("auth.rejected")}
          </p>
        )}

        <label className="block space-y-1">
          <span className="text-sm font-medium">{t("auth.credentialType")}</span>
          <select
            aria-label={t("auth.credentialType")}
            className={inputClass}
            value={kind}
            onChange={(e) => setKind(e.target.value as Credential["kind"])}
          >
            <option value="api-key">{t("auth.apiKey")}</option>
            <option value="bearer">{t("auth.bearer")}</option>
          </select>
        </label>

        <label className="block space-y-1">
          <span className="text-sm font-medium">{t("auth.secret")}</span>
          <input
            type="password"
            autoComplete="off"
            className={inputClass}
            placeholder={t("auth.secretPlaceholder")}
            value={secret}
            onChange={(e) => setSecret(e.target.value)}
          />
        </label>

        <div className="grid grid-cols-2 gap-3">
          <label className="block space-y-1">
            <span className="text-sm font-medium">{t("auth.org")}</span>
            <input
              className={inputClass}
              placeholder={t("auth.orgPlaceholder")}
              value={org}
              onChange={(e) => setOrg(e.target.value)}
            />
          </label>
          <label className="block space-y-1">
            <span className="text-sm font-medium">{t("auth.agent")}</span>
            <input
              className={inputClass}
              placeholder={t("auth.agentPlaceholder")}
              value={agent}
              onChange={(e) => setAgent(e.target.value)}
            />
          </label>
        </div>

        <p className="text-xs text-muted">{t("auth.secretWarning")}</p>
        <p className="text-xs text-muted">{t("auth.tokenNote")}</p>

        <div className="flex items-center gap-2">
          <Button type="submit">{t("auth.submit")}</Button>
          {!auth.cloudAuthRequired && (
            <button
              type="button"
              className="text-sm text-muted underline"
              onClick={() => auth.enterLocalPreview({ org: org.trim() || null, agent: agent.trim() || null })}
            >
              {t("auth.localPreview")}
            </button>
          )}
        </div>
        {auth.cloudAuthRequired && (
          <p className="text-xs text-muted">{t("auth.localPreviewBlocked")}</p>
        )}
      </form>
    </div>
  );
}
