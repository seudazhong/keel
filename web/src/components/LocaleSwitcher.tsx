import { LOCALES, LOCALE_LABELS, useTranslation } from "../lib/i18n";

export function LocaleSwitcher({ className }: { className?: string }) {
  const { locale, setLocale, t } = useTranslation();
  return (
    <label className={className}>
      <span className="sr-only">{t("shell.locale.label")}</span>
      <select
        aria-label={t("shell.locale.label")}
        className="rounded-sm border border-border bg-surface-2 px-2 py-1 text-xs text-text-soft outline-none focus:border-accent"
        value={locale}
        onChange={(e) => setLocale(e.target.value as (typeof LOCALES)[number])}
      >
        {LOCALES.map((l) => (
          <option key={l} value={l}>
            {LOCALE_LABELS[l]}
          </option>
        ))}
      </select>
    </label>
  );
}
