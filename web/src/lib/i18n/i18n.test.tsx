import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, test } from "vitest";
import { en } from "./en";
import { zhCN } from "./zh-CN";
import { I18nProvider, useTranslation } from "./index";

beforeEach(() => {
  window.localStorage.clear();
});
afterEach(() => {
  window.localStorage.clear();
  document.documentElement.lang = "";
});

test("every English key has a Simplified Chinese translation", () => {
  const missing = Object.keys(en).filter((key) => !(key in zhCN));
  expect(missing).toEqual([]);
});

function Probe() {
  const { t, locale, setLocale } = useTranslation();
  return (
    <div>
      <span data-testid="locale">{locale}</span>
      <span data-testid="label">{t("shell.workspaceName")}</span>
      <span data-testid="interp">{t("onboarding.step", { current: 1, total: 3 })}</span>
      <button onClick={() => setLocale("zh-CN")}>switch</button>
    </div>
  );
}

test("defaults to English, switches locale, persists to localStorage, and updates <html lang>", async () => {
  render(
    <I18nProvider>
      <Probe />
    </I18nProvider>,
  );

  expect(screen.getByTestId("locale").textContent).toBe("en");
  expect(screen.getByTestId("label").textContent).toBe("Personal workspace");
  expect(screen.getByTestId("interp").textContent).toBe("Step 1 of 3");
  expect(document.documentElement.lang).toBe("en");
  expect(document.title).toBe("Keel");

  await act(async () => {
    screen.getByText("switch").click();
  });

  expect(screen.getByTestId("locale").textContent).toBe("zh-CN");
  expect(screen.getByTestId("label").textContent).toBe("个人工作区");
  expect(document.documentElement.lang).toBe("zh-CN");
  expect(window.localStorage.getItem("keel.locale")).toBe("zh-CN");
});

test("restores a previously persisted locale on next mount", () => {
  window.localStorage.setItem("keel.locale", "zh-CN");
  render(
    <I18nProvider>
      <Probe />
    </I18nProvider>,
  );
  expect(screen.getByTestId("locale").textContent).toBe("zh-CN");
  expect(document.documentElement.lang).toBe("zh-CN");
});
