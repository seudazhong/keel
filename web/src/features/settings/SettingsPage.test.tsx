import { fireEvent, screen } from "@testing-library/react";
import { expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { SettingsPage } from "./SettingsPage";

const tick = (ms: number) => new Promise((r) => setTimeout(r, ms));

test("shows the current model and switches it on save", async () => {
  renderWithClient(<SettingsPage />);
  await tick(1200);
  const select = screen.getByLabelText("默认模型") as HTMLSelectElement;
  expect(select.value).toBe("github_copilot/claude-sonnet-4.5");

  fireEvent.change(select, { target: { value: "github_copilot/gpt-5.3-codex" } });
  fireEvent.click(screen.getByRole("button", { name: "保存" }));
  await tick(600);
  expect((screen.getByLabelText("默认模型") as HTMLSelectElement).value).toBe(
    "github_copilot/gpt-5.3-codex",
  );
});
