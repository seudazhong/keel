import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { ConnectorsPage } from "./ConnectorsPage";

test("renders the connected Gmail row with scope chips + the taint banner", async () => {
  renderWithClient(<ConnectorsPage />);
  expect(await screen.findByText("gmail.readonly")).toBeInTheDocument();
  expect(screen.getByText("gmail.send")).toBeInTheDocument();
  expect(screen.getByText(/Gmail/)).toBeInTheDocument();
  expect(screen.getByText(/污点（taint）规则/)).toBeInTheDocument();
  expect(screen.getByText("正常", { exact: false })).toBeInTheDocument();
});

test("shows a not-connected catalog entry with a disabled connect button", async () => {
  renderWithClient(<ConnectorsPage />);
  await screen.findByText("gmail.readonly");
  expect(screen.getByText("Google Calendar")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "连接" })).toBeDisabled();
});

test("revoking a connector removes it from the connected list", async () => {
  renderWithClient(<ConnectorsPage />);
  await screen.findByText("gmail.readonly");
  fireEvent.click(screen.getByRole("button", { name: "撤销" }));
  await waitFor(() => expect(screen.queryByText("gmail.readonly")).not.toBeInTheDocument());
});
