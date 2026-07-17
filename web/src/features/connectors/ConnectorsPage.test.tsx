import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, test, vi } from "vitest";
import { renderWithClient } from "../../test/utils";
import { ConnectorsPage } from "./ConnectorsPage";

test("renders a connected manifest with scope chips and taint guidance", async () => {
  renderWithClient(<ConnectorsPage />);
  expect(await screen.findByText("gmail.readonly")).toBeInTheDocument();
  expect(screen.getByText("gmail.send")).toBeInTheDocument();
  expect(screen.getByText(/External connector content is tainted/)).toBeInTheDocument();
  expect(screen.getByText("healthy")).toBeInTheDocument();
});

test("a not-connected oauth connector offers an in-browser connect", async () => {
  const open = vi.spyOn(window, "open").mockImplementation(() => null);
  renderWithClient(<ConnectorsPage />);
  await screen.findByText("gmail.readonly");
  expect(screen.getByText("OAuth fixture")).toBeInTheDocument();
  const btn = screen.getByRole("button", { name: "Connect" });
  expect(btn).not.toBeDisabled();
  fireEvent.click(btn);
  expect(open).toHaveBeenCalledWith("/v1/connectors/oauth-fixture/connect", "_blank");
  open.mockRestore();
});

test("revoking a connector removes it from the connected list", async () => {
  renderWithClient(<ConnectorsPage />);
  await screen.findByText("gmail.readonly");
  fireEvent.click(screen.getByRole("button", { name: "Disconnect" }));
  await waitFor(() => expect(screen.queryByText("gmail.readonly")).not.toBeInTheDocument());
});

test("secret setup never echoes the entered value", async () => {
  renderWithClient(<ConnectorsPage />);
  const input = await screen.findByLabelText("Secret");
  fireEvent.change(input, { target: { value: "top-secret" } });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await waitFor(() => expect(input).toHaveValue(""));
  expect(screen.queryByText("top-secret")).not.toBeInTheDocument();
});
