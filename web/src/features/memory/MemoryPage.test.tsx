import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { MemoryPage } from "./MemoryPage";

test("shows proposal evidence and safely approves only pending proposals", async () => {
  renderWithClient(<MemoryPage />);

  expect(await screen.findByText("human")).toBeInTheDocument();
  expect(screen.getByText(/Name: Dazhong/)).toBeInTheDocument();
  expect(screen.getByText(/Prefers Chinese responses/)).toBeInTheDocument();
  expect(await screen.findByText("user_preferences")).toBeInTheDocument();
  expect(screen.getByText("Source events: 101, 118, 125")).toBeInTheDocument();
  expect(screen.getByText(/Several remembered facts can live/)).toBeInTheDocument();

  const approve = screen.getAllByRole("button", { name: "Approve & apply" });
  expect(approve[0]).toBeEnabled();
  expect(approve[1]).toBeDisabled();
  fireEvent.click(approve[0]);

  await waitFor(() => expect(screen.getAllByText("applied")).toHaveLength(2));
  expect(screen.getAllByRole("button", { name: "Approve & apply" })[0]).toBeDisabled();
});

test("runs consolidation and reports that it was queued", async () => {
  renderWithClient(<MemoryPage />);
  await screen.findByText("user_preferences");
  fireEvent.click(screen.getByRole("button", { name: "Run consolidation" }));
  expect(await screen.findByText(/Consolidation queued at/)).toBeInTheDocument();
});
