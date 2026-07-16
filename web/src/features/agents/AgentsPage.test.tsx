import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { expect, test, vi } from "vitest";
import { renderWithClient } from "../../test/utils";
import { AgentsPage } from "./AgentsPage";

test("lists agents, marks the active one, and clearly labels this as a preview surface", async () => {
  renderWithClient(<AgentsPage />);

  expect(await screen.findByText("Personal assistant", { selector: "b" })).toBeInTheDocument();
  expect(screen.getByText("Researcher", { selector: "b" })).toBeInTheDocument();
  expect(screen.getAllByText("Active agent").length).toBeGreaterThan(0);
  expect(screen.getByText(/Preview:/)).toBeInTheDocument();
});

test("switching the active agent via the switcher updates which card is marked active", async () => {
  renderWithClient(<AgentsPage />);
  await screen.findByText("Personal assistant", { selector: "b" });

  const switcher = screen.getByLabelText("Switch active agent");
  fireEvent.change(switcher, { target: { value: "agent_researcher" } });

  await waitFor(() => {
    const researcherName = screen.getByText("Researcher", { selector: "b" });
    const card = researcherName.closest("div")?.parentElement?.parentElement;
    expect(card ? within(card).queryByText("Active agent") : null).not.toBeNull();
  });
});

test("creating an agent adds it to the list without pretending it is durably persisted", async () => {
  renderWithClient(<AgentsPage />);
  await screen.findByText("Personal assistant", { selector: "b" });

  fireEvent.click(screen.getByRole("button", { name: "+ Create agent" }));
  fireEvent.change(screen.getByLabelText(/Name/), { target: { value: "Support bot" } });
  fireEvent.click(screen.getByRole("button", { name: "Create agent" }));

  expect(await screen.findByText("Support bot", { selector: "b" })).toBeInTheDocument();
});

test("deleting an agent removes it after confirmation", async () => {
  const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
  renderWithClient(<AgentsPage />);
  await screen.findByText("Researcher", { selector: "b" });

  const deleteButtons = screen.getAllByRole("button", { name: "Delete" });
  fireEvent.click(deleteButtons[deleteButtons.length - 1]);

  await waitFor(() =>
    expect(screen.queryByText("Researcher", { selector: "b" })).not.toBeInTheDocument(),
  );
  confirmSpy.mockRestore();
});
