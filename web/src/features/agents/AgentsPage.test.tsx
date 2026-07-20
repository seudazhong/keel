import { fireEvent, screen, waitFor } from "@testing-library/react";
import { expect, test, vi } from "vitest";
import { renderWithClient } from "../../test/utils";
import { AgentsPage } from "./AgentsPage";

test("lists durable agent profiles and explains what this page controls", async () => {
  renderWithClient(<AgentsPage />);

  expect(await screen.findByText("Personal assistant", { selector: "b" })).toBeInTheDocument();
  expect(screen.getByText("Researcher", { selector: "b" })).toBeInTheDocument();
  expect(screen.getByText(/Model and tool access are configured elsewhere/)).toBeInTheDocument();
  expect(screen.getByText(/does not change the built-in Chat assistant/)).toBeInTheDocument();
  expect(screen.queryByLabelText("Model")).not.toBeInTheDocument();
});

test("creating an agent profile sends the real identity contract", async () => {
  renderWithClient(<AgentsPage />);
  await screen.findByText("Personal assistant", { selector: "b" });

  fireEvent.click(screen.getByRole("button", { name: "+ Create agent" }));
  fireEvent.change(screen.getByLabelText(/Name/), { target: { value: "Support bot" } });
  fireEvent.change(screen.getByLabelText(/Persona instructions/), {
    target: { value: "Help support engineers triage incidents." },
  });
  fireEvent.click(screen.getByRole("button", { name: "Create agent" }));

  expect(await screen.findByText("Support bot", { selector: "b" })).toBeInTheDocument();
});

test("archiving an agent keeps its durable record and marks it archived", async () => {
  const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
  renderWithClient(<AgentsPage />);
  await screen.findByText("Researcher", { selector: "b" });

  const archiveButtons = screen.getAllByRole("button", { name: "Archive" });
  fireEvent.click(archiveButtons[archiveButtons.length - 1]);

  await waitFor(() => expect(screen.getByText("archived")).toBeInTheDocument());
  expect(screen.getByText("Researcher", { selector: "b" })).toBeInTheDocument();
  confirmSpy.mockRestore();
});
