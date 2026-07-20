import { fireEvent, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { ProjectsPage } from "./ProjectsPage";

function renderPage() {
  return renderWithClient(
    <MemoryRouter>
      <ProjectsPage />
    </MemoryRouter>,
  );
}

test("lists durable projects and explains the GitHub App import requirement", async () => {
  renderPage();
  expect(await screen.findByText("Keel", { selector: "b" })).toBeInTheDocument();
  expect(screen.getByText(/performs a real repository fetch/)).toBeInTheDocument();
});

test("importing a project adds it to the list", async () => {
  renderPage();
  await screen.findByText("Keel", { selector: "b" });

  fireEvent.click(screen.getByRole("button", { name: "+ Import project" }));
  fireEvent.change(screen.getByLabelText(/^Name/), { target: { value: "Docs site" } });
  expect(screen.getByLabelText("Slug")).toHaveValue("docs-site");
  fireEvent.change(screen.getByLabelText(/Repository/), {
    target: { value: "example/docs" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Import" }));

  await waitFor(() => {
    expect(screen.getByText("Docs site", { selector: "b" })).toBeInTheDocument();
  });
  expect(screen.getByText("Imported Docs site.")).toBeInTheDocument();
});
