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

test("lists imported projects and labels project import as a UI-contract preview", async () => {
  renderPage();
  expect(await screen.findByText("Keel", { selector: "b" })).toBeInTheDocument();
  expect(screen.getByText(/does not clone or index a real repository/)).toBeInTheDocument();
});

test("importing a project adds it to the list", async () => {
  renderPage();
  await screen.findByText("Keel", { selector: "b" });

  fireEvent.click(screen.getByRole("button", { name: "+ Import project" }));
  fireEvent.change(screen.getByLabelText(/^Name/), { target: { value: "Docs site" } });
  fireEvent.change(screen.getByLabelText(/Repository/), {
    target: { value: "https://github.com/example/docs" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Import" }));

  await waitFor(() => {
    expect(screen.getByText("Docs site", { selector: "b" })).toBeInTheDocument();
  });
});
