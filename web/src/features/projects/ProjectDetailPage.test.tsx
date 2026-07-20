import { fireEvent, screen, waitFor } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { expect, test, vi } from "vitest";
import { renderWithClient } from "../../test/utils";
import { ProjectDetailPage } from "./ProjectDetailPage";

function renderDetail(projectId = "proj_keel") {
  const router = createMemoryRouter(
    [
      { path: "/projects/:id", element: <ProjectDetailPage /> },
      { path: "/projects", element: <div>Projects list</div> },
    ],
    { initialEntries: [`/projects/${projectId}`] },
  );
  return { router, ...renderWithClient(<RouterProvider router={router} />) };
}

test("shows fields from the real project API contract", async () => {
  renderDetail();
  expect(await screen.findByText("keel")).toBeInTheDocument();
  expect(screen.getByText("github")).toBeInTheDocument();
  expect(screen.getByText("1001")).toBeInTheDocument();
  expect(screen.getByText(/detailed coding timeline/)).toBeInTheDocument();
});

test("reports a not-found state for an unknown project", async () => {
  renderDetail("proj_missing");
  expect(await screen.findByText("Project not found.")).toBeInTheDocument();
});

test("deleting uses the versioned backend endpoint and returns to the list", async () => {
  vi.spyOn(window, "confirm").mockReturnValue(true);
  renderDetail();
  await screen.findByText("keel");
  fireEvent.click(screen.getByRole("button", { name: "Remove project" }));
  await waitFor(() => expect(screen.getByText("Projects list")).toBeInTheDocument());
  vi.restoreAllMocks();
});
