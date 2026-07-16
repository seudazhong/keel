import { act, fireEvent, screen, waitFor } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { expect, test, vi } from "vitest";
import { renderWithClient } from "../../test/utils";
import { ProjectDetailPage } from "./ProjectDetailPage";

function renderDetail(projectId = "proj_keel") {
  const router = createMemoryRouter(
    [{ path: "/projects/:id", element: <ProjectDetailPage /> }],
    { initialEntries: [`/projects/${projectId}`] },
  );
  return { router, ...renderWithClient(<RouterProvider router={router} />) };
}

test("shows project overview fields", async () => {
  renderDetail();
  expect(await screen.findByText("https://github.com/example/keel")).toBeInTheDocument();
  expect(screen.getByText("main")).toBeInTheDocument();
});

test("reports a not-found state for an unknown project instead of a blank page", async () => {
  renderDetail("proj_missing");
  expect(await screen.findByText("Project not found.")).toBeInTheDocument();
});

test("the Runs tab shows a timeline, and selecting a run shows its diff and approvals", async () => {
  renderDetail();
  await screen.findByText("https://github.com/example/keel");

  fireEvent.click(screen.getByRole("tab", { name: "Runs" }));

  expect(await screen.findByText("Add i18n foundation")).toBeInTheDocument();
  expect(screen.getByText("Rotate CI credentials")).toBeInTheDocument();
  expect(screen.getByText(/does not turn Keel into a permanent IDE/)).toBeInTheDocument();

  // First run is selected by default and has a diff with one file.
  expect(await screen.findByText("web/src/lib/i18n/en.ts")).toBeInTheDocument();

  // Selecting the run awaiting approval surfaces its pending approval.
  fireEvent.click(screen.getByText("Rotate CI credentials"));
  expect(await screen.findByText("credentials_rotate")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Approve" })).toBeInTheDocument();
});

test("approving a run's pending approval clears it from the panel", async () => {
  renderDetail();
  await screen.findByText("https://github.com/example/keel");
  fireEvent.click(screen.getByRole("tab", { name: "Runs" }));
  fireEvent.click(await screen.findByText("Rotate CI credentials"));

  const approveButton = await screen.findByRole("button", { name: "Approve" });
  fireEvent.click(approveButton);

  await waitFor(() => {
    expect(screen.queryByText("credentials_rotate")).not.toBeInTheDocument();
    expect(screen.getByText("Decision recorded for this preview session.")).toBeInTheDocument();
  });
});

test("navigating to a different project without remounting resets the tab and selected run, and only requests the new project's run data", async () => {
  const { router } = renderDetail("proj_keel");

  await screen.findByText("https://github.com/example/keel");
  fireEvent.click(screen.getByRole("tab", { name: "Runs" }));
  fireEvent.click(await screen.findByText("Rotate CI credentials"));
  expect(await screen.findByText("credentials_rotate")).toBeInTheDocument();

  const fetchSpy = vi.spyOn(window, "fetch");

  // Same route element, only the :id param changes — this is the scenario
  // where stale component state previously leaked across projects.
  await act(async () => {
    await router.navigate("/projects/proj_marketing");
  });

  await screen.findByText("https://github.com/example/marketing-site");
  expect(screen.getByRole("tab", { name: "Overview" })).toHaveAttribute("aria-selected", "true");
  expect(screen.queryByText("credentials_rotate")).not.toBeInTheDocument();
  expect(screen.queryByText("Rotate CI credentials")).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("tab", { name: "Runs" }));

  expect(await screen.findByText("Update pricing page copy")).toBeInTheDocument();
  expect(screen.queryByText("Rotate CI credentials")).not.toBeInTheDocument();
  expect(screen.queryByText("Add i18n foundation")).not.toBeInTheDocument();

  await screen.findByText("publish_page");
  expect(screen.queryByText("credentials_rotate")).not.toBeInTheDocument();
  expect(screen.queryByText("web/src/lib/i18n/en.ts")).not.toBeInTheDocument();

  const requestedUrls = fetchSpy.mock.calls.map((args) => String(args[0]));
  expect(requestedUrls.some((u) => u.includes("/v1/projects/proj_marketing/runs"))).toBe(true);
  expect(requestedUrls.some((u) => u.includes("/v1/runs/run_marketing_1/diff"))).toBe(true);
  expect(requestedUrls.some((u) => u.includes("/v1/runs/run_marketing_1/approvals"))).toBe(true);
  expect(requestedUrls.some((u) => u.includes("/v1/runs/run_2/"))).toBe(false);
  expect(requestedUrls.some((u) => u.includes("/v1/runs/run_1/"))).toBe(false);

  fetchSpy.mockRestore();
});
