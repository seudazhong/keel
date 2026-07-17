import { act, fireEvent, screen, waitFor } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { expect, test, vi } from "vitest";
import { renderWithClient } from "../../test/utils";
import { ProjectDetailPage } from "./ProjectDetailPage";
import * as useRunsModule from "./runs/useRuns";

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

test("navigating back and forth between two already-visited projects never renders/queries the other project's leftover selected run", async () => {
  // Pre-warm the cache for BOTH projects (including a selected run's diff and
  // approvals for each) so that, on the next navigation, react-query can
  // synchronously return cached data for the destination project on the very
  // first render — this is precisely the condition under which a one-render
  // (or one-effect-tick) stale reset would still let a mismatched
  // (project, run) pairing reach a query hook before being corrected.
  const { router } = renderDetail("proj_keel");

  await screen.findByText("https://github.com/example/keel");
  fireEvent.click(screen.getByRole("tab", { name: "Runs" }));
  fireEvent.click(await screen.findByText("Rotate CI credentials"));
  await screen.findByText("credentials_rotate"); // run_2's diff + approvals now cached

  await act(async () => {
    await router.navigate("/projects/proj_marketing");
  });
  await screen.findByText("https://github.com/example/marketing-site");
  fireEvent.click(screen.getByRole("tab", { name: "Runs" }));
  fireEvent.click(await screen.findByText("Update pricing page copy"));
  await screen.findByText("publish_page"); // run_marketing_1's diff + approvals now cached

  // Spy on the actual data hooks (not just `fetch`) so a stale render that
  // instantiates a query for the wrong run is caught even if react-query
  // happens to answer it from cache without a network round trip.
  const diffSpy = vi.spyOn(useRunsModule, "useRunDiff");
  const approvalsSpy = vi.spyOn(useRunsModule, "useRunApprovals");
  const fetchSpy = vi.spyOn(window, "fetch");

  // Navigate back to the first project. Both projects' own data, and both
  // runs' diff/approvals, are already cached at this point.
  await act(async () => {
    await router.navigate("/projects/proj_keel");
  });

  // The view must never show the other project's leftover selected run.
  expect(screen.getByRole("tab", { name: "Overview" })).toHaveAttribute("aria-selected", "true");
  expect(screen.queryByText("publish_page")).not.toBeInTheDocument();
  expect(screen.queryByText("docs/pricing.md")).not.toBeInTheDocument();
  expect(screen.queryByText("Update pricing page copy")).not.toBeInTheDocument();

  // The smoking gun: no render — not even a transient one superseded within
  // the same commit — may ever call useRunDiff/useRunApprovals with the
  // other project's run id once we've navigated to this project.
  expect(diffSpy.mock.calls.some(([runId]) => runId === "run_marketing_1")).toBe(false);
  expect(approvalsSpy.mock.calls.some(([runId]) => runId === "run_marketing_1")).toBe(false);

  const requestedUrls = fetchSpy.mock.calls.map((args) => String(args[0]));
  expect(requestedUrls.some((u) => u.includes("/v1/runs/run_marketing_1/"))).toBe(false);

  diffSpy.mockRestore();
  approvalsSpy.mockRestore();
  fetchSpy.mockRestore();
});
