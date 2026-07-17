import { fireEvent, render, screen } from "@testing-library/react";
import { expect, test, vi } from "vitest";
import { I18nProvider } from "../../../lib/i18n";
import { RunTimeline } from "./RunTimeline";
import type { Run } from "./types";

const runs: Run[] = [
  {
    id: "run_a",
    project_id: "proj_1",
    status: "succeeded",
    summary: "Fix flaky test",
    started_at: "2026-07-01T00:00:00Z",
    finished_at: "2026-07-01T00:05:00Z",
    steps: [{ id: "s1", label: "Run tests", status: "done", timestamp: "2026-07-01T00:05:00Z" }],
  },
  {
    id: "run_b",
    project_id: "proj_1",
    status: "awaiting_approval",
    summary: "Bump dependency",
    started_at: "2026-07-02T00:00:00Z",
    finished_at: null,
    steps: [{ id: "s1", label: "Plan", status: "done", timestamp: "2026-07-02T00:00:00Z" }],
  },
];

function renderTimeline(selectedId: string | null, onSelect = vi.fn()) {
  return render(
    <I18nProvider>
      <RunTimeline runs={runs} selectedId={selectedId} onSelect={onSelect} />
    </I18nProvider>,
  );
}

test("renders every run with its status and step labels", () => {
  renderTimeline(null);
  expect(screen.getByText("Fix flaky test")).toBeInTheDocument();
  expect(screen.getByText("Bump dependency")).toBeInTheDocument();
  expect(screen.getByText("succeeded")).toBeInTheDocument();
  expect(screen.getByText("awaiting approval")).toBeInTheDocument();
  expect(screen.getByText("Run tests")).toBeInTheDocument();
});

test("clicking a run calls onSelect with its id and marks it pressed", () => {
  const onSelect = vi.fn();
  renderTimeline("run_a", onSelect);

  fireEvent.click(screen.getByText("Bump dependency"));
  expect(onSelect).toHaveBeenCalledWith("run_b");

  const selectedButton = screen.getByText("Fix flaky test").closest("button")!;
  expect(selectedButton.getAttribute("aria-pressed")).toBe("true");
});

test("renders an empty state instead of a blank list when there are no runs", () => {
  render(
    <I18nProvider>
      <RunTimeline runs={[]} selectedId={null} onSelect={vi.fn()} />
    </I18nProvider>,
  );
  expect(screen.getByText("No runs yet for this project.")).toBeInTheDocument();
});
