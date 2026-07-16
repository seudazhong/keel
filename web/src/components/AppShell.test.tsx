import { screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { expect, test } from "vitest";
import { ApprovalsPage } from "../features/approvals/ApprovalsPage";
import { renderWithClient } from "../test/utils";
import { AppShell } from "./AppShell";

function shellAt(path: string) {
  return createMemoryRouter(
    [
      {
        element: <AppShell />,
        children: [
          { path: "chat", element: <div>chat</div> },
          { path: "sessions", element: <div>sessions</div> },
          { path: "connectors", element: <div>connectors</div> },
          { path: "knowledge", element: <div>knowledge</div> },
          { path: "memory", element: <div>memory</div> },
          { path: "jobs", element: <div>jobs</div> },
          { path: "approvals", element: <ApprovalsPage /> },
        ],
      },
    ],
    { initialEntries: [path] },
  );
}

test("the shell exposes real navigation including Jobs and Memory", () => {
  renderWithClient(<RouterProvider router={shellAt("/approvals")} />);
  const labels = screen.getAllByRole("link").map((l) => l.textContent ?? "");
  for (const name of [
    "Chat",
    "Sessions",
    "Connectors",
    "Knowledge",
    "Memory",
    "Jobs",
    "Schedules",
    "Approvals",
    "Observability",
  ]) {
    expect(labels.some((l) => l.includes(name))).toBe(true);
  }
});

test("unfinished nav items remain disabled while shipped surfaces are enabled", () => {
  renderWithClient(<RouterProvider router={shellAt("/approvals")} />);
  const disabled = Array.from(document.querySelectorAll("[aria-disabled='true']"));
  expect(disabled.some((el) => el.textContent?.includes("Agents"))).toBe(true);
  expect(disabled.some((el) => el.textContent?.includes("Knowledge"))).toBe(false);
  expect(disabled.some((el) => el.textContent?.includes("Memory"))).toBe(false);
  expect(disabled.some((el) => el.textContent?.includes("Jobs"))).toBe(false);
  expect(disabled.some((el) => el.textContent?.includes("Admin"))).toBe(true);
  expect(disabled.some((el) => el.textContent?.includes("Multi-agent"))).toBe(true);
});
