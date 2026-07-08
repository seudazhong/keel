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
          { path: "approvals", element: <ApprovalsPage /> },
        ],
      },
    ],
    { initialEntries: [path] },
  );
}

test("the shell exposes real nav links (Chat, Sessions, Connectors, Approvals)", () => {
  renderWithClient(<RouterProvider router={shellAt("/approvals")} />);
  const labels = screen.getAllByRole("link").map((l) => l.textContent ?? "");
  for (const name of ["Chat", "Sessions", "Connectors", "Approvals"]) {
    expect(labels.some((l) => l.includes(name))).toBe(true);
  }
});

test("other nav items render as disabled placeholders", () => {
  renderWithClient(<RouterProvider router={shellAt("/approvals")} />);
  const disabled = Array.from(document.querySelectorAll("[aria-disabled='true']"));
  expect(disabled.some((el) => el.textContent?.includes("Memory"))).toBe(true);
});
