import { render, screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { expect, test } from "vitest";
import { ApprovalsPage } from "../features/approvals/ApprovalsPage";
import { AppShell } from "./AppShell";

function shellAt(path: string) {
  return createMemoryRouter(
    [{ element: <AppShell />, children: [{ path: "approvals", element: <ApprovalsPage /> }] }],
    { initialEntries: [path] },
  );
}

test("the shell exposes exactly one real nav link (Approvals)", () => {
  render(<RouterProvider router={shellAt("/approvals")} />);
  const links = screen.getAllByRole("link");
  expect(links).toHaveLength(1);
  expect(links[0]).toHaveTextContent("Approvals");
});

test("other nav items render as disabled placeholders", () => {
  render(<RouterProvider router={shellAt("/approvals")} />);
  const disabled = Array.from(document.querySelectorAll("[aria-disabled='true']"));
  expect(disabled.some((el) => el.textContent?.includes("Chat"))).toBe(true);
});
