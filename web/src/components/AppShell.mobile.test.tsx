import { fireEvent, screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { afterEach, beforeEach, expect, test } from "vitest";
import { renderWithClient } from "../test/utils";
import { AppShell } from "./AppShell";
import { Topbar } from "./Topbar";

function FakePage() {
  return <Topbar title="Chat" sub="preview" />;
}

function shellAt(path: string) {
  return createMemoryRouter(
    [
      {
        element: <AppShell />,
        children: [
          { path: "chat", element: <FakePage /> },
          { path: "onboarding", element: <div>onboarding</div> },
        ],
      },
    ],
    { initialEntries: [path] },
  );
}

beforeEach(() => {
  window.localStorage.clear();
});
afterEach(() => {
  window.localStorage.clear();
});

test("exposes a skip link that targets the main landmark", () => {
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const skipLink = screen.getByText("Skip to main content");
  expect(skipLink.getAttribute("href")).toBe("#main-content");
  expect(document.getElementById("main-content")?.tagName).toBe("MAIN");
});

test("the mobile menu button opens the sidebar drawer, traps focus, and Escape returns focus", () => {
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);

  const openButton = screen.getByRole("button", { name: "Open navigation menu" });
  openButton.focus();
  fireEvent.click(openButton);

  const closeButton = screen.getByRole("button", { name: "Close navigation menu" });
  expect(document.activeElement).toBe(closeButton);

  fireEvent.keyDown(document, { key: "Escape" });
  expect(document.activeElement).toBe(openButton);
});

test("clicking the backdrop closes the mobile sidebar drawer", () => {
  const { container } = renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const openButton = screen.getByRole("button", { name: "Open navigation menu" });
  fireEvent.click(openButton);
  expect(container.querySelector(".bg-black\\/40")).not.toBeNull();

  fireEvent.click(container.querySelector(".bg-black\\/40")!);
  expect(container.querySelector(".bg-black\\/40")).toBeNull();
});

test("the shell renders semantic landmarks for navigation and main content", () => {
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  expect(screen.getByRole("navigation")).toBeInTheDocument();
  expect(screen.getByRole("main")).toBeInTheDocument();
});

test("prompts first-time visitors to the onboarding wizard and hides the prompt once completed", () => {
  window.localStorage.setItem(
    "keel.onboarding.v1",
    JSON.stringify({ completed: false, config: null, completedAt: null }),
  );
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  expect(screen.getByRole("link", { name: /Welcome to Keel/ })).toBeInTheDocument();

  window.localStorage.setItem(
    "keel.onboarding.v1",
    JSON.stringify({ completed: true, config: { workspaceName: "Ops" }, completedAt: "now" }),
  );
});

test("does not show the onboarding prompt once setup is already complete", () => {
  window.localStorage.setItem(
    "keel.onboarding.v1",
    JSON.stringify({ completed: true, config: { workspaceName: "Ops" }, completedAt: "now" }),
  );
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  expect(screen.queryByRole("link", { name: /Welcome to Keel/ })).not.toBeInTheDocument();
});
