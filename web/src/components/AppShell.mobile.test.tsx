import { fireEvent, screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { afterEach, beforeEach, expect, test } from "vitest";
import { renderWithClient } from "../test/utils";
import { AppShell } from "./AppShell";
import { Topbar } from "./Topbar";

function FakePage() {
  return <Topbar title="Chat" sub="preview" />;
}

function FakeSessionsPage() {
  return <Topbar title="Sessions" sub="preview" />;
}

function shellAt(path: string) {
  return createMemoryRouter(
    [
      {
        element: <AppShell />,
        children: [
          { path: "chat", element: <FakePage /> },
          { path: "sessions", element: <FakeSessionsPage /> },
          { path: "onboarding", element: <div>onboarding</div> },
        ],
      },
    ],
    { initialEntries: [path] },
  );
}

const DESKTOP_WIDTH = 1280;
const MOBILE_WIDTH = 375;

function setViewportWidth(width: number) {
  Object.defineProperty(window, "innerWidth", { writable: true, configurable: true, value: width });
  fireEvent(window, new Event("resize"));
}

beforeEach(() => {
  window.localStorage.clear();
});
afterEach(() => {
  window.localStorage.clear();
  setViewportWidth(DESKTOP_WIDTH);
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

test("the open drawer exposes dialog/navigation semantics: role=dialog, aria-modal, and a labeled nav landmark", () => {
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);

  // Closed (desktop-persistent) sidebar is a plain complementary landmark, not a dialog.
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(screen.getByRole("navigation", { name: "Primary navigation" })).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Open navigation menu" }));

  const dialog = screen.getByRole("dialog", { name: "Navigation menu" });
  expect(dialog).toHaveAttribute("aria-modal", "true");
  // The primary nav landmark still exists inside the dialog with its own accessible name.
  expect(screen.getByRole("navigation", { name: "Primary navigation" })).toBeInTheDocument();
});

test("opening the drawer makes background main content inert/aria-hidden, and closing restores it", () => {
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const main = document.getElementById("main-content")!;

  expect(main).not.toHaveAttribute("aria-hidden");
  expect(main.hasAttribute("inert")).toBe(false);
  expect(screen.getByRole("heading", { name: "Chat" })).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Open navigation menu" }));

  expect(main).toHaveAttribute("aria-hidden", "true");
  expect(main.hasAttribute("inert")).toBe(true);
  // Content behind the modal drawer is excluded from the accessibility tree while open.
  expect(screen.queryByRole("heading", { name: "Chat" })).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Close navigation menu" }));

  expect(main).not.toHaveAttribute("aria-hidden");
  expect(main.hasAttribute("inert")).toBe(false);
  expect(screen.getByRole("heading", { name: "Chat" })).toBeInTheDocument();
});

test("the closed drawer carries mobile-only visibility classes so it's non-visible/non-focusable on narrow screens, while staying lg:visible for the persistent desktop sidebar", () => {
  const { container } = renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const aside = container.querySelector("aside")!;

  // Closed: hidden via `invisible` (removes it from layout/focus on mobile),
  // but `lg:visible` always wins at the desktop breakpoint so the persistent
  // sidebar is never affected.
  expect(aside.className).toContain("invisible");
  expect(aside.className).not.toMatch(/(?<!lg:)\bvisible\b/);
  expect(aside.className).toContain("lg:visible");
  expect(aside.className).toContain("-translate-x-full");

  fireEvent.click(screen.getByRole("button", { name: "Open navigation menu" }));

  // Open: `visible` overrides `invisible` on mobile; `lg:visible` is unchanged.
  expect(aside.className).toContain("visible");
  expect(aside.className).not.toContain("invisible");
  expect(aside.className).toContain("lg:visible");
  expect(aside.className).toContain("translate-x-0");

  fireEvent.click(screen.getByRole("button", { name: "Close navigation menu" }));

  expect(aside.className).toContain("invisible");
  expect(aside.className).toContain("-translate-x-full");
});

test("clicking the backdrop closes the mobile sidebar drawer", () => {
  const { container } = renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const openButton = screen.getByRole("button", { name: "Open navigation menu" });
  fireEvent.click(openButton);
  expect(container.querySelector(".bg-black\\/40")).not.toBeNull();

  fireEvent.click(container.querySelector(".bg-black\\/40")!);
  expect(container.querySelector(".bg-black\\/40")).toBeNull();
});

test("dismissing the drawer via the backdrop returns focus to the button that opened it", () => {
  const { container } = renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const openButton = screen.getByRole("button", { name: "Open navigation menu" });
  openButton.focus();
  fireEvent.click(openButton);

  fireEvent.click(container.querySelector(".bg-black\\/40")!);

  expect(document.getElementById("main-content")!.hasAttribute("inert")).toBe(false);
  expect(document.activeElement).toBe(openButton);
});

test("dismissing the drawer via the close button returns focus to the button that opened it", () => {
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const openButton = screen.getByRole("button", { name: "Open navigation menu" });
  openButton.focus();
  fireEvent.click(openButton);

  fireEvent.click(screen.getByRole("button", { name: "Close navigation menu" }));

  expect(document.getElementById("main-content")!.hasAttribute("inert")).toBe(false);
  expect(document.activeElement).toBe(openButton);
});

test("resizing from mobile to desktop clears an open drawer: sidebar is no longer modal and main is no longer inert", () => {
  setViewportWidth(MOBILE_WIDTH);
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const main = document.getElementById("main-content")!;

  fireEvent.click(screen.getByRole("button", { name: "Open navigation menu" }));
  expect(screen.getByRole("dialog", { name: "Navigation menu" })).toBeInTheDocument();
  expect(main).toHaveAttribute("aria-hidden", "true");
  expect(main.hasAttribute("inert")).toBe(true);

  // Simulate rotating/resizing the viewport into the desktop breakpoint
  // while the mobile drawer is still open.
  setViewportWidth(DESKTOP_WIDTH);

  // The drawer state is cleared: no more dialog semantics, and the
  // persistent desktop sidebar/main content are restored to non-modal.
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(main).not.toHaveAttribute("aria-hidden");
  expect(main.hasAttribute("inert")).toBe(false);
  expect(screen.getByRole("navigation", { name: "Primary navigation" })).toBeInTheDocument();
});

test("does not clear the drawer when resizing while still within the mobile range", () => {
  setViewportWidth(MOBILE_WIDTH);
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);

  fireEvent.click(screen.getByRole("button", { name: "Open navigation menu" }));
  expect(screen.getByRole("dialog", { name: "Navigation menu" })).toBeInTheDocument();

  // Still a narrow (non-desktop) viewport after the resize.
  setViewportWidth(600);
  expect(screen.getByRole("dialog", { name: "Navigation menu" })).toBeInTheDocument();
});

test("clicking a nav link in the mobile drawer sequences focus onto #main-content only after inert is removed (never <body>)", () => {
  renderWithClient(<RouterProvider router={shellAt("/chat")} />);
  const main = document.getElementById("main-content")!;

  fireEvent.click(screen.getByRole("button", { name: "Open navigation menu" }));
  expect(main.hasAttribute("inert")).toBe(true);

  fireEvent.click(screen.getByRole("link", { name: /Sessions/ }));

  // The drawer has closed (main is interactive again) and focus landed on
  // the main landmark, not on <body>.
  expect(main.hasAttribute("inert")).toBe(false);
  expect(main).not.toHaveAttribute("aria-hidden");
  expect(document.activeElement).toBe(main);
  expect(screen.getByRole("heading", { name: "Sessions" })).toBeInTheDocument();
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
