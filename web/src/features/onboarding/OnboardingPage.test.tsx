import { fireEvent, screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { afterEach, beforeEach, expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { OnboardingPage } from "./OnboardingPage";
import { isOnboardingComplete } from "./useOnboarding";

const STEP_COUNT = 5;

beforeEach(() => {
  window.localStorage.clear();
});
afterEach(() => {
  window.localStorage.clear();
});

function renderWizard() {
  const router = createMemoryRouter(
    [
      { path: "/onboarding", element: <OnboardingPage /> },
      { path: "/chat", element: <div>chat landing</div> },
    ],
    { initialEntries: ["/onboarding"] },
  );
  renderWithClient(<RouterProvider router={router} />);
  return router;
}

test("is not marked complete before the wizard has run", () => {
  expect(isOnboardingComplete()).toBe(false);
});

test("renders the shared manifest-driven connector setup step", async () => {
  renderWizard();
  // welcome -> locale -> workspace -> connectors
  fireEvent.click(screen.getByRole("button", { name: "Next" }));
  fireEvent.click(screen.getByRole("button", { name: "Next" }));
  fireEvent.click(screen.getByRole("button", { name: "Next" }));
  expect(await screen.findByText("Gmail")).toBeInTheDocument();
  expect(await screen.findByText("OAuth fixture")).toBeInTheDocument();
  expect(screen.getByText("Secret fixture")).toBeInTheDocument();
});

test("walking through every step stores completion + workspace name locally and redirects to Chat", () => {
  const router = renderWizard();

  fireEvent.click(screen.getByRole("button", { name: "Next" })); // -> locale
  fireEvent.click(screen.getByRole("button", { name: "Next" })); // -> workspace

  fireEvent.change(screen.getByLabelText("Workspace name"), {
    target: { value: "Ops team" },
  });

  fireEvent.click(screen.getByRole("button", { name: "Next" })); // -> connectors
  fireEvent.click(screen.getByRole("button", { name: "Next" })); // -> finish
  fireEvent.click(screen.getByRole("button", { name: "Finish" }));

  expect(isOnboardingComplete()).toBe(true);
  expect(JSON.parse(window.localStorage.getItem("keel.onboarding.v1")!).config.workspaceName).toBe(
    "Ops team",
  );
  expect(router.state.location.pathname).toBe("/chat");
});

test("connector loading does not prevent completing the local wizard", () => {
  renderWizard();
  for (let i = 0; i < STEP_COUNT - 1; i++) {
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
  }
  fireEvent.click(screen.getByRole("button", { name: "Finish" }));
  expect(isOnboardingComplete()).toBe(true);
});
