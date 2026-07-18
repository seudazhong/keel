import { act, render, screen } from "@testing-library/react";
import { beforeEach, expect, test, vi } from "vitest";
import { AuthGate } from "./App";
import { AuthProvider } from "./features/auth/AuthContext";
import { notifyAuthError } from "./features/auth/authState";
import { I18nProvider } from "./lib/i18n";

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react-router-dom")>();
  return {
    ...actual,
    RouterProvider: () => <div>Application content</div>,
  };
});

beforeEach(() => {
  sessionStorage.clear();
});

test("an auth rejection clears the stored secret and shows credential recovery", () => {
  sessionStorage.setItem(
    "keel.auth.v1",
    JSON.stringify({
      credential: { kind: "api-key", secret: "rejected-secret" },
      org: "kept-org",
      agent: "kept-agent",
    }),
  );

  render(
    <I18nProvider>
      <AuthProvider>
        <AuthGate />
      </AuthProvider>
    </I18nProvider>,
  );
  expect(screen.getByText("Application content")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Sign out" })).toBeInTheDocument();

  act(() => notifyAuthError(401));

  expect(screen.getByRole("heading", { name: "Sign in to your workspace" })).toBeInTheDocument();
  expect(screen.getByRole("alert")).toHaveTextContent(/credential or workspace was rejected/i);
  const persisted = JSON.parse(sessionStorage.getItem("keel.auth.v1") ?? "{}");
  expect(persisted.credential).toBeNull();
  expect(persisted.org).toBe("kept-org");
  expect(persisted.agent).toBe("kept-agent");
  expect(sessionStorage.getItem("keel.auth.v1")).not.toContain("rejected-secret");
});

test("with nothing persisted, the app defaults to local preview and grants access without any sign-in", () => {
  render(
    <I18nProvider>
      <AuthProvider>
        <AuthGate />
      </AuthProvider>
    </I18nProvider>,
  );

  expect(screen.getByText("Application content")).toBeInTheDocument();
  expect(screen.queryByRole("heading", { name: "Sign in to your workspace" })).not.toBeInTheDocument();
});

test("cloud denial: once the server rejects an unauthenticated request, local preview is hidden and only sign-in remains", () => {
  render(
    <I18nProvider>
      <AuthProvider>
        <AuthGate />
      </AuthProvider>
    </I18nProvider>,
  );
  expect(screen.getByText("Application content")).toBeInTheDocument(); // default local preview

  act(() => notifyAuthError(403));

  expect(screen.getByRole("heading", { name: "Sign in to your workspace" })).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Continue in local preview" })).not.toBeInTheDocument();
  expect(screen.getByText(/local preview is not available/i)).toBeInTheDocument();
});

test("sign-out returns to the choice screen, and choosing local preview recovers app access", () => {
  render(
    <I18nProvider>
      <AuthProvider>
        <AuthGate />
      </AuthProvider>
    </I18nProvider>,
  );
  expect(screen.getByText("Application content")).toBeInTheDocument();

  act(() => screen.getByRole("button", { name: "Sign out" }).click());

  expect(screen.getByRole("heading", { name: "Sign in to your workspace" })).toBeInTheDocument();
  const localPreview = screen.getByRole("button", { name: "Continue in local preview" });

  act(() => localPreview.click());

  expect(screen.getByText("Application content")).toBeInTheDocument();
});
