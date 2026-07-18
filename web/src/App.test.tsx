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
