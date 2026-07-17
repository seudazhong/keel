import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { ReactNode } from "react";
import { AuthProvider, useAuth } from "./AuthContext";
import { authHeaders, getAuthSnapshot } from "./authState";

const wrapper = ({ children }: { children: ReactNode }) => <AuthProvider>{children}</AuthProvider>;

beforeEach(() => {
  sessionStorage.clear();
  localStorage.clear();
});
afterEach(() => {
  sessionStorage.clear();
  localStorage.clear();
});

describe("AuthContext", () => {
  it("signs in, syncs headers, and persists to sessionStorage (never localStorage)", () => {
    const { result } = renderHook(() => useAuth(), { wrapper });
    act(() => {
      result.current.signIn({ credential: { kind: "bearer", secret: "s3cret" }, org: "o", agent: "a" });
    });
    expect(result.current.isAuthenticated).toBe(true);
    expect(authHeaders()).toMatchObject({
      Authorization: "Bearer s3cret",
      "X-Keel-Org": "o",
      "X-Keel-Agent": "a",
    });
    // Persisted to tab-scoped sessionStorage only.
    expect(sessionStorage.getItem("keel.auth.v1")).toContain("s3cret");
    // The secret is NEVER written to localStorage.
    expect(JSON.stringify(localStorage)).not.toContain("s3cret");
  });

  it("signs out and redacts every secret from memory and storage", () => {
    const { result } = renderHook(() => useAuth(), { wrapper });
    act(() => {
      result.current.signIn({ credential: { kind: "api-key", secret: "vkey" }, org: "o", agent: "a" });
    });
    act(() => result.current.signOut());
    expect(result.current.isAuthenticated).toBe(false);
    expect(getAuthSnapshot().credential).toBeNull();
    expect(authHeaders()).toEqual({});
    // Any residual persisted snapshot carries no secret (the credential is nulled out).
    expect(sessionStorage.getItem("keel.auth.v1") ?? "").not.toContain("vkey");
  });

  it("restores a persisted snapshot on next mount", () => {
    sessionStorage.setItem(
      "keel.auth.v1",
      JSON.stringify({ credential: { kind: "api-key", secret: "kk" }, org: "o2", agent: null }),
    );
    const { result } = renderHook(() => useAuth(), { wrapper });
    expect(result.current.isAuthenticated).toBe(true);
    expect(result.current.org).toBe("o2");
    expect(authHeaders()).toMatchObject({ "X-API-Key": "kk", "X-Keel-Org": "o2" });
  });
});
