/**
 * Auth / Workspace context: credential + selected org + Agent (M3.6 finding 1).
 *
 * Holds the caller's API key **or** OIDC bearer token plus the selected organization and Agent.
 * Secrets are kept in React state (memory) and mirrored only into tab-scoped `sessionStorage`
 * — never `localStorage` — so closing the tab or signing out fully clears them. The non-secret
 * workspace selection (org/Agent) is also persisted to `sessionStorage` for convenience.
 *
 * This does **not** implement an OIDC authorization-code flow: a bearer token is supplied
 * directly (e.g. by an external login page or an authenticating reverse proxy) and pasted or
 * injected here. The context surfaces a truthful sign-in/context screen when the server rejects
 * a request for auth (401/403).
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  emptyAuth,
  onAuthError,
  setAuthSnapshot,
  type AuthSnapshot,
  type Credential,
} from "./authState";

const STORAGE_KEY = "keel.auth.v1";

export interface AuthContextValue extends AuthSnapshot {
  /** True once the server has rejected a request for missing/invalid auth (cloud mode). */
  readonly needsAuth: boolean;
  /** True when a usable credential is present. */
  readonly isAuthenticated: boolean;
  signIn(input: { credential: Credential | null; org: string | null; agent: string | null }): void;
  setOrg(org: string | null): void;
  setAgent(agent: string | null): void;
  /** Clear every secret + selection from memory and `sessionStorage`. */
  signOut(): void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

function loadPersisted(): AuthSnapshot {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return emptyAuth;
    const parsed = JSON.parse(raw) as Partial<AuthSnapshot>;
    const credential =
      parsed.credential &&
      (parsed.credential.kind === "api-key" || parsed.credential.kind === "bearer") &&
      typeof parsed.credential.secret === "string"
        ? parsed.credential
        : null;
    return {
      credential,
      org: typeof parsed.org === "string" ? parsed.org : null,
      agent: typeof parsed.agent === "string" ? parsed.agent : null,
    };
  } catch {
    return emptyAuth;
  }
}

function persist(snapshot: AuthSnapshot): void {
  try {
    // Tab-scoped only. NEVER localStorage — a secret must not outlive the tab/session.
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(snapshot));
  } catch {
    // Storage may be unavailable (private mode / disabled). Memory state still works.
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [snapshot, setSnapshot] = useState<AuthSnapshot>(() => loadPersisted());
  const snapshotRef = useRef(snapshot);
  const [needsAuth, setNeedsAuth] = useState(false);

  // Keep the framework-agnostic fetch layer in sync with the live snapshot.
  useEffect(() => {
    snapshotRef.current = snapshot;
    setAuthSnapshot(snapshot);
    persist(snapshot);
  }, [snapshot]);

  // Surface the sign-in/context screen when the server rejects a request for auth.
  useEffect(() => {
    onAuthError((status) => {
      if (status === 401 || status === 403) {
        const cleared = { ...snapshotRef.current, credential: null };
        snapshotRef.current = cleared;
        setAuthSnapshot(cleared);
        persist(cleared);
        setSnapshot(cleared);
        setNeedsAuth(true);
      }
    });
    return () => onAuthError(null);
  }, []);

  const signIn = useCallback(
    (input: { credential: Credential | null; org: string | null; agent: string | null }) => {
      setSnapshot({ credential: input.credential, org: input.org, agent: input.agent });
      setNeedsAuth(false);
    },
    [],
  );

  const setOrg = useCallback((org: string | null) => {
    setSnapshot((prev) => ({ ...prev, org }));
  }, []);

  const setAgent = useCallback((agent: string | null) => {
    setSnapshot((prev) => ({ ...prev, agent }));
  }, []);

  const signOut = useCallback(() => {
    setSnapshot(emptyAuth);
    setNeedsAuth(false);
    try {
      sessionStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore
    }
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({
      ...snapshot,
      needsAuth,
      isAuthenticated: snapshot.credential !== null,
      signIn,
      setOrg,
      setAgent,
      signOut,
    }),
    [snapshot, needsAuth, signIn, setOrg, setAgent, signOut],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (ctx === null) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
