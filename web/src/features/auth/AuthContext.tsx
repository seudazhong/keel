/**
 * Auth / Workspace context: credential + selected org + Agent (M3.6 finding 1 + follow-up).
 *
 * Holds the caller's API key **or** OIDC bearer token plus the selected organization and Agent.
 * Secrets are kept in React state (memory) and mirrored only into tab-scoped `sessionStorage`
 * — never `localStorage` — so closing the tab or signing out fully clears them. The non-secret
 * workspace selection (org/Agent/mode) is also persisted to `sessionStorage` for convenience.
 *
 * **Local preview is an explicit auth mode**, not merely "no credential yet". With nothing
 * persisted the app optimistically starts in local-preview mode so a non-cloud deployment is
 * usable immediately — this mirrors the server's own fallback (an unauthenticated request maps
 * to the `web:local` scope outside cloud mode, per `keel_server.endpoint_auth`), so local
 * preview never sends a credential header and needs none. The first time any request comes
 * back 401/403 the server has truthfully told us a credential is required: local preview is
 * then blocked for the rest of this tab session (`cloudAuthRequired`, never silently re-offered
 * or re-entered), any rejected credential is cleared (it never falls back into local preview),
 * and the truthful sign-in screen is shown until the caller supplies a valid credential.
 * Signing out always returns to that same choice screen — never straight back into an
 * authenticated state.
 *
 * This does **not** implement an OIDC authorization-code flow: a bearer token is supplied
 * directly (e.g. by an external login page or an authenticating reverse proxy) and pasted or
 * injected here.
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
import { onAuthError, setAuthSnapshot, type Credential } from "./authState";

const STORAGE_KEY = "keel.auth.v1";

/** `null` means no explicit choice is currently in effect (fresh sign-out / rejected auth). */
export type AuthMode = "local-preview" | "credential" | null;

interface PersistedAuthState {
  readonly mode: AuthMode;
  readonly credential: Credential | null;
  readonly org: string | null;
  readonly agent: string | null;
  /** Learned only from an actual 401/403 — never guessed ahead of time. */
  readonly cloudAuthRequired: boolean;
}

// Nothing persisted yet: optimistically default to local preview (no credential, no headers)
// so a non-cloud deployment works out of the box. The first real auth rejection corrects this.
const DEFAULT_STATE: PersistedAuthState = {
  mode: "local-preview",
  credential: null,
  org: null,
  agent: null,
  cloudAuthRequired: false,
};

const SIGNED_OUT: Omit<PersistedAuthState, "cloudAuthRequired"> = {
  mode: null,
  credential: null,
  org: null,
  agent: null,
};

export interface AuthContextValue {
  readonly mode: AuthMode;
  readonly credential: Credential | null;
  readonly org: string | null;
  readonly agent: string | null;
  /** True once the server has rejected a request for missing/invalid auth. */
  readonly needsAuth: boolean;
  /** True when local preview (or a usable credential) currently grants access. */
  readonly isAuthenticated: boolean;
  /** True once a 401/403 has been observed — local preview must stay hidden/impossible. */
  readonly cloudAuthRequired: boolean;
  signIn(input: { credential: Credential | null; org: string | null; agent: string | null }): void;
  /** Explicitly choose local preview. A no-op once `cloudAuthRequired` is true (fail closed). */
  enterLocalPreview(input: { org: string | null; agent: string | null }): void;
  setOrg(org: string | null): void;
  setAgent(agent: string | null): void;
  /** Clear every secret + selection from memory and `sessionStorage`; returns to the choice screen. */
  signOut(): void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

function normalizeMode(parsed: Partial<PersistedAuthState>, credential: Credential | null): AuthMode {
  if (parsed.mode === "local-preview" || parsed.mode === "credential") return parsed.mode;
  if (credential) return "credential";
  // An explicitly persisted `mode: null` (sign-out / rejection) must stay null, never default
  // back to local preview just because this key happens to be present.
  if ("mode" in parsed) return null;
  return DEFAULT_STATE.mode;
}

function loadPersisted(): PersistedAuthState {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return DEFAULT_STATE;
    const parsed = JSON.parse(raw) as Partial<PersistedAuthState>;
    const credential =
      parsed.credential &&
      (parsed.credential.kind === "api-key" || parsed.credential.kind === "bearer") &&
      typeof parsed.credential.secret === "string"
        ? parsed.credential
        : null;
    return {
      mode: normalizeMode(parsed, credential),
      credential,
      org: typeof parsed.org === "string" ? parsed.org : null,
      agent: typeof parsed.agent === "string" ? parsed.agent : null,
      cloudAuthRequired: parsed.cloudAuthRequired === true,
    };
  } catch {
    return DEFAULT_STATE;
  }
}

function persist(state: PersistedAuthState): void {
  try {
    // Tab-scoped only. NEVER localStorage — a secret must not outlive the tab/session.
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // Storage may be unavailable (private mode / disabled). Memory state still works.
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<PersistedAuthState>(() => loadPersisted());
  const stateRef = useRef(state);
  const [needsAuth, setNeedsAuth] = useState(false);

  // Keep the framework-agnostic fetch layer in sync with the live state (headers only ever
  // come from `credential`/`org`/`agent` — local preview sends no credential header at all).
  useEffect(() => {
    stateRef.current = state;
    setAuthSnapshot({ credential: state.credential, org: state.org, agent: state.agent });
    persist(state);
  }, [state]);

  // The server has truthfully rejected a request for auth: clear any credential, drop out of
  // local preview, and remember that local preview must stay blocked for the rest of this tab
  // session — a rejected credential/local-preview attempt must never silently fall back into
  // (or stay in) local preview.
  useEffect(() => {
    onAuthError((status) => {
      if (status === 401 || status === 403) {
        const cleared: PersistedAuthState = {
          ...stateRef.current,
          mode: null,
          credential: null,
          cloudAuthRequired: true,
        };
        stateRef.current = cleared;
        setAuthSnapshot({ credential: null, org: cleared.org, agent: cleared.agent });
        persist(cleared);
        setState(cleared);
        setNeedsAuth(true);
      }
    });
    return () => onAuthError(null);
  }, []);

  const enterLocalPreview = useCallback((input: { org: string | null; agent: string | null }) => {
    setState((prev) =>
      // Fail closed: once the server has said auth is required, local preview is impossible —
      // never silently grant it again for the rest of this tab session.
      prev.cloudAuthRequired
        ? prev
        : { ...prev, mode: "local-preview", credential: null, org: input.org, agent: input.agent },
    );
    setNeedsAuth(false);
  }, []);

  const signIn = useCallback(
    (input: { credential: Credential | null; org: string | null; agent: string | null }) => {
      if (input.credential === null) {
        enterLocalPreview({ org: input.org, agent: input.agent });
        return;
      }
      setState((prev) => ({
        ...prev,
        mode: "credential",
        credential: input.credential,
        org: input.org,
        agent: input.agent,
      }));
      setNeedsAuth(false);
    },
    [enterLocalPreview],
  );

  const setOrg = useCallback((org: string | null) => {
    setState((prev) => ({ ...prev, org }));
  }, []);

  const setAgent = useCallback((agent: string | null) => {
    setState((prev) => ({ ...prev, agent }));
  }, []);

  const signOut = useCallback(() => {
    // Always returns to the choice screen — sign-out never silently re-enters local preview,
    // even in a non-cloud deployment; the caller must explicitly choose again.
    setState((prev) => ({ ...SIGNED_OUT, cloudAuthRequired: prev.cloudAuthRequired }));
    setNeedsAuth(false);
    try {
      sessionStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore
    }
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({
      mode: state.mode,
      credential: state.credential,
      org: state.org,
      agent: state.agent,
      needsAuth,
      isAuthenticated: state.mode === "local-preview" || (state.mode === "credential" && state.credential !== null),
      cloudAuthRequired: state.cloudAuthRequired,
      signIn,
      enterLocalPreview,
      setOrg,
      setAgent,
      signOut,
    }),
    [state, needsAuth, signIn, enterLocalPreview, setOrg, setAgent, signOut],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (ctx === null) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
