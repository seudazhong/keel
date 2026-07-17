/**
 * Framework-agnostic auth snapshot shared with the fetch layer (M3.6 finding 1).
 *
 * The React {@link ./AuthContext} owns the source of truth and pushes an immutable snapshot
 * here so the centralized `api` fetch wrapper can attach the right credential and workspace
 * headers to every request **without importing React**. Secrets live in memory (and, optionally,
 * tab-scoped `sessionStorage`) — never `localStorage` — so closing the tab or signing out fully
 * clears them.
 */

export type Credential =
  | { readonly kind: "api-key"; readonly secret: string }
  | { readonly kind: "bearer"; readonly secret: string };

export interface AuthSnapshot {
  /** The API key or OIDC bearer token, or null when unauthenticated (local preview). */
  readonly credential: Credential | null;
  /** The selected organization (`X-Keel-Org`), or null. */
  readonly org: string | null;
  /** The selected Agent (`X-Keel-Agent`), or null. */
  readonly agent: string | null;
}

export const emptyAuth: AuthSnapshot = { credential: null, org: null, agent: null };

let current: AuthSnapshot = emptyAuth;

/** Replace the current snapshot the fetch layer reads. Called by the AuthContext provider. */
export function setAuthSnapshot(next: AuthSnapshot): void {
  current = next;
}

export function getAuthSnapshot(): AuthSnapshot {
  return current;
}

/**
 * The credential + workspace headers for the current snapshot.
 *
 * A bearer token is sent as `Authorization: Bearer …`; an API key as `X-API-Key`. The selected
 * org/Agent are sent as `X-Keel-Org` / `X-Keel-Agent` so the server can derive the caller's
 * per-Agent data-plane scope. Nothing here is logged; the caller must not echo these into errors.
 */
export function authHeaders(snapshot: AuthSnapshot = current): Record<string, string> {
  const headers: Record<string, string> = {};
  const credential = snapshot.credential;
  if (credential?.kind === "bearer" && credential.secret) {
    headers["Authorization"] = `Bearer ${credential.secret}`;
  } else if (credential?.kind === "api-key" && credential.secret) {
    headers["X-API-Key"] = credential.secret;
  }
  if (snapshot.org) headers["X-Keel-Org"] = snapshot.org;
  if (snapshot.agent) headers["X-Keel-Agent"] = snapshot.agent;
  return headers;
}

/** Whether an HTTP status means the current credential/context is missing or rejected. */
export function isAuthError(status: number): boolean {
  return status === 401 || status === 403;
}

// The fetch layer notifies the AuthContext when a request is rejected for auth so the UI can
// surface the truthful sign-in/context screen. Registered by the provider; a no-op otherwise.
let authErrorListener: ((status: number) => void) | null = null;

export function onAuthError(listener: ((status: number) => void) | null): void {
  authErrorListener = listener;
}

export function notifyAuthError(status: number): void {
  if (isAuthError(status)) authErrorListener?.(status);
}
