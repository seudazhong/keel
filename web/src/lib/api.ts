import { authHeaders, notifyAuthError } from "../features/auth/authState";

export class ApiError extends Error {
  readonly status: number;
  readonly detail?: unknown;

  constructor(status: number, message: string, detail?: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

const MUTATING = new Set(["POST", "PUT", "PATCH", "DELETE"]);

/**
 * Attach the credential + workspace + idempotency headers every request needs (M3.6 finding 1).
 *
 * - `Authorization` / `X-API-Key` carry the OIDC bearer or API-key credential;
 * - `X-Keel-Org` / `X-Keel-Agent` carry the selected workspace so the server derives the
 *   caller's per-Agent data-plane scope;
 * - a mutating request gets an `Idempotency-Key` (unless the caller set one) so a transport
 *   retry cannot double-apply it.
 *
 * Caller-supplied headers win, so a route that needs a *stable* idempotency key (e.g. Knowledge
 * mutations keyed by content) can set its own.
 */
function withAuth(init?: RequestInit): RequestInit {
  const headers = new Headers(init?.headers);
  for (const [key, value] of Object.entries(authHeaders())) {
    if (!headers.has(key)) headers.set(key, value);
  }
  const method = (init?.method ?? "GET").toUpperCase();
  if (MUTATING.has(method) && !headers.has("Idempotency-Key")) {
    headers.set("Idempotency-Key", crypto.randomUUID());
  }
  return { ...init, headers };
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, withAuth(init));
  if (!res.ok) {
    // Let the auth layer surface the sign-in/context screen on a rejected credential/context.
    notifyAuthError(res.status);
    let detail: unknown;
    try {
      detail = await res.json();
    } catch {
      detail = undefined;
    }
    const message =
      typeof detail === "object" &&
      detail !== null &&
      "detail" in detail &&
      typeof detail.detail === "string"
        ? detail.detail
        : `HTTP ${res.status}`;
    // The message/detail come from the server body only — never the request headers — so a
    // credential/token is never echoed back into an error surfaced to the UI or logs.
    throw new ApiError(res.status, message, detail);
  }
  return res.status === 204 ? (undefined as T) : ((await res.json()) as T);
}

function jsonInit(method: string, body: unknown, init?: RequestInit): RequestInit {
  const headers = new Headers(init?.headers);
  headers.set("Content-Type", "application/json");
  return {
    ...init,
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  };
}

export const api = {
  get: <T>(path: string, init?: RequestInit) => req<T>(path, init),
  post: <T>(path: string, body?: unknown, init?: RequestInit) =>
    req<T>(path, jsonInit("POST", body, init)),
  put: <T>(path: string, body: unknown, init?: RequestInit) =>
    req<T>(path, jsonInit("PUT", body, init)),
  del: <T>(path: string, init?: RequestInit) => req<T>(path, { ...init, method: "DELETE" }),
};
