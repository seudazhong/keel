async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
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
