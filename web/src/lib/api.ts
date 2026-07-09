async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.status === 204 ? (undefined as T) : ((await res.json()) as T);
}

export const api = {
  get: <T>(path: string) => req<T>(path),
  post: <T>(path: string) => req<T>(path, { method: "POST" }),
  put: <T>(path: string, body: unknown) =>
    req<T>(path, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  del: <T>(path: string) => req<T>(path, { method: "DELETE" }),
};
