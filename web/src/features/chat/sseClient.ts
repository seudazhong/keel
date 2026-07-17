/**
 * Authenticated SSE client over `fetch` + `ReadableStream` (M3.6 finding 1).
 *
 * The native `EventSource` cannot send `Authorization` / `X-API-Key` / `X-Keel-Org` /
 * `X-Keel-Agent` headers, so an authenticated per-Agent chat stream needs a hand-rolled client.
 * This reader:
 *
 * - sends the credential + workspace headers (via {@link authHeaders});
 * - parses the SSE wire format line-by-line (`id:` / `event:` / `data:` / `retry:`, blank line
 *   dispatches, `:`-comments ignored), buffering across chunk boundaries;
 * - tracks the last event id and, on a dropped connection, reconnects with `Last-Event-ID` so
 *   the server resumes exactly where it left off — **no missed or duplicated events** (an id at
 *   or below the last dispatched id is skipped);
 * - honors a server `retry:` backoff hint (with a bounded default);
 * - aborts cleanly on unmount (an `AbortController`), so no callback fires after teardown.
 *
 * The caller drives it via {@link openSse}, which returns a `close()` handle.
 */

import { authHeaders } from "../auth/authState";

export interface SseMessage {
  readonly id: string | null;
  readonly event: string;
  readonly data: string;
}

export interface SseOptions {
  readonly onMessage: (message: SseMessage) => void;
  /** Called once per (re)connection error, before the client schedules a reconnect. */
  readonly onError?: (error: unknown) => void;
  /** Initial replay cursor when there is no prior `Last-Event-ID`. */
  readonly lastEventId?: string | null;
  readonly defaultRetryMs?: number;
  readonly maxRetryMs?: number;
  /** Injectable for tests. */
  readonly fetchImpl?: typeof fetch;
}

export interface SseHandle {
  close(): void;
}

const DEFAULT_RETRY_MS = 1_000;
const MAX_RETRY_MS = 30_000;

/** Open an authenticated, auto-reconnecting SSE stream. Returns a `close()` handle. */
export function openSse(url: string, options: SseOptions): SseHandle {
  const controller = new AbortController();
  const doFetch = options.fetchImpl ?? fetch;
  const defaultRetry = options.defaultRetryMs ?? DEFAULT_RETRY_MS;
  const maxRetry = options.maxRetryMs ?? MAX_RETRY_MS;

  let closed = false;
  let retryMs = defaultRetry;
  let lastEventId: string | null = options.lastEventId ?? null;
  // Highest numeric id already dispatched, so a replay after reconnect never re-emits an event.
  let lastNumericId = numeric(lastEventId);
  let timer: ReturnType<typeof setTimeout> | null = null;

  function scheduleReconnect() {
    if (closed) return;
    timer = setTimeout(connect, retryMs);
    // Exponential-ish backoff, capped, until a successful read resets it.
    retryMs = Math.min(retryMs * 2, maxRetry);
  }

  async function connect() {
    if (closed) return;
    const headers = new Headers({ Accept: "text/event-stream", ...authHeaders() });
    if (lastEventId !== null) headers.set("Last-Event-ID", lastEventId);
    try {
      const res = await doFetch(url, {
        method: "GET",
        headers,
        signal: controller.signal,
        cache: "no-store",
      });
      if (!res.ok || res.body === null) {
        throw new Error(`SSE HTTP ${res.status}`);
      }
      retryMs = defaultRetry; // a healthy connection resets the backoff
      await pump(res.body);
      // The server closed the stream cleanly (run ended / cap). Reconnect only if still open;
      // callers close on the terminal event, so this is a no-op in the common case.
      scheduleReconnect();
    } catch (error) {
      if (closed || controller.signal.aborted) return;
      options.onError?.(error);
      scheduleReconnect();
    }
  }

  async function pump(body: ReadableStream<Uint8Array>) {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let index: number;
        // SSE events are separated by a blank line ("\n\n"); handle CRLF too.
        while ((index = indexOfDelimiter(buffer)) !== -1) {
          const [rawEvent, rest] = splitAt(buffer, index);
          buffer = rest;
          dispatch(parseEvent(rawEvent));
        }
      }
    } finally {
      reader.releaseLock();
    }
  }

  function dispatch(message: SseMessage | null) {
    if (closed || message === null) return;
    if (message.id !== null) {
      const asNumber = numeric(message.id);
      // Drop a replayed event we already dispatched (dedupe across a reconnect).
      if (asNumber !== null && lastNumericId !== null && asNumber <= lastNumericId) {
        lastEventId = message.id;
        return;
      }
      lastEventId = message.id;
      if (asNumber !== null) lastNumericId = asNumber;
    }
    options.onMessage(message);
  }

  function parseEvent(raw: string): SseMessage | null {
    let id: string | null = null;
    let event = "message";
    const dataLines: string[] = [];
    let sawField = false;
    for (const line of raw.split("\n")) {
      const clean = line.replace(/\r$/, "");
      if (clean === "" || clean.startsWith(":")) continue; // blank or comment
      const colon = clean.indexOf(":");
      const field = colon === -1 ? clean : clean.slice(0, colon);
      let val = colon === -1 ? "" : clean.slice(colon + 1);
      if (val.startsWith(" ")) val = val.slice(1);
      sawField = true;
      switch (field) {
        case "id":
          // A NUL in an id is invalid per spec; ignore such ids.
          if (!val.includes("\0")) id = val;
          break;
        case "event":
          event = val;
          break;
        case "data":
          dataLines.push(val);
          break;
        case "retry": {
          const ms = Number.parseInt(val, 10);
          if (Number.isFinite(ms) && ms >= 0) retryMs = Math.min(ms, maxRetry);
          break;
        }
        default:
          break; // unknown field: ignore
      }
    }
    if (!sawField || dataLines.length === 0) return id !== null ? { id, event, data: "" } : null;
    return { id, event, data: dataLines.join("\n") };
  }

  void connect();

  return {
    close() {
      closed = true;
      if (timer !== null) clearTimeout(timer);
      controller.abort();
    },
  };
}

function numeric(id: string | null): number | null {
  if (id === null) return null;
  const n = Number.parseInt(id, 10);
  return Number.isFinite(n) ? n : null;
}

function indexOfDelimiter(buffer: string): number {
  const lf = buffer.indexOf("\n\n");
  const crlf = buffer.indexOf("\r\n\r\n");
  if (lf === -1) return crlf;
  if (crlf === -1) return lf;
  return Math.min(lf, crlf);
}

function splitAt(buffer: string, index: number): [string, string] {
  const isCrlf = buffer.startsWith("\r\n\r\n", index);
  const delimiterLength = isCrlf ? 4 : 2;
  return [buffer.slice(0, index), buffer.slice(index + delimiterLength)];
}
