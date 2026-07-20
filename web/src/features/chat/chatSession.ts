import { getAuthSnapshot } from "../auth/authState";

const ACTIVE_SESSION_KEY = "keel.chat.activeSession.v1";

function activeSessionKey(): string {
  const auth = getAuthSnapshot();
  const workspace = [
    auth.credential?.kind ?? "local-preview",
    auth.org ?? "personal",
    auth.agent ?? "default",
  ]
    .map(encodeURIComponent)
    .join(":");
  return `${ACTIVE_SESSION_KEY}:${workspace}`;
}

export function rememberActiveChatSession(sessionId: string): void {
  try {
    sessionStorage.setItem(activeSessionKey(), sessionId);
  } catch {
    // The route still carries the session id when tab storage is unavailable.
  }
}

export function createChatSessionId(): string {
  const sessionId = crypto.randomUUID();
  rememberActiveChatSession(sessionId);
  return sessionId;
}

export function getOrCreateActiveChatSessionId(): string {
  try {
    const existing = sessionStorage.getItem(activeSessionKey())?.trim();
    if (existing) return existing;
  } catch {
    // Fall through to a fresh route id.
  }
  return createChatSessionId();
}

export function clearRememberedChatSessions(): void {
  try {
    for (let index = sessionStorage.length - 1; index >= 0; index -= 1) {
      const key = sessionStorage.key(index);
      if (key === ACTIVE_SESSION_KEY || key?.startsWith(`${ACTIVE_SESSION_KEY}:`)) {
        sessionStorage.removeItem(key);
      }
    }
  } catch {
    // A new chat id will be created when tab storage is unavailable.
  }
}
