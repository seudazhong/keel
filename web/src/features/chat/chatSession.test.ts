import { beforeEach, expect, test } from "vitest";
import { emptyAuth, setAuthSnapshot } from "../auth/authState";
import {
  clearRememberedChatSessions,
  getOrCreateActiveChatSessionId,
  rememberActiveChatSession,
} from "./chatSession";

beforeEach(() => {
  sessionStorage.clear();
  setAuthSnapshot(emptyAuth);
});

test("active chat ids are isolated by workspace selection", () => {
  setAuthSnapshot({ ...emptyAuth, org: "org-a", agent: "agent-a" });
  rememberActiveChatSession("session-a");
  expect(getOrCreateActiveChatSessionId()).toBe("session-a");

  setAuthSnapshot({ ...emptyAuth, org: "org-b", agent: "agent-b" });
  expect(getOrCreateActiveChatSessionId()).not.toBe("session-a");
});

test("clearing auth state removes every remembered workspace chat", () => {
  rememberActiveChatSession("session-a");
  clearRememberedChatSessions();
  expect(
    Array.from({ length: sessionStorage.length }, (_, index) => sessionStorage.key(index)).filter(
      (key) => key?.startsWith("keel.chat.activeSession.v1"),
    ),
  ).toEqual([]);
});
