import { screen } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { beforeAll, expect, test, vi } from "vitest";
import { renderWithClient } from "../../test/utils";
import { ChatPage } from "./ChatPage";

beforeAll(() => {
  Element.prototype.scrollIntoView = vi.fn();
});

test("loads durable history for the route session and remembers it as the active chat", async () => {
  const router = createMemoryRouter(
    [{ path: "/chat/:sessionId", element: <ChatPage /> }],
    { initialEntries: ["/chat/s1"] },
  );
  renderWithClient(<RouterProvider router={router} />);

  expect(await screen.findByText("帮我看看明天有没有空")).toBeInTheDocument();
  expect(screen.getByText("你明天下午空闲。")).toBeInTheDocument();
  expect(
    Array.from({ length: sessionStorage.length }, (_, index) => sessionStorage.key(index)).some(
      (key) => key?.startsWith("keel.chat.activeSession.v1:"),
    ),
  ).toBe(true);
  expect(screen.getByRole("button", { name: "New chat" })).toBeInTheDocument();
});
