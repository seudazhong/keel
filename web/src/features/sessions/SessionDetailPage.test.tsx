import { screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { SessionDetailPage } from "./SessionDetailPage";

test("replays a session's history as a thread", async () => {
  renderWithClient(
    <MemoryRouter initialEntries={["/sessions/s1"]}>
      <Routes>
        <Route path="/sessions/:id" element={<SessionDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
  expect(await screen.findByText("帮我看看明天有没有空")).toBeInTheDocument(); // user
  expect(screen.getByText("你明天下午空闲。")).toBeInTheDocument(); // assistant
  expect(screen.getByText("calendar_list")).toBeInTheDocument(); // tool step
  expect(screen.getByRole("link", { name: "Continue conversation" })).toHaveAttribute(
    "href",
    "/chat/s1",
  );
});
