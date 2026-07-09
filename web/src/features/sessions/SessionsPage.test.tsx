import { fireEvent, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { SessionsPage } from "./SessionsPage";

const tick = (ms: number) => new Promise((r) => setTimeout(r, ms));

test("lists sessions, then narrows via server-side search", async () => {
  renderWithClient(
    <MemoryRouter>
      <SessionsPage />
    </MemoryRouter>,
  );
  await tick(1200); // let the (cold-start) list query resolve
  expect(screen.getByText("安排明天的行程")).toBeInTheDocument();
  expect(screen.getByText("It is your scheduled morning run…")).toBeInTheDocument();

  fireEvent.change(screen.getByPlaceholderText(/混合检索/), { target: { value: "行程" } });
  await tick(700); // 250ms debounce + the search query
  expect(screen.queryByText("It is your scheduled morning run…")).not.toBeInTheDocument();
  expect(screen.getByText("安排明天的行程")).toBeInTheDocument();
});
