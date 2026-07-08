import { fireEvent, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { SessionsPage } from "./SessionsPage";

test("lists sessions and filters by the search box", async () => {
  renderWithClient(
    <MemoryRouter>
      <SessionsPage />
    </MemoryRouter>,
  );
  expect(await screen.findByText("安排明天的行程")).toBeInTheDocument();
  expect(screen.getByText("It is your scheduled morning run…")).toBeInTheDocument();
  expect(screen.getByText("6")).toBeInTheDocument(); // message count

  fireEvent.change(screen.getByPlaceholderText(/过滤会话/), { target: { value: "行程" } });
  expect(screen.getByText("安排明天的行程")).toBeInTheDocument();
  expect(screen.queryByText("It is your scheduled morning run…")).not.toBeInTheDocument();
});
