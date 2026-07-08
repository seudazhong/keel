import { fireEvent, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { expect, test } from "vitest";
import { server } from "../../test/setup";
import { renderWithClient } from "../../test/utils";
import { ApprovalsPage } from "./ApprovalsPage";

test("renders a pending approval with tool, target, and taint banner", async () => {
  renderWithClient(<ApprovalsPage />);
  await waitFor(() => expect(screen.getByText(/外发动作/)).toBeInTheDocument());
  expect(screen.getByText("zhangwei@example.com")).toBeInTheDocument();
  expect(screen.getByText(/Confused-deputy/)).toBeInTheDocument();
});

test("empty API shows the empty state", async () => {
  server.use(http.get("/v1/approvals", () => HttpResponse.json([])));
  renderWithClient(<ApprovalsPage />);
  await waitFor(() => expect(screen.getByText("没有待处理的审批。")).toBeInTheDocument());
});

test("a failing API shows the error banner", async () => {
  server.use(http.get("/v1/approvals", () => new HttpResponse(null, { status: 500 })));
  renderWithClient(<ApprovalsPage />);
  await waitFor(() => expect(screen.getByText(/加载审批失败/)).toBeInTheDocument());
});

test("clicking 批准 removes the card", async () => {
  renderWithClient(<ApprovalsPage />);
  await waitFor(() => expect(screen.getByText(/外发动作/)).toBeInTheDocument());
  fireEvent.click(screen.getByRole("button", { name: "批准" }));
  await waitFor(() => expect(screen.queryByText(/外发动作/)).not.toBeInTheDocument());
});
