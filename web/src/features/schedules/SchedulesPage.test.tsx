import { fireEvent, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { expect, test } from "vitest";
import { server } from "../../test/setup";
import { renderWithClient } from "../../test/utils";
import { SchedulesPage } from "./SchedulesPage";

test("renders the interval schedule row (agent, interval, enabled)", async () => {
  renderWithClient(<SchedulesPage />);
  expect(await screen.findByText("digest")).toBeInTheDocument();
  expect(screen.getByText("每 1 天")).toBeInTheDocument();
  expect(screen.getByText("已启用", { exact: false })).toBeInTheDocument();
});

test("暂停 flips the schedule to paused and offers 启用", async () => {
  renderWithClient(<SchedulesPage />);
  await screen.findByText("digest");
  fireEvent.click(screen.getByRole("button", { name: "暂停" }));
  await waitFor(() => expect(screen.getByText("已暂停", { exact: false })).toBeInTheDocument());
  expect(screen.getByRole("button", { name: "启用" })).toBeInTheDocument();
});

test("立即运行 triggers the run endpoint", async () => {
  let ran = false;
  server.use(
    http.post("/v1/schedules/:id/run", () => {
      ran = true;
      return HttpResponse.json({ ok: true });
    }),
  );
  renderWithClient(<SchedulesPage />);
  await screen.findByText("digest");
  fireEvent.click(screen.getByRole("button", { name: "立即运行" }));
  await waitFor(() => expect(ran).toBe(true));
});
