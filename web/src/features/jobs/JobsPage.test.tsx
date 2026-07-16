import { fireEvent, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { expect, test } from "vitest";
import { server } from "../../test/setup";
import { renderWithClient } from "../../test/utils";
import { JobsPage } from "./JobsPage";
import type { Job } from "./types";

test("lists jobs, loads detail, and requests cancellation only for an active job", async () => {
  renderWithClient(<JobsPage />);

  expect(await screen.findByText("memory.consolidation")).toBeInTheDocument();
  expect(await screen.findByText("Reviewing recent events")).toBeInTheDocument();
  const cancel = screen.getByRole("button", { name: "Cancel" });
  expect(cancel).toBeEnabled();
  fireEvent.click(cancel);
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Cancel requested" })).toBeDisabled(),
  );

  fireEvent.click(screen.getAllByText("knowledge.ingest")[0]);
  expect(await screen.findByText("Embedding provider did not respond.")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Cancel" })).toBeDisabled();
});

test("shows bounded empty and retry states", async () => {
  let attempts = 0;
  server.use(
    http.get("/v1/jobs", () => {
      attempts += 1;
      return attempts === 1
        ? HttpResponse.json({ detail: "temporary" }, { status: 503 })
        : HttpResponse.json([] satisfies Job[]);
    }),
  );
  renderWithClient(<JobsPage />);
  expect(await screen.findByText("Could not load jobs.")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Retry" }));
  expect(await screen.findByText("No durable jobs have been created for this scope.")).toBeInTheDocument();
});
