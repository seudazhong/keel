import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll } from "vitest";
import {
  handlers,
  resetApprovals,
  resetConnectors,
  resetKnowledge,
  resetJobs,
  resetMemory,
  resetModel,
  resetSchedules,
} from "./handlers";

export const server = setupServer(...handlers);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => {
  cleanup();
  server.resetHandlers();
  resetApprovals();
  resetConnectors();
  resetKnowledge();
  resetJobs();
  resetMemory();
  resetModel();
  resetSchedules();
});
afterAll(() => server.close());
