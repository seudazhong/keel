import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll } from "vitest";
import {
  handlers,
  resetAgents,
  resetApprovals,
  resetConnectors,
  resetKnowledge,
  resetJobs,
  resetMemory,
  resetModel,
  resetProjects,
  resetSchedules,
} from "./handlers";

export const server = setupServer(...handlers);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => {
  cleanup();
  server.resetHandlers();
  resetAgents();
  resetApprovals();
  resetConnectors();
  resetKnowledge();
  resetJobs();
  resetMemory();
  resetModel();
  resetProjects();
  resetSchedules();
});
afterAll(() => server.close());
