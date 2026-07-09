import { screen } from "@testing-library/react";
import { expect, test } from "vitest";
import { renderWithClient } from "../../test/utils";
import { ObservabilityPage } from "./ObservabilityPage";

test("renders scope overview stat tiles (tokens, cost, schedules)", async () => {
  renderWithClient(<ObservabilityPage />);
  expect(await screen.findByText("会话")).toBeInTheDocument();
  expect(screen.getByText("120")).toBeInTheDocument(); // total tokens: 100 prompt + 20 completion
  expect(screen.getByText("$0.0120")).toBeInTheDocument(); // cost_usd formatted
  expect(screen.getByText("2/3")).toBeInTheDocument(); // enabled/total schedules
});
