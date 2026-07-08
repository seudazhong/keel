import { render, screen } from "@testing-library/react";
import { expect, test } from "vitest";
import { Badge } from "./badge";
import { Button } from "./button";

test("badge renders its tone color class + text", () => {
  render(<Badge tone="red">高风险</Badge>);
  expect(screen.getByText("高风险").className).toContain("text-red");
});

test("primary button carries the accent background + label", () => {
  render(<Button variant="primary">批准</Button>);
  expect(screen.getByRole("button", { name: "批准" }).className).toContain("bg-accent");
});
