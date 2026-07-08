import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Composer } from "./Composer";
import { InlineApproval } from "./InlineApproval";
import { MessageBubble } from "./MessageBubble";
import { ToolStep } from "./ToolStep";

describe("chat components", () => {
  it("MessageBubble renders its text", () => {
    render(<MessageBubble role="assistant" text="hello there" />);
    expect(screen.getByText("hello there")).toBeInTheDocument();
  });

  it("ToolStep shows the tool name and its result output", () => {
    render(<ToolStep tool="read" args={{ path: "a" }} result={{ ok: true, output: "data" }} />);
    expect(screen.getByText("read")).toBeInTheDocument();
    expect(screen.getByText("data")).toBeInTheDocument();
  });

  it("InlineApproval resolves on 批准 and disables when resolved", () => {
    const onResolve = vi.fn();
    const { rerender } = render(
      <InlineApproval tool="write" args={{ path: "x" }} onResolve={onResolve} />,
    );
    fireEvent.click(screen.getByText("批准"));
    expect(onResolve).toHaveBeenCalledWith("allow");
    rerender(
      <InlineApproval tool="write" args={{ path: "x" }} resolved="allow" onResolve={onResolve} />,
    );
    expect(screen.getByText("批准")).toBeDisabled();
  });

  it("Composer sends trimmed text on Enter, then ignores empty and disabled", () => {
    const onSend = vi.fn();
    const { rerender } = render(<Composer disabled={false} onSend={onSend} />);
    const input = screen.getByRole("textbox");
    fireEvent.change(input, { target: { value: "  hi  " } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onSend).toHaveBeenCalledWith("hi");
    expect((input as HTMLInputElement).value).toBe("");

    fireEvent.keyDown(input, { key: "Enter" }); // empty -> no-op
    expect(onSend).toHaveBeenCalledTimes(1);

    rerender(<Composer disabled onSend={onSend} />);
    const disabledInput = screen.getByRole("textbox");
    fireEvent.change(disabledInput, { target: { value: "x" } });
    fireEvent.keyDown(disabledInput, { key: "Enter" }); // disabled -> no-op
    expect(onSend).toHaveBeenCalledTimes(1);
  });
});
