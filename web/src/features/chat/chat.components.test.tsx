import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ChatThread } from "./ChatThread";
import { ChatContext } from "./ChatContext";
import { Composer } from "./Composer";
import { InlineApproval } from "./InlineApproval";
import { MessageBubble } from "./MessageBubble";
import { ToolStep } from "./ToolStep";
import type { ChatItem } from "./types";

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

  it("ChatThread renders every item kind", () => {
    const items: ChatItem[] = [
      { kind: "user", id: "1", text: "hello" },
      { kind: "assistant", id: "2", text: "hi there", streaming: false },
      { kind: "tool", id: "3", callId: "c", tool: "read", args: {}, result: { ok: true, output: "res" } },
      { kind: "approval", id: "4", approvalId: "a", tool: "write", args: { path: "x" } },
      { kind: "meta", id: "5", text: "! oops", tone: "error" },
    ];
    render(<ChatThread items={items} onResolve={vi.fn()} />);
    expect(screen.getByText("hello")).toBeInTheDocument();
    expect(screen.getByText("hi there")).toBeInTheDocument();
    expect(screen.getByText("read")).toBeInTheDocument();
    expect(screen.getByText("res")).toBeInTheDocument();
    expect(screen.getByText(/需要你批准/)).toBeInTheDocument();
    expect(screen.getByText("! oops")).toBeInTheDocument();
  });

  it("ChatContext shows total tokens, cost, and tool count", () => {
    const items: ChatItem[] = [
      { kind: "tool", id: "1", callId: "c", tool: "read", args: {} },
      { kind: "user", id: "2", text: "hi" },
    ];
    render(
      <ChatContext
        items={items}
        usage={{ promptTokens: 100, completionTokens: 20, cacheReadTokens: 50, costUsd: 0.012 }}
      />,
    );
    expect(screen.getByText("120")).toBeInTheDocument(); // total tokens
    expect(screen.getByText("$0.0120")).toBeInTheDocument();
    expect(screen.getByText("50 (50%)")).toBeInTheDocument(); // cache hit
  });
});
