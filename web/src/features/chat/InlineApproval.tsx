import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";

export function InlineApproval({
  tool,
  args,
  resolved,
  onResolve,
}: {
  tool: string;
  args: Record<string, unknown>;
  resolved?: "allow" | "deny";
  onResolve: (decision: "allow" | "deny") => void;
}) {
  const to = typeof args.to === "string" ? args.to : "";
  return (
    <Card className="self-stretch border-amber/40">
      <div className="flex items-center gap-2.5 border-b border-amber/20 bg-amber/5 px-4 py-3">
        <h4 className="text-sm font-semibold text-amber">⚠ 需要你批准：{tool}（外发/写操作）</h4>
        {resolved && (
          <Badge tone={resolved === "allow" ? "green" : "red"}>
            {resolved === "allow" ? "已批准" : "已拒绝"}
          </Badge>
        )}
      </div>
      <div className="flex flex-col gap-3 p-4">
        <Banner tone="warn">
          <span>🛡️</span>
          <div>
            外部动作需你确认——若本次运行读入了外部（邮件/网页）<b>tainted</b>{" "}
            内容，注入内容无法自动促成动作。
          </div>
        </Banner>
        {to && (
          <div className="text-xs text-text-soft">
            <b>目标</b> <span className="font-mono text-red">{to}</span>
          </div>
        )}
        <details className="text-xs text-text-muted">
          <summary className="cursor-pointer">参数</summary>
          <pre className="mt-1 overflow-x-auto rounded-sm bg-surface-2 p-2">
            {JSON.stringify(args, null, 2)}
          </pre>
        </details>
        <div className="flex gap-2">
          <Button variant="danger" disabled={!!resolved} onClick={() => onResolve("deny")}>
            拒绝
          </Button>
          <Button
            variant="primary"
            className="ml-auto"
            disabled={!!resolved}
            onClick={() => onResolve("allow")}
          >
            批准
          </Button>
        </div>
      </div>
    </Card>
  );
}
