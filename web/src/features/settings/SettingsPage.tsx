import { useState } from "react";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Chip } from "../../components/ui/chip";
import { Skeleton } from "../../components/ui/skeleton";
import { useModel, useSetModel } from "./useModel";

const OTHER_PROVIDERS = ["OpenAI", "Anthropic", "Gemini", "本地 Ollama", "litellm 代理"];

export function SettingsPage() {
  const { data, isLoading } = useModel();
  const setModel = useSetModel();
  const [choice, setChoice] = useState("");
  const selected = choice || data?.current || "";

  return (
    <>
      <Topbar title="Settings" sub="· 模型与提供方" />
      <div className="w-full max-w-[700px] p-[22px]">
        {isLoading && <Skeleton className="h-40" />}

        {data && (
          <Card className="mb-4">
            <div className="flex items-center gap-2.5 border-b border-border px-4 py-3.5">
              <h3 className="text-sm font-semibold">🔌 GitHub Copilot</h3>
              <Badge tone="green">● 已登录</Badge>
            </div>
            <div className="flex flex-col gap-3 p-4">
              <p className="text-xs text-text-muted">
                设备码登录已完成，token 已缓存。Responses-only 模型（如 gpt-5.3-codex）现已可用。
              </p>
              <div>
                <div className="mb-1 text-xs font-semibold text-text-soft">默认模型</div>
                <div className="flex gap-2">
                  <select
                    aria-label="默认模型"
                    className="flex-1 rounded-sm border border-border bg-surface-2 px-3 py-2 text-sm outline-none focus:border-accent"
                    value={selected}
                    onChange={(e) => setChoice(e.target.value)}
                  >
                    {data.available.map((m) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))}
                  </select>
                  <Button
                    variant="primary"
                    disabled={setModel.isPending || selected === data.current}
                    onClick={() => setModel.mutate(selected)}
                  >
                    保存
                  </Button>
                </div>
                {setModel.isSuccess && (
                  <p className="mt-2 text-xs text-green">已切换到 {data.current}</p>
                )}
              </div>
            </div>
          </Card>
        )}

        <Banner tone="info" className="mb-4">
          <span>🔑</span>
          <div>其他提供方通过环境变量配置（API key / base）。浏览器内配置即将上线。</div>
        </Banner>

        <div className="flex flex-wrap gap-2">
          {OTHER_PROVIDERS.map((p) => (
            <Chip key={p}>
              {p} <span className="ml-1 text-text-muted">未配置</span>
            </Chip>
          ))}
        </div>
      </div>
    </>
  );
}
