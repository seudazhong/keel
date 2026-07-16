import { useState } from "react";
import { Topbar } from "../../components/Topbar";
import { Badge } from "../../components/ui/badge";
import { Banner } from "../../components/ui/banner";
import { Button } from "../../components/ui/button";
import { Card } from "../../components/ui/card";
import { Skeleton } from "../../components/ui/skeleton";
import { useModel, useSetModel } from "./useModel";

export function SettingsPage() {
  const { data, isLoading, isError, refetch } = useModel();
  const setModel = useSetModel();
  const [choice, setChoice] = useState("");
  const selected = choice || data?.current || "";

  return (
    <>
      <Topbar title="Settings" sub="· 模型与提供方" />
      <div className="w-full max-w-[700px] p-[22px]">
        {isLoading && <Skeleton className="h-40" />}

        {isError && (
          <Banner tone="danger" className="mb-4">
            <div>
              无法读取模型配置。页面不会推断登录状态或可用提供方。
              <button className="ml-2 underline" onClick={() => void refetch()}>
                重试
              </button>
            </div>
          </Banner>
        )}

        {data && (
          <Card className="mb-4">
            <div className="flex items-center gap-2.5 border-b border-border px-4 py-3.5">
              <h3 className="text-sm font-semibold">🔌 API 模型配置</h3>
              <Badge tone="green">API 返回 {data.available.length} 个模型选项</Badge>
            </div>
            <div className="flex flex-col gap-3 p-4">
              <p className="text-xs text-text-muted">
                以下模型由设置 API 返回。此响应不包含认证状态，因此本页不声明任何提供方已登录。
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
                  <p className="mt-2 text-xs text-green">
                    已切换到 {setModel.data?.current ?? selected}
                  </p>
                )}
                {setModel.isError && (
                  <p className="mt-2 text-xs text-red">切换失败：{setModel.error.message}</p>
                )}
              </div>
            </div>
          </Card>
        )}

        <Banner tone="info" className="mb-4">
          <span>🔑</span>
          <div>
            提供方认证与环境变量配置不在当前 API 响应中；请以服务端配置和实际返回的模型列表为准。
          </div>
        </Banner>
      </div>
    </>
  );
}
