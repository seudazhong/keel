import { Topbar } from "../../components/Topbar";

export function ApprovalsPage() {
  return (
    <>
      <Topbar title="Approvals" sub="· 无人值守运行的挂起审批（G5，超时 fail-closed）" />
      <div className="w-full max-w-[900px] p-[22px]">
        <p className="text-text-muted">审批队列即将在这里显示。</p>
      </div>
    </>
  );
}
