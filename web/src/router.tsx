import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { ApprovalsPage } from "./features/approvals/ApprovalsPage";
import { ChatPage } from "./features/chat/ChatPage";
import { ConnectorsPage } from "./features/connectors/ConnectorsPage";

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to="/chat" replace /> },
      { path: "chat", element: <ChatPage /> },
      { path: "connectors", element: <ConnectorsPage /> },
      { path: "approvals", element: <ApprovalsPage /> },
    ],
  },
]);
