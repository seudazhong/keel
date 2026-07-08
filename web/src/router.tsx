import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { ApprovalsPage } from "./features/approvals/ApprovalsPage";
import { ChatPage } from "./features/chat/ChatPage";
import { ConnectorsPage } from "./features/connectors/ConnectorsPage";
import { SessionDetailPage } from "./features/sessions/SessionDetailPage";
import { SessionsPage } from "./features/sessions/SessionsPage";

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to="/chat" replace /> },
      { path: "chat", element: <ChatPage /> },
      { path: "sessions", element: <SessionsPage /> },
      { path: "sessions/:id", element: <SessionDetailPage /> },
      { path: "connectors", element: <ConnectorsPage /> },
      { path: "approvals", element: <ApprovalsPage /> },
    ],
  },
]);
