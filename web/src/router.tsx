import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { ApprovalsPage } from "./features/approvals/ApprovalsPage";
import { ChatPage } from "./features/chat/ChatPage";

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to="/chat" replace /> },
      { path: "chat", element: <ChatPage /> },
      { path: "approvals", element: <ApprovalsPage /> },
    ],
  },
]);
