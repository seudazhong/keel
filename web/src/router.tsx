import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { ApprovalsPage } from "./features/approvals/ApprovalsPage";
import { ChatPage } from "./features/chat/ChatPage";
import { ConnectorsPage } from "./features/connectors/ConnectorsPage";
import { ObservabilityPage } from "./features/observability/ObservabilityPage";
import { SchedulesPage } from "./features/schedules/SchedulesPage";
import { SessionDetailPage } from "./features/sessions/SessionDetailPage";
import { SessionsPage } from "./features/sessions/SessionsPage";
import { SettingsPage } from "./features/settings/SettingsPage";

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to="/chat" replace /> },
      { path: "chat", element: <ChatPage /> },
      { path: "sessions", element: <SessionsPage /> },
      { path: "sessions/:id", element: <SessionDetailPage /> },
      { path: "connectors", element: <ConnectorsPage /> },
      { path: "schedules", element: <SchedulesPage /> },
      { path: "approvals", element: <ApprovalsPage /> },
      { path: "observability", element: <ObservabilityPage /> },
      { path: "settings", element: <SettingsPage /> },
    ],
  },
]);
