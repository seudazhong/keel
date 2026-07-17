import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { ApprovalsPage } from "./features/approvals/ApprovalsPage";
import { AgentsPage } from "./features/agents/AgentsPage";
import { ChatPage } from "./features/chat/ChatPage";
import { ConnectorsPage } from "./features/connectors/ConnectorsPage";
import { KnowledgePage } from "./features/knowledge/KnowledgePage";
import { JobsPage } from "./features/jobs/JobsPage";
import { MemoryPage } from "./features/memory/MemoryPage";
import { ObservabilityPage } from "./features/observability/ObservabilityPage";
import { OnboardingPage } from "./features/onboarding/OnboardingPage";
import { ProjectDetailPage } from "./features/projects/ProjectDetailPage";
import { ProjectsPage } from "./features/projects/ProjectsPage";
import { SchedulesPage } from "./features/schedules/SchedulesPage";
import { SessionDetailPage } from "./features/sessions/SessionDetailPage";
import { SessionsPage } from "./features/sessions/SessionsPage";
import { SettingsPage } from "./features/settings/SettingsPage";

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      // "/" always lands on Chat; onboarding is discoverable (not forced) via the
      // sidebar prompt and the /onboarding route so deep links keep working.
      { index: true, element: <Navigate to="/chat" replace /> },
      { path: "onboarding", element: <OnboardingPage /> },
      { path: "chat", element: <ChatPage /> },
      { path: "sessions", element: <SessionsPage /> },
      { path: "sessions/:id", element: <SessionDetailPage /> },
      { path: "agents", element: <AgentsPage /> },
      { path: "projects", element: <ProjectsPage /> },
      { path: "projects/:id", element: <ProjectDetailPage /> },
      { path: "connectors", element: <ConnectorsPage /> },
      { path: "knowledge", element: <KnowledgePage /> },
      { path: "memory", element: <MemoryPage /> },
      { path: "jobs", element: <JobsPage /> },
      { path: "schedules", element: <SchedulesPage /> },
      { path: "approvals", element: <ApprovalsPage /> },
      { path: "observability", element: <ObservabilityPage /> },
      { path: "settings", element: <SettingsPage /> },
    ],
  },
]);
