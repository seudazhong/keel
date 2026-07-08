import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { ApprovalsPage } from "./features/approvals/ApprovalsPage";

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to="/approvals" replace /> },
      { path: "approvals", element: <ApprovalsPage /> },
    ],
  },
]);
