import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "react-router-dom";
import { AuthProvider, useAuth } from "./features/auth/AuthContext";
import { SignInScreen } from "./features/auth/SignInScreen";
import { I18nProvider } from "./lib/i18n";
import { queryClient } from "./lib/queryClient";
import { router } from "./router";

function AuthGate() {
  const { needsAuth, isAuthenticated } = useAuth();
  // Local preview needs no credential; the sign-in screen appears only once the server has
  // rejected a request for auth (cloud mode) and no usable credential is present yet.
  if (needsAuth && !isAuthenticated) return <SignInScreen />;
  return <RouterProvider router={router} />;
}

export default function App() {
  return (
    <I18nProvider>
      <AuthProvider>
        <QueryClientProvider client={queryClient}>
          <AuthGate />
        </QueryClientProvider>
      </AuthProvider>
    </I18nProvider>
  );
}
