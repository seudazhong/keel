import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "react-router-dom";
import { Button } from "./components/ui/button";
import { AuthProvider, useAuth } from "./features/auth/AuthContext";
import { SignInScreen } from "./features/auth/SignInScreen";
import { I18nProvider, useTranslation } from "./lib/i18n";
import { queryClient } from "./lib/queryClient";
import { router } from "./router";

export function AuthGate() {
  const { needsAuth, isAuthenticated, signOut } = useAuth();
  const { t } = useTranslation();
  if (needsAuth || !isAuthenticated) return <SignInScreen />;
  return (
    <>
      <Button className="fixed bottom-3 right-3 z-50" onClick={signOut}>
        {t("auth.signOut")}
      </Button>
      <RouterProvider router={router} />
    </>
  );
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
