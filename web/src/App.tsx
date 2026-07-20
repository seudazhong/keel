import { QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";
import { RouterProvider } from "react-router-dom";
import { Button } from "./components/ui/button";
import { AuthProvider, useAuth } from "./features/auth/AuthContext";
import { SignInScreen } from "./features/auth/SignInScreen";
import { I18nProvider, useTranslation } from "./lib/i18n";
import { createQueryClient } from "./lib/queryClient";
import { router } from "./router";

export function AuthGate() {
  const auth = useAuth();
  const { needsAuth, isAuthenticated, signOut } = auth;
  const { t } = useTranslation();
  if (needsAuth || !isAuthenticated) return <SignInScreen />;
  return (
    <AuthenticatedApp
      key={auth.cacheGeneration}
      signOut={signOut}
      signOutLabel={t("auth.signOut")}
    />
  );
}

function AuthenticatedApp({
  signOut,
  signOutLabel,
}: {
  signOut: () => void;
  signOutLabel: string;
}) {
  const [queryClient] = useState(createQueryClient);
  return (
    <QueryClientProvider client={queryClient}>
      <Button className="fixed bottom-3 right-3 z-50" onClick={signOut}>
        {signOutLabel}
      </Button>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

export default function App() {
  return (
    <I18nProvider>
      <AuthProvider>
        <AuthGate />
      </AuthProvider>
    </I18nProvider>
  );
}
