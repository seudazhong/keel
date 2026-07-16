import { useEffect, useRef, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { isOnboardingComplete } from "../features/onboarding/useOnboarding";
import { useTranslation } from "../lib/i18n";
import { ShellContextProvider } from "./ShellContext";
import { Sidebar } from "./Sidebar";

export function AppShell() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [onboardingComplete, setOnboardingComplete] = useState(() => isOnboardingComplete());
  const location = useLocation();
  const { t } = useTranslation();
  const mainRef = useRef<HTMLElement>(null);
  const menuButtonRef = useRef<HTMLElement | null>(null);
  const firstRender = useRef(true);

  // Close the mobile drawer whenever the route changes (link click, back/forward, etc.),
  // and re-check onboarding completion (it's set from the /onboarding route via a plain
  // client-side navigation, not a remount, so this state needs an explicit refresh).
  useEffect(() => {
    setSidebarOpen(false);
    setOnboardingComplete(isOnboardingComplete());
  }, [location.pathname]);

  // Move focus to the main landmark on navigation so keyboard/screen-reader users
  // land in new content instead of staying on a now-stale sidebar link.
  useEffect(() => {
    if (firstRender.current) {
      firstRender.current = false;
      return;
    }
    mainRef.current?.focus();
  }, [location.pathname]);

  function closeSidebar() {
    setSidebarOpen(false);
    menuButtonRef.current?.focus();
  }

  return (
    <ShellContextProvider
      value={{
        openSidebar: () => {
          menuButtonRef.current = document.activeElement as HTMLElement | null;
          setSidebarOpen(true);
        },
      }}
    >
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:fixed focus:left-3 focus:top-3 focus:z-50 focus:rounded-sm focus:bg-accent focus:px-3 focus:py-2 focus:text-sm focus:font-semibold focus:text-white"
      >
        {t("common.skipToContent")}
      </a>
      <div className="min-h-screen lg:grid lg:grid-cols-[240px_1fr]">
        <Sidebar isOpen={sidebarOpen} onClose={closeSidebar} onboardingComplete={onboardingComplete} />
        <main
          id="main-content"
          ref={mainRef}
          tabIndex={-1}
          // While the mobile drawer is open it behaves as a modal dialog, so
          // the rest of the page must be inert/hidden from assistive tech and
          // unfocusable until the drawer closes (state flips back automatically).
          inert={sidebarOpen ? true : undefined}
          aria-hidden={sidebarOpen ? "true" : undefined}
          className="flex min-w-0 flex-col focus:outline-none"
        >
          <Outlet />
        </main>
      </div>
    </ShellContextProvider>
  );
}
