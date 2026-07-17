import { useEffect, useRef, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { isOnboardingComplete } from "../features/onboarding/useOnboarding";
import { useTranslation } from "../lib/i18n";
import { ShellContextProvider } from "./ShellContext";
import { Sidebar } from "./Sidebar";

// Keep in sync with Tailwind's default `lg` breakpoint used throughout the
// shell (`lg:grid`, `lg:hidden`, `lg:visible`, ...): 1024px and up is the
// persistent desktop layout where the sidebar is never a modal drawer.
const DESKTOP_MEDIA_QUERY = "(min-width: 1024px)";

function isDesktopViewport(): boolean {
  if (typeof window === "undefined") return false;
  if (typeof window.matchMedia === "function") {
    return window.matchMedia(DESKTOP_MEDIA_QUERY).matches;
  }
  // matchMedia isn't implemented in some test environments (jsdom); fall
  // back to a plain width check so the behavior still works there.
  return window.innerWidth >= 1024;
}

export function AppShell() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [onboardingComplete, setOnboardingComplete] = useState(() => isOnboardingComplete());
  const location = useLocation();
  const { t } = useTranslation();
  const mainRef = useRef<HTMLElement>(null);
  const menuButtonRef = useRef<HTMLElement | null>(null);
  const firstRender = useRef(true);
  // What to focus once `sidebarOpen` next settles to `false` and the DOM
  // commit that removes `inert`/`aria-hidden` from main has landed: "main"
  // after a route change, "opener" after a drawer dismissal (Escape,
  // backdrop click, close button). `null` means nothing is owed.
  const pendingFocus = useRef<"main" | "opener" | null>(null);

  // Close the mobile drawer whenever the route changes (link click, back/forward, etc.),
  // and re-check onboarding completion (it's set from the /onboarding route via a plain
  // client-side navigation, not a remount, so this state needs an explicit refresh).
  useEffect(() => {
    setSidebarOpen(false);
    setOnboardingComplete(isOnboardingComplete());
  }, [location.pathname]);

  // Force-close the mobile drawer once the viewport crosses into the desktop
  // (`lg`) breakpoint. Desktop renders the sidebar as persistent, non-modal
  // navigation, so a drawer left open from a narrower viewport must never
  // survive a resize/rotation: otherwise main stays `inert`/`aria-hidden`,
  // the aside keeps `role=dialog`/`aria-modal`, and the mobile-only close
  // button (hidden via `lg:hidden`) disappears with no way to dismiss it.
  useEffect(() => {
    if (typeof window === "undefined") return;
    function handleViewportChange() {
      if (isDesktopViewport()) {
        setSidebarOpen(false);
      }
    }
    if (typeof window.matchMedia === "function") {
      const mql = window.matchMedia(DESKTOP_MEDIA_QUERY);
      const listener = () => handleViewportChange();
      if (typeof mql.addEventListener === "function") {
        mql.addEventListener("change", listener);
        return () => mql.removeEventListener("change", listener);
      }
      // Safari < 14 fallback API.
      mql.addListener(listener);
      return () => mql.removeListener(listener);
    }
    window.addEventListener("resize", handleViewportChange);
    return () => window.removeEventListener("resize", handleViewportChange);
  }, []);

  // Mark that a route change happened (skipping the initial mount) so the
  // effect below knows a focus restoration to main is owed.
  useEffect(() => {
    if (firstRender.current) {
      firstRender.current = false;
      return;
    }
    pendingFocus.current = "main";
  }, [location.pathname]);

  // Move focus to the main landmark on navigation so keyboard/screen-reader
  // users land in new content instead of staying on a now-stale sidebar
  // link, and restore focus to the button that opened the drawer after it's
  // dismissed via Escape/backdrop/close button (see `closeSidebar`). Both
  // cases only fire once `sidebarOpen` has actually settled to `false`:
  // while the mobile drawer is still open/closing, main (and everything
  // inside it, including the opener button rendered by the page's Topbar)
  // is `inert`, and calling `.focus()` on an inert element/descendant is a
  // no-op in real browsers — it silently drops focus to <body> or leaves it
  // on an element that's about to become non-interactive, instead of
  // landing on the intended target. Depending on both `sidebarOpen` and the
  // pathname means desktop navigations (drawer never opens) focus
  // immediately, while mobile navigations/dismissals wait for the
  // drawer-close commit (and the inert removal it carries) to land first.
  useEffect(() => {
    const target = pendingFocus.current;
    if (!target || sidebarOpen) return;
    pendingFocus.current = null;
    if (target === "opener") {
      menuButtonRef.current?.focus();
    } else {
      mainRef.current?.focus();
    }
  }, [location.pathname, sidebarOpen]);

  // Closing via Escape, the backdrop, or the close button never navigates,
  // so it only needs to record the intended focus target and flip the
  // state; the effect above performs the actual `.focus()` call once the
  // close has committed and inert is removed.
  function closeSidebar() {
    pendingFocus.current = "opener";
    setSidebarOpen(false);
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
