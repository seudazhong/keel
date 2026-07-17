import { createContext, useContext, type ReactNode } from "react";

interface ShellContextValue {
  /** Opens the mobile sidebar drawer. No-op on layouts where the sidebar is always visible. */
  openSidebar: () => void;
}

const ShellContext = createContext<ShellContextValue>({ openSidebar: () => {} });

export function ShellContextProvider({
  value,
  children,
}: {
  value: ShellContextValue;
  children: ReactNode;
}) {
  return <ShellContext.Provider value={value}>{children}</ShellContext.Provider>;
}

export function useShell(): ShellContextValue {
  return useContext(ShellContext);
}
