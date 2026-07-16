import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: false,
    setupFiles: ["./src/test/setup.ts"],
    // Playwright owns ./e2e (browser smoke); keep it out of the Vitest run.
    exclude: ["node_modules/**", "dist/**", "e2e/**"],
  },
});
