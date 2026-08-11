import path from "path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    exclude: ["**/node_modules/**", "**/dist/**", "**/.next/**", "e2e/**"],
    setupFiles: ["./vitest.setup.ts"],
    globals: false,
    // Server-side code (stores, config, route handlers) needs real Node
    // builtins; the React component/hook tests opt into jsdom per-file via
    // an `@vitest-environment jsdom` docblock.
    environment: "node",
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
});
