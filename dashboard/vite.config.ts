import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // Relative asset URLs (./assets/...) instead of root-absolute ones, so the
  // build can be served under any path prefix (see --prefix on "sparkles
  // serve") without a rebuild: the server injects a matching <base href> at
  // serve time and relative URLs resolve against that.
  base: "./",
  server: {
    proxy: { "/api": { target: "http://localhost:8080", changeOrigin: true } },
  },
});
