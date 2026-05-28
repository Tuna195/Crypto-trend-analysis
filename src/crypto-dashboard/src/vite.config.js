// vite.config.js
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    // Proxy /api/* → FastAPI :8000 để tránh CORS khi dev
    proxy: {
      "/api": { target: "http://localhost:8000", changeOrigin: true },
    },
  },
});
