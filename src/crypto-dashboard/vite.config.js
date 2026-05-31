// src/crypto-dashboard/vite.config.js
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // Root của Vite là src/crypto-dashboard/
  // nên index.html và src/ đều nằm ở đây
  server: {
    port: 5173,
    proxy: {
      // /api/* → FastAPI đang chạy tại :8000
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
