import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    // Proxy NATS monitoring through Vite so the dashboard can read
    // stream stats without a CORS preflight.
    proxy: {
      '/nats': {
        target: 'http://127.0.0.1:8222',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/nats/, ''),
      },
      '/publisher': {
        target: 'http://127.0.0.1:9090',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/publisher/, ''),
      },
      '/os': {
        target: 'http://127.0.0.1:9200',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/os/, ''),
      },
      '/admin': {
        // Engine's admin HTTP server (VS_ADMIN_LISTEN). Used by the
        // dashboard's "Full sync" button to trigger /admin/resync.
        target: 'http://127.0.0.1:4042',
        changeOrigin: true,
      },
    },
  },
});
