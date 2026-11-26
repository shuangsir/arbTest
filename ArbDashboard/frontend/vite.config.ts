import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import path from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    host: true,
    // Fixed to 5273 so it never collides with the CTA frontend on 5173.
    port: 5273,
    strictPort: true,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8100',
        changeOrigin: true,
        configure: (proxy, _options) => {
          // Suppress ECONNREFUSED spam when backend isn't ready yet
          proxy.on('error', (err, req, res) => {
            if (err.message?.includes('ECONNREFUSED')) {
              // Send 503 ourselves — prevents Vite's internal logger from printing the error
              if (res && typeof res.writeHead === 'function' && !res.headersSent) {
                res.writeHead(503);
                res.end('Backend not ready');
              }
            } else {
              console.warn('[Vite Proxy Error]', err.message);
            }
          });
        },
      },
    },
  },
})
