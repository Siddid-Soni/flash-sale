import { defineConfig } from 'vitest/config';
import preact from '@preact/preset-vite';

export default defineConfig({
  // Served by NGINX at `/` in production, with immutable hashed assets.
  base: '/',
  plugins: [preact()],
  build: {
    target: 'es2022',
    assetsInlineLimit: 2048,
    cssCodeSplit: true,
    reportCompressedSize: true,
    rollupOptions: {
      output: {
        entryFileNames: 'assets/[name]-[hash].js',
        chunkFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash][extname]',
      },
    },
  },
  server: {
    port: 5173,
    strictPort: true,
    // Two Rust services, as behind the gateway: the site service (:8082) takes sessions, sale
    // details, the list, search and admin; checkout and order status stay on the Order Service
    // (:8080). The payment page is the provider's own (:8090) and is opened directly.
    proxy: {
      '^/v1/(session|admin/.*|sales(/search|/[0-9]+)?)(\\?.*)?$': { target: 'http://localhost:8082' },
      '/v1': { target: 'http://localhost:8080' },
    },
  },
  preview: {
    port: 4173,
    strictPort: true,
  },
  test: {
    include: ['src/**/*.test.ts'],
    environment: 'node',
  },
});
