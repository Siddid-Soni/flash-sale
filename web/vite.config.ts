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
    // The Rust API and the fake payment page run on :8080.
    proxy: {
      '/v1': { target: 'http://localhost:8080' },
      '/fake-pay': { target: 'http://localhost:8080' },
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
