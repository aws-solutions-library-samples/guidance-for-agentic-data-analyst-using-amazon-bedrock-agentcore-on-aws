import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/invocations': 'http://localhost:8080',
    },
  },
  build: { outDir: 'build' },
});
