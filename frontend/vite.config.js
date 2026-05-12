import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// Vite 开发服务器：固定 5176 端口，避免端口冲突时自动跳号
export default defineConfig({
  plugins: [vue()],
  server: {
    host: '127.0.0.1',
    port: 5176,
    strictPort: true,
  },
})
