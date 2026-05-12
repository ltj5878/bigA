// 统一 API 客户端：注入 VITE_API_BASE + 错误处理。
const BASE = (import.meta.env.VITE_API_BASE || 'http://localhost:8006').replace(/\/$/, '')

export async function api(path, params) {
  const qs = params ? '?' + new URLSearchParams(params) : ''
  const r = await fetch(BASE + '/api' + path + qs)
  let j = {}
  try { j = await r.json() } catch { /* ignore */ }
  if (!r.ok || j.ok === false) {
    const msg = j.error || j.detail || r.statusText || 'Request failed'
    throw new Error(msg)
  }
  return j.data
}

export const API_BASE = BASE
