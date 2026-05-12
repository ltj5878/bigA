<template>
  <div class="result">
    <div v-if="loading" class="loading"><span class="spinner"></span>加载中…</div>

    <template v-else-if="error">
      <div class="result-status"><span class="badge err">错误</span></div>
      <div class="error">{{ error }}</div>
    </template>

    <template v-else-if="rows && rows.length">
      <div class="result-status">
        <span class="badge accent">{{ rows.length }} 条</span>
        <span v-if="took != null">{{ took }} ms</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th v-for="c in columns" :key="c">{{ c }}</th></tr>
          </thead>
          <tbody>
            <tr v-for="(r, i) in rows" :key="i">
              <td v-for="c in columns" :key="c" v-html="renderCell(r[c])"></td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>

    <div v-else class="empty">无数据</div>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  rows: { type: Array, default: () => [] },
  loading: Boolean,
  error: { type: String, default: '' },
  took: { type: Number, default: null },
})

const columns = computed(() =>
  props.rows && props.rows.length ? Object.keys(props.rows[0]) : []
)

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

function renderCell(val) {
  if (val == null) return ''
  const s = String(val)
  // URL → 可点击链接
  if (/^https?:\/\//.test(s)) {
    const txt = s.length > 56 ? s.slice(0, 56) + '…' : s
    return `<a href="${escapeHtml(s)}" target="_blank" rel="noopener">${escapeHtml(txt)}</a>`
  }
  return escapeHtml(s.length > 220 ? s.slice(0, 220) + '…' : s)
}
</script>
