<template>
  <section>
    <div class="page-head">
      <h1>公告层</h1>
      <p>巨潮 cninfo · 全量公告 + PDF 链接</p>
    </div>

    <div class="card">
      <div class="toolbar">
        <label class="field">股票代码
          <input class="input" v-model="symbol" />
        </label>
        <label class="field">最近天数
          <input class="input" type="number" min="1" max="365" style="min-width:90px" v-model="days" />
        </label>
        <button class="btn" @click="run">查询</button>
      </div>
      <DataTable :rows="rows" :loading="loading" :error="error" :took="took" />
    </div>
  </section>
</template>

<script setup>
import { ref } from 'vue'
import DataTable from '../DataTable.vue'
import { api } from '../api.js'

const symbol = ref('000001')
const days = ref(30)
const rows = ref([])
const loading = ref(false)
const error = ref('')
const took = ref(null)

async function run() {
  loading.value = true; error.value = ''; took.value = null
  const t0 = performance.now()
  try {
    const data = await api('/announcements/cninfo', { symbol: symbol.value, days: days.value })
    rows.value = data || []
    took.value = Math.round(performance.now() - t0)
  } catch (e) {
    error.value = e.message || String(e)
    rows.value = []
  } finally {
    loading.value = false
  }
}
</script>
