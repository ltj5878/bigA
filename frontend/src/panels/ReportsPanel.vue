<template>
  <section>
    <div class="page-head">
      <h1>研报层</h1>
      <p>东财 reportapi · akshare 个股研报 · iwencai 问财（需 cookie）</p>
    </div>

    <div class="card">
      <div class="toolbar">
        <label class="field">数据源
          <select class="select" v-model="source">
            <option value="eastmoney">东财 · 最新研报列表</option>
            <option value="akshare">akshare · 个股研报</option>
            <option value="iwencai">iwencai · 问句查询</option>
          </select>
        </label>
        <label v-if="source === 'akshare'" class="field">股票代码
          <input class="input" v-model="symbol" />
        </label>
        <label v-if="source === 'iwencai'" class="field">问句
          <input class="input" style="min-width:280px" v-model="query" />
        </label>
        <button class="btn" @click="run">查询</button>
      </div>
      <DataTable :rows="rows" :loading="loading" :error="error" :took="took" />
      <div class="hint">提示：iwencai 需在后端环境变量配置 <code>IWENCAI_COOKIE</code></div>
    </div>
  </section>
</template>

<script setup>
import { ref } from 'vue'
import DataTable from '../DataTable.vue'
import { api } from '../api.js'

const source = ref('eastmoney')
const symbol = ref('000001')
const query = ref('平安银行的研报')
const rows = ref([])
const loading = ref(false)
const error = ref('')
const took = ref(null)

async function run() {
  loading.value = true; error.value = ''; took.value = null
  const t0 = performance.now()
  try {
    let data
    if (source.value === 'eastmoney') data = await api('/reports/eastmoney', { page_size: 30 })
    else if (source.value === 'akshare') data = await api('/reports/akshare', { symbol: symbol.value })
    else data = await api('/reports/iwencai', { query: query.value })
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
