<template>
  <section>
    <div class="page-head">
      <h1>基础数据层</h1>
      <p>akshare 个股概况 / 财务指标 / 财务摘要</p>
    </div>

    <div class="card">
      <div class="toolbar">
        <label class="field">数据类型
          <select class="select" v-model="source">
            <option value="info">个股概况</option>
            <option value="indicator">财务指标</option>
            <option value="abstract">财务摘要</option>
          </select>
        </label>
        <label class="field">股票代码
          <input class="input" v-model="symbol" />
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

const source = ref('info')
const symbol = ref('000001')
const rows = ref([])
const loading = ref(false)
const error = ref('')
const took = ref(null)

async function run() {
  loading.value = true; error.value = ''; took.value = null
  const t0 = performance.now()
  try {
    const data = await api(`/fundamentals/${source.value}`, { symbol: symbol.value })
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
