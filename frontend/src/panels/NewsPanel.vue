<template>
  <section>
    <div class="page-head">
      <h1>新闻层</h1>
      <p>财联社快讯（秒级）· 东财全球资讯 · 个股新闻（东财）</p>
    </div>

    <div class="card">
      <div class="toolbar">
        <label class="field">数据源
          <select class="select" v-model="source">
            <option value="cls">财联社快讯</option>
            <option value="global">东财全球资讯</option>
            <option value="stock">东财个股新闻</option>
          </select>
        </label>
        <label v-if="source === 'stock'" class="field">股票代码
          <input class="input" v-model="symbol" />
        </label>
        <button class="btn" @click="run">查询</button>
        <label class="checkbox" style="margin-left:6px">
          <input type="checkbox" v-model="auto" /> 自动刷新 30s
        </label>
      </div>
      <DataTable :rows="rows" :loading="loading" :error="error" :took="took" />
    </div>
  </section>
</template>

<script setup>
import { ref, watch, onBeforeUnmount } from 'vue'
import DataTable from '../DataTable.vue'
import { api } from '../api.js'

const source = ref('cls')
const symbol = ref('000001')
const rows = ref([])
const loading = ref(false)
const error = ref('')
const took = ref(null)
const auto = ref(false)
let timer = null

async function run() {
  loading.value = true; error.value = ''; took.value = null
  const t0 = performance.now()
  try {
    let data
    if (source.value === 'stock')  data = await api('/news/stock',  { symbol: symbol.value })
    else if (source.value === 'cls') data = await api('/news/cls',    { n: 50 })
    else data = await api('/news/global', { n: 50 })
    rows.value = data || []
    took.value = Math.round(performance.now() - t0)
  } catch (e) {
    error.value = e.message || String(e)
    rows.value = []
  } finally {
    loading.value = false
  }
}

watch(auto, (on) => {
  if (timer) { clearInterval(timer); timer = null }
  if (on) timer = setInterval(run, 30000)
})

onBeforeUnmount(() => { if (timer) clearInterval(timer) })
</script>
