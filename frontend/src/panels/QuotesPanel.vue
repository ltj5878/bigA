<template>
  <section>
    <div class="page-head">
      <h1>行情层</h1>
      <p>三个数据源：mootdx（主源）· 腾讯/efinance（对账源）· 同花顺热点（问句查询）</p>
    </div>

    <div class="card">
      <div class="toolbar">
        <label class="field">数据源
          <select class="select" v-model="source">
            <option value="mootdx">mootdx · 通达信日线</option>
            <option value="tencent">腾讯/efinance · 日线</option>
            <option value="ths_hotspot">同花顺热点 · 问句查询</option>
          </select>
        </label>
        <label v-if="source !== 'ths_hotspot'" class="field">股票代码
          <input class="input" v-model="symbol" />
        </label>
        <label v-else class="field">问句
          <input class="input" style="min-width:280px" v-model="hotspotQuery" />
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

const source = ref('mootdx')
const symbol = ref('000001')
const hotspotQuery = ref('今日热点板块涨幅排行')
const rows = ref([])
const loading = ref(false)
const error = ref('')
const took = ref(null)

async function run() {
  loading.value = true; error.value = ''; took.value = null
  const t0 = performance.now()
  try {
    let data
    if (source.value === 'ths_hotspot') {
      data = await api('/quotes/ths_hotspot', { query: hotspotQuery.value })
    } else {
      data = await api(`/quotes/${source.value}`, { symbol: symbol.value })
    }
    rows.value = data || []
    took.value = Math.round(performance.now() - t0)
  } catch (e) {
    error.value = e.message || String(e)
    rows.value = []
  } finally {
    loading.value = false
  }
}

// 首屏自动加载
run()
</script>
