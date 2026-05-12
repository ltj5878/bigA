<template>
  <div class="app">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">A</div>
        <div>
          <div class="brand-name">Big A</div>
          <div class="brand-sub">量化数据接入层</div>
        </div>
      </div>

      <div class="nav-section-label">数据模块</div>
      <nav class="nav">
        <button
          v-for="m in modules"
          :key="m.key"
          class="nav-item"
          :class="{ active: current === m.key }"
          @click="current = m.key"
        >
          <span class="nav-icon">{{ m.icon }}</span><span>{{ m.label }}</span>
        </button>
      </nav>

      <div class="sidebar-foot">
        <span>
          <span class="health-dot" :class="healthClass"></span>{{ healthText }}
        </span>
        <span>v0.2</span>
      </div>
    </aside>

    <main>
      <KeepAlive>
        <component :is="currentComponent" />
      </KeepAlive>
    </main>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import QuotesPanel from './panels/QuotesPanel.vue'
import ReportsPanel from './panels/ReportsPanel.vue'
import NewsPanel from './panels/NewsPanel.vue'
import FundamentalsPanel from './panels/FundamentalsPanel.vue'
import AnnouncementsPanel from './panels/AnnouncementsPanel.vue'
import StrategyPanel from './panels/StrategyPanel.vue'
import { api } from './api.js'

const modules = [
  { key: 'quotes',        label: '行情层',     icon: '📈', comp: QuotesPanel },
  { key: 'reports',       label: '研报层',     icon: '📑', comp: ReportsPanel },
  { key: 'news',          label: '新闻层',     icon: '📰', comp: NewsPanel },
  { key: 'fundamentals',  label: '基础数据',   icon: '📊', comp: FundamentalsPanel },
  { key: 'announcements', label: '公告层',     icon: '📄', comp: AnnouncementsPanel },
  { key: 'strategy',      label: '选股 · 魔法公式', icon: '✨', comp: StrategyPanel },
]

const current = ref('quotes')
const currentComponent = computed(
  () => modules.find(m => m.key === current.value)?.comp
)

// 健康检查
const healthOk = ref(null)
let healthTimer = null
const healthClass = computed(() =>
  healthOk.value === null ? '' : healthOk.value ? 'ok' : 'err'
)
const healthText = computed(() =>
  healthOk.value === null ? '检测中' : healthOk.value ? '服务在线' : '服务离线'
)
async function check() {
  try { await api('/health'); healthOk.value = true }
  catch { healthOk.value = false }
}
onMounted(() => { check(); healthTimer = setInterval(check, 15000) })
onBeforeUnmount(() => { if (healthTimer) clearInterval(healthTimer) })
</script>
