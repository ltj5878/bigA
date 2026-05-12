<template>
  <section>
    <div class="page-head">
      <h1>量化选股</h1>
      <p>
        三因子模型 (Quality + Value + Momentum) · Piotroski F-Score 排雷 · 沪深 300 / 200 日均线大盘择时 · 月调仓对比 + 回测。
      </p>
    </div>

    <!-- 子页签：选股 vs 回测 -->
    <div class="subtab-bar">
      <button class="subtab" :class="{active: tab==='picks'}"   @click="tab='picks'">📋 当前选股</button>
      <button class="subtab" :class="{active: tab==='backtest'}" @click="tab='backtest'">📊 历史回测</button>
    </div>

    <!-- ============ 选股 Tab ============ -->
    <div v-if="tab==='picks'" class="card">
      <div class="toolbar">
        <label class="field">策略
          <select class="select" v-model="strategy">
            <option value="three_factor">三因子升级版（推荐）</option>
            <option value="magic_formula">魔法公式（旧版对照）</option>
          </select>
        </label>
        <label class="field">股池
          <select class="select" v-model="scope">
            <option value="hs300_zz500">沪深 300 + 中证 500</option>
            <option value="hs300">仅沪深 300</option>
            <option value="zz500">仅中证 500</option>
            <option value="all">全部 A 股（慢）</option>
          </select>
        </label>
        <label class="field">持仓数
          <input class="input" type="number" min="5" max="100" v-model.number="topN" style="min-width:90px" />
        </label>
        <label class="field">最小市值(亿)
          <input class="input" type="number" min="0" v-model.number="minMcap" style="min-width:110px" />
        </label>
        <label v-if="strategy === 'three_factor'" class="field">F-Score 最低
          <input class="input" type="number" min="0" max="9" v-model.number="minFScore" style="min-width:90px" />
        </label>
        <label v-if="strategy === 'three_factor'" class="field">单行业上限
          <input class="input" type="number" min="0" max="25" v-model.number="maxPerIndustry" style="min-width:90px" />
        </label>
        <label v-if="strategy === 'three_factor'" class="field">情绪权重
          <input class="input" type="number" min="0" max="1" step="0.05" v-model.number="weightS" style="min-width:90px" />
        </label>
        <label class="checkbox" style="margin-left:6px">
          <input type="checkbox" v-model="excludeST" /> 排除 ST
        </label>
        <label class="checkbox">
          <input type="checkbox" v-model="useCache" /> 用缓存
        </label>
        <button class="btn" :disabled="loading" @click="runPicks">
          {{ loading ? '运行中…' : '运行策略' }}
        </button>
      </div>

      <div v-if="timing" class="timing-bar" :class="timing.position >= 1 ? 'ok' : 'warn'">
        <span class="dot"></span>
        <strong>建议仓位 {{ Math.round(timing.position * 100) }}%</strong>
        <span class="sep">·</span>
        <span>{{ timing.reason }}</span>
      </div>

      <div v-if="stats" class="result-status">
        <span class="badge accent">{{ picks.length }} 只</span>
        <span v-if="stats.universe != null">股池 {{ stats.universe }}</span>
        <span v-if="stats.cleaned != null">合格 {{ stats.cleaned }}</span>
        <span v-if="stats.elapsed_sec != null">耗时 {{ stats.elapsed_sec }} s</span>
        <span v-if="fromCache" class="badge">命中缓存</span>
        <span v-if="changes && changes.new && changes.new.length" class="badge new">+ {{ changes.new.length }} 新入</span>
        <span v-if="changes && changes.dropped && changes.dropped.length" class="badge dropped">- {{ changes.dropped.length }} 剔除</span>
        <span style="margin-left:auto;color:var(--text-mute)">{{ asOf }}</span>
      </div>

      <div v-if="loading" class="loading"><span class="spinner"></span>加载中…（首次约 8-15 分钟）</div>
      <div v-else-if="error" class="error">❌ {{ error }}</div>
      <div v-else-if="picks.length === 0" class="empty">点击"运行策略"</div>
      <div v-else class="table-wrap">
        <table>
          <thead><tr><th v-for="c in columns" :key="c">{{ c }}</th></tr></thead>
          <tbody>
            <tr v-for="(r, i) in picks" :key="i" :class="rowTag(r)">
              <td v-for="c in columns" :key="c" v-html="cell(r, c)"></td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="hint">
        <strong>四因子权重</strong>：质量 0.35 / 价值 0.35 / 动量 0.15 / 情绪 {{ weightS }}；F-Score &lt; {{ minFScore }} 直接排除；单一申万一级行业最多 {{ maxPerIndustry }} 只。
        <strong>调仓对比</strong>：本月新入用 <span style="color:var(--ok)">绿色</span> 标记。
        <br><strong>仅供研究，不构成投资建议。</strong>
      </div>
    </div>

    <!-- ============ 回测 Tab ============ -->
    <div v-else class="card">
      <div class="toolbar">
        <label class="field">回测起始
          <input class="input" type="date" v-model="bt.start" />
        </label>
        <label class="field">回测终止
          <input class="input" type="date" v-model="bt.end" />
        </label>
        <label class="field">持仓数
          <input class="input" type="number" min="5" max="100" v-model.number="bt.topN" style="min-width:90px" />
        </label>
        <label class="field">股池
          <select class="select" v-model="bt.scope">
            <option value="hs300">仅沪深 300（快）</option>
            <option value="hs300_zz500">沪深 300 + 中证 500</option>
          </select>
        </label>
        <label class="field">单行业上限
          <input class="input" type="number" min="0" max="25" v-model.number="bt.maxPerIndustry" style="min-width:90px" />
        </label>
        <label class="checkbox" style="margin-left:6px">
          <input type="checkbox" v-model="bt.useTiming" /> 启用大盘择时
        </label>
        <button class="btn" :disabled="btLoading" @click="runBacktest">
          {{ btLoading ? '回测中…' : '运行回测' }}
        </button>
      </div>

      <div v-if="btLoading" class="loading">
        <span class="spinner"></span>回测进行中…首次约 10-20 分钟（拉历史财报和估值）
      </div>
      <div v-else-if="btError" class="error">❌ {{ btError }}</div>
      <div v-else-if="btResult">
        <!-- 关键指标 4 张卡 -->
        <div class="metric-grid">
          <div class="metric"><div class="metric-label">年化收益</div>
            <div class="metric-value" :class="posCls(btResult.metrics.annual_return)">
              {{ pctSigned(btResult.metrics.annual_return) }}
            </div>
            <div class="metric-sub">基准 {{ pctSigned(btResult.metrics.bench_annual) }}</div>
          </div>
          <div class="metric"><div class="metric-label">最大回撤</div>
            <div class="metric-value neg">{{ pctSigned(btResult.metrics.max_drawdown) }}</div>
            <div class="metric-sub">期间最高跌幅</div>
          </div>
          <div class="metric"><div class="metric-label">夏普比率</div>
            <div class="metric-value" :class="posCls(btResult.metrics.sharpe)">
              {{ btResult.metrics.sharpe }}
            </div>
            <div class="metric-sub">风险调整后收益</div>
          </div>
          <div class="metric"><div class="metric-label">超额年化</div>
            <div class="metric-value" :class="posCls(btResult.metrics.excess_annual)">
              {{ pctSigned(btResult.metrics.excess_annual) }}
            </div>
            <div class="metric-sub">vs 沪深 300</div>
          </div>
        </div>

        <!-- 净值曲线 -->
        <div style="padding: 0 18px 18px;">
          <canvas ref="chartCanvas" height="320"></canvas>
        </div>

        <div class="result-status" style="border-top:1px solid var(--border)">
          <span class="badge">{{ btResult.start }} → {{ btResult.end }}</span>
          <span>调仓 {{ btResult.metrics.n_months }} 次</span>
          <span v-if="btResult.metrics.elapsed_sec != null">耗时 {{ btResult.metrics.elapsed_sec }} s</span>
        </div>
      </div>
      <div v-else class="empty">配置参数后点击"运行回测"</div>

      <div class="hint">
        回测说明：每月初按当时的财报和估值排名 Top N，等权重持有 1 个月；启用择时则跌破 200 日均线时仅 30% 仓位。
        股池采用<strong>当前</strong>成分股（存在幸存者偏差，实际收益可能略低），结果作为方向性参考。
      </div>
    </div>
  </section>
</template>

<script setup>
import { ref, computed, nextTick } from 'vue'
import { api } from '../api.js'

// ============ 选股部分 ============
const tab = ref('picks')

const strategy = ref('three_factor')
const scope = ref('hs300_zz500')
const topN = ref(25)
const minMcap = ref(30)
const minFScore = ref(5)
const maxPerIndustry = ref(3)
const weightS = ref(0.15)
const excludeST = ref(true)
const useCache = ref(true)

const picks = ref([])
const stats = ref(null)
const timing = ref(null)
const changes = ref(null)
const asOf = ref('')
const fromCache = ref(false)
const loading = ref(false)
const error = ref('')

const columns = computed(() =>
  picks.value.length ? Object.keys(picks.value[0]) : []
)

function escapeHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
                  .replace(/>/g,'&gt;').replace(/"/g,'&quot;')
}
function cell(row, col) {
  const v = row[col]
  if (v == null) return ''
  const s = String(v)
  if (/^https?:\/\//.test(s)) {
    return `<a href="${escapeHtml(s)}" target="_blank">${escapeHtml(s.slice(0,56))}</a>`
  }
  if (col === 'F-Score') {
    const n = Number(v)
    const color = n >= 7 ? 'var(--ok)' : n <= 4 ? 'var(--err)' : 'var(--text)'
    return `<span style="color:${color};font-weight:600">${escapeHtml(s)}</span>`
  }
  return escapeHtml(s.length > 100 ? s.slice(0,100) + '…' : s)
}
function rowTag(row) {
  if (!changes.value) return ''
  if ((changes.value.new || []).includes(row['代码'])) return 'row-new'
  return ''
}

async function runPicks() {
  loading.value = true
  error.value = ''
  picks.value = []
  stats.value = null
  timing.value = null
  changes.value = null

  const endpoint = strategy.value === 'three_factor'
    ? '/strategy/three_factor' : '/strategy/magic_formula'
  const params = {
    scope: scope.value, top_n: topN.value, min_mcap_yi: minMcap.value,
    exclude_st: excludeST.value, use_cache: useCache.value,
  }
  if (strategy.value === 'three_factor') {
    params.min_f_score = minFScore.value
    params.max_per_industry = maxPerIndustry.value
    params.weight_s = weightS.value
    // 三因子内部 Q/V/M 用默认 0.35/0.35/0.15
  } else {
    params.momentum_weight = 0.3
  }

  try {
    const data = await api(endpoint, params)
    picks.value = data.picks || []
    stats.value = data.stats || {}
    timing.value = data.timing || null
    changes.value = data.changes || null
    asOf.value = data.as_of || ''
    fromCache.value = !!data.from_cache
  } catch (e) {
    error.value = e.message || String(e)
  } finally {
    loading.value = false
  }
}

// ============ 回测部分 ============
const today = new Date().toISOString().slice(0, 10)
const bt = ref({
  start: '2022-01-01',
  end:   today,
  topN:  25,
  scope: 'hs300',
  useTiming: false,           // 上次验证：择时反而拖累，默认关闭
  maxPerIndustry: 3,
})
const btLoading = ref(false)
const btError = ref('')
const btResult = ref(null)
const chartCanvas = ref(null)
let chartInstance = null

function pctSigned(v) {
  if (v == null) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v}%`
}
function posCls(v) {
  if (v == null) return ''
  return v > 0 ? 'pos' : v < 0 ? 'neg' : ''
}

async function runBacktest() {
  btLoading.value = true; btError.value = ''; btResult.value = null
  try {
    const data = await api('/strategy/backtest', {
      start: bt.value.start,
      end:   bt.value.end,
      top_n: bt.value.topN,
      universe_scope: bt.value.scope,
      use_timing: bt.value.useTiming,
      max_per_industry: bt.value.maxPerIndustry,
    })
    btResult.value = data
    await nextTick()
    drawChart(data)
  } catch (e) {
    btError.value = e.message || String(e)
  } finally {
    btLoading.value = false
  }
}

function drawChart(data) {
  if (!chartCanvas.value) return
  if (chartInstance) { chartInstance.destroy(); chartInstance = null }
  const ctx = chartCanvas.value.getContext('2d')
  const labels = data.nav.map(p => p.date)
  const stratData = data.nav.map(p => p.value)
  const benchData = data.hs300_nav.map(p => p.value)
  chartInstance = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: '三因子策略',
          data: stratData,
          borderColor: '#C97B4A',
          backgroundColor: 'rgba(201,123,74,0.08)',
          borderWidth: 2,
          tension: 0.15,
          pointRadius: 0,
          fill: true,
        },
        {
          label: '沪深 300 基准',
          data: benchData,
          borderColor: '#6B6962',
          borderWidth: 1.5,
          borderDash: [4, 4],
          tension: 0.15,
          pointRadius: 0,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'top', align: 'end' },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${(ctx.parsed.y * 100 - 100).toFixed(2)}%`,
          },
        },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 12 }, grid: { display: false } },
        y: { ticks: { callback: (v) => `${((v - 1) * 100).toFixed(0)}%` }, grid: { color: '#EEEAE0' } },
      },
    },
  })
}
</script>

<style scoped>
.subtab-bar {
  display: flex; gap: 6px; margin-bottom: 14px;
}
.subtab {
  border: 1px solid var(--border-strong);
  background: var(--surface);
  color: var(--text-mute);
  padding: 7px 16px;
  border-radius: 999px;
  font-size: 13px;
  cursor: pointer;
  font-family: inherit;
}
.subtab:hover { color: var(--accent-ink); border-color: var(--accent); }
.subtab.active {
  background: var(--accent-soft);
  color: var(--accent-ink);
  border-color: var(--accent);
  font-weight: 600;
}

.timing-bar {
  display: flex; align-items: center; gap: 10px;
  padding: 12px 18px;
  border-bottom: 1px solid var(--border);
  font-size: 13px;
}
.timing-bar.ok   { background: #EDF6EE; color: #2D5A33; }
.timing-bar.warn { background: #FBEDE7; color: #7A2F1F; }
.timing-bar .dot { width: 10px; height: 10px; border-radius: 50%; background: currentColor; }
.timing-bar .sep { color: var(--text-mute); }

.row-new td { background: #F0F7F1 !important; }
.badge.new     { background: #E1F0E3; border-color: #BFD9C3; color: #2D5A33; }
.badge.dropped { background: #F0E1E1; border-color: #D9BFBF; color: #7A2F1F; }

.metric-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 14px;
  padding: 18px;
}
.metric {
  background: var(--surface-2);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 14px 16px;
}
.metric-label { font-size: 12px; color: var(--text-mute); margin-bottom: 6px; }
.metric-value { font-size: 22px; font-family: var(--font-serif); font-weight: 500; }
.metric-value.pos { color: var(--ok); }
.metric-value.neg { color: var(--err); }
.metric-sub { font-size: 11px; color: var(--text-mute); margin-top: 4px; }

@media (max-width: 900px) {
  .metric-grid { grid-template-columns: repeat(2, 1fr); }
}
</style>
