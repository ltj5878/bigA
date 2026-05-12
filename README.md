# Big A - A 股量化研究工作台

Big A 是一个面向 A 股研究的数据接入、策略选股和本地回测系统。项目采用前后端分离架构，后端用 FastAPI 封装多源金融数据，前端用 Vue 3 提供数据查询、策略运行和回测结果展示。

当前版本更适合本地研究、数据验证和策略原型开发，不是生产级自动交易系统。

## 功能概览

- 多源数据接入：行情、研报、新闻、基本面、公告。
- 本地缓存：远端数据落盘为 Parquet，减少重复请求。
- 定时同步：APScheduler 周期刷新新闻、热点、公告和研报 PDF。
- 策略研究：魔法公式、四因子选股、F-Score 排雷、行业中性化、大盘择时。
- 历史回测：月度调仓、净值曲线、收益指标、基准对比。
- Web UI：Vue 3 面板化展示数据模块和策略结果。

## 技术栈

| 层级 | 技术 |
| --- | --- |
| 后端 API | FastAPI, Uvicorn |
| 数据处理 | pandas, pyarrow |
| 数据源 | mootdx, efinance, akshare, pywencai, 巨潮资讯等 |
| 调度 | APScheduler |
| 前端 | Vue 3, Vite |
| 缓存 | 本地 Parquet 文件 |

## 数据模块

| 模块 | 说明 | 主要数据源 |
| --- | --- | --- |
| 行情层 | 日线行情、热点板块 | mootdx, 腾讯财经/efinance, 同花顺问财 |
| 研报层 | 最新研报、个股研报、问财研报 | 东方财富 reportapi, akshare, iwencai |
| 新闻层 | 个股新闻、财联社快讯、全球资讯 | akshare 东方财富/财联社接口 |
| 基础数据层 | 个股概况、财务指标、F10 信息 | akshare, mootdx |
| 公告层 | 上市公司公告与 PDF | 巨潮资讯 cninfo, mootdx F10 |
| 策略层 | 选股、择时、回测 | 内部策略模块 |

## 快速开始

### 1. 安装 Python 依赖

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

首次使用 mootdx 时建议选择最快的通达信服务器：

```bash
./.venv/bin/python -m mootdx bestip -v
```

### 2. 配置问财 Cookie

`pywencai` 相关接口需要 `IWENCAI_COOKIE`。如果不配置，问财研报和同花顺热点可能返回空数据。

```bash
export IWENCAI_COOKIE='从浏览器复制的 v 字段'
```

获取方式：

1. 打开 `http://www.iwencai.com/` 并登录。
2. 打开浏览器开发者工具。
3. 在 Cookie 中找到 `v` 字段。
4. 将其写入 `IWENCAI_COOKIE`。

### 3. 启动服务

```bash
./start.sh
```

首次启动时，脚本会自动在 `frontend/` 下执行 `npm install`。

启动完成后访问：

- 前端 UI：http://localhost:5176/
- 后端健康检查：http://127.0.0.1:8006/api/health
- FastAPI 文档：http://127.0.0.1:8006/docs

### 4. 常用命令

```bash
./start.sh status    # 查看 Web、Scheduler、Frontend 状态
./start.sh logs      # 实时跟随日志
./start.sh restart   # 重启所有服务
./start.sh stop      # 停止所有服务
```

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `BIG_A_HOST` | `127.0.0.1` | 后端 API 绑定地址 |
| `BIG_A_PORT` | `8006` | 后端 API 端口 |
| `BIG_A_FRONTEND_PORT` | `5176` | 前端 Vite 端口 |
| `BIG_A_CORS_ORIGINS` | `http://localhost:5176,http://127.0.0.1:5176` | 允许跨域访问的前端来源 |
| `IWENCAI_COOKIE` | 空 | 同花顺问财 Cookie |

前端 API 地址可通过 `frontend/.env` 配置：

```bash
VITE_API_BASE=http://localhost:8006
```

## 项目结构

```text
.
├── data_layer/                  # 数据接入层
│   ├── base.py                  # BaseProvider 抽象：抓取、标准化、缓存
│   ├── cache.py                 # Parquet 本地缓存
│   ├── config.py                # 缓存路径、UA、Cookie、限频等配置
│   ├── scheduler.py             # APScheduler 定时任务
│   ├── pdf_downloader.py        # 研报/公告 PDF 下载
│   ├── quotes/                  # 行情数据源
│   ├── reports/                 # 研报数据源
│   ├── news/                    # 新闻数据源
│   ├── fundamentals/            # 基础数据源
│   └── announcements/           # 公告数据源
├── strategy_layer/              # 策略与回测
│   ├── magic_formula.py         # 魔法公式选股
│   ├── three_factor.py          # 四因子选股
│   ├── factors.py               # 单股因子提取
│   ├── f_score.py               # Piotroski F-Score
│   ├── market_timing.py         # 大盘择时
│   └── backtest.py              # 月度调仓回测
├── web/
│   └── app.py                   # FastAPI API 服务
├── frontend/                    # Vue 3 + Vite 前端
│   ├── index.html
│   ├── package.json
│   └── src/
│       ├── App.vue              # 主布局和模块切换
│       ├── api.js               # fetch 封装
│       ├── DataTable.vue        # 通用表格组件
│       └── panels/              # 各数据模块面板
├── tests/
│   └── smoke_test.py            # 数据源冒烟测试
├── logs/                        # 服务日志和 PID 文件
├── requirements.txt             # Python 依赖
└── start.sh                     # 一键启停脚本
```

## 后端 API

所有业务接口都挂在 `/api` 下，返回结构统一为：

```json
{
  "ok": true,
  "data": {}
}
```

异常时返回：

```json
{
  "ok": false,
  "error": "错误信息"
}
```

### 健康检查

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 服务健康检查 |

### 行情

| 方法 | 路径 | 参数 |
| --- | --- | --- |
| GET | `/api/quotes/mootdx` | `symbol=000001` |
| GET | `/api/quotes/tencent` | `symbol=000001` |
| GET | `/api/quotes/ths_hotspot` | `query=今日热点板块涨幅排行` |

### 研报

| 方法 | 路径 | 参数 |
| --- | --- | --- |
| GET | `/api/reports/eastmoney` | `page_no=1&page_size=30&q_type=0` |
| GET | `/api/reports/akshare` | `symbol=000001` |
| GET | `/api/reports/iwencai` | `query=平安银行研报` |

### 新闻

| 方法 | 路径 | 参数 |
| --- | --- | --- |
| GET | `/api/news/stock` | `symbol=000001` |
| GET | `/api/news/cls` | `n=50` |
| GET | `/api/news/global` | `n=50` |

### 基础数据

| 方法 | 路径 | 参数 |
| --- | --- | --- |
| GET | `/api/fundamentals/info` | `symbol=000001` |
| GET | `/api/fundamentals/indicator` | `symbol=000001` |
| GET | `/api/fundamentals/abstract` | `symbol=000001` |

### 公告

| 方法 | 路径 | 参数 |
| --- | --- | --- |
| GET | `/api/announcements/cninfo` | `symbol=000001&days=30` |

### 策略

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/strategy/magic_formula` | 魔法公式选股 |
| GET | `/api/strategy/three_factor` | 四因子选股 |
| GET | `/api/strategy/backtest` | 月度调仓回测 |

示例：

```bash
curl --noproxy '*' 'http://127.0.0.1:8006/api/strategy/three_factor?scope=hs300_zz500&top_n=25&use_cache=true'
```

## 策略说明

### 魔法公式

旧版对照策略，核心思路为：

- 价值：`1 / PE`
- 质量：`ROE`
- 动量加分：默认使用短期动量权重
- 过滤：市值、ST、异常估值

### 四因子选股

当前主策略由四类因子组成：

- Quality：ROE、毛利率、经营现金流/营收。
- Value：`1 / PE`、`1 / PB`、`1 / PS`。
- Momentum：12-1 动量。
- Sentiment：近 30 天研报评级情绪。

同时叠加：

- Piotroski F-Score 排雷。
- 最小市值过滤。
- ST 过滤。
- 单行业持仓上限。
- 沪深 300 / 200 日均线择时建议。

### 回测

回测模块采用月度调仓：

- 每月初按当时可见财报和估值数据选股。
- 等权持有至下一个调仓期。
- 可选择启用大盘择时。
- 输出策略净值、沪深 300 基准净值和关键指标。

注意：当前回测使用当前股池回看历史，存在幸存者偏差；不考虑滑点、佣金、停牌和真实成交约束。

## 缓存和文件输出

默认数据目录为：

```text
~/.big_a/
├── cache/       # Parquet 缓存
├── pdfs/        # 研报和公告 PDF
└── jobs.sqlite  # 预留任务数据库路径
```

缓存路径定义在 `data_layer/config.py`。如果外部数据源临时不可用，系统会优先使用仍在有效期内的缓存。

## 定时任务

调度器入口为 `data_layer/scheduler.py`，由 `./start.sh` 自动启动。

默认任务：

| 任务 | 频率 |
| --- | --- |
| 财联社快讯 | 每 60 秒 |
| 东财全球资讯 | 每 2 分钟 |
| 个股新闻自选股轮询 | 每 5 分钟 |
| 同花顺热点 | 每 10 分钟 |
| 收盘后全量同步 | 每天 17:00 |

自选股清单在 `data_layer/config.py` 的 `watchlist` 中配置。

## 测试和验证

运行后端数据源冒烟测试：

```bash
./.venv/bin/python -m tests.smoke_test
```

运行 Python 静态编译检查：

```bash
./.venv/bin/python -m compileall -q data_layer strategy_layer web tests
```

检查服务健康状态：

```bash
curl --noproxy '*' http://127.0.0.1:8006/api/health
```

如果本机设置了 HTTP 代理，访问本地服务时建议加 `--noproxy '*'`，避免请求被代理转发。

## 常见问题

### 问财或同花顺热点返回空

通常是 `IWENCAI_COOKIE` 未配置或已过期。重新登录问财并更新 Cookie。

### AkShare 接口报 JSON 解析失败

AkShare 的部分数据源依赖第三方网页接口，可能因为限流、接口变更或网络环境导致失败。建议先查看缓存和日志，再尝试稍后重试。

### 回测首次运行很慢

首次回测会拉取大量历史价格、估值和财务数据。缓存生成后，后续运行会明显变快。

### 端口被占用

默认端口：

- 后端：`8006`
- 前端：`5176`

可以通过环境变量修改：

```bash
BIG_A_PORT=8010 BIG_A_FRONTEND_PORT=5180 ./start.sh
```

## 已知限制

- 当前是本地研究系统，不包含交易下单、账户、风控执行和权限管理。
- 多数接口依赖第三方免费数据源，稳定性不保证。
- 回测未复原历史指数成分股，存在幸存者偏差。
- 回测未计入佣金、滑点、停牌、涨跌停、成交量约束等交易细节。
- 长耗时策略接口目前由 FastAPI 请求同步执行，后续适合改造成异步任务。

## 投资声明

本项目仅用于数据研究和策略原型验证，不构成任何投资建议。任何基于本项目结果做出的交易决策，风险均由使用者自行承担。
