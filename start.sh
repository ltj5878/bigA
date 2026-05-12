#!/usr/bin/env bash
# Big A 数据接入层一键启停（前后端分离架构）
#
# 用法：
#   ./start.sh           启动全部：后端 API + 调度器 + Vue 前端
#   ./start.sh stop      停止全部
#   ./start.sh restart   重启
#   ./start.sh status    查看状态
#   ./start.sh logs      实时跟随日志（三份）
#
# 环境变量：
#   BIG_A_HOST          后端绑定 IP，默认 127.0.0.1
#   BIG_A_PORT          后端端口，默认 8006
#   BIG_A_FRONTEND_PORT 前端端口，默认 5176

set -u
cd "$(dirname "$0")"

# ---------- 配置 ----------
HOST="${BIG_A_HOST:-127.0.0.1}"
PORT="${BIG_A_PORT:-8006}"
FRONTEND_PORT="${BIG_A_FRONTEND_PORT:-5176}"

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

WEB_PID="$LOG_DIR/web.pid"
SCH_PID="$LOG_DIR/scheduler.pid"
FE_PID="$LOG_DIR/frontend.pid"
WEB_LOG="$LOG_DIR/web.log"
SCH_LOG="$LOG_DIR/scheduler.log"
FE_LOG="$LOG_DIR/frontend.log"

# Python 解释器：优先 .venv
if [ -x "./.venv/bin/python" ]; then
  PY="./.venv/bin/python"
elif [ -x "./venv/bin/python" ]; then
  PY="./venv/bin/python"
else
  PY="$(command -v python3 || command -v python || true)"
fi
[ -n "$PY" ] || { echo "❌ 没找到 python 可执行文件"; exit 1; }

NPM="$(command -v npm || true)"

# ---------- 通用 ----------
is_running() {
  local pidfile="$1"
  [ -f "$pidfile" ] || return 1
  local pid
  pid="$(cat "$pidfile" 2>/dev/null)"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

stop_one() {
  local name="$1" pidfile="$2"
  if is_running "$pidfile"; then
    local pid; pid="$(cat "$pidfile")"
    echo "⏹  停止 $name (PID $pid)..."
    # 先 SIGTERM 主进程，再清理它的子进程（vite 会拉起 esbuild service）
    local kids; kids="$(pgrep -P "$pid" 2>/dev/null | tr '\n' ' ')"
    kill "$pid" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "   强制 kill -9"
      kill -9 "$pid" 2>/dev/null || true
    fi
    # 清扫遗留子进程
    for k in $kids; do kill -9 "$k" 2>/dev/null || true; done
    rm -f "$pidfile"
    echo "   ✅ $name 已停止"
  else
    echo "ℹ  $name 未在运行"
    rm -f "$pidfile"
  fi
}

# ---------- 启动 ----------
start_web() {
  if is_running "$WEB_PID"; then
    echo "ℹ  Web 已在运行 (PID $(cat "$WEB_PID"))，跳过"
    return
  fi
  echo "▶  启动 Web API (uvicorn, http://$HOST:$PORT) ..."
  nohup "$PY" -m uvicorn web.app:app --host "$HOST" --port "$PORT" \
    >> "$WEB_LOG" 2>&1 &
  echo $! > "$WEB_PID"
  sleep 1
  if is_running "$WEB_PID"; then
    echo "   ✅ Web 已启动 (PID $(cat "$WEB_PID"))，日志: $WEB_LOG"
  else
    echo "   ❌ Web 启动失败，请看 $WEB_LOG"; return 1
  fi
}

start_scheduler() {
  if is_running "$SCH_PID"; then
    echo "ℹ  Scheduler 已在运行 (PID $(cat "$SCH_PID"))，跳过"
    return
  fi
  echo "▶  启动 Scheduler (APScheduler) ..."
  nohup "$PY" -m data_layer.scheduler \
    >> "$SCH_LOG" 2>&1 &
  echo $! > "$SCH_PID"
  sleep 1
  if is_running "$SCH_PID"; then
    echo "   ✅ Scheduler 已启动 (PID $(cat "$SCH_PID"))，日志: $SCH_LOG"
  else
    echo "   ❌ Scheduler 启动失败，请看 $SCH_LOG"; return 1
  fi
}

start_frontend() {
  if is_running "$FE_PID"; then
    echo "ℹ  Frontend 已在运行 (PID $(cat "$FE_PID"))，跳过"
    return
  fi
  [ -n "$NPM" ] || { echo "❌ 没找到 npm，请先安装 Node.js"; return 1; }
  if [ ! -d "frontend/node_modules" ]; then
    echo "▶  首次启动：在 frontend/ 跑 npm install（可能要 1-3 分钟）..."
    ( cd frontend && "$NPM" install ) >> "$FE_LOG" 2>&1
    [ -d "frontend/node_modules" ] || { echo "   ❌ npm install 失败，请看 $FE_LOG"; return 1; }
    echo "   ✅ 依赖安装完成"
  fi
  echo "▶  启动 Frontend (Vite, http://localhost:$FRONTEND_PORT) ..."
  # 直接调用 vite 二进制，避免经 npm wrapper 后 PID 不指向真实 vite 进程
  local VITE_BIN="frontend/node_modules/.bin/vite"
  if [ ! -x "$VITE_BIN" ]; then
    echo "   ❌ 找不到 $VITE_BIN（npm install 未完成？）"; return 1
  fi
  (
    cd frontend
    nohup ./node_modules/.bin/vite --port "$FRONTEND_PORT" --host 127.0.0.1 \
      >> "../$FE_LOG" 2>&1 &
    echo $! > "../$FE_PID"
  )
  sleep 3
  if is_running "$FE_PID"; then
    echo "   ✅ Frontend 已启动 (PID $(cat "$FE_PID"))，日志: $FE_LOG"
    echo "   🌐 打开浏览器: http://localhost:$FRONTEND_PORT/"
  else
    echo "   ❌ Frontend 启动失败，请看 $FE_LOG"; return 1
  fi
}

status_one() {
  local name="$1" pidfile="$2"
  if is_running "$pidfile"; then
    echo "  $name: ● running  (PID $(cat "$pidfile"))"
  else
    echo "  $name: ○ stopped"
  fi
}

# ---------- 入口 ----------
CMD="${1:-start}"
case "$CMD" in
  start|"")
    echo "=== Big A 启动 ==="
    echo "Python: $PY"
    [ -n "$NPM" ] && echo "Node:   $(node --version 2>/dev/null) / npm $($NPM --version 2>/dev/null)"
    start_web
    start_scheduler
    start_frontend
    echo
    echo "全部就绪。"
    echo "  后端 API : http://$HOST:$PORT/api/health"
    echo "  前端 UI  : http://localhost:$FRONTEND_PORT/"
    echo "  ./start.sh status | logs | stop"
    ;;

  stop)
    echo "=== Big A 停止全部 ==="
    stop_one "Frontend"  "$FE_PID"
    stop_one "Scheduler" "$SCH_PID"
    stop_one "Web"       "$WEB_PID"
    ;;

  restart)
    "$0" stop
    sleep 1
    "$0" start
    ;;

  status)
    echo "=== 服务状态 ==="
    status_one "Web      ($HOST:$PORT)"   "$WEB_PID"
    status_one "Scheduler              "  "$SCH_PID"
    status_one "Frontend (:$FRONTEND_PORT)" "$FE_PID"
    ;;

  logs)
    echo "=== 跟随日志（Ctrl+C 退出） ==="
    touch "$WEB_LOG" "$SCH_LOG" "$FE_LOG"
    tail -n 50 -f "$WEB_LOG" "$SCH_LOG" "$FE_LOG"
    ;;

  *)
    echo "用法: $0 [start|stop|restart|status|logs]"
    exit 1
    ;;
esac
