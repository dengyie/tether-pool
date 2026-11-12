#!/usr/bin/env bash
# start.sh - Dual-Mode Launcher for IP Pool System
# Launches:
#   1. Control Plane & Dashboard REST Service on :9100
#   2. Local Unified Forward Proxy Gateway on :9101

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

VENV_PYTHON="$DIR/.venv/bin/python3"
if [ ! -f "$VENV_PYTHON" ]; then
    echo "[-] Virtualenv not found at $DIR/.venv. Please initialize first."
    exit 1
fi

PORT=${PORT:-9100}
GATEWAY_PORT=${GATEWAY_PORT:-9101}
# Control plane is unauthenticated by default; keep it loopback-only unless the
# operator explicitly opts into exposing it (and sets IP_POOL_API_TOKEN).
HOST=${HOST:-"127.0.0.1"}
GATEWAY_HOST=${GATEWAY_HOST:-"127.0.0.1"}
LOG_DIR="$DIR/logs"
DATA_DIR="$DIR/data"

PID_API="$DATA_DIR/ip_pool.pid"
PID_GW="$DATA_DIR/gateway.pid"
LOG_API="$LOG_DIR/ip_pool.log"
LOG_GW="$LOG_DIR/gateway.log"

# Standalone mihomo bridge core (TetherPool-owned, e.g. teleapi residential
# batch): per-node socks listeners that the gateway dials as socks5 upstream.
# NOT the user's Clash Verge / tebi / pxed configs — lives entirely in ip-pool.
BRIDGE_BIN="$DIR/bin/mihomo"
BRIDGE_CFG="$DIR/config/teleapi-bridge.yaml"
BRIDGE_HOME="$DIR/config/teleapi-bridge"
PID_BRIDGE="$DATA_DIR/teleapi-bridge.pid"
LOG_BRIDGE="$LOG_DIR/teleapi-bridge.log"

if [ -n "$PYTEST_CURRENT_TEST" ]; then
    echo "[-] Refusing to start production daemons from pytest execution: $PYTEST_CURRENT_TEST" >&2
    exit 1
fi

# Guard against inherited ephemeral test database paths
case "${IP_POOL_DB:-}" in
    *pytest*|*tmp*|*test_isolated.db*)
        unset IP_POOL_DB
        ;;
esac

mkdir -p "$LOG_DIR" "$DATA_DIR"

wait_for_pid_exit() {
    local pid=$1
    local name=$2
    local count=0
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do
        sleep 0.2
        count=$((count + 1))
        if [ "$count" -ge 25 ]; then
            echo "[-] $name (PID $pid) did not exit within 5s, sending SIGKILL..."
            kill -9 "$pid" 2>/dev/null || true
            break
        fi
    done
    echo "[+] Stopped $name (PID: $pid)"
}

stop_services() {
    echo "[*] Stopping IP Pool services..."
    if [ -f "$PID_BRIDGE" ]; then
        PID=$(cat "$PID_BRIDGE")
        if ps -p "$PID" > /dev/null 2>&1; then
            wait_for_pid_exit "$PID" "Bridge Core Keeper"
        fi
        rm -f "$PID_BRIDGE"
    fi
    # PID 文件漂移兜底: 按命令行特征清扫自家桥核残留 (绝不匹配 Verge 的 core)。
    pkill -f "bin/mihomo -d .*config/teleapi-bridge" 2>/dev/null && \
        echo "[+] Swept stray teleapi-bridge mihomo processes" || true

    if [ -f "$PID_API" ]; then
        PID=$(cat "$PID_API")
        if ps -p "$PID" > /dev/null 2>&1; then
            wait_for_pid_exit "$PID" "Control Plane"
        fi
        rm -f "$PID_API"
    fi

    if [ -f "$PID_GW" ]; then
        PID=$(cat "$PID_GW")
        if ps -p "$PID" > /dev/null 2>&1; then
            wait_for_pid_exit "$PID" "Forward Gateway"
        fi
        rm -f "$PID_GW"
    fi
}

start_services() {
    # 0. Start standalone mihomo bridge core (if configured & binary present)
    if [ -f "$BRIDGE_CFG" ] && [ -x "$BRIDGE_BIN" ]; then
        if [ -f "$PID_BRIDGE" ] && ps -p "$(cat "$PID_BRIDGE")" > /dev/null 2>&1; then
            echo "[*] Bridge Core already running with PID $(cat "$PID_BRIDGE")"
        else
            echo "[*] Starting Bridge Core (mihomo, keeper-supervised) on 127.0.0.1:11101-11239..."
            mkdir -p "$BRIDGE_HOME"
            # keeper 守护: mihomo 崩溃 2s 内自动拉起 (SPOF 根修, 2026-11-12);
            # PID 文件记 keeper, stop 时 trap 连带杀 mihomo 子进程。
            nohup "$DIR/scripts/bridge_keeper.sh" \
                "$BRIDGE_BIN" "$BRIDGE_HOME" "$BRIDGE_CFG" "$LOG_BRIDGE" \
                > /dev/null 2>&1 &
            echo $! > "$PID_BRIDGE"
            sleep 1
        fi
    elif [ -f "$BRIDGE_CFG" ]; then
        echo "[!] Bridge Core 配置存在但 bin/mihomo 缺失 — 跳过 (从 Clash Verge 复制: cp \"/Applications/Clash Verge.app/Contents/MacOS/verge-mihomo\" bin/mihomo)"
    fi

    # 1. Start Control Plane (FastAPI) on :9100
    if [ -f "$PID_API" ] && ps -p "$(cat "$PID_API")" > /dev/null 2>&1; then
        echo "[*] Control Plane already running with PID $(cat "$PID_API")"
    else
        echo "[*] Starting Control Plane on $HOST:$PORT..."
        nohup "$DIR/.venv/bin/uvicorn" api.server:app --host "$HOST" --port "$PORT" --workers 1 > "$LOG_API" 2>&1 &
        echo $! > "$PID_API"
        sleep 1
    fi

    # 2. Start Unified Forward Proxy Gateway on :9101
    if [ -f "$PID_GW" ] && ps -p "$(cat "$PID_GW")" > /dev/null 2>&1; then
        echo "[*] Forward Gateway already running with PID $(cat "$PID_GW")"
    else
        echo "[*] Starting Unified Forward Gateway on $GATEWAY_HOST:$GATEWAY_PORT..."
        # GATEWAY_ATTEMPT_TIMEOUT: 单次上游拨号总预算。AWS 中转家宽链
        # (网关→桥→mihomo→AWS→vendor) CONNECT 建立需 1.5–4s, 默认 2.0s 必超时。
        GATEWAY_HOST="$GATEWAY_HOST" GATEWAY_PORT="$GATEWAY_PORT" \
            GATEWAY_ATTEMPT_TIMEOUT="${GATEWAY_ATTEMPT_TIMEOUT:-8}" \
            nohup "$VENV_PYTHON" gateway/server.py > "$LOG_GW" 2>&1 &
        echo $! > "$PID_GW"
        sleep 1
    fi

    echo ""
    echo "=========================================================="
    echo "✔ IP Pool System v2.0 (Dual-Mode) Running Successfully!"
    echo "  - Control Plane API : http://127.0.0.1:$PORT"
    echo "  - Web Dashboard     : http://127.0.0.1:$PORT/dashboard"
    echo "  - Prometheus Metrics: http://127.0.0.1:$PORT/metrics"
    echo "  - Forward Gateway   : http://127.0.0.1:$GATEWAY_PORT (HTTP/SOCKS5)"
    echo "=========================================================="

    # 启动尾部强制全量探活: 清掉重启窗口内积压的熔断债 (dead 节点跳过冷却
    # 立即 canary)。确认式重试: 拿到 200 (已排队) 才算成功, 最多等 ~30s —
    # 控制面慢启动或后台首轮扫描占锁都不会再丢触发 (api 侧已改为锁等待)。
    PROBE_OK=0
    for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
        sleep 2
        CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:$PORT/v1/probe?force=true")
        if [ "$CODE" = "200" ]; then
            echo "[*] Force full probe queued (circuit debt cleanup)"
            PROBE_OK=1
            break
        fi
    done
    [ "$PROBE_OK" = "1" ] || echo "[!] Force probe NOT queued after 30s — check $LOG_API"
}

status_services() {
    echo "=== IP Pool Service Status ==="
    if [ -f "$PID_BRIDGE" ] && ps -p "$(cat "$PID_BRIDGE")" > /dev/null 2>&1; then
        echo "  [●] Bridge Core mihomo (PID $(cat "$PID_BRIDGE")) is RUNNING (11101-11239)"
    else
        echo "  [○] Bridge Core mihomo is STOPPED"
    fi
    if [ -f "$PID_API" ] && ps -p "$(cat "$PID_API")" > /dev/null 2>&1; then
        echo "  [●] Control Plane (PID $(cat "$PID_API")) is RUNNING on :$PORT"
    else
        echo "  [○] Control Plane is STOPPED"
    fi

    if [ -f "$PID_GW" ] && ps -p "$(cat "$PID_GW")" > /dev/null 2>&1; then
        echo "  [●] Forward Gateway (PID $(cat "$PID_GW")) is RUNNING on :$GATEWAY_PORT"
    else
        echo "  [○] Forward Gateway is STOPPED"
    fi
}

CMD=${1:-start}
case "$CMD" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        stop_services
        sleep 1
        start_services
        ;;
    status)
        status_services
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac
