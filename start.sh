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
        GATEWAY_HOST="$GATEWAY_HOST" GATEWAY_PORT="$GATEWAY_PORT" \
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
}

status_services() {
    echo "=== IP Pool Service Status ==="
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
