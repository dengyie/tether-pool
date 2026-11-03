#!/usr/bin/env bash
# start.sh - Launch IP Pool FastAPI Service on port 9100

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

VENV_PYTHON="$DIR/.venv/bin/python3"
if [ ! -f "$VENV_PYTHON" ]; then
    echo "[-] Virtualenv not found at $DIR/.venv. Please initialize first."
    exit 1
fi

PORT=${PORT:-9100}
HOST=${HOST:-"0.0.0.0"}
LOG_FILE="$DIR/logs/ip_pool.log"
PID_FILE="$DIR/data/ip_pool.pid"

mkdir -p "$DIR/logs" "$DIR/data"

# Check if already running
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "[*] IP Pool service already running with PID $OLD_PID"
        exit 0
    else
        rm -f "$PID_FILE"
    fi
fi

echo "[*] Starting IP Pool service on $HOST:$PORT..."
nohup "$DIR/.venv/bin/uvicorn" api.server:app --host "$HOST" --port "$PORT" --workers 1 > "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"

sleep 2
if ps -p "$NEW_PID" > /dev/null 2>&1; then
    echo "[+] IP Pool service started successfully (PID: $NEW_PID)."
    echo "[+] Health endpoint: http://127.0.0.1:$PORT/health"
    echo "[+] Docs endpoint: http://127.0.0.1:$PORT/docs"
else
    echo "[-] Failed to start service. Check $LOG_FILE for details."
    exit 1
fi
