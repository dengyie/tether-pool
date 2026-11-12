#!/usr/bin/env bash
# bridge_keeper.sh — standalone mihomo 桥核守护循环 (respawn on crash).
# 用法: bridge_keeper.sh <bin> <home> <config> <log>
# 生命周期归 start.sh 管理: kill 本进程 (TERM/INT) 会连带杀掉当前 mihomo 子进程。
# 退避: 核存活 <10s 视为快速失败, 重生间隔 2s→4s→…→60s 封顶 (配置错误不刷日志);
# 存活 ≥10s 自动重置回 2s。
BIN="$1"; HOME_DIR="$2"; CFG="$3"; LOG="$4"
CHILD=""
FAILS=0

log() { echo "[bridge-keeper] $* at $(date '+%F %T')" >> "$LOG"; }

stop() {
    log "keeper stopping (signal), killing child ${CHILD:-none}"
    [ -n "$CHILD" ] && kill "$CHILD" 2>/dev/null
    exit 0
}
trap stop TERM INT

log "keeper started (pid $$, bin=$BIN)"
START=$(date +%s)
while :; do
    "$BIN" -d "$HOME_DIR" -f "$CFG" >> "$LOG" 2>&1 &
    CHILD=$!
    log "spawned core pid=$CHILD"
    wait "$CHILD"
    RC=$?
    now=$(date +%s)
    if [ $((now - START)) -ge 10 ]; then
        FAILS=0
    else
        FAILS=$((FAILS + 1))
    fi
    if [ "$FAILS" -le 1 ]; then
        DELAY=2
    else
        DELAY=$(( 2 * (1 << (FAILS - 1)) ))
        [ "$DELAY" -gt 60 ] && DELAY=60
    fi
    log "core exited (rc=$RC, uptime=$((now - START))s), respawning in ${DELAY}s (fails=$FAILS)"
    START=$now
    sleep "$DELAY"
done
