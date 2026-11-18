#!/bin/bash
# easytier-watchdog.sh — tebi 数据面看门狗 (2026-11-19, 移植自 Mac v2 §八.12 看门狗)
#
# tebi 特化 (与 Mac v2 的差异):
#   1. no_tun 宿主内核 ping 不通 mesh IP —— 数据面探针改为【应用层穿透】:
#      probe1 = HTTP GET 127.0.0.1:9100/health   (easytier 转发 → mesh → huawei ip-pool api)
#      probe2 = SOCKS5 握手 127.0.0.1:10880      (easytier 转发 → mesh → huawei-sg sing-box)
#      两者都真实穿过 easytier 数据面; 纯 TCP connect 不足为凭 (listener 秒 accept,
#      mesh 流式失败是异步的 —— v1 教训的同款指纹陷阱)。
#   2. 自愈动作 = supervisorctl restart easytier (supervisord 托管, 非 launchctl)。
#   3. stat 用 Linux 语法 (-c %Y)。
#   4. 无动态补探针 (tebi 仅 7 条转发且 dst 异构, app 级探测不可泛化);
#      双静态探针同时失败才会误报 —— 两个探针对端 (huawei api / huawei-sg)
#      同时挂的概率远低于 mesh 数据面僵死本身。
#
# 指纹 (与 Mac v2 同源):
#   A. RPC 活 (easytier-cli peer 有响应) + 数据面死 → 精确指纹, 连续 2 轮自愈
#   B. RPC 也失联 → 连续 3 轮自愈 (KeepAlive 只救退出不救 hang)
# 动作后复探验证; 冷却 600s 防抖; 日志裁剪 + 每日心跳。

PATH=/usr/local/bin:/usr/bin:/bin:/sbin

LOG=${WATCHDOG_LOG:-/var/log/easytier-watchdog.log}
ERR=${WATCHDOG_ERR:-/var/log/easytier-watchdog.err.log}
RUNDIR=${WATCHDOG_RUNDIR:-/root/easytier-watchdog}
CLI=${WATCHDOG_CLI:-/usr/local/bin/easytier-cli}
PROBE1_URL=${WATCHDOG_PROBE1_URL:-http://127.0.0.1:9100/health}
PROBE2_ADDR=${WATCHDOG_PROBE2_ADDR:-127.0.0.1:10880}
FAILS_NEEDED=${WATCHDOG_FAILS_NEEDED:-2}
DEAD_THRESHOLD=${WATCHDOG_DEAD_THRESHOLD:-3}
COOLDOWN=${WATCHDOG_COOLDOWN:-600}
LOCK_STALE=${WATCHDOG_LOCK_STALE:-300}
DRYRUN=${WATCHDOG_DRYRUN:-0}

STATE=${RUNDIR}/fails
LAST=${RUNDIR}/last
LOCK=${RUNDIR}/lock
HB=${RUNDIR}/heartbeat

mkdir -p "${RUNDIR}" 2>/dev/null || exit 1
chmod 700 "${RUNDIR}" 2>/dev/null

# ---- 锁: mkdir 原子 + 陈旧残留自愈 (Linux stat -c %Y) ----
if ! mkdir "${LOCK}" 2>/dev/null; then
    now=$(date +%s)
    mtime=$(stat -c %Y "${LOCK}" 2>/dev/null || echo "${now}")
    if [ $((now - mtime)) -gt "${LOCK_STALE}" ]; then
        rmdir "${LOCK}" 2>/dev/null
        mkdir "${LOCK}" 2>/dev/null || exit 0
    else
        exit 0
    fi
fi
trap 'rmdir "${LOCK}" 2>/dev/null' EXIT

log() { echo "$(date '+%F %T') $*" >> "${LOG}" 2>/dev/null; }

trim() {
    [ -f "$1" ] || return 0
    [ "$(wc -l < "$1")" -gt 1000 ] || return 0
    tail -n 500 "$1" > "$1.tmp" 2>/dev/null && mv "$1.tmp" "$1"
}
trim "${LOG}"
trim "${ERR}"

TODAY=$(date +%F)
if [ "$(cat "${HB}" 2>/dev/null)" != "${TODAY}" ]; then
    echo "${TODAY}" > "${HB}" 2>/dev/null
    log "INFO heartbeat tebi-v1 alive"
fi

num() { case "$1" in ''|*[!0-9]*) echo "$2" ;; *) echo "$1" ;; esac; }

# ---- 探测 1: HTTP 穿透 (转发 → mesh → huawei ip-pool api /health) ----
p1_ok() {
    curl -s --max-time 6 "${PROBE1_URL}" 2>/dev/null | grep -q '"status":"ok"'
}

# ---- 探测 2: SOCKS5 握手穿透 (转发 → mesh → huawei-sg sing-box) ----
p2_ok() {
    timeout 6 bash -c 'exec 3<>/dev/tcp/'"${PROBE2_ADDR}" 2>/dev/null || return 1
    printf '\x05\x01\x00' >&3
    resp=""
    while [ ${#resp} -lt 2 ]; do
        IFS= read -r -n1 -t 3 ch <&3 || break
        resp="${resp}${ch}"
    done
    exec 3>&-
    # 05 00 = no-auth 握手成功 (shell 无法可靠比对 \x05\x00, 用 od 数值判定)
    printf '%s' "$resp" | od -An -tx1 | tr -d ' \n' | grep -q '^0500$'
}

# ---- 探测: 短路式, 任一通即健康 ----
HEALTHY=0
CLIRC=0
if p1_ok; then
    HEALTHY=1
elif p2_ok; then
    HEALTHY=1
else
    "${CLI}" peer >/dev/null 2>&1; CLIRC=$?
fi

prev=$(num "$(cat "${STATE}" 2>/dev/null)" 0)

if [ "${HEALTHY}" -eq 1 ]; then
    if [ "${prev}" -gt 0 ]; then
        log "RECOVER 数据面恢复（此前连续失败 ${prev} 轮）"
    fi
    echo 0 > "${STATE}"
    exit 0
fi

fails=$((prev + 1))
echo "${fails}" > "${STATE}"

if [ "${fails}" -lt "${FAILS_NEEDED}" ]; then
    log "DEGRADED 数据面探测失败 ${fails}/${FAILS_NEEDED}，观察中"
    exit 0
fi

# ---- 达到阈值: 区分「精确指纹」与「RPC 也失联」----
rpc_ok=0
[ "${CLIRC}" -eq 0 ] && rpc_ok=1
if [ "${rpc_ok}" -eq 0 ] && [ "${fails}" -lt "${DEAD_THRESHOLD}" ]; then
    log "DEAD RPC 无响应（easytier-cli rc=${CLIRC}），等待 ${fails}/${DEAD_THRESHOLD}"
    exit 0
fi

now=$(date +%s)
last=$(num "$(cat "${LAST}" 2>/dev/null)" 0)
if [ $((now - last)) -lt "${COOLDOWN}" ]; then
    if [ $((fails % 10)) -eq 0 ]; then
        log "SKIP 冷却期内（$((now - last))s < ${COOLDOWN}s，fails=${fails}，rpc_ok=${rpc_ok}）"
    fi
    exit 0
fi
echo "${now}" > "${LAST}"

if [ "${DRYRUN}" = "1" ]; then
    log "HEAL [DRYRUN] 命中（fails=${fails}，rpc_ok=${rpc_ok}）→ 跳过实际 restart"
    exit 0
fi

log "HEAL 指纹命中（fails=${fails}，rpc_ok=${rpc_ok}）→ supervisorctl restart easytier"
if supervisorctl restart easytier >/dev/null 2>&1; then
    sleep 15
    if p1_ok || p2_ok; then
        log "HEAL 已执行并验证恢复 ✓"
        echo 0 > "${STATE}"
    else
        log "HEAL 已执行但 15s 后未恢复，冷却后自动重试（若持续失败请人工查 mesh hub / huawei 对端）"
    fi
else
    log "ERROR supervisorctl restart 失败，请人工检查 supervisord"
    echo 0 > "${STATE}"
fi
