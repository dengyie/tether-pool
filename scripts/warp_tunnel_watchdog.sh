#!/bin/bash
# warp-tunnel-watchdog.sh — cue 宿主机 WARP 隧道看门狗 (2026-11-19)
#
# 背景 (今晚事故): WARP WireGuard endpoint 无 persistent_keepalive 时, 空闲
# 隧道的 UDP NAT 映射老化 → 链路层假活 (endpoint 不断开) + 隧道内 TCP 全部
# 8m28s 超时。keepalive=25 已打 (config.json 100 peers), 本看门狗兜底
# keepalone 仍无法覆盖的场景 (CF 侧踢会话等)。
#
# 特化 (与 Mac v2 / tebi watchdog 的差异):
#   1. 自愈动作 = systemctl restart sing-box (warp endpoint 全量重握手)
#   2. 探针 = 抽 3 个 warp inbound 端口走 CF trace 并断言 IPv6 出口
#      (warp_ipv6 契约与池侧 prober 一致; v4 出口说明隧道出口漂移)
#   3. 对照 = native socks-v4 端口 (10801) —— native 也挂说明是宿主机
#      网络问题而非 warp 隧道问题, 此时重启无意义, 只记录不重启
#   4. 冷却 1800s (重启代价 = 全量握手 ~1min, 与 relay_watchdog 对齐)
#   5. stat 用 Linux 语法 (-c %Y); 无 flock → mkdir 原子锁 (同 tebi 版)

PATH=/usr/local/bin:/usr/bin:/bin:/sbin

LOG=${WARP_WD_LOG:-/var/log/warp-tunnel-watchdog.log}
RUNDIR=${WARP_WD_RUNDIR:-/root/warp-tunnel-watchdog}
PROBE_HOST=${WARP_WD_HOST:-10.144.144.81}
NATIVE_PORT=${WARP_WD_NATIVE_PORT:-10801}
# 抽 3 个 warp 端口 (head/mid/tail 覆盖 10881-10980 段)
WARP_PORTS=${WARP_WD_PORTS:-"10882 10930 10978"}
FAILS_NEEDED=${WARP_WD_FAILS_NEEDED:-2}
COOLDOWN=${WARP_WD_COOLDOWN:-1800}
LOCK_STALE=${WARP_WD_LOCK_STALE:-600}
DRYRUN=${WARP_WD_DRYRUN:-0}

STATE=${RUNDIR}/fails
LAST=${RUNDIR}/last
LOCK=${RUNDIR}/lock
HB=${RUNDIR}/heartbeat

mkdir -p "${RUNDIR}" 2>/dev/null || exit 1
chmod 700 "${RUNDIR}" 2>/dev/null

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

TODAY=$(date +%F)
if [ "$(cat "${HB}" 2>/dev/null)" != "${TODAY}" ]; then
    echo "${TODAY}" > "${HB}" 2>/dev/null
    log "INFO heartbeat warp-wd-v1 alive (keepalive=25 era)"
fi

num() { case "$1" in ''|*[!0-9]*) echo "$2" ;; *) echo "$1" ;; esac; }

# ---- 对照: native v4 出口 (宿主机网络健康基线) ----
native_ok() {
    curl -s --max-time 10 -x "socks5h://${PROBE_HOST}:${NATIVE_PORT}" \
        "https://api.ipify.org" 2>/dev/null | grep -qE '^[0-9.]+$'
}

# ---- warp 探针: CF trace + IPv6 出口断言 (warp_ipv6 契约) ----
warp_ok() {  # $1 = port
    local trace
    trace=$(curl -s --max-time 12 -x "socks5h://${PROBE_HOST}:$1" \
        "https://cloudflare.com/cdn-cgi/trace" 2>/dev/null)
    echo "${trace}" | grep -qE '^ip=2a09:bac' && return 0
    return 1
}

# 短路判定: native 死 → 宿主机网络问题, 不作为 warp 故障计数
if ! native_ok; then
    log "SKIP native 出口也不通 (宿主机网络问题), 不计 warp 故障"
    exit 0
fi

bad=0
for p in ${WARP_PORTS}; do
    if ! warp_ok "${p}"; then
        bad=$((bad+1))
        log "DEGRADED warp probe :${p} 无 IPv6 出口 (bad=${bad})"
    fi
done

if [ "${bad}" -lt 2 ]; then
    prev=$(num "$(cat "${STATE}" 2>/dev/null)" 0)
    if [ "${prev}" -gt 0 ]; then
        log "RECOVER warp 隧道恢复 (此前连续失败 ${prev} 轮)"
    fi
    echo 0 > "${STATE}"
    exit 0
fi

fails=$(( $(num "$(cat "${STATE}" 2>/dev/null)" 0) + 1 ))
echo "${fails}" > "${STATE}"

if [ "${fails}" -lt "${FAILS_NEEDED}" ]; then
    log "DEGRADED warp 探测失败 ${fails}/${FAILS_NEEDED} (bad=${bad}/3)，观察中"
    exit 0
fi

now=$(date +%s)
last=$(num "$(cat "${LAST}" 2>/dev/null)" 0)
if [ $((now - last)) -lt "${COOLDOWN}" ]; then
    log "SKIP 冷却期内 ($((now - last))s < ${COOLDOWN}s)"
    exit 0
fi
echo "${now}" > "${LAST}"

if [ "${DRYRUN}" = "1" ]; then
    log "HEAL [DRYRUN] 命中 (fails=${fails}, bad=${bad}/3) → 跳过 restart"
    exit 0
fi

log "HEAL 指纹命中 (fails=${fails}, bad=${bad}/3) → systemctl restart sing-box"
if systemctl restart sing-box >/dev/null 2>&1; then
    sleep 20
    okcnt=0
    for p in ${WARP_PORTS}; do
        warp_ok "${p}" && okcnt=$((okcnt+1))
    done
    if [ "${okcnt}" -ge 2 ]; then
        log "HEAL 已执行并验证恢复 ✓ (${okcnt}/3)"
        echo 0 > "${STATE}"
    else
        log "HEAL 已执行但 20s 后仅 ${okcnt}/3 恢复，冷却后自动重试 (持续失败请查 CF 会话/宿主机 UDP)"
    fi
else
    log "ERROR systemctl restart sing-box 失败，请人工检查 systemd"
    echo 0 > "${STATE}"
fi
