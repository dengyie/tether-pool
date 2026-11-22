#!/bin/bash
# relay_watchdog.sh — teleapi 桥核中继自动守护 (2026-11-19, review 硬化版)
# 背景: EasyTier mesh 路径质量随时变 (Oregon 87→1175ms, SG 87→299ms 同晚实测),
#       中继选型不可依赖单次时点延迟。每 5 分钟用 easytier peer 指标判断当前
#       中继是否劣化, 劣化且存在优质备选时自动切换 (import_standalone.py relay)。
# 策略: 高门槛双条件 + 30 分钟冷却, 防止路径抖动导致反复横跳
#       (每次切换 = BrightData 源 IP 变化, 频繁切换伤 vendor 信誉)。
# 硬化 (2026-11-19 review): ①easytier-cli 有界执行 (mesh 拥塞时可无限挂,
#       不加超时会随 cron 堆积僵尸进程); ②锁目录记 PID + 陈旧锁接管时 kill;
#       ③peer 行消失 (链路全断, 最恶劣态) 连续 2 轮同样触发 failover;
#       ④direct/AMBIGUOUS 状态区分 + 可 grep 告警标记; ⑤依赖 cmd_relay 的
#       else-pop 根修, 不再需要行级 YAML hack。
set -u
set -o pipefail
REPO="${RELAY_WD_REPO:-/Users/mango/project/ip-pool}"
LOG_TAG="[relay_watchdog]"
ET_CLI="${RELAY_WD_ET_CLI:-/opt/homebrew/bin/easytier-cli}"
PY="${RELAY_WD_PY:-$REPO/.venv/bin/python}"
BRIDGE_CFG="${RELAY_WD_BRIDGE_CFG:-$REPO/config/teleapi-bridge.yaml}"
export RELAY_WD_BRIDGE_CFG="$BRIDGE_CFG"
LOCKDIR="${RELAY_WD_LOCKDIR:-/tmp/relay-watchdog.lockdir}"
TSF="${RELAY_WD_TSF:-/tmp/relay-watchdog.last-flip}"
DOWNF="${RELAY_WD_DOWNF:-/tmp/relay-watchdog.down-count}"
CLI_OUT="${RELAY_WD_CLI_OUT:-/tmp/relay-watchdog.cli.out}"

# 候选: mesh IP | 桥核中继节点名 (须已在 relay-nodes.local.yaml / 桥核配置)
CANDIDATES=(
  "10.144.144.14|[自建] 🇺🇸VLESS-AWS-Oregon-Reality"
  "10.144.144.3|[自建] 🇯🇵VLESS-AWS-Tokyo-Reality"
)
BAD_LAT=800      # 当前中继劣化阈值 ms
BAD_LOSS=5       # 当前中继劣化阈值 %
GOOD_LAT=250     # 备选优质阈值 ms
GOOD_LOSS=2      # 备选优质阈值 %
COOLDOWN=1800    # 切换冷却秒
CLI_TIMEOUT=25   # easytier-cli 有界执行秒数 (cron 5min, 超时即放弃本轮)

# ---------- 锁: mkdir 原子 + PID 记录 + 陈旧锁接管时杀前任 ----------
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  OLD_PID=$(cat "$LOCKDIR/pid" 2>/dev/null || echo "")
  STALE=$(find "$LOCKDIR" -maxdepth 0 -mmin +10 2>/dev/null)
  if [ -n "$STALE" ]; then
    [ -n "$OLD_PID" ] && kill "$OLD_PID" 2>/dev/null
    rm -rf "$LOCKDIR"
    mkdir "$LOCKDIR" 2>/dev/null || exit 0
  else
    exit 0   # 他实例持锁正常运行
  fi
fi
echo $$ > "$LOCKDIR/pid"
trap 'rm -rf "$LOCKDIR" 2>/dev/null' EXIT

log() { echo "$(date '+%F %T') $LOG_TAG $*"; }

# ---------- 有界执行器: macOS 无 timeout, 用后台+wait+kill ----------
run_bounded() {  # $1=timeout_s, 其余=命令 → stdout; 超时返回空
  local t="$1"; shift
  "$@" > "$CLI_OUT" 2>/dev/null &
  local p=$!
  local waited=0
  while kill -0 "$p" 2>/dev/null && [ "$waited" -lt "$t" ]; do
    sleep 1; waited=$((waited+1))
  done
  if kill -0 "$p" 2>/dev/null; then
    kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null
    return 1
  fi
  wait "$p" 2>/dev/null
  rc=$?
  cat "$CLI_OUT"
  return "$rc"
}

# ---------- 当前中继读取 ----------
current_relay() {
  "$PY" - <<'PYEOF'
import os
import yaml
c = yaml.safe_load(open(os.environ['RELAY_WD_BRIDGE_CFG']))
vals = {p.get('dialer-proxy') for p in c.get('proxies', []) if p.get('dialer-proxy')}
vals = [v for v in vals if 'VLESS-AWS' in (v or '')]
if len(vals) == 1:
    print(vals[0])
elif not vals:
    print('DIRECT')
else:
    print('AMBIGUOUS:' + ','.join(sorted(vals)))
PYEOF
}

peer_metric() {  # $1 = mesh IP → "lat loss"; 行首 '|' 产生空 $1: $2=ipv4 $5=lat $6=loss
  local out rc
  # Keep CLI execution separate from parsing: an empty parsed result can mean either
  # a missing peer row or a CLI failure/timeout, and those states have different actions.
  out=$(run_bounded "$CLI_TIMEOUT" "$ET_CLI" peer)
  rc=$?
  [ "$rc" -eq 0 ] || return "$rc"
  # awk always exits successfully, so an absent row remains an empty metric rather
  # than being confused with a failed easytier-cli invocation.
  out=$(printf '%s\n' "$out" | awk -F'|' -v needle="$1/" \
    'index($0, needle) && ($0 ~ /p2p|relay/) { gsub(/ms|%/,"",$5); gsub(/ms|%/,"",$6); print $5, $6; exit }')
  printf '%s\n' "$out"
}

flip_to() {  # $1 = 目标中继节点名
  local target="$1" now output rc cfg_dir pidf
  log "⚡ 切换 → $target"
  output=$("$PY" "$REPO/scripts/import_standalone.py" relay --group teleapi --relay-node "$target" --mode on 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    printf '%s\n' "$output" | while IFS= read -r l; do log "$l"; done
    log "FAIL 中继配置切换失败 (rc=$rc), 保留桥核与冷却状态"
    return "$rc"
  fi
  printf '%s\n' "$output" | while IFS= read -r l; do log "$l"; done
  # cmd_relay 只改配置; 杀桥核进程, bridge_keeper.sh 以新配置重生。
  # kill 前必须验证目标进程命令行确实是本桥 mihomo (pidfile 可能陈旧:
  # PID 复用后 kill 会误杀无辜进程 —— pgrep 兜底同样只信完整 -f 参数匹配)。
  local pidf="$REPO/data/teleapi-bridge.pid"
  local pid=""
  [ -f "$pidf" ] && pid=$(cat "$pidf" 2>/dev/null)
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null \
     && ps -p "$pid" -o args= 2>/dev/null | grep -q "bin/mihomo.*$BRIDGE_CFG"; then
    kill "$pid" 2>/dev/null
  else
    # pidfile 失效兜底: 精确匹配完整 -f 参数路径
    pid=$(pgrep -f "bin/mihomo -d $REPO/config/teleapi-bridge -f $BRIDGE_CFG" | head -1)
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
  fi
  now=$(date +%s); echo "$now" > "$TSF"
  rm -f "$DOWNF"
  log "✅ 已切换, 桥核由 keeper 接管重生"
}

# ---------- 主流程 ----------
CUR=$(current_relay)
case "$CUR" in
  DIRECT)
    log "WARN direct 模式 (无 dialer-proxy) — 无中继可守护"; exit 0;;
  AMBIGUOUS*)
    log "FAIL 当前中继多义 ($CUR) — watchdog 无法判断, 请人工核查桥核配置 (grep dialer-proxy $BRIDGE_CFG)"; exit 1;;
  "")
    log "[-] 桥核配置读取失败"; exit 0;;
esac

CUR_IP=""
for c in "${CANDIDATES[@]}"; do
  ip="${c%%|*}"; name="${c#*|}"
  [ "$name" = "$CUR" ] && CUR_IP="$ip"
done
if [ -z "$CUR_IP" ]; then log "[-] 当前中继 $CUR 不在候选表"; exit 0; fi

METRIC=$(peer_metric "$CUR_IP")
PEER_RC=$?
if [ "$PEER_RC" -ne 0 ]; then
  log "WARN easytier-cli peer 失败/超时 (rc=$PEER_RC), 本轮不改变状态"
  exit 0
fi
if [ -z "$METRIC" ]; then
  # 2026-11-19 review P2: peer 行消失 = 到当前中继的链路整体不在 mesh 里
  # (比劣化更恶劣), 旧版在此直接 exit 0 —— 最恶劣态反而永不切换。
  DOWN=$(cat "$DOWNF" 2>/dev/null || echo 0); DOWN=$((DOWN+1)); echo "$DOWN" > "$DOWNF"
  log "WARN 当前中继 ($CUR_IP) peer 行缺失 (连续 $DOWN 轮)"
  if [ "$DOWN" -lt 2 ]; then exit 0; fi
  CLAT=""; CLOSS=""
else
  rm -f "$DOWNF"
  read -r CLAT CLOSS <<< "$METRIC"
  if [ -z "$CLAT" ]; then log "[-] peer 指标解析失败 ($CUR_IP)"; exit 0; fi
fi

DEGRADED=0
if [ -n "$CLAT" ]; then
  { [ "${CLAT%%.*}" -ge "$BAD_LAT" ] || [ "${CLOSS%%.*}" -ge "$BAD_LOSS" ]; } && DEGRADED=1
  log "当前=$CUR ($CUR_IP) lat=${CLAT}ms loss=${CLOSS}% degraded=$DEGRADED"
else
  DEGRADED=1
  log "当前=$CUR ($CUR_IP) 链路无 peer 行 (视为 degraded)"
fi
[ "$DEGRADED" -eq 1 ] || exit 0

NOW=$(date +%s)
if [ -f "$TSF" ]; then
  LAST=$(cat "$TSF" 2>/dev/null || echo 0)
  [ $((NOW - LAST)) -lt "$COOLDOWN" ] && { log "冷却中, 跳过"; exit 0; }
fi

BEST=""
for c in "${CANDIDATES[@]}"; do
  ip="${c%%|*}"; name="${c#*|}"
  [ "$name" = "$CUR" ] && continue
  BMETRIC=$(peer_metric "$ip")
  PEER_RC=$?
  if [ "$PEER_RC" -ne 0 ]; then
    log "备选=$name ($ip) easytier-cli 失败/超时 (rc=$PEER_RC), 跳过"
    continue
  fi
  [ -z "$BMETRIC" ] && { log "备选=$name ($ip) peer 行缺失, 跳过"; continue; }
  read -r BLAT BLOSS <<< "$BMETRIC"
  [ -z "$BLAT" ] && continue
  log "备选=$name ($ip) lat=${BLAT}ms loss=${BLOSS}%"
  if [ "${BLAT%%.*}" -le "$GOOD_LAT" ] && [ "${BLOSS%%.*}" -le "$GOOD_LOSS" ]; then
    BEST="$name"; break
  fi
done
[ -z "$BEST" ] && { log "无优质备选, 保持现状 (链路全断时等待冷却后重试)"; exit 0; }
flip_to "$BEST" || exit 1
