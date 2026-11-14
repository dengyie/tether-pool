#!/bin/bash
# TetherPool HA 看门狗 (2026-11-14 深度修复): 分级处置, 一次自动重启, 持续异常打 [HA_ALERT].
# 判据: ① 三服务 active ② 桥核 listener 数 >= 3000 ③ api /health ④ 根分区 <90% ⑤ 探活新鲜度 <15min
STATE=/tmp/ip_pool_watchdog_state
declare -A FAIL

# ① 服务
for svc in ip-pool-api ip-pool-gateway ip-pool-huawei-bridge; do
  systemctl is-active --quiet "$svc" || FAIL[$svc]=1
done
# ② 桥核 listener (mihomo 静默跳过冲突端口的教训 -> 必须数端口)
if [ -z "${FAIL[ip-pool-huawei-bridge]:-}" ]; then
  L=$(ss -tln | grep -cE ":(111[0-9]{2}|120[5-7][0-9]|2[1-4][0-9]{3}|250[0-7][0-9]) ")
  if [ "${L:-0}" -lt 3000 ]; then FAIL[bridge_listeners]="$L"; fi
fi
# ③ api 健康
curl -sf -m 8 http://127.0.0.1:9100/health > /dev/null || FAIL[api_health]=1
# ④ 磁盘 (97% 事故教训)
U=$(df --output=pcent / | tail -1 | tr -dc 0-9)
if [ "${U:-0}" -ge 90 ]; then FAIL[disk]="$U"; fi
# ⑤ 探活新鲜度 (api 死循环/探活线程挂死时服务仍 active 的盲区)
LAST=$(journalctl -u ip-pool-api --since "15 min ago" --no-pager 2>/dev/null | grep -c "probe finished")
if [ "${LAST:-0}" -eq 0 ]; then FAIL[probe_stale]=1; fi

if [ "${#FAIL[@]}" -eq 0 ]; then
  [ -f "$STATE" ] && rm -f "$STATE"
  exit 0
fi

KEY=""
for k in "${!FAIL[@]}"; do KEY="$KEY$k=${FAIL[$k]} "; done
PREV=$(cat "$STATE" 2>/dev/null)
if [ -z "$PREV" ]; then
  echo "$KEY" > "$STATE"
  echo "[HA_SELFHEAL] first-seen failures: $KEY — restarting services"
  for svc in ip-pool-api ip-pool-gateway ip-pool-huawei-bridge; do
    systemctl try-restart "$svc"
  done
  exit 0
fi
echo "[HA_ALERT] persistent failures (previous: $PREV, now: $KEY) — manual intervention needed"
