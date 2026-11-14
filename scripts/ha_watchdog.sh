#!/bin/bash
# TetherPool HA 看门狗 (2026-11-14 深度修复): 分级处置, 一次自动重启, 持续异常打 [HA_ALERT].
# 判据: ① 三服务 active ② 桥核 listener 数 >= 3000 ③ api /health ④ 根分区<90% + /tmp<85% ⑤ 探活新鲜度 <30min
STATE=/tmp/ip_pool_watchdog_state
declare -A FAIL

# ① 服务 (pigeonw-bridge 是独立生产桥核, 2026-11-14 review 补入守护)
for svc in ip-pool-api ip-pool-gateway ip-pool-huawei-bridge pigeonw-bridge; do
  systemctl is-active --quiet "$svc" || FAIL[$svc]=1
done
# ② 桥核 listener (mihomo 静默跳过冲突端口的教训 -> 必须数端口)
# 端段: teleapi 11101-11239 / pigeonw 12053-12099 (扩容上限) / gen 21001-24873 / sub 25001-25099 / skyward 25501-25699 / teleapisub 25701+ (2[56]xx 通配)
if [ -z "${FAIL[ip-pool-huawei-bridge]:-}" ]; then
  L=$(ss -tln | grep -cE ":(111[0-9]{2}|120[5-9][0-9]|2[1-4][0-9]{3}|2[56][0-9]{3}) ")
  if [ "${L:-0}" -lt 3000 ]; then FAIL[bridge_listeners]="$L"; fi
fi
# ③ api 健康
curl -sf -m 8 http://127.0.0.1:9100/health > /dev/null || FAIL[api_health]=1
# ④ 磁盘 (97% 事故教训) — 根分区 + /tmp (tmpfs 打满会让测试门禁/临时文件全挂,
# 2026-11-14 事故: 1780 个泄漏测试 db-wal 塞满 987M tmpfs)
U=$(df --output=pcent / | tail -1 | tr -dc 0-9)
if [ "${U:-0}" -ge 90 ]; then FAIL[disk]="$U"; fi
T=$(df --output=pcent /tmp | tail -1 | tr -dc 0-9)
if [ "${T:-0}" -ge 85 ]; then FAIL[tmp_disk]="$T"; fi
# ⑥ WAL 无限增长守卫 (HA 加固): 长事务读者存在时 wal_autocheckpoint 压不住,
# WAL 超过 100MB 告警人工 checkpoint (自动 checkpoint 会与活跃读者竞争锁,
# 不自愈 —— 100MB 级别在 40G 盘上是趋势信号而非紧急态)。
WAL_SIZE=$(stat -c %s /opt/ip-pool/data/ip_pool.db-wal 2>/dev/null || echo 0)
if [ "${WAL_SIZE:-0}" -ge 104857600 ]; then FAIL[wal_giant]="$((WAL_SIZE / 1048576))MB"; fi
# /tmp 泄漏自愈: pytest NamedTemporaryFile (tmp??????.db*) 只在进程死亡时泄漏,
# 活测试进程持有中删除无害 (fd 仍有效)。mtime >1h 的才清, 持续增长则下轮告警。
if [ "${T:-0}" -ge 85 ]; then
  find /tmp -maxdepth 1 -name 'tmp??????.db*' -mmin +60 -delete 2>/dev/null
  find /tmp -maxdepth 1 -name 'tmp??????.db*' -mmin +60 -delete 2>/dev/null
  rm -rf /tmp/pytest-of-root 2>/dev/null
fi
# ⑤ 探活新鲜度 (api 死循环/探活线程挂死时服务仍 active 的盲区)
# 窗口 30min 且不触发重启 (二轮 review): 全池降级时单轮 sweep 可达 ~15-20min
# (3000 节点 × 重试 + 超时), 15min 窗口 + 重启会造成 降级->重启->更慢->循环重启;
# 挂死的探活循环 30min 内必无日志, 足以区分。且重启 api 无法治愈降级,
# 只会清空内存态 (target_health 坏集缓存等) 恶化恢复 —— 故本项只告警。
LAST=$(journalctl -u ip-pool-api --since "30 min ago" --no-pager 2>/dev/null | grep -c "probe finished")
if [ "${LAST:-0}" -eq 0 ]; then FAIL[probe_stale_alert_only]=1; fi

if [ "${#FAIL[@]}" -eq 0 ]; then
  [ -f "$STATE" ] && rm -f "$STATE"
  exit 0
fi

# probe_stale 只告警不重启 (见 ⑤ 注释): 从处置集里摘掉
DISPOSITION_FAIL=()
for k in "${!FAIL[@]}"; do
  [ "$k" = "probe_stale_alert_only" ] || DISPOSITION_FAIL+=("$k=${FAIL[$k]}")
done

if [ "${#DISPOSITION_FAIL[@]}" -eq 0 ]; then
  echo "[HA_ALERT] probe_stale (no 'probe finished' in 30min) — sweep slow or probe loop degraded; NOT restarting (degradation restart-loop guard)"
  exit 0
fi

KEY=""
for k in "${DISPOSITION_FAIL[@]}"; do KEY="$KEY$k "; done
PREV=$(cat "$STATE" 2>/dev/null)
if [ -z "$PREV" ]; then
  echo "$KEY" > "$STATE"
  echo "[HA_SELFHEAL] first-seen failures: $KEY — restarting services"
  for svc in ip-pool-api ip-pool-gateway ip-pool-huawei-bridge pigeonw-bridge; do
    systemctl try-restart "$svc"
  done
  exit 0
fi
echo "[HA_ALERT] persistent failures (previous: $PREV, now: $KEY) — manual intervention needed"
