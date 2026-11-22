#!/usr/bin/env bash
# pull_deploy.sh — 生产一键部署 (TetherPool, 在 huawei-vps /opt/ip-pool 执行)
#
# 流程: git pull → 测试门禁 → DB 在线备份 → systemd restart → 健康门禁 → 失败自动回退
#       部署成功后清理旧备份: 保留最近 3 份 + 每月 1 日份永久
#
# 用法:
#   scripts/pull_deploy.sh            # 有新提交才部署; 已是最新则 no-op
#   scripts/pull_deploy.sh --force    # 无新提交也强制走一遍完整周期 (含重启)
#
# 设计要点:
# - 测试门禁失败时只回退代码, 不动运行中的服务 (服务此刻还在跑旧代码)
# - DB 备份用 sqlite 在线 backup API (WAL 安全), 不用 cp (活库 cp 可能撕裂)
# - 健康门禁 = /health + 双 systemd unit active; 失败即回退代码再重启。
#   DB 快照保留供人工恢复；不自动覆盖服务启动后的业务写入。
set -euo pipefail
cd "$(dirname "$0")/.."

# 四轮 review W2: 并发部署锁 —— 两个部署周期交错 (手动 + cron) 会双重
# restart / 相互回退, flock 串行化全部周期。
exec 9>/var/lock/ip-pool-deploy.lock
if ! flock -n 9; then
    echo "[-] 另一个部署周期正在运行 (锁 /var/lock/ip-pool-deploy.lock), 退出"
    exit 1
fi

# 四轮 review W2: 回退路径用 git reset --hard, 会无声销毁未提交修改 ——
# 2026-11-22 生产 exclusivity 热修在脏树里险些被 pull 直接回退。部署前
# 检查脏树: 有未提交修改直接中止 (人工收编), 绝不在脏树上 pull。
if ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]; then
    echo "[-] 工作树有未提交修改 — 拒绝部署 (回退路径 reset --hard 会销毁它们)。"
    echo "    先提交/收编或 stash 后重试。当前脏文件:"
    git status --short
    exit 1
fi

PREV=$(git rev-parse HEAD)
echo "[1/6] git pull (当前 $PREV)"
# 境内云主机 → github.com 的 TLS 常被掐断 (GnuTLS -110 等), 三次退避重试
pull_ok=0
for i in 1 2 3; do
    if git pull --ff-only origin main >/dev/null 2>&1; then pull_ok=1; break; fi
    echo "      pull 第 $i 次失败 (网络抖动), $((i * 5))s 后重试..."
    sleep $((i * 5))
done
if [ "$pull_ok" != "1" ]; then
    echo "[-] pull 3 次均失败 — 保持原状, 未动任何服务 (稍后重跑即可)"
    exit 1
fi
NEW=$(git rev-parse HEAD)
if [ "$PREV" = "$NEW" ] && [ "${1:-}" != "--force" ]; then
    echo "[=] 已是最新 $NEW, 无需部署 (--force 可强制走完整周期)"
    exit 0
fi
echo "      $PREV -> $NEW"

echo "[2/6] 测试门禁"
if ! .venv/bin/python -m pytest tests/ -q; then
    echo "[-] 测试未过 → 代码回退 $PREV (运行中的服务未受影响, 仍跑旧代码)"
    git reset --hard "$PREV"
    exit 1
fi

TAG=$(date +%Y%m%d-%H%M%S)
echo "[3/6] DB 在线备份 → backups/$TAG/"
mkdir -p "backups/$TAG"
# 2026-11-15 review P2: 备份失败 (WAL 忙/磁盘满) 必须中止部署 —— 无备份重启
# 生产库, 健康门禁失败后的回退就没有恢复点。--skip-backup 显式豁免。
SKIP_BACKUP="${1:-}"
backup_ok=0
if .venv/bin/python - "$TAG" <<'PYEOF'
import sqlite3, sys, os
tag = sys.argv[1]
if not os.path.exists("data/ip_pool.db"):
    print("      (无 DB 文件 — 全新部署, 视为备份成功)")
    sys.exit(0)
try:
    src = sqlite3.connect("data/ip_pool.db")
    dst = sqlite3.connect(f"backups/{tag}/ip_pool.db")
    src.backup(dst)
    dst.close(); src.close()
    print("      ip_pool.db 已备份")
    sys.exit(0)
except sqlite3.Error as e:
    print(f"      (备份失败: {e})")
    sys.exit(1)
PYEOF
then
    backup_ok=1
fi
if [ "$backup_ok" != "1" ] && [ "$SKIP_BACKUP" != "--skip-backup" ]; then
    echo "[-] DB 备份失败且未指定 --skip-backup → 中止部署 (服务未动, 仍跑 $PREV)"
    rmdir "backups/$TAG" 2>/dev/null || true
    exit 1
fi
[ "$backup_ok" != "1" ] && echo "(!!) --skip-backup: 无备份继续部署"
echo "$PREV" > "backups/$TAG/prev_commit.txt"

echo "[4/6] systemd restart"
systemctl restart ip-pool-api ip-pool-gateway
sleep 5

healthy() {
    curl -sf --max-time 10 http://127.0.0.1:9100/health >/dev/null \
        && systemctl is-active --quiet ip-pool-api \
        && systemctl is-active --quiet ip-pool-gateway
}

echo "[5/6] 健康门禁"
# 宽限重试 (2026-11-14 假回退事故): api 装载 3000 节点需 ~7s, 单次 curl 在
# startup 中途必失败 -> 误回退 (回退后的二次检查同样查得太早, 报"人工介入"
# 实则服务稍后自行就绪)。30s x 6 次重试, 期间服务继续启动。
gate_ok=0
for _ in 1 2 3 4 5 6; do
    if healthy; then gate_ok=1; break; fi
    echo "      健康门禁未过, 5s 后重试..."
    sleep 5
done
if [ "$gate_ok" = "1" ]; then
    echo "[6/6] 备份保留清理 (最近 3 份 + 每月 1 日份永久)"
    ls -1dt backups/2* 2>/dev/null | tail -n +4 | while read -r d; do
        b=$(basename "$d")
        [ "${b:6:2}" = "01" ] && continue
        rm -rf "$d"
    done
    echo "[✓] 部署成功: $NEW (prev=$PREV)"
else
    echo "[-] 健康门禁失败 → 自动回退 $PREV + 重启"
    git reset --hard "$PREV"
    systemctl restart ip-pool-api ip-pool-gateway
    sleep 5
    rollback_ok=0
    for _ in 1 2 3 4 5 6; do
        if healthy; then rollback_ok=1; break; fi
        sleep 5
    done
    if [ "$rollback_ok" = "1" ]; then
        echo "[✓] 已回退并恢复: $PREV (故障 commit: $NEW)"
    else
        echo "[!!] 回退后仍不健康 — 需人工介入 (DB 备份: backups/$TAG/, 上一可用提交: $PREV)"
    fi
    exit 1
fi
