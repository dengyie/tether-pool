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
# - 健康门禁 = /health + 双 systemd unit active; 失败即回退上一 commit 再重启,
#   回退后仍不健康则明确报人工介入 (备份目录里 prev_commit.txt 可追溯)
set -euo pipefail
cd "$(dirname "$0")/.."

PREV=$(git rev-parse HEAD)
echo "[1/6] git pull (当前 $PREV)"
if ! git pull --ff-only origin main >/dev/null 2>&1; then
    echo "[-] pull 失败 (远端分叉或网络) — 保持原状, 未动任何服务"
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
.venv/bin/python - "$TAG" <<'PYEOF'
import sqlite3, sys
tag = sys.argv[1]
try:
    src = sqlite3.connect("data/ip_pool.db")
    dst = sqlite3.connect(f"backups/{tag}/ip_pool.db")
    src.backup(dst)
    dst.close(); src.close()
    print("      ip_pool.db 已备份")
except sqlite3.OperationalError as e:
    print(f"      (无 DB 或不可读, 跳过: {e})")
PYEOF
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
if healthy; then
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
    if healthy; then
        echo "[✓] 已回退并恢复: $PREV (故障 commit: $NEW)"
    else
        echo "[!!] 回退后仍不健康 — 需人工介入 (DB 备份: backups/$TAG/, 上一可用提交: $PREV)"
    fi
    exit 1
fi
