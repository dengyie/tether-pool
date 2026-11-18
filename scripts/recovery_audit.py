#!/usr/bin/env python3
"""recovery_audit.py — 断路恢复审计日报 (2026-11-19 P2-6)。

从 circuit_events 统计近 24h 的 dead→healthy 恢复时延 (dead_since 锚 →
恢复时刻) 与主机级 dead 聚集, 挂 cron 每日输出。供容量/质量趋势与
"恢复比底层慢" 类问题的量化追踪。

用法: python3 scripts/recovery_audit.py [--days 1]
"""
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.storage_base import DB_PATH  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=1, help="统计窗口 (天, 默认 1)")
    args = ap.parse_args()
    since = (datetime.now() - timedelta(days=args.days)).isoformat()

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    # 1. 恢复事件 (dead/degraded/half_open -> healthy) 时延
    rows = conn.execute("""
        SELECT e.node_name, e.created_at AS recovered_at, n.dead_since, n.host
        FROM circuit_events e JOIN nodes n ON n.name = e.node_name
        WHERE e.to_state = 'healthy' AND e.from_state IN ('dead', 'half_open')
          AND e.created_at >= ?
        ORDER BY e.created_at DESC
    """, (since,)).fetchall()

    latencies = []
    for r in rows:
        if r["dead_since"]:
            try:
                dt = (datetime.fromisoformat(r["recovered_at"])
                      - datetime.fromisoformat(r["dead_since"])).total_seconds()
                if 0 <= dt < 86400 * 7:
                    latencies.append(dt)
            except ValueError:
                pass

    print(f"== 恢复审计 (近 {args.days} 天, since {since[:19]}) ==")
    print(f"恢复事件: {len(rows)} 次")
    if latencies:
        latencies.sort()
        n = len(latencies)
        print(f"不可达时长: p50={latencies[n // 2]:.0f}s "
              f"p90={latencies[int(n * 0.9)]:.0f}s max={latencies[-1]:.0f}s")
    slow = [r for r in rows if r["dead_since"] and (
        datetime.fromisoformat(r["recovered_at"])
        - datetime.fromisoformat(r["dead_since"])).total_seconds() > 1800]
    if slow:
        print(f"⚠️ 恢复 >30min 的节点 ({len(slow)}):")
        for r in slow[:10]:
            print(f"   {r['node_name']} (host={r['host']})")

    # 2. 主机级 dead 聚集 (单 sweep 把主机事件打散成 N 断路的量化)
    hosts = conn.execute("""
        SELECT n.host, COUNT(*) AS dead
        FROM nodes n WHERE n.status = 'dead'
        GROUP BY n.host HAVING dead >= 3 ORDER BY dead DESC LIMIT 10
    """).fetchall()
    if hosts:
        print("当前主机级 dead 聚集 (≥3):")
        for h in hosts:
            print(f"   {h['host']}: {h['dead']} dead")


if __name__ == "__main__":
    main()
