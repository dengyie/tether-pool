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
import warnings
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.storage_base import DB_PATH  # noqa: E402


def _parse_timestamp(value: str):
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return parsed.astimezone(datetime.now().astimezone().tzinfo)


def recovery_latency(row):
    """Return a sane historical dead duration, or None when timestamps are unusable."""
    dead_started = _parse_timestamp(row.get("dead_started"))
    recovered_at = _parse_timestamp(row.get("recovered_at"))
    if dead_started is None or recovered_at is None:
        return None
    seconds = (recovered_at - dead_started).total_seconds()
    return seconds if 0 <= seconds < 86400 * 7 else None


def recovery_rows(conn: sqlite3.Connection, since: str):
    """Match healthy transitions with the preceding dead event, even via DEGRADED."""
    since_dt = _parse_timestamp(since)
    if since_dt is None:
        raise ValueError(f"invalid audit window timestamp: {since!r}")

    nodes = {
        row[0]: row[1]
        for row in conn.execute("SELECT name, host FROM nodes")
    }
    events = conn.execute("""
        SELECT id, node_name, from_state, to_state, created_at
        FROM circuit_events
        ORDER BY id
    """).fetchall()

    dead_started = {}
    rows = []
    for event in events:
        event_id, name, from_state, to_state, created_at = event
        event_time = _parse_timestamp(created_at)
        if event_time is None:
            warnings.warn(
                f"skipping circuit event {event_id} with invalid timestamp "
                f"{created_at!r}", RuntimeWarning,
            )
            continue

        if to_state == "dead":
            dead_started[name] = created_at
        elif to_state == "healthy":
            if from_state in ("dead", "degraded", "half_open") and event_time >= since_dt:
                rows.append({
                    "node_name": name,
                    "recovered_at": created_at,
                    "dead_started": dead_started.get(name),
                    "host": nodes.get(name),
                })
            dead_started.pop(name, None)

    rows.sort(key=lambda row: _parse_timestamp(row["recovered_at"]), reverse=True)
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=1, help="统计窗口 (天, 默认 1)")
    args = ap.parse_args()
    since = (datetime.now().astimezone() - timedelta(days=args.days)).isoformat()

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    # 1. 恢复事件关联 circuit_events 中的 DEAD 锚点，兼容分阶段恢复。
    rows = recovery_rows(conn, since)
    latencies = [seconds for row in rows
                 if (seconds := recovery_latency(row)) is not None]

    print(f"== 恢复审计 (近 {args.days} 天, since {since[:19]}) ==")
    print(f"恢复事件: {len(rows)} 次")
    if latencies:
        latencies.sort()
        n = len(latencies)
        print(f"不可达时长: p50={latencies[n // 2]:.0f}s "
              f"p90={latencies[int(n * 0.9)]:.0f}s max={latencies[-1]:.0f}s")
    slow = [row for row in rows
            if (seconds := recovery_latency(row)) is not None and seconds > 1800]
    if slow:
        print(f"⚠️ 恢复 >30min 的节点 ({len(slow)}):")
        for row in slow[:10]:
            print(f"   {row['node_name']} (host={row['host']})")

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
