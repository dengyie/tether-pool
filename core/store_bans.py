"""
core/storage.py - SQLite storage engine for IP Pool System with Circuit Breaker support
"""

import sqlite3
import json
import os
import logging
import threading
from typing import List, Optional, Dict, Tuple, Any
from datetime import datetime, timedelta
from contextlib import contextmanager
from core.models import (
    ProxyNode, ProbeResult, LeaseInfo, PoolStats, HealthStatus,
    CircuitEvent, RelayMode, IpCooldownRecord, BatteryEventReport,
    Tier, BindingChangeRecord, BanRecord
)
from core.circuit import CircuitBreaker
from core.storage_base import region_from_country
from core.quality import IPQualityEvaluator


class BanStore:
    """封禁域: IP 冷却 / quarantine / ban 流水。"""

    def add_ip_cooldown(self, exit_ip: str, target_service: str, node_name: str, cooldown_seconds: int = 1800) -> IpCooldownRecord:
        """Locks an exit IP into cooldown for target_service to prevent registration anti-sybil bans."""
        now = datetime.now()
        cooled_until = (now + timedelta(seconds=cooldown_seconds)).isoformat()
        now_str = now.isoformat()
        with self._get_conn() as conn:
            # Keep one live row per (exit_ip, target_service) instead of appending
            # duplicates. 2026-11-15 review P2: DELETE+INSERT 原子化 —— 并发 add
            # 曾交错成 DELETE/DELETE/INSERT/INSERT 落两行活记录。
            with self._immediate_txn(conn):
                conn.execute(
                    "DELETE FROM ip_cooldowns WHERE exit_ip = ? AND target_service = ?",
                    (exit_ip, target_service)
                )
                cur = conn.execute("""
                    INSERT INTO ip_cooldowns (exit_ip, target_service, node_name, cooled_until, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (exit_ip, target_service, node_name, cooled_until, now_str))
                _rowid = cur.lastrowid
        return IpCooldownRecord(
            id=_rowid,
            exit_ip=exit_ip,
            target_service=target_service,
            node_name=node_name,
            cooled_until=cooled_until,
            created_at=now_str
        )

    def is_ip_cooled(self, exit_ip: str, target_service: str) -> bool:
        """Checks if an exit IP is currently cooled for the specific target service."""
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT id FROM ip_cooldowns
                WHERE exit_ip = ? AND target_service = ? AND cooled_until > ?
                LIMIT 1
            """, (exit_ip, target_service, now_str)).fetchone()
            return row is not None

    def get_active_cooldowns(self, target_service: Optional[str] = None) -> List[IpCooldownRecord]:
        now_str = datetime.now().isoformat()
        query = "SELECT * FROM ip_cooldowns WHERE cooled_until > ?"
        params = [now_str]
        if target_service:
            query += " AND target_service = ?"
            params.append(target_service)
        query += " ORDER BY cooled_until ASC"
        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [
                IpCooldownRecord(
                    id=r["id"],
                    exit_ip=r["exit_ip"],
                    target_service=r["target_service"],
                    node_name=r["node_name"],
                    cooled_until=r["cooled_until"],
                    created_at=r["created_at"]
                )
                for r in rows
            ]

    def release_ip_cooldown(self, exit_ip: str, target_service: str) -> bool:
        """Manually unlocks an IP from cooldown (e.g. if registration failed before submission)."""
        with self._get_conn() as conn:
            cur = conn.execute("""
                DELETE FROM ip_cooldowns
                WHERE exit_ip = ? AND target_service = ?
            """, (exit_ip, target_service))
            return cur.rowcount > 0

    def clean_expired_cooldowns(self) -> int:
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("DELETE FROM ip_cooldowns WHERE cooled_until <= ?", (now_str,))
            return cur.rowcount

    def _ban_row_to_record(self, r: sqlite3.Row) -> BanRecord:
        # evidence 落库时是 json.dumps 的字符串, 出参反序列化回对象 —— 消费方
        # 不必二次 parse (2026-11-14 review F8)。历史坏行兜底原样字符串。
        raw_evidence = r["evidence"] or "{}"
        try:
            evidence: object = json.loads(raw_evidence)
            if not isinstance(evidence, dict):
                evidence = raw_evidence
        except (ValueError, TypeError):
            evidence = raw_evidence
        return BanRecord(
            id=r["id"],
            ban_id=r["ban_id"],
            service=r["service"],
            account=r["account"],
            node_name=r["node_name"],
            exit_ip=r["exit_ip"],
            isp=r["isp"],
            reason=r["reason"],
            evidence=evidence,
            quarantined_until=r["quarantined_until"],
            released_at=r["released_at"],
            created_at=r["created_at"],
        )

    def record_ban(
        self,
        service: str,
        account: str,
        node_name: str,
        exit_ip: Optional[str],
        isp: Optional[str],
        reason: str,
        evidence: Optional[Dict[str, object]] = None,
        quarantine_hours: int = 24,
    ) -> Tuple[BanRecord, bool]:
        """落库 ban 事件并返回 (record, idempotent)。

        幂等语义（需求 §4.1）：同 (service, node_name, reason) 存在未解除且未过期
        的隔离行时返回原行（ban_id 不变、时长不叠加），quarantine_hours=0 仅记录
        （quarantined_until=NULL，永不进入 active 集）。
        """
        import uuid
        now = datetime.now()
        now_str = now.isoformat()
        until_str = (now + timedelta(hours=quarantine_hours)).isoformat() if quarantine_hours > 0 else None
        with self._get_conn() as conn:
            # BEGIN IMMEDIATE 提前拿写锁: 幂等 SELECT 与 INSERT 必须在同一写事务里,
            # 否则两线程可在 SELECT(空)->INSERT 间隙各插一行, 产生双 active 行
            # (ban_id 分叉、gauge 虚高)。busy_timeout 兜住等待方。
            conn.execute("BEGIN IMMEDIATE")
            if quarantine_hours > 0:
                row = conn.execute("""
                    SELECT * FROM ban_events
                    WHERE service = ? AND node_name = ? AND reason = ?
                      AND quarantined_until IS NOT NULL AND quarantined_until > ?
                      AND released_at IS NULL
                    ORDER BY id DESC LIMIT 1
                """, (service, node_name, reason, now_str)).fetchone()
                if row:
                    return self._ban_row_to_record(row), True
            ban_id = f"ban-{uuid.uuid4().hex[:12]}"
            cur = conn.execute("""
                INSERT INTO ban_events (ban_id, service, account, node_name, exit_ip, isp, reason, evidence, quarantined_until, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ban_id, service, account, node_name, exit_ip, isp, reason,
                json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True),
                until_str, now_str,
            ))
            row = conn.execute("SELECT * FROM ban_events WHERE id = ?", (cur.lastrowid,)).fetchone()
            return self._ban_row_to_record(row), False

    def get_active_quarantines(self, service: Optional[str] = None) -> List[BanRecord]:
        now_str = datetime.now().isoformat()
        query = """
            SELECT * FROM ban_events
            WHERE quarantined_until IS NOT NULL AND quarantined_until > ? AND released_at IS NULL
        """
        params = [now_str]
        if service:
            query += " AND service = ?"
            params.append(service)
        query += " ORDER BY quarantined_until ASC"
        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._ban_row_to_record(r) for r in rows]

    def list_bans(self, service: Optional[str] = None, limit: int = 100) -> List[BanRecord]:
        query = "SELECT * FROM ban_events"
        params: List[object] = []
        if service:
            query += " WHERE service = ?"
            params.append(service)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(int(limit))
        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._ban_row_to_record(r) for r in rows]

    def count_bans_since(self, service: str, since_iso: str) -> int:
        """限流用：该 service 自 since 以来的 ban 上报条数（含只记录的）。"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM ban_events WHERE service = ? AND created_at > ?",
                (service, since_iso),
            ).fetchone()
            return int(row["cnt"]) if row else 0

    def release_quarantine(self, service: str, node_name: str) -> int:
        """人工解除（P1）：置 released_at，返回解除的行数。"""
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("""
                UPDATE ban_events SET released_at = ?
                WHERE service = ? AND node_name = ? AND released_at IS NULL
                  AND quarantined_until IS NOT NULL AND quarantined_until > ?
            """, (now_str, service, node_name, now_str))
            return cur.rowcount

    def clean_expired_quarantines(self) -> int:
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("""
                UPDATE ban_events SET released_at = ?
                WHERE quarantined_until IS NOT NULL AND quarantined_until <= ? AND released_at IS NULL
            """, (now_str, now_str))
            return cur.rowcount

    def prune_ban_events(self, keep_days: int = 30) -> int:
        """ban_events 保留窗: 注意注册进 prune_event_tables 时用固定 30 天
        (封禁历史是归因审计数据, 比其他 event 表的 7 天窗口长)。直接调用时可
        传 keep_days=0 清空 (测试/运维)。"""
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        return self._chunked_prune("ban_events", "created_at", cutoff)

    def prune_expired_cooldowns(self) -> int:
        """ip_cooldowns 保留窗 (#8, 二轮 review): 过期行原先只在注册分配路径
        (_acquire_for_registration) 清理 —— 不跑注册业务的池子无限膨胀。挂进
        prune_event_tables 随每轮探活清扫。"""
        return self.clean_expired_cooldowns()
