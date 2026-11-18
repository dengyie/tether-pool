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


class EventStore:
    """事件域: 断路事件与 battery 事件流水。"""

    def record_circuit_event(self, node_name: str, from_state: str, to_state: str, reason: str) -> CircuitEvent:
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("""
                INSERT INTO circuit_events (node_name, from_state, to_state, reason, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (node_name, from_state, to_state, reason, now_str))
            return CircuitEvent(
                id=cur.lastrowid,
                node_name=node_name,
                from_state=from_state,
                to_state=to_state,
                reason=reason,
                created_at=now_str
            )

    def list_circuit_events(self, limit: int = 50) -> List[CircuitEvent]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT id, node_name, from_state, to_state, reason, created_at
                FROM circuit_events
                ORDER BY id DESC LIMIT ?
            """, (limit,)).fetchall()
            return [
                CircuitEvent(
                    id=r["id"],
                    node_name=r["node_name"],
                    from_state=r["from_state"],
                    to_state=r["to_state"],
                    reason=r["reason"],
                    created_at=r["created_at"]
                )
                for r in rows
            ]

    def record_battery_event(self, report: BatteryEventReport) -> int:
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("""
                INSERT INTO battery_events (account_id, node_name, event_type, target_service, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (report.account_id, report.node_name, report.event_type, report.target_service, now_str))
            return cur.lastrowid

    def list_battery_events(self, account_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        query = "SELECT * FROM battery_events"
        params = []
        if account_id:
            query += " WHERE account_id = ?"
            params.append(account_id)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def prune_circuit_events(self, keep_days: int = 7) -> int:
        """Deletes circuit_events rows older than keep_days (append-only table)."""
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        return self._chunked_prune("circuit_events", "created_at", cutoff)

    def prune_battery_events(self, keep_days: int = 7) -> int:
        """Deletes battery_events rows older than keep_days (append-only table)."""
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        return self._chunked_prune("battery_events", "created_at", cutoff)

    def prune_event_tables(self, keep_days: int = 7) -> Dict[str, int]:
        """Applies the same retention window to every append-only event table.

        ban_events 固定 30 天窗 (归因审计数据, 不随 keep_days 收窄), 但始终在
        返回值里出现, 便于监控核对。
        """
        return {
            "probe_history": self.prune_probe_history(keep_days),
            "circuit_events": self.prune_circuit_events(keep_days),
            "battery_events": self.prune_battery_events(keep_days),
            "binding_changes": self.prune_binding_changes(keep_days),
            "ban_events": self.prune_ban_events(keep_days=30),
            "ip_cooldowns": self.prune_expired_cooldowns(),
        }
