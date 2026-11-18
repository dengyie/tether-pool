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


class MetricsStore:
    """指标域: runtime_metrics 读写与聚合视图。"""

    def get_metrics_node_distribution(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT region, egress_type, status, tier, COUNT(*) as cnt
                FROM nodes
                GROUP BY region, egress_type, status, tier
            """).fetchall()
            return [{"region": r["region"], "egress_type": r["egress_type"], "status": r["status"], "tier": r["tier"] or "standard", "count": r["cnt"]} for r in rows]

    def get_metrics_bindings_distribution(self) -> List[Dict[str, Any]]:
        """
        Active account bindings grouped by region and egress type.

        Reads `account_allocations` (the authoritative binding table) rather than the
        legacy `leases` mirror; otherwise every account bound through the universal
        allocation layer is invisible to monitoring.
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT n.region, n.egress_type, n.tier, COUNT(*) as cnt
                FROM account_allocations a
                JOIN nodes n ON a.node_name = n.name
                GROUP BY n.region, n.egress_type, n.tier
            """).fetchall()
            return [{"region": r["region"], "egress_type": r["egress_type"], "tier": r["tier"] or "standard", "count": r["cnt"]} for r in rows]

    def get_metrics_probe_latencies(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT region, egress_type, AVG(latency_ms) as avg_latency, MAX(latency_ms) as max_latency, COUNT(*) as cnt
                FROM nodes
                WHERE latency_ms IS NOT NULL AND status = 'healthy'
                GROUP BY region, egress_type
            """).fetchall()
            return [{
                "region": r["region"],
                "egress_type": r["egress_type"],
                "avg_latency": round(r["avg_latency"] or 0.0, 2),
                "max_latency": round(r["max_latency"] or 0.0, 2),
                "count": r["cnt"]
            } for r in rows]

    def get_metrics_circuit_transitions(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT n.region, c.from_state, c.to_state, COUNT(*) as cnt
                FROM circuit_events c
                JOIN nodes n ON c.node_name = n.name
                GROUP BY n.region, c.from_state, c.to_state
            """).fetchall()
            return [{"region": r["region"], "from_state": r["from_state"], "to_state": r["to_state"], "count": r["cnt"]} for r in rows]

    def incr_metric(self, name: str, labels: Optional[Dict[str, str]] = None, value: float = 1.0):
        """Atomically increments a runtime counter persisted in SQLite (survives gateway restarts)."""
        label_json = json.dumps({k: str(v) for k, v in (labels or {}).items()}, sort_keys=True)
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO runtime_metrics (name, labels, value, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name, labels) DO UPDATE SET
                    value = runtime_metrics.value + excluded.value,
                    updated_at = excluded.updated_at
            """, (name, label_json, value, now_str))

    def incr_metrics(self, entries: List[Tuple[str, Optional[Dict[str, str]], float]]):
        """
        Increments several counters in ONE transaction.

        The gateway records up to seven histogram counters per proxied request; doing that
        as seven separate commits put SQLite write latency on the request's success path.
        """
        if not entries:
            return
        now_str = datetime.now().isoformat()
        rows = [
            (name, json.dumps({k: str(v) for k, v in (labels or {}).items()}, sort_keys=True), value, now_str)
            for name, labels, value in entries
        ]
        with self._get_conn() as conn:
            conn.executemany("""
                INSERT INTO runtime_metrics (name, labels, value, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name, labels) DO UPDATE SET
                    value = runtime_metrics.value + excluded.value,
                    updated_at = excluded.updated_at
            """, rows)

    def set_metric(self, name: str, labels: Optional[Dict[str, str]] = None, value: float = 0.0):
        """Overwrites a runtime gauge (last writer wins)."""
        label_json = json.dumps({k: str(v) for k, v in (labels or {}).items()}, sort_keys=True)
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO runtime_metrics (name, labels, value, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name, labels) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
            """, (name, label_json, value, now_str))

    def get_runtime_metrics(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute("SELECT name, labels, value, updated_at FROM runtime_metrics").fetchall()
            return [{
                "name": r["name"],
                "labels": json.loads(r["labels"]),
                "value": r["value"],
                "updated_at": r["updated_at"]
            } for r in rows]
