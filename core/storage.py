"""
core/storage.py - SQLite storage engine for IP Pool System
"""

import sqlite3
import json
import os
from typing import List, Optional, Dict
from datetime import datetime
from core.models import ProxyNode, ProbeResult, LeaseInfo, PoolStats, HealthStatus, EgressType

DB_PATH = os.environ.get("IP_POOL_DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "ip_pool.db"))


class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS nodes (
                    name TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    protocol TEXT NOT NULL DEFAULT 'socks5h',
                    region TEXT NOT NULL,
                    egress_type TEXT NOT NULL,
                    tags TEXT NOT NULL DEFAULT '[]',
                    weight INTEGER NOT NULL DEFAULT 100,
                    exit_ip TEXT,
                    latency_ms REAL,
                    status TEXT NOT NULL DEFAULT 'unprobed',
                    fail_count INTEGER NOT NULL DEFAULT 0,
                    last_probed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS leases (
                    account_id TEXT PRIMARY KEY,
                    node_name TEXT NOT NULL,
                    assigned_at TEXT NOT NULL,
                    FOREIGN KEY (node_name) REFERENCES nodes(name)
                );

                CREATE TABLE IF NOT EXISTS probe_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_name TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    exit_ip TEXT,
                    latency_ms REAL,
                    error TEXT,
                    probed_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_nodes_region ON nodes(region);
                CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
                CREATE INDEX IF NOT EXISTS idx_nodes_egress ON nodes(egress_type);
                CREATE INDEX IF NOT EXISTS idx_history_time ON probe_history(probed_at);
            """)

    def upsert_node(self, node: ProxyNode):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO nodes (name, host, port, protocol, region, egress_type, tags, weight, exit_ip, latency_ms, status, fail_count, last_probed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    host=excluded.host,
                    port=excluded.port,
                    protocol=excluded.protocol,
                    region=excluded.region,
                    egress_type=excluded.egress_type,
                    tags=excluded.tags,
                    weight=excluded.weight
            """, (
                node.name, node.host, node.port, node.protocol, node.region,
                node.egress_type, json.dumps(node.tags), node.weight,
                node.exit_ip, node.latency_ms, node.status, node.fail_count, node.last_probed_at
            ))

    def update_probe_result(self, res: ProbeResult):
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            # Record history
            conn.execute("""
                INSERT INTO probe_history (node_name, success, exit_ip, latency_ms, error, probed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (res.node_name, 1 if res.success else 0, res.exit_ip, res.latency_ms, res.error, now_str))

            # Update node
            if res.success:
                conn.execute("""
                    UPDATE nodes SET
                        exit_ip = ?,
                        latency_ms = ?,
                        status = 'healthy',
                        fail_count = 0,
                        last_probed_at = ?
                    WHERE name = ?
                """, (res.exit_ip, res.latency_ms, now_str, res.node_name))
            else:
                conn.execute("""
                    UPDATE nodes SET
                        fail_count = fail_count + 1,
                        status = CASE WHEN fail_count >= 2 THEN 'dead' ELSE 'degraded' END,
                        last_probed_at = ?
                    WHERE name = ?
                """, (now_str, res.node_name))

    def get_node(self, name: str) -> Optional[ProxyNode]:
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE name = ?", (name,)).fetchone()
            if row:
                return self._row_to_node(row)
        return None

    def list_nodes(self, region: Optional[str] = None, egress_type: Optional[str] = None, status: Optional[str] = None) -> List[ProxyNode]:
        query = "SELECT * FROM nodes WHERE 1=1"
        params = []
        if region:
            query += " AND region = ?"
            params.append(region)
        if egress_type:
            query += " AND egress_type = ?"
            params.append(egress_type)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY name ASC"

        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_node(r) for r in rows]

    def _row_to_node(self, row: sqlite3.Row) -> ProxyNode:
        return ProxyNode(
            name=row["name"],
            host=row["host"],
            port=row["port"],
            protocol=row["protocol"],
            region=row["region"],
            egress_type=row["egress_type"],
            tags=json.loads(row["tags"]),
            weight=row["weight"],
            exit_ip=row["exit_ip"],
            latency_ms=row["latency_ms"],
            status=row["status"],
            fail_count=row["fail_count"],
            last_probed_at=row["last_probed_at"]
        )

    def set_lease(self, account_id: str, node_name: str) -> LeaseInfo:
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO leases (account_id, node_name, assigned_at)
                VALUES (?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    node_name = excluded.node_name,
                    assigned_at = excluded.assigned_at
            """, (account_id, node_name, now_str))

        node = self.get_node(node_name)
        return LeaseInfo(
            account_id=account_id,
            node_name=node_name,
            proxy_url=node.proxy_url if node else "",
            exit_ip=node.exit_ip if node else None,
            region=node.region if node else "",
            egress_type=node.egress_type if node else "",
            assigned_at=now_str
        )

    def get_lease(self, account_id: str) -> Optional[LeaseInfo]:
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT l.account_id, l.node_name, l.assigned_at, n.host, n.port, n.protocol, n.exit_ip, n.region, n.egress_type
                FROM leases l
                JOIN nodes n ON l.node_name = n.name
                WHERE l.account_id = ?
            """, (account_id,)).fetchone()
            if row:
                return LeaseInfo(
                    account_id=row["account_id"],
                    node_name=row["node_name"],
                    proxy_url=f"{row['protocol']}://{row['host']}:{row['port']}",
                    exit_ip=row["exit_ip"],
                    region=row["region"],
                    egress_type=row["egress_type"],
                    assigned_at=row["assigned_at"]
                )
        return None

    def get_stats(self) -> PoolStats:
        with self._get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            healthy = conn.execute("SELECT COUNT(*) FROM nodes WHERE status = 'healthy'").fetchone()[0]
            degraded = conn.execute("SELECT COUNT(*) FROM nodes WHERE status = 'degraded'").fetchone()[0]
            dead = conn.execute("SELECT COUNT(*) FROM nodes WHERE status = 'dead'").fetchone()[0]
            unprobed = conn.execute("SELECT COUNT(*) FROM nodes WHERE status = 'unprobed'").fetchone()[0]

            warp_v6 = conn.execute("SELECT COUNT(*) FROM nodes WHERE egress_type = 'warp_ipv6'").fetchone()[0]
            nat_v4 = conn.execute("SELECT COUNT(*) FROM nodes WHERE egress_type = 'native_ipv4'").fetchone()[0]
            nat_v6 = conn.execute("SELECT COUNT(*) FROM nodes WHERE egress_type = 'native_ipv6'").fetchone()[0]

            regions = {}
            for row in conn.execute("SELECT region, COUNT(*) as c FROM nodes GROUP BY region").fetchall():
                regions[row["region"]] = row["c"]

            return PoolStats(
                total_nodes=total,
                healthy_nodes=healthy,
                degraded_nodes=degraded,
                dead_nodes=dead,
                unprobed_nodes=unprobed,
                warp_ipv6_count=warp_v6,
                native_ipv4_count=nat_v4,
                native_ipv6_count=nat_v6,
                region_distribution=regions
            )
