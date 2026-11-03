"""
core/storage.py - SQLite storage engine for IP Pool System with Circuit Breaker support
"""

import sqlite3
import json
import os
from typing import List, Optional, Dict, Tuple, Any
from datetime import datetime
from contextlib import contextmanager
from core.models import ProxyNode, ProbeResult, LeaseInfo, PoolStats, HealthStatus, EgressType, CircuitEvent
from core.circuit import CircuitBreaker

DB_PATH = os.environ.get("IP_POOL_DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "ip_pool.db"))


class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
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
                    last_probed_at TEXT,
                    circuit_state TEXT DEFAULT 'unprobed',
                    consecutive_failures INTEGER DEFAULT 0,
                    consecutive_successes INTEGER DEFAULT 0,
                    next_probe_allowed_at TEXT
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

                CREATE TABLE IF NOT EXISTS circuit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_name TEXT NOT NULL,
                    from_state TEXT NOT NULL,
                    to_state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_nodes_region ON nodes(region);
                CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
                CREATE INDEX IF NOT EXISTS idx_nodes_egress ON nodes(egress_type);
                CREATE INDEX IF NOT EXISTS idx_history_time ON probe_history(probed_at);
                CREATE INDEX IF NOT EXISTS idx_circuit_events_node ON circuit_events(node_name);
                CREATE INDEX IF NOT EXISTS idx_circuit_events_time ON circuit_events(created_at);
            """)

            # Migration for existing databases
            existing_cols = [r["name"] for r in conn.execute("PRAGMA table_info(nodes)").fetchall()]
            for col, col_type, default_val in [
                ("circuit_state", "TEXT", "'unprobed'"),
                ("consecutive_failures", "INTEGER", "0"),
                ("consecutive_successes", "INTEGER", "0"),
                ("next_probe_allowed_at", "TEXT", "NULL")
            ]:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE nodes ADD COLUMN {col} {col_type} DEFAULT {default_val}")

    def upsert_node(self, node: ProxyNode):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO nodes (
                    name, host, port, protocol, region, egress_type, tags, weight,
                    exit_ip, latency_ms, status, fail_count, last_probed_at,
                    circuit_state, consecutive_failures, consecutive_successes, next_probe_allowed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    host=excluded.host,
                    port=excluded.port,
                    protocol=excluded.protocol,
                    region=excluded.region,
                    egress_type=excluded.egress_type,
                    tags=excluded.tags,
                    weight=excluded.weight,
                    status=CASE WHEN excluded.status != 'unprobed' THEN excluded.status ELSE nodes.status END,
                    circuit_state=CASE WHEN excluded.circuit_state != 'unprobed' THEN excluded.circuit_state ELSE nodes.circuit_state END,
                    consecutive_failures=CASE WHEN excluded.status != 'unprobed' THEN excluded.consecutive_failures ELSE nodes.consecutive_failures END,
                    consecutive_successes=CASE WHEN excluded.status != 'unprobed' THEN excluded.consecutive_successes ELSE nodes.consecutive_successes END,
                    next_probe_allowed_at=CASE WHEN excluded.status != 'unprobed' THEN excluded.next_probe_allowed_at ELSE nodes.next_probe_allowed_at END
            """, (
                node.name, node.host, node.port, node.protocol, node.region,
                node.egress_type, json.dumps(node.tags), node.weight,
                node.exit_ip, node.latency_ms, node.status, node.fail_count, node.last_probed_at,
                node.circuit_state or node.status, node.consecutive_failures,
                node.consecutive_successes, node.next_probe_allowed_at
            ))

    def update_node_status(self, node_name: str, status: str):
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE nodes SET
                    status = ?,
                    circuit_state = ?
                WHERE name = ?
            """, (status, status, node_name))

    def update_probe_result(self, res: ProbeResult) -> Tuple[ProbeResult, Optional[CircuitEvent]]:
        """
        Records probe history and evaluates circuit breaker transition.
        Returns: (res, optional_circuit_event_if_transitioned)
        """
        now_str = datetime.now().isoformat()
        transition_event: Optional[CircuitEvent] = None

        with self._get_conn() as conn:
            # 1. Record probe history
            conn.execute("""
                INSERT INTO probe_history (node_name, success, exit_ip, latency_ms, error, probed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (res.node_name, 1 if res.success else 0, res.exit_ip, res.latency_ms, res.error, now_str))

            # 2. Get current node state
            row = conn.execute("SELECT * FROM nodes WHERE name = ?", (res.node_name,)).fetchone()
            if not row:
                return res, None

            node = self._row_to_node(row)
            old_state = node.status

            # 3. Evaluate circuit transition
            new_state, reason, next_allowed = CircuitBreaker.evaluate(
                node=node,
                success=res.success,
                latency_ms=res.latency_ms,
                error=res.error
            )

            next_allowed_str = next_allowed.isoformat() if next_allowed else None

            if res.success:
                new_fails = 0
                new_succs = node.consecutive_successes + 1
                conn.execute("""
                    UPDATE nodes SET
                        exit_ip = ?,
                        latency_ms = ?,
                        status = ?,
                        circuit_state = ?,
                        fail_count = 0,
                        consecutive_failures = ?,
                        consecutive_successes = ?,
                        next_probe_allowed_at = ?,
                        last_probed_at = ?
                    WHERE name = ?
                """, (
                    res.exit_ip, res.latency_ms, new_state, new_state,
                    new_fails, new_succs, next_allowed_str, now_str, res.node_name
                ))
            else:
                new_fails = node.consecutive_failures + 1
                new_succs = 0
                conn.execute("""
                    UPDATE nodes SET
                        fail_count = fail_count + 1,
                        status = ?,
                        circuit_state = ?,
                        consecutive_failures = ?,
                        consecutive_successes = ?,
                        next_probe_allowed_at = ?,
                        last_probed_at = ?
                    WHERE name = ?
                """, (
                    new_state, new_state, new_fails, new_succs,
                    next_allowed_str, now_str, res.node_name
                ))

            # 4. Record circuit event if state changed
            if old_state != new_state:
                cur = conn.execute("""
                    INSERT INTO circuit_events (node_name, from_state, to_state, reason, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (res.node_name, old_state, new_state, reason, now_str))
                event_id = cur.lastrowid
                transition_event = CircuitEvent(
                    id=event_id,
                    node_name=res.node_name,
                    from_state=old_state,
                    to_state=new_state,
                    reason=reason,
                    created_at=now_str
                )

        return res, transition_event

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
        keys = row.keys()
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
            last_probed_at=row["last_probed_at"],
            circuit_state=row["circuit_state"] if "circuit_state" in keys and row["circuit_state"] else row["status"],
            consecutive_failures=row["consecutive_failures"] if "consecutive_failures" in keys and row["consecutive_failures"] else 0,
            consecutive_successes=row["consecutive_successes"] if "consecutive_successes" in keys and row["consecutive_successes"] else 0,
            next_probe_allowed_at=row["next_probe_allowed_at"] if "next_probe_allowed_at" in keys else None
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

    def get_leases_for_node(self, node_name: str) -> List[LeaseInfo]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT l.account_id, l.node_name, l.assigned_at, n.host, n.port, n.protocol, n.exit_ip, n.region, n.egress_type
                FROM leases l
                JOIN nodes n ON l.node_name = n.name
                WHERE l.node_name = ?
            """, (node_name,)).fetchall()
            return [
                LeaseInfo(
                    account_id=r["account_id"],
                    node_name=r["node_name"],
                    proxy_url=f"{r['protocol']}://{r['host']}:{r['port']}",
                    exit_ip=r["exit_ip"],
                    region=r["region"],
                    egress_type=r["egress_type"],
                    assigned_at=r["assigned_at"]
                )
                for r in rows
            ]

    def count_all_leases(self) -> int:
        with self._get_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM leases").fetchone()[0]

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

    def reset_node_circuit(self, node_name: str) -> bool:
        """Manual operator reset of a tripped node back to healthy"""
        node = self.get_node(node_name)
        if not node:
            return False
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE nodes SET
                    status = 'healthy',
                    circuit_state = 'healthy',
                    fail_count = 0,
                    consecutive_failures = 0,
                    next_probe_allowed_at = NULL
                WHERE name = ?
            """, (node_name,))
        self.record_circuit_event(node_name, node.status, "healthy", "operator_manual_reset")
        return True

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

    def get_metrics_node_distribution(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT region, egress_type, status, COUNT(*) as cnt
                FROM nodes
                GROUP BY region, egress_type, status
            """).fetchall()
            return [{"region": r["region"], "egress_type": r["egress_type"], "status": r["status"], "count": r["cnt"]} for r in rows]

    def get_metrics_leases_distribution(self) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT n.region, n.egress_type, COUNT(*) as cnt
                FROM leases l
                JOIN nodes n ON l.node_name = n.name
                GROUP BY n.region, n.egress_type
            """).fetchall()
            return [{"region": r["region"], "egress_type": r["egress_type"], "count": r["cnt"]} for r in rows]

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

