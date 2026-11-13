"""
core/storage.py - SQLite storage engine for IP Pool System with Circuit Breaker support
"""

import sqlite3
import json
import os
import threading
from typing import List, Optional, Dict, Tuple, Any
from datetime import datetime, timedelta
from contextlib import contextmanager
from core.models import (
    ProxyNode, ProbeResult, LeaseInfo, PoolStats, HealthStatus,
    CircuitEvent, RelayMode, IpCooldownRecord, BatteryEventReport,
    Tier, BindingChangeRecord
)
from core.circuit import CircuitBreaker
from core.quality import IPQualityEvaluator

DB_PATH = os.environ.get("IP_POOL_DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "ip_pool.db"))


class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        # abspath() keeps bare filenames ("pool.db") from yielding an empty dirname
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        # 线程本地连接复用 (数据流优化 2026-11-14): 旧实现每次 _get_conn 都
        # sqlite3.connect+close —— 网关粘性请求一次要开 4 个连接。WAL 模式下
        # 复用同线程连接是安全的 (每操作仍 commit/rollback, 语义不变);
        # sqlite3.connect(timeout=10.0) 自带 busy handler, 与逐次新建一致。
        self._thread_local = threading.local()
        self._init_db()

    @contextmanager
    def _get_conn(self):
        conn = getattr(self._thread_local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            self._thread_local.conn = conn
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

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
                    next_probe_allowed_at TEXT,
                    relay_mode TEXT DEFAULT 'relay_fallback',
                    relay_hop_node TEXT,
                    tier TEXT DEFAULT 'standard',
                    configured_tier TEXT,
                    quality_score INTEGER DEFAULT 50,
                    fault_domain TEXT DEFAULT '',
                    provider_region TEXT DEFAULT '',
                    self_reported_at TEXT
                );

                CREATE TABLE IF NOT EXISTS leases (
                    account_id TEXT PRIMARY KEY,
                    node_name TEXT NOT NULL,
                    assigned_at TEXT NOT NULL,
                    rebind_count INTEGER DEFAULT 0,
                    last_rebind_reason TEXT DEFAULT '',
                    bound_region TEXT DEFAULT '',
                    bound_region_strict INTEGER DEFAULT 0,
                    bound_egress_type TEXT DEFAULT '',
                    bound_tier TEXT DEFAULT '',
                    bound_tier_policy TEXT DEFAULT '',
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

                CREATE TABLE IF NOT EXISTS ip_cooldowns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exit_ip TEXT NOT NULL,
                    target_service TEXT NOT NULL,
                    node_name TEXT NOT NULL,
                    cooled_until TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS battery_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    node_name TEXT,
                    event_type TEXT NOT NULL,
                    target_service TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS account_allocations (
                    service TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    node_name TEXT NOT NULL,
                    allocated_at TEXT NOT NULL,
                    rebind_count INTEGER DEFAULT 0,
                    last_rebind_reason TEXT DEFAULT '',
                    bound_region TEXT DEFAULT '',
                    bound_region_strict INTEGER DEFAULT 0,
                    bound_egress_type TEXT DEFAULT '',
                    bound_tier TEXT DEFAULT '',
                    bound_tier_policy TEXT DEFAULT '',
                    PRIMARY KEY (service, account_id),
                    FOREIGN KEY (node_name) REFERENCES nodes(name)
                );

                CREATE TABLE IF NOT EXISTS binding_epoch_state (
                    service TEXT PRIMARY KEY,
                    epoch INTEGER NOT NULL DEFAULT 0,
                    changed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS binding_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    service TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    account_id TEXT NOT NULL,
                    from_node TEXT,
                    to_node TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runtime_metrics (
                    name TEXT NOT NULL,
                    labels TEXT NOT NULL,
                    value REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (name, labels)
                );

                CREATE INDEX IF NOT EXISTS idx_nodes_region ON nodes(region);
                CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
                CREATE INDEX IF NOT EXISTS idx_nodes_egress ON nodes(egress_type);
                CREATE INDEX IF NOT EXISTS idx_history_time ON probe_history(probed_at);
                CREATE INDEX IF NOT EXISTS idx_circuit_events_node ON circuit_events(node_name);
                CREATE INDEX IF NOT EXISTS idx_circuit_events_time ON circuit_events(created_at);
                CREATE INDEX IF NOT EXISTS idx_cooldown_lookup ON ip_cooldowns(exit_ip, target_service, cooled_until);
                CREATE INDEX IF NOT EXISTS idx_battery_events_acc ON battery_events(account_id);
                CREATE INDEX IF NOT EXISTS idx_allocations_service ON account_allocations(service);
                CREATE INDEX IF NOT EXISTS idx_allocations_node ON account_allocations(node_name);
            """)

            # Migration for existing databases
            existing_cols = [r["name"] for r in conn.execute("PRAGMA table_info(nodes)").fetchall()]
            for col, col_type, default_val in [
                ("circuit_state", "TEXT", "'unprobed'"),
                ("consecutive_failures", "INTEGER", "0"),
                ("consecutive_successes", "INTEGER", "0"),
                ("next_probe_allowed_at", "TEXT", "NULL"),
                ("relay_mode", "TEXT", "'relay_fallback'"),
                ("relay_hop_node", "TEXT", "NULL"),
                ("tier", "TEXT", "'standard'"),
                ("quality_score", "INTEGER", "50"),
                ("configured_tier", "TEXT", "NULL"),
                ("fault_domain", "TEXT", "''"),
                ("provider_region", "TEXT", "''"),
                ("self_reported_at", "TEXT", "NULL")
            ]:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE nodes ADD COLUMN {col} {col_type} DEFAULT {default_val}")

            lease_cols = [r["name"] for r in conn.execute("PRAGMA table_info(leases)").fetchall()]
            for col, col_type, default_val in [
                ("rebind_count", "INTEGER", "0"),
                ("last_rebind_reason", "TEXT", "''"),
                # Binding-constraint set persisted with the lease mirror (auto_rehome must
                # respect the constraints the credential declared when the binding was made).
                ("bound_region", "TEXT", "''"),
                ("bound_region_strict", "INTEGER", "0"),
                ("bound_egress_type", "TEXT", "''"),
                ("bound_tier", "TEXT", "''"),
                ("bound_tier_policy", "TEXT", "''"),
            ]:
                if col not in lease_cols:
                    conn.execute(f"ALTER TABLE leases ADD COLUMN {col} {col_type} DEFAULT {default_val}")

            alloc_cols = [r["name"] for r in conn.execute("PRAGMA table_info(account_allocations)").fetchall()]
            for col, col_type, default_val in [
                ("rebind_count", "INTEGER", "0"),
                ("last_rebind_reason", "TEXT", "''"),
                ("bound_region", "TEXT", "''"),
                ("bound_region_strict", "INTEGER", "0"),
                ("bound_egress_type", "TEXT", "''"),
                ("bound_tier", "TEXT", "''"),
                ("bound_tier_policy", "TEXT", "''"),
            ]:
                if col not in alloc_cols:
                    conn.execute(f"ALTER TABLE account_allocations ADD COLUMN {col} {col_type} DEFAULT {default_val}")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_tier ON nodes(tier);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_status_tier_region ON nodes(status, tier, region);")
            # Created here (not in the script above) so legacy DBs get the columns migrated
            # first — CREATE INDEX on a yet-unmigrated table would raise
            # "no such column" on databases created before fault domains existed.
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_fault_domain ON nodes(fault_domain, host);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_binding_changes_epoch ON binding_changes(service, epoch);")

    def upsert_node(self, node: ProxyNode):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO nodes (
                    name, host, port, protocol, region, egress_type, tags, weight,
                    exit_ip, latency_ms, status, fail_count, last_probed_at,
                    circuit_state, consecutive_failures, consecutive_successes, next_probe_allowed_at,
                    relay_mode, relay_hop_node, tier, configured_tier, quality_score,
                    fault_domain, provider_region, self_reported_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    host=excluded.host,
                    port=excluded.port,
                    protocol=excluded.protocol,
                    region=excluded.region,
                    egress_type=excluded.egress_type,
                    tags=excluded.tags,
                    weight=excluded.weight,
                    relay_mode=excluded.relay_mode,
                    relay_hop_node=excluded.relay_hop_node,
                    configured_tier=COALESCE(excluded.configured_tier, nodes.configured_tier),
                    tier=CASE WHEN nodes.status != 'unprobed' AND excluded.status = 'unprobed' THEN nodes.tier ELSE excluded.tier END,
                    quality_score=CASE WHEN nodes.status != 'unprobed' AND excluded.status = 'unprobed' THEN nodes.quality_score ELSE excluded.quality_score END,
                    status=CASE WHEN excluded.status != 'unprobed' THEN excluded.status ELSE nodes.status END,
                    circuit_state=CASE WHEN excluded.circuit_state != 'unprobed' THEN excluded.circuit_state ELSE nodes.circuit_state END,
                    consecutive_failures=CASE WHEN excluded.status != 'unprobed' THEN excluded.consecutive_failures ELSE nodes.consecutive_failures END,
                    consecutive_successes=CASE WHEN excluded.status != 'unprobed' THEN excluded.consecutive_successes ELSE nodes.consecutive_successes END,
                    next_probe_allowed_at=CASE WHEN excluded.status != 'unprobed' THEN excluded.next_probe_allowed_at ELSE nodes.next_probe_allowed_at END,
                    fault_domain=CASE WHEN excluded.fault_domain != '' THEN excluded.fault_domain ELSE nodes.fault_domain END,
                    provider_region=CASE WHEN excluded.provider_region != '' THEN excluded.provider_region ELSE nodes.provider_region END,
                    self_reported_at=COALESCE(excluded.self_reported_at, nodes.self_reported_at)
            """, (
                node.name, node.host, node.port, node.protocol, node.region,
                node.egress_type, json.dumps(node.tags), node.weight,
                node.exit_ip, node.latency_ms, node.status, node.fail_count, node.last_probed_at,
                node.circuit_state or node.status, node.consecutive_failures,
                node.consecutive_successes, node.next_probe_allowed_at,
                node.relay_mode, node.relay_hop_node,
                node.tier or Tier.STANDARD.value, node.configured_tier, node.quality_score or 50,
                node.fault_domain or "", node.provider_region or "", node.self_reported_at
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

            # Calculate updated quality score & tier
            new_fails = 0 if res.success else (node.consecutive_failures + 1)
            new_succs = (node.consecutive_successes + 1) if res.success else 0
            cur_lat = res.latency_ms if res.success else node.latency_ms
            cur_ip = res.exit_ip if res.success else node.exit_ip
            q_score, q_tier, _ = IPQualityEvaluator.evaluate_node(
                tags=node.tags,
                egress_type=node.egress_type,
                latency_ms=cur_lat,
                status=new_state,
                consecutive_failures=new_fails,
                exit_ip=cur_ip,
                configured_tier=node.configured_tier
            )

            if res.success:
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
                        last_probed_at = ?,
                        tier = ?,
                        quality_score = ?
                    WHERE name = ?
                """, (
                    res.exit_ip, res.latency_ms, new_state, new_state,
                    new_fails, new_succs, next_allowed_str, now_str,
                    q_tier, q_score, res.node_name
                ))
            else:
                conn.execute("""
                    UPDATE nodes SET
                        fail_count = fail_count + 1,
                        status = ?,
                        circuit_state = ?,
                        consecutive_failures = ?,
                        consecutive_successes = ?,
                        next_probe_allowed_at = ?,
                        last_probed_at = ?,
                        tier = ?,
                        quality_score = ?
                    WHERE name = ?
                """, (
                    new_state, new_state, new_fails, new_succs,
                    next_allowed_str, now_str,
                    q_tier, q_score, res.node_name
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

    def list_nodes(
        self,
        region: Optional[str] = None,
        egress_type: Optional[str] = None,
        status: Optional[str] = None,
        tier: Optional[str] = None
    ) -> List[ProxyNode]:
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
        if tier:
            query += " AND tier = ?"
            params.append(tier)
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
            next_probe_allowed_at=row["next_probe_allowed_at"] if "next_probe_allowed_at" in keys else None,
            relay_mode=row["relay_mode"] if "relay_mode" in keys and row["relay_mode"] else RelayMode.RELAY_FALLBACK.value,
            relay_hop_node=row["relay_hop_node"] if "relay_hop_node" in keys else None,
            tier=row["tier"] if "tier" in keys and row["tier"] else Tier.STANDARD.value,
            configured_tier=row["configured_tier"] if "configured_tier" in keys and row["configured_tier"] else None,
            quality_score=row["quality_score"] if "quality_score" in keys and row["quality_score"] is not None else 50,
            fault_domain=row["fault_domain"] if "fault_domain" in keys and row["fault_domain"] else "",
            provider_region=row["provider_region"] if "provider_region" in keys and row["provider_region"] else "",
            self_reported_at=row["self_reported_at"] if "self_reported_at" in keys else None
        )

    def set_lease(self, account_id: str, node_name: str, rebind_reason: Optional[str] = None) -> LeaseInfo:
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            # A rebind (as opposed to a first bind) bumps the account's counter so the
            # "hot account" signal in R2 is observable.
            existing = conn.execute(
                "SELECT node_name FROM leases WHERE account_id = ?", (account_id,)
            ).fetchone()
            is_rebind = bool(existing and existing["node_name"] != node_name)
            conn.execute("""
                INSERT INTO leases (account_id, node_name, assigned_at, rebind_count, last_rebind_reason)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    node_name = excluded.node_name,
                    assigned_at = excluded.assigned_at,
                    rebind_count = leases.rebind_count + CASE WHEN leases.node_name != excluded.node_name THEN 1 ELSE 0 END,
                    last_rebind_reason = CASE WHEN leases.node_name != excluded.node_name THEN excluded.last_rebind_reason ELSE leases.last_rebind_reason END
            """, (account_id, node_name, now_str, 1 if is_rebind else 0, rebind_reason or ""))
            row = conn.execute(
                "SELECT rebind_count, last_rebind_reason FROM leases WHERE account_id = ?",
                (account_id,)
            ).fetchone()

        node = self.get_node(node_name)
        return LeaseInfo(
            account_id=account_id,
            node_name=node_name,
            proxy_url=node.proxy_url if node else "",
            exit_ip=node.exit_ip if node else None,
            region=node.region if node else "",
            egress_type=node.egress_type if node else "",
            assigned_at=now_str,
            tier=node.tier if node else None,
            rebind_count=row["rebind_count"] if row else 0,
            last_rebind_reason=(row["last_rebind_reason"] if row else "") or ""
        )

    def get_lease(self, account_id: str) -> Optional[LeaseInfo]:
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT l.account_id, l.node_name, l.assigned_at, l.rebind_count, l.last_rebind_reason,
                       n.host, n.port, n.protocol, n.exit_ip, n.region, n.egress_type, n.tier
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
                    assigned_at=row["assigned_at"],
                    tier=row["tier"],
                    rebind_count=row["rebind_count"] if "rebind_count" in row.keys() and row["rebind_count"] else 0,
                    last_rebind_reason=(row["last_rebind_reason"] if "last_rebind_reason" in row.keys() and row["last_rebind_reason"] else "")
                )
        return None

    def get_leases_for_node(self, node_name: str) -> List[LeaseInfo]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT l.account_id, l.node_name, l.assigned_at, l.rebind_count, l.last_rebind_reason,
                       n.host, n.port, n.protocol, n.exit_ip, n.region, n.egress_type, n.tier
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
                    assigned_at=r["assigned_at"],
                    tier=r["tier"],
                    rebind_count=r["rebind_count"] if "rebind_count" in r.keys() and r["rebind_count"] else 0,
                    last_rebind_reason=(r["last_rebind_reason"] if "last_rebind_reason" in r.keys() and r["last_rebind_reason"] else "")
                )
                for r in rows
            ]

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
            clash_proxy = conn.execute("SELECT COUNT(*) FROM nodes WHERE egress_type = 'clash_proxy'").fetchone()[0]

            regions = {}
            for row in conn.execute("SELECT region, COUNT(*) as c FROM nodes GROUP BY region").fetchall():
                regions[row["region"]] = row["c"]

            tier_dist = {}
            for row in conn.execute("SELECT tier, COUNT(*) as c FROM nodes GROUP BY tier").fetchall():
                tier_dist[row["tier"] or "standard"] = row["c"]

            avg_score_row = conn.execute("SELECT AVG(quality_score) FROM nodes WHERE status != 'dead'").fetchone()
            avg_score = float(avg_score_row[0]) if avg_score_row and avg_score_row[0] is not None else 50.0

            return PoolStats(
                total_nodes=total,
                healthy_nodes=healthy,
                degraded_nodes=degraded,
                dead_nodes=dead,
                unprobed_nodes=unprobed,
                warp_ipv6_count=warp_v6,
                native_ipv4_count=nat_v4,
                native_ipv6_count=nat_v6,
                clash_proxy_count=clash_proxy,
                region_distribution=regions,
                tier_distribution=tier_dist,
                average_quality_score=round(avg_score, 1)
            )

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

    # --- Anti-Sybil IP Cooldown for Registration Bots ---

    def add_ip_cooldown(self, exit_ip: str, target_service: str, node_name: str, cooldown_seconds: int = 1800) -> IpCooldownRecord:
        """Locks an exit IP into cooldown for target_service to prevent registration anti-sybil bans."""
        now = datetime.now()
        cooled_until = (now + timedelta(seconds=cooldown_seconds)).isoformat()
        now_str = now.isoformat()
        with self._get_conn() as conn:
            # Keep one live row per (exit_ip, target_service) instead of appending duplicates
            conn.execute(
                "DELETE FROM ip_cooldowns WHERE exit_ip = ? AND target_service = ?",
                (exit_ip, target_service)
            )
            cur = conn.execute("""
                INSERT INTO ip_cooldowns (exit_ip, target_service, node_name, cooled_until, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (exit_ip, target_service, node_name, cooled_until, now_str))
            return IpCooldownRecord(
                id=cur.lastrowid,
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

    # --- Battery Lifecycle Events ---

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

    # --- Universal Account Allocation & Distribution ---

    def batch_save_allocations(
        self,
        records: List[Tuple[str, str, str, str]],
        reason: str = "bind",
        constraints_by_account: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None
    ):
        """
        Atomically upserts account allocations: List of (service, account_id, node_name, allocated_at).
        When a binding's node changes, its rebind counter is incremented, the legacy lease
        mirror is updated, the service binding epoch is advanced and a change-log row is
        appended — all in the same transaction so rehomes are all-or-nothing and a
        downstream reconciler can never observe a half-migrated binding (R2/R3/R5).

        `reason` labels transitions (`bind`, `auto_rehome`, `domain_rehome`, ...) so the
        incremental export can report why accounts moved.

        `constraints_by_account` optionally carries the credential's constraint set
        (region / region_strict / egress_type / tier / tier_policy) keyed by
        (service, account_id); rows it covers get those bound_* columns written, all
        other rows PRESERVE their existing bound_* values (rehome sweeps must never
        rewrite the constraints an account was pinned with).
        """
        if not records:
            return
        now_str = datetime.now().isoformat()
        constraints_by_account = constraints_by_account or {}
        with self._get_conn() as conn:
            # Pre-image lookup so rebind counts and change logs reflect real transitions.
            # Later duplicates win, matching the upsert ordering executemany applies.
            desired: Dict[Tuple[str, str], str] = {}
            for service, account_id, node_name, _ in records:
                desired[(service, account_id)] = node_name
            pre_images: Dict[Tuple[str, str], Optional[str]] = {}
            for key in desired:
                row = conn.execute(
                    "SELECT node_name FROM account_allocations WHERE service = ? AND account_id = ?",
                    key
                ).fetchone()
                pre_images[key] = row["node_name"] if row else None

            def _c(key: Tuple[str, str]) -> Tuple[int, str, int, str, str, str]:
                c = constraints_by_account.get(key)
                if not c:
                    return (0, "", 0, "", "", "")
                return (
                    1,
                    c.get("region") or "",
                    1 if c.get("region_strict") else 0,
                    c.get("egress_type") or "",
                    c.get("tier") or "",
                    c.get("tier_policy") or "",
                )

            conn.executemany("""
                INSERT INTO account_allocations (
                    service, account_id, node_name, allocated_at,
                    bound_region, bound_region_strict, bound_egress_type, bound_tier, bound_tier_policy
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(service, account_id) DO UPDATE SET
                    node_name = excluded.node_name,
                    allocated_at = excluded.allocated_at,
                    rebind_count = CASE WHEN account_allocations.node_name != excluded.node_name
                                        THEN account_allocations.rebind_count + 1
                                        ELSE account_allocations.rebind_count END,
                    last_rebind_reason = CASE WHEN account_allocations.node_name != excluded.node_name
                                              THEN ?
                                              ELSE account_allocations.last_rebind_reason END,
                    bound_region = CASE WHEN ?=1 THEN ? ELSE account_allocations.bound_region END,
                    bound_region_strict = CASE WHEN ?=1 THEN ? ELSE account_allocations.bound_region_strict END,
                    bound_egress_type = CASE WHEN ?=1 THEN ? ELSE account_allocations.bound_egress_type END,
                    bound_tier = CASE WHEN ?=1 THEN ? ELSE account_allocations.bound_tier END,
                    bound_tier_policy = CASE WHEN ?=1 THEN ? ELSE account_allocations.bound_tier_policy END
            """, [
                (
                    service, account_id, node_name, allocated_at,
                    _c((service, account_id))[1], _c((service, account_id))[2],
                    _c((service, account_id))[3], _c((service, account_id))[4], _c((service, account_id))[5],
                    reason,
                    _c((service, account_id))[0], _c((service, account_id))[1],
                    _c((service, account_id))[0], _c((service, account_id))[2],
                    _c((service, account_id))[0], _c((service, account_id))[3],
                    _c((service, account_id))[0], _c((service, account_id))[4],
                    _c((service, account_id))[0], _c((service, account_id))[5],
                )
                for service, account_id, node_name, allocated_at in records
            ])

            # Advance each service's epoch once per call and append one change row per move.
            changed = [
                (s, a, old, desired[(s, a)])
                for (s, a), old in pre_images.items()
                if old != desired[(s, a)]
            ]
            if changed:
                epochs = self._advance_binding_epochs(conn, {s for s, _, _, _ in changed}, now_str)
                conn.executemany("""
                    INSERT INTO binding_changes (service, epoch, account_id, from_node, to_node, reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    (s, epochs[s], a, old, new, reason, now_str)
                    for s, a, old, new in changed
                ])

                # Keep the legacy account-keyed lease mirror in the SAME transaction as the
                # allocation it mirrors. The old code did this with one `set_lease` call per
                # account, which turned a whole-host rehome into N separate commits.
                lease_moves: Dict[str, Tuple[str, int]] = {}
                for s, a, old, new in changed:
                    # A first bind is not a rebind; a move is. If the account has several
                    # services, any pre-existing binding makes this a rebind.
                    lease_moves[a] = (new, 1 if old is not None else 0)
                # The mirror is account-keyed while constraints are (service, account)-keyed;
                # prebuild the per-account lookup once (first service wins in insertion
                # order) instead of scanning the map for every lease row.
                first_c_by_acc: Dict[str, Tuple[int, str, int, str, str, str]] = {}
                if constraints_by_account:
                    for (s, a) in constraints_by_account.keys():
                        if a not in first_c_by_acc:
                            first_c_by_acc[a] = _c((s, a))
                conn.executemany("""
                    INSERT INTO leases (account_id, node_name, assigned_at, rebind_count, last_rebind_reason,
                                        bound_region, bound_region_strict, bound_egress_type, bound_tier, bound_tier_policy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(account_id) DO UPDATE SET
                        node_name = excluded.node_name,
                        assigned_at = excluded.assigned_at,
                        rebind_count = leases.rebind_count + CASE WHEN leases.node_name != excluded.node_name THEN 1 ELSE 0 END,
                        last_rebind_reason = CASE WHEN leases.node_name != excluded.node_name THEN excluded.last_rebind_reason ELSE leases.last_rebind_reason END,
                        bound_region = CASE WHEN ?=1 THEN ? ELSE leases.bound_region END,
                        bound_region_strict = CASE WHEN ?=1 THEN ? ELSE leases.bound_region_strict END,
                        bound_egress_type = CASE WHEN ?=1 THEN ? ELSE leases.bound_egress_type END,
                        bound_tier = CASE WHEN ?=1 THEN ? ELSE leases.bound_tier END,
                        bound_tier_policy = CASE WHEN ?=1 THEN ? ELSE leases.bound_tier_policy END
                """, [
                    (acc, node, now_str, initial, reason,
                     _lc[1], _lc[2], _lc[3], _lc[4], _lc[5],
                     _lc[0], _lc[1],
                     _lc[0], _lc[2],
                     _lc[0], _lc[3],
                     _lc[0], _lc[4],
                     _lc[0], _lc[5],
                     )
                    for acc, (node, initial) in lease_moves.items()
                    for _lc in (first_c_by_acc.get(acc, (0, "", 0, "", "", "")),)
                ])

    def find_allocation_by_account(self, account_id: str, service: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Finds active allocation for an account, optionally scoped by service.
        The bound_* columns carry the constraint set the credential declared at bind
        time; auto_rehome reads them to keep the binding inside its promised region.
        """
        query = """
            SELECT a.service, a.account_id, a.node_name, a.allocated_at,
                   a.bound_region, a.bound_region_strict, a.bound_egress_type,
                   a.bound_tier, a.bound_tier_policy,
                   n.host, n.port, n.protocol, n.region, n.egress_type, n.exit_ip, n.status
            FROM account_allocations a
            JOIN nodes n ON a.node_name = n.name
            WHERE a.account_id = ?
        """
        params = [account_id]
        if service:
            query += " AND a.service = ?"
            params.append(service)
        query += " ORDER BY a.allocated_at DESC LIMIT 1"

        with self._get_conn() as conn:
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else None

    def update_allocation_constraints(self, service: str, account_id: str, constraints: Dict[str, Any]) -> bool:
        """
        Overwrites the bound_* constraint set of one allocation (called by the router when
        a constrained request detects that the stored set differs from the current call).
        Unconstrained values are written as '' / 0; the ROUTER decides whether an update
        is worth a write — this method itself is a plain overwrite.
        """
        with self._get_conn() as conn:
            cur = conn.execute("""
                UPDATE account_allocations SET
                    bound_region = ?,
                    bound_region_strict = ?,
                    bound_egress_type = ?,
                    bound_tier = ?,
                    bound_tier_policy = ?
                WHERE service = ? AND account_id = ?
            """, (
                constraints.get("region") or "",
                1 if constraints.get("region_strict") else 0,
                constraints.get("egress_type") or "",
                constraints.get("tier") or "",
                constraints.get("tier_policy") or "",
                service, account_id
            ))
            return cur.rowcount > 0

    def update_allocation_node(self, service: str, account_id: str, new_node_name: str) -> bool:
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("""
                UPDATE account_allocations
                SET node_name = ?, allocated_at = ?
                WHERE service = ? AND account_id = ?
            """, (new_node_name, now_str, service, account_id))
            return cur.rowcount > 0

    def get_allocation(self, service: str, account_id: str) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT a.service, a.account_id, a.node_name, a.allocated_at,
                       n.host, n.port, n.protocol, n.region, n.egress_type, n.exit_ip, n.status
                FROM account_allocations a
                JOIN nodes n ON a.node_name = n.name
                WHERE a.service = ? AND a.account_id = ?
            """, (service, account_id)).fetchone()
            return dict(row) if row else None

    def list_allocations_by_service(self, service: str) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT a.service, a.account_id, a.node_name, a.allocated_at,
                       n.host, n.port, n.protocol, n.region, n.egress_type, n.exit_ip, n.status
                FROM account_allocations a
                JOIN nodes n ON a.node_name = n.name
                WHERE a.service = ?
                ORDER BY a.account_id ASC
            """, (service,)).fetchall()
            return [dict(r) for r in rows]

    def delete_allocation(self, service: str, account_id: str) -> bool:
        """
        Unbinds an account from a service.

        The legacy `leases` table is a mirror of the `default` namespace, so it must be
        cleared in the same transaction once the account has no bindings left anywhere.
        Otherwise `acquire_for_account` would resurrect the released lease and the
        account would silently remain bound while no longer counting against capacity.
        """
        with self._get_conn() as conn:
            cur = conn.execute("""
                DELETE FROM account_allocations
                WHERE service = ? AND account_id = ?
            """, (service, account_id))
            remaining = conn.execute(
                "SELECT COUNT(*) FROM account_allocations WHERE account_id = ?",
                (account_id,)
            ).fetchone()[0]
            if remaining == 0:
                conn.execute("DELETE FROM leases WHERE account_id = ?", (account_id,))
            return cur.rowcount > 0

    def migrate_legacy_leases(self, service: str = "default") -> int:
        """
        Backfills `account_allocations` from `leases` rows that have no allocation.

        Keeps the invariant "a lease row implies at least one allocation row" so that
        capacity accounting, monitoring and re-homing all agree. Idempotent.
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT l.account_id, l.node_name, l.assigned_at
                FROM leases l
                WHERE NOT EXISTS (
                    SELECT 1 FROM account_allocations a WHERE a.account_id = l.account_id
                )
            """).fetchall()
            if not rows:
                return 0
            conn.executemany("""
                INSERT INTO account_allocations (service, account_id, node_name, allocated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(service, account_id) DO NOTHING
            """, [(service, r["account_id"], r["node_name"], r["assigned_at"]) for r in rows])
            return len(rows)

    def get_allocations_overview(self) -> Dict[str, Any]:
        """
        Returns statistical overview of account distribution across nodes.
        """
        with self._get_conn() as conn:
            total_allocations = conn.execute("SELECT COUNT(*) FROM account_allocations").fetchone()[0]

            services = {}
            for row in conn.execute("SELECT service, COUNT(*) as c FROM account_allocations GROUP BY service").fetchall():
                services[row["service"]] = row["c"]

            node_rows = conn.execute("""
                SELECT n.name as node_name, n.region, n.egress_type, n.exit_ip, COUNT(a.account_id) as account_count
                FROM nodes n
                LEFT JOIN account_allocations a ON n.name = a.node_name
                GROUP BY n.name, n.region, n.egress_type, n.exit_ip
                HAVING account_count > 0
                ORDER BY account_count DESC
            """).fetchall()

            return {
                "total_allocations": total_allocations,
                "service_distribution": services,
                "node_stats": [dict(r) for r in node_rows]
            }

    def count_allocations_per_node(self, service: str) -> Dict[str, int]:
        """O(1)-query occupancy map: {node_name: account_count} for a service."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT node_name, COUNT(*) AS cnt
                FROM account_allocations
                WHERE service = ?
                GROUP BY node_name
            """, (service,)).fetchall()
            return {r["node_name"]: r["cnt"] for r in rows}

    def count_allocations_per_exit(self, service: str) -> Dict[str, int]:
        """
        Occupancy keyed by real exit identity: exit_ip when probed, else 'node:<name>'.
        Nodes that have not been probed yet cannot be proven to share an exit, so they
        fall back to their node name.
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT COALESCE(NULLIF(n.exit_ip, ''), 'node:' || n.name) AS exit_key,
                       COUNT(*) AS cnt
                FROM account_allocations a
                JOIN nodes n ON a.node_name = n.name
                WHERE a.service = ?
                GROUP BY exit_key
            """, (service,)).fetchall()
            return {r["exit_key"]: r["cnt"] for r in rows}

    def get_allocations_for_node(self, node_name: str) -> List[Dict[str, Any]]:
        """All service/account bindings currently pointing at a node (with their bound constraint set)."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT service, account_id, node_name, bound_region, bound_region_strict, "
                "bound_egress_type, bound_tier, bound_tier_policy "
                "FROM account_allocations WHERE node_name = ?",
                (node_name,)
            ).fetchall()
            return [dict(r) for r in rows]

    def prune_probe_history(self, keep_days: int = 7) -> int:
        """Deletes probe_history rows older than keep_days to bound table growth."""
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("DELETE FROM probe_history WHERE probed_at < ?", (cutoff,))
            return cur.rowcount

    def prune_circuit_events(self, keep_days: int = 7) -> int:
        """Deletes circuit_events rows older than keep_days (append-only table)."""
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("DELETE FROM circuit_events WHERE created_at < ?", (cutoff,))
            return cur.rowcount

    def prune_battery_events(self, keep_days: int = 7) -> int:
        """Deletes battery_events rows older than keep_days (append-only table)."""
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("DELETE FROM battery_events WHERE created_at < ?", (cutoff,))
            return cur.rowcount

    def prune_binding_changes(self, keep_days: int = 7) -> int:
        """
        Deletes binding_changes rows older than keep_days. Without this the incremental
        reconciliation log (one row per rebind, 511+ rows per host outage) would grow
        unbounded. Downstreams whose `since_epoch` predates the retention window must do a
        full cold-start export instead of trusting an empty delta.
        """
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        with self._get_conn() as conn:
            cur = conn.execute("DELETE FROM binding_changes WHERE created_at < ?", (cutoff,))
            return cur.rowcount

    def prune_event_tables(self, keep_days: int = 7) -> Dict[str, int]:
        """Applies the same retention window to every append-only event table."""
        return {
            "probe_history": self.prune_probe_history(keep_days),
            "circuit_events": self.prune_circuit_events(keep_days),
            "battery_events": self.prune_battery_events(keep_days),
            "binding_changes": self.prune_binding_changes(keep_days),
        }

    def mark_nodes_missing_as_dead(self, keep_names: List[str]) -> List[str]:
        """
        Marks registry nodes absent from the authoritative inventory as DEAD.
        Excludes dynamic API-managed nodes (tagged 'api_managed') from being marked dead.

        Deliberately non-destructive: bindings stay intact and the existing self-healing
        path re-homes them to healthy nodes. Hard deletion is only performed when the
        operator explicitly opts in (see `prune_nodes_except`).
        """
        with self._get_conn() as conn:
            existing = [
                r["name"] for r in conn.execute(
                    "SELECT name FROM nodes WHERE tags NOT LIKE '%\"api_managed\"%'"
                ).fetchall()
            ]
            keep = set(keep_names)
            stale = [n for n in existing if n not in keep]
            if not stale:
                return []
            placeholders = ",".join("?" * len(stale))
            conn.execute(
                f"UPDATE nodes SET status = ?, circuit_state = ? "
                f"WHERE name IN ({placeholders})",
                [HealthStatus.DEAD.value, HealthStatus.DEAD.value, *stale]
            )
            return stale

    def prune_nodes_except(self, keep_names: List[str]) -> List[str]:
        """
        Removes nodes absent from the authoritative inventory along with their bindings.
        Excludes dynamic API-managed nodes (tagged 'api_managed') from being pruned.
        Returns the pruned node names. Callers must validate `keep_names` first.
        """
        with self._get_conn() as conn:
            existing = [
                r["name"] for r in conn.execute(
                    "SELECT name FROM nodes WHERE tags NOT LIKE '%\"api_managed\"%'"
                ).fetchall()
            ]
            stale = [n for n in existing if n not in set(keep_names)]
            if not stale:
                return []
            placeholders = ",".join("?" * len(stale))
            conn.execute(f"DELETE FROM leases WHERE node_name IN ({placeholders})", stale)
            conn.execute(f"DELETE FROM account_allocations WHERE node_name IN ({placeholders})", stale)
            conn.execute(f"DELETE FROM nodes WHERE name IN ({placeholders})", stale)
            return stale

    def delete_node(self, name: str, prune_bindings: bool = True) -> bool:
        """
        Deletes a single node by name.
        If prune_bindings is True, also deletes any attached allocations and leases.
        Returns True if the node existed and was deleted, False otherwise.
        """
        with self._get_conn() as conn:
            if prune_bindings:
                conn.execute("DELETE FROM leases WHERE node_name = ?", (name,))
                conn.execute("DELETE FROM account_allocations WHERE node_name = ?", (name,))
            cur = conn.execute("DELETE FROM nodes WHERE name = ?", (name,))
            return cur.rowcount > 0

    # --- Binding Epoch & Change Log (R5 downstream reconciliation) ---

    def _advance_binding_epochs(self, conn, services: set, now_str: str) -> Dict[str, int]:
        """
        Advances the global monotonic binding epoch for each service inside the caller's
        transaction. Kept private: epoch bumps MUST happen in the same transaction as the
        binding write that triggered them, otherwise a downstream reconciler could read a
        half-migrated state.
        """
        epochs: Dict[str, int] = {}
        for service in sorted(services):
            conn.execute("""
                INSERT INTO binding_epoch_state (service, epoch, changed_at)
                VALUES (?, 1, ?)
                ON CONFLICT(service) DO UPDATE SET
                    epoch = binding_epoch_state.epoch + 1,
                    changed_at = excluded.changed_at
            """, (service, now_str))
            row = conn.execute(
                "SELECT epoch FROM binding_epoch_state WHERE service = ?", (service,)
            ).fetchone()
            epochs[service] = row["epoch"] if row else 0
        return epochs

    def get_binding_epoch(self, service: str) -> Optional[Dict[str, Any]]:
        """Returns the current binding epoch for a service (or None when never written)."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT epoch, changed_at FROM binding_epoch_state WHERE service = ?",
                (service,)
            ).fetchone()
            if not row:
                return None
            info: Dict[str, Any] = {"service": service, "epoch": row["epoch"], "changed_at": row["changed_at"]}
            last = conn.execute(
                "SELECT reason FROM binding_changes WHERE service = ? ORDER BY id DESC LIMIT 1",
                (service,)
            ).fetchone()
            info["last_change_reason"] = last["reason"] if last else None
            return info

    def list_binding_changes_since(self, service: str, epoch: int) -> List[BindingChangeRecord]:
        """Incremental binding diff since an epoch, for downstream reconcile pulls (R5b)."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT id, service, epoch, account_id, from_node, to_node, reason, created_at
                FROM binding_changes
                WHERE service = ? AND epoch > ?
                ORDER BY id ASC
            """, (service, epoch)).fetchall()
            return [BindingChangeRecord(**dict(r)) for r in rows]

    def move_account_binding(
        self,
        service: str,
        account_id: str,
        new_node_name: str,
        reason: str = "auto_rehome",
        cross_region_rebind: bool = False
    ) -> Dict[str, Any]:
        """
        Atomically rewrites ONE account's binding: allocation row + legacy lease mirror +
        rebind counter + last_rebind_reason + binding epoch + change log, all inside one
        transaction. This is the storage primitive behind R2 (request-time retry exhaustion)
        and R3 (domain isolation) — the router picks the target node, storage guarantees the
        write is atomic and idempotent.
        """
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            prev = conn.execute(
                "SELECT node_name FROM account_allocations WHERE service = ? AND account_id = ?",
                (service, account_id)
            ).fetchone()
            prev_node = prev["node_name"] if prev else None
            if prev_node == new_node_name:
                return {
                    "moved": False, "already_on_target": True, "account_id": account_id,
                    "service": service, "from_node": prev_node, "to_node": new_node_name
                }

            conn.execute("""
                INSERT INTO account_allocations (service, account_id, node_name, allocated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(service, account_id) DO UPDATE SET
                    node_name = excluded.node_name,
                    allocated_at = excluded.allocated_at,
                    rebind_count = account_allocations.rebind_count + 1,
                    last_rebind_reason = ?
            """, (service, account_id, new_node_name, now_str, reason))

            # Legacy lease mirror (account-keyed); keep its own rebind counter in sync.
            conn.execute("""
                INSERT INTO leases (account_id, node_name, assigned_at, rebind_count, last_rebind_reason)
                VALUES (?, ?, ?, 1, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    node_name = excluded.node_name,
                    assigned_at = excluded.assigned_at,
                    rebind_count = leases.rebind_count + CASE WHEN leases.node_name != excluded.node_name THEN 1 ELSE 0 END,
                    last_rebind_reason = CASE WHEN leases.node_name != excluded.node_name THEN excluded.last_rebind_reason ELSE leases.last_rebind_reason END
            """, (account_id, new_node_name, now_str, reason))

            epoch = self._advance_binding_epochs(conn, {service}, now_str)[service]
            conn.execute("""
                INSERT INTO binding_changes (service, epoch, account_id, from_node, to_node, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (service, epoch, account_id, prev_node, new_node_name, reason, now_str))

        return {
            "moved": True,
            "account_id": account_id,
            "service": service,
            "from_node": prev_node,
            "to_node": new_node_name,
            "epoch": epoch,
            "reason": reason,
            "cross_region_rebind": cross_region_rebind
        }

    # --- Failure Domain Isolation (R3) ---

    def list_nodes_by_domain(self, fault_domain: str) -> List[ProxyNode]:
        """All nodes sharing a failure domain: explicit `fault_domain` or their connection host."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM nodes
                WHERE fault_domain = ? OR (fault_domain = '' AND host = ?)
                ORDER BY name ASC
            """, (fault_domain, fault_domain)).fetchall()
            return [self._row_to_node(r) for r in rows]

    def domain_status_counts(self, fault_domain: str) -> Dict[str, int]:
        """Per-status node counts inside one failure domain (for quorum/alarm policy)."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) AS c FROM nodes
                WHERE fault_domain = ? OR (fault_domain = '' AND host = ?)
                GROUP BY status
            """, (fault_domain, fault_domain)).fetchall()
            return {
                nr["status"]: nr["c"] for nr in rows
            }

    def mark_domain_dead(self, fault_domain: str, reason: str = "failure_domain_isolation") -> Tuple[List[ProxyNode], Optional[CircuitEvent]]:
        """
        Marks every non-dead node sharing a failure domain as DEAD in one sweep (single
        transaction, single circuit event) instead of N independent per-node trips.
        Returns (affected_nodes, domain_event).
        """
        now = datetime.now()
        cooldown_sec = CircuitBreaker.compute_cooldown(2)  # baseline ~60s domain cooldown
        next_allowed = (now + timedelta(seconds=cooldown_sec)).isoformat()
        now_str = now.isoformat()

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM nodes
                WHERE (fault_domain = ? OR (fault_domain = '' AND host = ?))
                  AND status != ?
                ORDER BY name ASC
            """, (fault_domain, fault_domain, HealthStatus.DEAD.value)).fetchall()
            affected = [self._row_to_node(r) for r in rows]
            if not affected:
                return [], None

            names = [n.name for n in affected]
            placeholders = ",".join("?" * len(names))
            conn.execute(
                f"UPDATE nodes SET status = ?, circuit_state = ?, consecutive_failures = MAX(consecutive_failures, 2), "
                f"consecutive_successes = 0, next_probe_allowed_at = ?, last_probed_at = ? WHERE name IN ({placeholders})",
                [HealthStatus.DEAD.value, HealthStatus.DEAD.value, next_allowed, now_str, *names]
            )
            cur = conn.execute("""
                INSERT INTO circuit_events (node_name, from_state, to_state, reason, created_at)
                VALUES (?, 'domain_healthy', 'dead', ?, ?)
            """, (fault_domain, f"{reason}_{len(affected)}_listeners", now_str))
            event = CircuitEvent(
                id=cur.lastrowid,
                node_name=fault_domain,
                from_state="domain_healthy",
                to_state=HealthStatus.DEAD.value,
                reason=f"{reason}_{len(affected)}_listeners",
                created_at=now_str
            )
        return affected, event

    def get_allocations_for_domain(self, fault_domain: str) -> List[Dict[str, Any]]:
        """All service/account bindings pointing at any node inside a failure domain."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT a.service, a.account_id, a.node_name, n.host, n.fault_domain, n.region, n.egress_type
                FROM account_allocations a
                JOIN nodes n ON a.node_name = n.name
                WHERE n.fault_domain = ? OR (n.fault_domain = '' AND n.host = ?)
                ORDER BY a.service ASC, a.account_id ASC
            """, (fault_domain, fault_domain)).fetchall()
            return [dict(r) for r in rows]

    # --- Runtime Metrics Persistence (R7, cross-process safe) ---

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

    def update_node(self, name: str, updates: Dict[str, Any]) -> Optional[ProxyNode]:
        """
        Updates specific fields of a node by name.
        Supported fields: host, port, protocol, region, egress_type, tags, weight,
        status, circuit_state, tier, configured_tier, relay_mode, relay_hop_node,
        quality_score, fault_domain, provider_region, self_reported_at.
        Automatically synchronizes circuit_state with status and records circuit transition audit event.
        """
        if not updates:
            return self.get_node(name)

        allowed_fields = {
            "host", "port", "protocol", "region", "egress_type", "tags",
            "weight", "status", "circuit_state", "tier", "configured_tier",
            "relay_mode", "relay_hop_node", "quality_score",
            "fault_domain", "provider_region", "self_reported_at"
        }
        valid_updates = {k: v for k, v in updates.items() if k in allowed_fields}
        if not valid_updates:
            return self.get_node(name)

        # Synchronize status and circuit_state to prevent split-brain state machine
        if "status" in valid_updates and "circuit_state" not in valid_updates:
            valid_updates["circuit_state"] = valid_updates["status"]
        elif "circuit_state" in valid_updates and "status" not in valid_updates:
            valid_updates["status"] = valid_updates["circuit_state"]

        # Convert tags to json if provided as list
        if "tags" in valid_updates and isinstance(valid_updates["tags"], list):
            valid_updates["tags"] = json.dumps(valid_updates["tags"])

        current_node = self.get_node(name)
        if not current_node:
            return None

        set_clauses = [f"{k} = ?" for k in valid_updates.keys()]
        values = list(valid_updates.values())
        values.append(name)

        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            cur = conn.execute(
                f"UPDATE nodes SET {', '.join(set_clauses)} WHERE name = ?",
                values
            )
            if cur.rowcount == 0:
                return None

            # Record circuit audit transition event if status changed
            if "status" in valid_updates and current_node.status != valid_updates["status"]:
                conn.execute("""
                    INSERT INTO circuit_events (node_name, from_state, to_state, reason, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (name, current_node.status, valid_updates["status"], "api_manual_patch", now_str))

        return self.get_node(name)


