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
from core.quality import IPQualityEvaluator

DB_PATH = os.environ.get("IP_POOL_DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "ip_pool.db"))


def region_from_country(cc: Optional[str]) -> Optional[str]:
    """出口国家码 -> 池内 region 约定值 (与 sub_pool_refresh.region_of 同语义):
    us -> us-west (区域桶名), 其他国家 -> 小写 ISO 码 (de/ca/nl/...)。
    输入为空/非法时返回 None (调用方据此跳过纠偏, 宁缺勿错)。
    """
    if not cc or not isinstance(cc, str):
        return None
    c = cc.strip().lower()
    if len(c) != 2 or not c.isalpha():
        return None
    return "us-west" if c == "us" else c

class StorageBase:
    """存储基座: 连接管理 / IMMEDIATE 事务 / 建表迁移 / 行映射。"""

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
            try:
                # WAL 自动 checkpoint 上限 (HA 加固): 默认 1000 页在长事务读者
                # (探活线程池) 存在时可能压不住, WAL 无限增长最终打满磁盘
                # (97% 事故同型)。10000 页 ≈ 40MB, checkpoint 由后续写自然触发。
                conn.execute("PRAGMA wal_autocheckpoint = 10000;")
                # busy_timeout 必须每连接设置 (2026-11-15 review storage#6:
                # init 线程的 5000 pragma 只作用于那一根连接, 其余连接的
                # 实际锁等待由 connect timeout=10.0 决定, pragma 是误导)
                conn.execute("PRAGMA busy_timeout = 10000;")
            except sqlite3.OperationalError:
                pass  # 只读连接/磁盘异常时不阻断建连
            self._thread_local.conn = conn
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    @contextmanager
    def _immediate_txn(self, conn):
        """显式 BEGIN IMMEDIATE 写事务 (2026-11-15 review storage#3):
        python sqlite3 默认延迟开事务, SELECT 在 autocommit 里跑 —— 读改写
        方法的 SELECT 与 UPDATE 不是同一快照, 并发写互相 clobber (熔断计数
        丢失、状态回退、mass-guard 用陈旧比率)。IMMEDIATE 在事务开头就抓
        写锁, 读在锁内, 整段原子。调用方仍处在外层 _get_conn 的 commit 里,
        这里的 COMMIT/RALLBACK 只结束 IMMEDIATE 事务本身。"""
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass  # 连接级故障时 rollback 自身不可用, 外层 _get_conn 再兜底
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
                    probe_url TEXT DEFAULT '',
                    self_reported_at TEXT,
                    last_probe_error TEXT
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

                CREATE TABLE IF NOT EXISTS ban_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ban_id TEXT NOT NULL UNIQUE,
                    service TEXT NOT NULL,
                    account TEXT NOT NULL,
                    node_name TEXT NOT NULL,
                    exit_ip TEXT,
                    isp TEXT,
                    reason TEXT NOT NULL,
                    evidence TEXT DEFAULT '{}',
                    quarantined_until TEXT,
                    released_at TEXT,
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
                    to_node TEXT,
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
                CREATE INDEX IF NOT EXISTS idx_ban_events_lookup ON ban_events(service, node_name, reason, quarantined_until);
                CREATE INDEX IF NOT EXISTS idx_ban_events_active ON ban_events(service, quarantined_until);
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
                ("probe_url", "TEXT", "''"),
                ("self_reported_at", "TEXT", "NULL"),
                # 节点生命周期 janitor (2026-11-15): 连续 DEAD 24h -> 冻结
                # (探活降频 + 不进分配), 连续 7d -> 驱逐 (DB 删除 + tombstone
                # 防 yaml 重载复活)。DEAD 落锤时写, 离开 DEAD 时清。
                ("dead_since", "TEXT", "NULL"),
                # 最近探活失败摘要 (2026-11-19 P1-5 skip 语义可见化)
                ("last_probe_error", "TEXT", "NULL")
            ]:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE nodes ADD COLUMN {col} {col_type} DEFAULT {default_val}")

            # 2026-11-15 review P1: 删除类绑定变更 (to_node=NULL, "绑定已消失")
            # 需要可空 to_node —— 存量库按旧 schema 建的表是 NOT NULL, 重建。
            bc_info = conn.execute("PRAGMA table_info(binding_changes)").fetchall()
            to_node_notnull = any(
                r["name"] == "to_node" and r["notnull"] for r in bc_info
            )
            if to_node_notnull:
                conn.execute("ALTER TABLE binding_changes RENAME TO binding_changes_old")
                conn.execute("""
                    CREATE TABLE binding_changes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        service TEXT NOT NULL,
                        epoch INTEGER NOT NULL,
                        account_id TEXT NOT NULL,
                        from_node TEXT,
                        to_node TEXT,
                        reason TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                """)
                conn.execute("""
                    INSERT INTO binding_changes
                        (id, service, epoch, account_id, from_node, to_node, reason, created_at)
                    SELECT id, service, epoch, account_id, from_node, to_node, reason, created_at
                    FROM binding_changes_old
                """)
                conn.execute("DROP TABLE binding_changes_old")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_binding_changes_epoch ON binding_changes(service, epoch);")
                logging.getLogger("tetherpool.storage").info(
                    "[schema] binding_changes.to_node relaxed to nullable (deletion change rows)")

            # janitor 存量回填 (2026-11-15): 升级前已 DEAD 的节点没有 dead_since
            # 锚点, 续期分支会一直保留 NULL —— 它们永远进不了冻结/驱逐倒计时。
            # 统一补成迁移时刻 (连续 DEAD 从此起算, 24h 后冻结 / 7d 后驱逐)。
            backfilled = conn.execute(
                "UPDATE nodes SET dead_since = ? "
                "WHERE status = 'dead' AND dead_since IS NULL",
                (datetime.now().isoformat(),)).rowcount
            if backfilled:
                print(f"[schema] backfilled dead_since anchor for {backfilled} legacy dead node(s)")

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
            dead_since=row["dead_since"] if "dead_since" in keys else None,
            relay_mode=row["relay_mode"] if "relay_mode" in keys and row["relay_mode"] else RelayMode.RELAY_FALLBACK.value,
            relay_hop_node=row["relay_hop_node"] if "relay_hop_node" in keys else None,
            tier=row["tier"] if "tier" in keys and row["tier"] else Tier.STANDARD.value,
            configured_tier=row["configured_tier"] if "configured_tier" in keys and row["configured_tier"] else None,
            quality_score=row["quality_score"] if "quality_score" in keys and row["quality_score"] is not None else 50,
            fault_domain=row["fault_domain"] if "fault_domain" in keys and row["fault_domain"] else "",
            provider_region=row["provider_region"] if "provider_region" in keys and row["provider_region"] else "",
            probe_url=row["probe_url"] if "probe_url" in keys and row["probe_url"] else "",
            self_reported_at=row["self_reported_at"] if "self_reported_at" in keys else None,
            last_probe_error=row["last_probe_error"] if "last_probe_error" in keys else None
        )
