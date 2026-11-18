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


class BindingStore:
    """绑定域: leases 镜像 / account_allocations 权威表 / epoch 与变更流。"""

    def set_lease(self, account_id: str, node_name: str, rebind_reason: Optional[str] = None) -> LeaseInfo:
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            # 读改写原子化 (2026-11-15 review storage#3): rebind 计数基线 SELECT
            # 与 INSERT 并发下必须同快照
            with self._immediate_txn(conn):
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
            # 2026-11-15 review storage#6: 2100 个逐行 SELECT (N+1) 换成两条
            # 集合 IN 查询 —— 每行一次往返既慢又拉长写锁持有时间。
            desired: Dict[Tuple[str, str], str] = {}
            for service, account_id, node_name, _ in records:
                desired[(service, account_id)] = node_name
            pre_images: Dict[Tuple[str, str], Optional[str]] = {}
            keys = list(desired)
            # sqlite IN 参数上限 999 (老版本 32766): 按 900 一批切片
            for i in range(0, len(keys), 900):
                chunk = keys[i:i + 900]
                by_service: Dict[str, List[str]] = {}
                for svc, acc in chunk:
                    by_service.setdefault(svc, []).append(acc)
                for svc, accs in by_service.items():
                    ph = ",".join("?" * len(accs))
                    for row in conn.execute(
                        f"SELECT account_id, node_name FROM account_allocations "
                        f"WHERE service = ? AND account_id IN ({ph})",
                        [svc, *accs]
                    ):
                        pre_images[(svc, row["account_id"])] = row["node_name"]
            for key in keys:
                pre_images.setdefault(key, None)

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

        # 2026-11-15 review storage#6: 单事务写 2100 行会长期持有 WAL 写锁,
        # 探活写全部排队 (超过 busy timeout 直接 OperationalError)。按 500 一批
        # 分事务提交 —— 每批自身原子 (pre-image/epoch/变更日志同批计算), 批间
        # 短暂可见属可接受 (下游 reconciler 按 epoch 增量拉, 不会看到半迁移行)。
        CH = 500
        for i in range(0, len(records), CH):
            recs = records[i:i + CH]
            sub_desired: Dict[Tuple[str, str], str] = {}
            for service, account_id, node_name, _ in recs:
                sub_desired[(service, account_id)] = node_name
            with self._get_conn() as conn:
                with self._immediate_txn(conn):
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
                        for service, account_id, node_name, allocated_at in recs
                    ])

                    # Advance each service's epoch once per chunk and append one change row per move.
                    changed = [
                        (s, a, pre_images.get((s, a)), sub_desired[(s, a)])
                        for (s, a) in sub_desired
                        if pre_images.get((s, a)) != sub_desired[(s, a)]
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

        2026-11-15 review P1: 删除也是绑定变更 —— epoch 不推进的话下游
        (grok2api /v1/bindings/epoch 轮询) 永远看不到这次释放, 继续拨已解绑
        的出口。to_node=NULL 即 "绑定已消失" 的 change 语义。
        """
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            with self._immediate_txn(conn):
                prev = conn.execute(
                    "SELECT node_name FROM account_allocations WHERE service = ? AND account_id = ?",
                    (service, account_id)
                ).fetchone()
                if not prev:
                    return False
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
                epoch = self._advance_binding_epochs(conn, {service}, now_str)[service]
                conn.execute("""
                    INSERT INTO binding_changes (service, epoch, account_id, from_node, to_node, reason, created_at)
                    VALUES (?, ?, ?, ?, NULL, 'allocation_deleted', ?)
                """, (service, epoch, account_id, prev["node_name"], now_str))
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
            # 读改写原子化 (2026-11-15 review storage#3): prev 读取与全部写入
            # 同一 IMMEDIATE 快照 —— 并发 move 下两个事务都读到 prev=A 会把
            # binding_changes 的 from_node 写错
            with self._immediate_txn(conn):
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

    def prune_binding_changes(self, keep_days: int = 7) -> int:
        """
        Deletes binding_changes rows older than keep_days. Without this the incremental
        reconciliation log (one row per rebind, 511+ rows per host outage) would grow
        unbounded. Downstreams whose `since_epoch` predates the retention window must do a
        full cold-start export instead of trusting an empty delta.
        """
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        return self._chunked_prune("binding_changes", "created_at", cutoff)

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
