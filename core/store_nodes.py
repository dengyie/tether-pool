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


class NodeStore:
    """节点域: CRUD / 探活结果 / 断路计数 / 冻结与墓碑 / 域隔离 / 剪枝。"""

    # 分块剪枝批大小 (类级常量, 测试会 monkeypatch 它验证分块边界)
    _PRUNE_CHUNK_SIZE = 5000

    def upsert_node(self, node: ProxyNode):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO nodes (
                    name, host, port, protocol, region, egress_type, tags, weight,
                    exit_ip, latency_ms, status, fail_count, last_probed_at,
                    circuit_state, consecutive_failures, consecutive_successes, next_probe_allowed_at,
                    relay_mode, relay_hop_node, tier, configured_tier, quality_score,
                    fault_domain, provider_region, probe_url, self_reported_at, dead_since
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    status=CASE WHEN nodes.status = 'disabled' THEN nodes.status
                                WHEN excluded.status != 'unprobed' THEN excluded.status
                                ELSE nodes.status END,
                    circuit_state=CASE WHEN nodes.status = 'disabled' THEN nodes.status
                                       WHEN excluded.circuit_state != 'unprobed' THEN excluded.circuit_state
                                       ELSE nodes.circuit_state END,
                    consecutive_failures=CASE WHEN excluded.status != 'unprobed' THEN excluded.consecutive_failures ELSE nodes.consecutive_failures END,
                    consecutive_successes=CASE WHEN excluded.status != 'unprobed' THEN excluded.consecutive_successes ELSE nodes.consecutive_successes END,
                    next_probe_allowed_at=CASE WHEN excluded.status != 'unprobed' THEN excluded.next_probe_allowed_at ELSE nodes.next_probe_allowed_at END,
                    fault_domain=CASE WHEN excluded.fault_domain != '' THEN excluded.fault_domain ELSE nodes.fault_domain END,
                    provider_region=CASE WHEN excluded.provider_region != '' THEN excluded.provider_region ELSE nodes.provider_region END,
                    -- probe_url: 节点级探活覆盖 (mesh CN 节点绕 GFW Cloudflare 段拦
                    -- 截)。无条件写入 —— yaml/DTO 为权威源, 移除字段即回默认阶梯
                    -- (review 根修: 非空覆盖会让已移除的探活目标永驻 DB 且 check 不报)。
                    probe_url=excluded.probe_url,
                    self_reported_at=COALESCE(excluded.self_reported_at, nodes.self_reported_at),
                    -- janitor 锚点: upsert 不得清掉既有 DEAD 倒计时 (探活恢复由
                    -- update_probe_result 清); 新节点携带的种子值也照写
                    dead_since=COALESCE(excluded.dead_since, nodes.dead_since)
            """, (
                node.name, node.host, node.port, node.protocol, node.region,
                node.egress_type, json.dumps(node.tags), node.weight,
                node.exit_ip, node.latency_ms, node.status, node.fail_count, node.last_probed_at,
                node.circuit_state or node.status, node.consecutive_failures,
                node.consecutive_successes, node.next_probe_allowed_at,
                node.relay_mode, node.relay_hop_node,
                node.tier or Tier.STANDARD.value, node.configured_tier, node.quality_score or 50,
                node.fault_domain or "", node.provider_region or "", node.probe_url or "",
                node.self_reported_at,
                node.dead_since
            ))

    def update_node_status(self, node_name: str, status: str):
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE nodes SET
                    status = ?,
                    circuit_state = ?,
                    -- 2026-11-15 review #5: 离开 dead 必须清锚点, 否则旧
                    -- dead_since 会被下次 DEAD 落锤沿用 (janitor 秒冻结)
                    dead_since = CASE WHEN ? != 'dead' THEN NULL ELSE dead_since END
                WHERE name = ?
            """, (status, status, status, node_name))

    def update_probe_result(self, res: ProbeResult, force: bool = False) -> Tuple[ProbeResult, Optional[CircuitEvent]]:
        """
        Records probe history and evaluates circuit breaker transition.
        Returns: (res, optional_circuit_event_if_transitioned)
        """
        now_str = datetime.now().isoformat()
        transition_event: Optional[CircuitEvent] = None
        drift_labels: Optional[Dict[str, str]] = None

        with self._get_conn() as conn:
            # 读改写必须整段原子 (2026-11-15 review storage#3): 默认延迟事务下
            # SELECT 在 autocommit 跑, 两个并发 probe 各读到同一基线, 后写 clobber
            # 前写 (熔断计数丢失 / DEAD 状态回退 / mass-guard 用陈旧比率)。
            # BEGIN IMMEDIATE 在事务头抓写锁, 以下 SELECT/UPDATE 全在同一快照。
            with self._immediate_txn(conn):
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

                # 管理态防护 (2026-11-15 review P1): DISABLED 是管理员显式禁用,
                # 探活不碰、永不自动恢复。全量轮 3400 节点耗时 ~14min, 轮内
                # disable/冻结的节点可能仍有在途探活结果飞回来 —— 这里在事务内
                # 重读状态, 飞回的旧结果不再把 admin_disabled 覆写回活
                # (upsert_node CASE 守卫与 prober 过滤之外的第三层)。
                # tags 含 "frozen" 同理: janitor 冻结节点的复核探活不应把它写
                # 回 degraded/healthy (恢复走 revive/enable 管理通道)。
                # row_tags 取自事务内重读的行, 而非外层快照 node.tags。
                row_tags = json.loads(row["tags"]) if row["tags"] else []
                # Disabled is an explicit administrator decision and can only be
                # lifted through enable/revive. Force probes may inspect frozen
                # nodes; a successful forced result unfreezes them below.
                if old_state == HealthStatus.DISABLED.value:
                    return res, None
                if "frozen" in row_tags and not force:
                    return res, None
                if "frozen" in row_tags and force and res.success:
                    row_tags = [tag for tag in row_tags if tag != "frozen"]

                # 3. Evaluate circuit transition
                # Calculate updated counters BEFORE evaluate: rise=2 recovery gate
                # (circuit.py) needs the post-increment consecutive_successes to
                # decide observation -> HEALTHY promotion.
                new_fails = 0 if res.success else (node.consecutive_failures + 1)
                new_succs = (node.consecutive_successes + 1) if res.success else 0

                new_state, reason, next_allowed = CircuitBreaker.evaluate(
                    node=node,
                    success=res.success,
                    latency_ms=res.latency_ms,
                    error=res.error,
                    consecutive_successes=new_succs,
                    conn=conn,  # 2026-11-19: host_dampen_cooldown 需事务内查询同主机失败窗口
                )

                # Mass-ejection guard (Envoy outlier safeguards, 2026-11-15): before
                # committing a NEW DEAD transition (DEGRADED/HALF_OPEN -> DEAD edge),
                # check the domain-bucket and global DEAD ratios. Renewals of
                # already-DEAD nodes (cooldown extension) are never blocked.
                # 2026-11-17 根因修复: 拦截只改写状态, 不丢弃 next_allowed ——
                # 之前的置 None 让被拦节点失去退避锚点, 每轮 sweep 立即重探立即
                # 再失败立即再被拦, 生产 1871 节点 fail_count>=20 永卡 degraded。
                if (not res.success and new_state == HealthStatus.DEAD.value
                        and old_state != HealthStatus.DEAD.value):
                    allowed, guard_reason = CircuitBreaker.allow_trip(node, conn)
                    if not allowed:
                        new_state = HealthStatus.DEGRADED.value
                        reason = f"{guard_reason}: {reason[:40]}"

                next_allowed_str = next_allowed.isoformat() if next_allowed else None
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
                    # isp tag 防漂移 (ban-aware 2026-11-14): pigeonw 等家宽节点的上游
                    # 出口会在 ISP 段间漂移, 探活成功时按 exit_ip 段校正 (core/ban),
                    # 与 exit_ip 写入同一事务, avoid_same_isp 判定拿到的是新鲜 ISP。
                    from core.ban import refresh_isp_tag
                    updated_node = refresh_isp_tag(node, res.exit_ip)
                    new_tags = updated_node.tags if updated_node else node.tags
                    if force and "frozen" in new_tags:
                        new_tags = [tag for tag in new_tags if tag != "frozen"]
                    # gen 区域漂移纠偏 (R2 2026-11-14): CF trace loc= 是出口真实国家,
                    # gen 批次当初硬编码 us-west 但 ~50% 出口实际在 DE/CA —— 标签失真
                    # 击穿 region_strict 与 auto_rehome。与 exit_ip 同事务校正为真实
                    # 国家码 (us->us-west), 仅限 gen 池 (tags 含 genproxy); sub 池每日
                    # 刷新自带 region_of, pigeonw/teleapi 出界不动。
                    new_region, new_provider_region = node.region, node.provider_region
                    probe_region = region_from_country(res.country_code)
                    if probe_region and probe_region != node.region and "genproxy" in node.tags:
                        new_region = probe_region
                        new_provider_region = probe_region
                        if "geo-fixed" not in new_tags:
                            new_tags = list(new_tags) + ["geo-fixed"]
                        # incr_metric 在 with 块外调 (线程本地连接复用下嵌套 with 的
                        # commit 会截断本事务的原子性)
                        drift_labels = {"from": node.region, "to": probe_region}
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
                            quality_score = ?,
                            tags = ?,
                            region = ?,
                            provider_region = ?,
                            dead_since = NULL,
                            last_probe_error = NULL
                        WHERE name = ?
                    """, (
                        res.exit_ip, res.latency_ms, new_state, new_state,
                        new_fails, new_succs, next_allowed_str, now_str,
                        q_tier, q_score,
                        # 无需校正时原样回写 (JSON 序列化与列存储同形)
                        json.dumps(new_tags, ensure_ascii=False),
                        new_region, new_provider_region,
                        res.node_name
                    ))
                else:
                    # dead_since 生命周期锚 (2026-11-15): 新落 DEAD 时记录锚点, 已
                    # DEAD 续期保持原锚 (连续不可达时长不因续期重置)。离开 DEAD
                    # (成功分支) 时清空。
                    dead_since_val = node.dead_since if old_state == HealthStatus.DEAD.value else (
                        getattr(node, "dead_since", None) or now_str if new_state == HealthStatus.DEAD.value else None)
                    if new_state != HealthStatus.DEAD.value:
                        dead_since_val = None
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
                            quality_score = ?,
                            dead_since = ?,
                            last_probe_error = ?
                        WHERE name = ?
                    """, (
                        new_state, new_state, new_fails, new_succs,
                        next_allowed_str, now_str,
                        q_tier, q_score, dead_since_val,
                        (res.error or "")[:120] or None, res.node_name
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

        if drift_labels:
            self.incr_metric("ip_pool_region_drift_fixed_total", drift_labels)

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
                    next_probe_allowed_at = NULL,
                    dead_since = NULL
                WHERE name = ?
            """, (node_name,))
        self.record_circuit_event(node_name, node.status, "healthy", "operator_manual_reset")
        return True

    def list_dead_since_before(self, iso_cutoff: str) -> List[ProxyNode]:
        """连续 DEAD 且 dead_since 早于 cutoff 的节点 (janitor 候选集)。"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM nodes WHERE status = 'dead' AND dead_since IS NOT NULL "
                "AND dead_since <= ? ORDER BY dead_since ASC", (iso_cutoff,)).fetchall()
            return [self._row_to_node(r) for r in rows]

    def freeze_node(self, node_name: str) -> bool:
        """冻结: tags 加 frozen + status 保持 dead (探活降频由 prober 按标签识别,
        分配本就排除 DEAD)。幂等。
        2026-11-15 review P2: 快照与写入之间节点可能被 revive/enable 成
        UNPROBED —— 状态复检不过就不冻 (否则活节点被 frozen 标签挡出探活)。
        2026-11-19 review P3: 复检+写入合入同一 IMMEDIATE 事务, UPDATE 带
        status='dead' 条件 —— 消除"读旧快照判 dead, 写前被并发救活又被盖回
        dead+frozen"的状态腐坏窗口 (frozen 标签会挡住非强制探活, 卡死到人工 revive)。"""
        with self._get_conn() as conn:
            with self._immediate_txn(conn):
                row = conn.execute("SELECT status, tags FROM nodes WHERE name = ?",
                                   (node_name,)).fetchone()
                if not row:
                    return False
                if row["status"] != HealthStatus.DEAD.value:
                    return False
                tags = json.loads(row["tags"] or "[]")
                newly_frozen = "frozen" not in tags
                if newly_frozen:
                    tags.append("frozen")
                cur = conn.execute(
                    "UPDATE nodes SET tags = ?, circuit_state = ? WHERE name = ? AND status = ?",
                    (json.dumps(tags), HealthStatus.DEAD.value,
                     node_name, HealthStatus.DEAD.value))
                if cur.rowcount == 0:
                    return False
        if newly_frozen:
            self.record_circuit_event(node_name, "dead", "dead", "janitor_frozen_dead_24h")
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

    def record_tombstone(self, node_name: str, reason: str):
        """驱逐登记: node_tombstones 表防 yaml 重载/自上报复活。"""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS node_tombstones (
                    node_name TEXT PRIMARY KEY,
                    reason TEXT,
                    created_at TEXT
                )
            """)
            conn.execute(
                "INSERT INTO node_tombstones (node_name, reason, created_at) VALUES (?, ?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET reason = excluded.reason",
                (node_name, reason, datetime.now().isoformat()))

    def get_tombstones(self) -> set:
        # 2026-11-15 review: fail-OPEN 会把驱逐节点当复活候选 —— 只容忍缺表
        # (未写入过任何 tombstone 的正常态), 其他错误 (locked/disk) 必须炸响。
        try:
            with self._get_conn() as conn:
                return {r[0] for r in conn.execute("SELECT node_name FROM node_tombstones").fetchall()}
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                return set()
            raise

    def revive_tombstoned(self, node_name: str) -> bool:
        """人工复活: 清 tombstone (管理员判断节点真的回来了)。"""
        with self._get_conn() as conn:
            # 表是 record_tombstone 惰性建的: 从未写过 tombstone 的库直接 DELETE
            # 会 no such table (2026-11-15 review #4)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS node_tombstones (
                    node_name TEXT PRIMARY KEY,
                    reason TEXT,
                    created_at TEXT
                )
            """)
            cur = conn.execute("DELETE FROM node_tombstones WHERE node_name = ?", (node_name,))
            return cur.rowcount > 0

    def mark_nodes_missing_as_dead(self, keep_names: List[str]) -> List[str]:
        """
        Marks registry nodes absent from the authoritative inventory as DEAD.
        Excludes dynamic API-managed nodes (tagged 'api_managed') from being marked dead.

        Deliberately non-destructive: bindings stay intact and the existing self-healing
        path re-homes them to healthy nodes. Hard deletion is only performed when the
        operator explicitly opts in (see `prune_nodes_except`).
        """
        with self._get_conn() as conn:
            # 读改写原子化 (2026-11-15 review storage#2): SELECT 基线与批量
            # UPDATE 曾跨事务, 探活成功可在两步之间把节点救活, 随后被本方法
            # 用陈旧基线无条件的 UPDATE 盖回 dead。加 status != 'dead' 条件,
            # 只标记仍非 dead 的节点 (且 COALESCE 保锚点)。
            with self._immediate_txn(conn):
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
                # 2026-11-15 review #2: 非探活路径落 DEAD 也必须写 dead_since 锚点,
                # 否则续期分支永远保留 NULL —— 这批节点永远进不了冻结/驱逐倒计时
                conn.execute(
                    f"UPDATE nodes SET status = ?, circuit_state = ?, "
                    f"dead_since = COALESCE(dead_since, ?) "
                    f"WHERE status != 'dead' AND name IN ({placeholders})",
                    [HealthStatus.DEAD.value, HealthStatus.DEAD.value,
                     datetime.now().isoformat(), *stale]
                )
                return stale

    def prune_nodes_except(self, keep_names: List[str]) -> List[str]:
        """
        Removes nodes absent from the authoritative inventory along with their bindings.
        Excludes dynamic API-managed nodes (tagged 'api_managed') from being pruned.
        Returns the pruned node names. Callers must validate `keep_names` first.

        2026-11-15 review P1: 与 delete_node 同理, 被清节点的绑定必须推进
        epoch + 落 change 行, 否则下游 reconcile 看不到这批释放。
        """
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            with self._immediate_txn(conn):
                existing = [
                    r["name"] for r in conn.execute(
                        "SELECT name FROM nodes WHERE tags NOT LIKE '%\"api_managed\"%'"
                    ).fetchall()
                ]
                stale = [n for n in existing if n not in set(keep_names)]
                if not stale:
                    return []
                orphaned = [
                    (r["service"], r["account_id"], r["node_name"])
                    for r in conn.execute(
                        "SELECT service, account_id, node_name FROM account_allocations "
                        f"WHERE node_name IN ({','.join('?' * len(stale))})",
                        stale,
                    ).fetchall()
                ]
                placeholders = ",".join("?" * len(stale))
                conn.execute(f"DELETE FROM leases WHERE node_name IN ({placeholders})", stale)
                conn.execute(f"DELETE FROM account_allocations WHERE node_name IN ({placeholders})", stale)
                conn.execute(f"DELETE FROM nodes WHERE name IN ({placeholders})", stale)
                if orphaned:
                    touched = {s for s, _, _ in orphaned}
                    epoch = self._advance_binding_epochs(conn, touched, now_str)
                    for service, account_id, node_name in orphaned:
                        conn.execute("""
                            INSERT INTO binding_changes
                                (service, epoch, account_id, from_node, to_node, reason, created_at)
                            VALUES (?, ?, ?, ?, NULL, 'node_pruned_binding_orphaned', ?)
                        """, (service, epoch[service], account_id, node_name, now_str))
                return stale

    def delete_node(self, name: str, prune_bindings: bool = True) -> bool:
        """
        Deletes a single node by name.
        If prune_bindings is True, also deletes any attached allocations and leases.
        Returns True if the node existed and was deleted, False otherwise.

        2026-11-15 review P1: 被删节点上的绑定必须推进 epoch 并落 change 行
        (to_node=NULL), 否则下游 reconcile 永远不知道这些账户已失效 —— 正是
        R5b 要防的 "N 个账户挂在已消失出口" 故障类。janitor 驱逐路径
        (api/server.py) 在调用本方法前已先 rehome, 这里兜的是剩余绑定。
        """
        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            with self._immediate_txn(conn):
                cur = conn.execute("SELECT 1 FROM nodes WHERE name = ?", (name,)).fetchone()
                if not cur:
                    return False
                orphaned: List[tuple] = []
                if prune_bindings:
                    orphaned = [
                        (r["service"], r["account_id"], r["node_name"])
                        for r in conn.execute(
                            "SELECT service, account_id, node_name FROM account_allocations "
                            "WHERE node_name = ?", (name,)
                        ).fetchall()
                    ]
                    conn.execute("DELETE FROM leases WHERE node_name = ?", (name,))
                    conn.execute("DELETE FROM account_allocations WHERE node_name = ?", (name,))
                conn.execute("DELETE FROM nodes WHERE name = ?", (name,))
                if orphaned:
                    touched = {s for s, _, _ in orphaned}
                    epoch = self._advance_binding_epochs(conn, touched, now_str)
                    for service, account_id, node_name in orphaned:
                        conn.execute("""
                            INSERT INTO binding_changes
                                (service, epoch, account_id, from_node, to_node, reason, created_at)
                            VALUES (?, ?, ?, ?, NULL, 'node_deleted_binding_orphaned', ?)
                        """, (service, epoch[service], account_id, node_name, now_str))
                return True

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
            # 读改写原子化 (2026-11-15 review storage#2): SELECT 快照与 UPDATE
            # 之间探活可能把节点救活, UPDATE 用陈旧基线盖回 dead。
            # UPDATE 自带 status != 'dead' 条件兜底, 快照 affected 仅用于返回值
            # 与事件计数 (标签为 domain 级, 个别漂移可接受)。
            with self._immediate_txn(conn):
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
                # 2026-11-15 review #2: 域级隔离落 DEAD 同样要写 dead_since 锚点
                conn.execute(
                    f"UPDATE nodes SET status = ?, circuit_state = ?, consecutive_failures = MAX(consecutive_failures, 2), "
                    f"consecutive_successes = 0, next_probe_allowed_at = ?, last_probed_at = ?, "
                    f"dead_since = COALESCE(dead_since, ?) WHERE status != 'dead' AND name IN ({placeholders})",
                    [HealthStatus.DEAD.value, HealthStatus.DEAD.value, next_allowed, now_str,
                     now_str, *names]
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
            "fault_domain", "provider_region", "self_reported_at",
            "dead_since",
            # 断路器计数与退避锚点 (2026-11-19 review P1): enable_node/revive_node
            # 传这三个键做真正的"新鲜重启"; 旧白名单静默丢弃导致复活节点带着
            # 陈旧 fail_count 重启, 一次 hiccup 退避直接钉到 2**stale 上限。
            "consecutive_failures", "consecutive_successes", "next_probe_allowed_at",
        }
        valid_updates = {k: v for k, v in updates.items() if k in allowed_fields}
        dropped = sorted(set(updates) - set(valid_updates))
        if dropped:
            # 2026-11-19 review P1: 静默丢键是陷阱 —— 非白名单键一律告警可见
            print(f"[storage] update_node({name}): ignored non-whitelisted keys: {dropped}")
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

        now_str = datetime.now().isoformat()
        with self._get_conn() as conn:
            # 读改写原子化 (2026-11-15 review storage#1): get_node + UPDATE 曾是
            # 两个独立事务 —— janitor freeze 读 tags=[a], 并发探活成功提交
            # tags=[a,isp=x], freeze 再写 tags=[a,frozen] 会静默丢掉 isp=x。
            with self._immediate_txn(conn):
                row = conn.execute("SELECT * FROM nodes WHERE name = ?", (name,)).fetchone()
                if not row:
                    return None
                current_node = self._row_to_node(row)

                set_clauses = [f"{k} = ?" for k in valid_updates.keys()]
                values = list(valid_updates.values())
                values.append(name)
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

    def _chunked_prune(self, table: str, where_col: str, cutoff: str) -> int:
        """按 rowid 分块删除 `<where_col> < cutoff` 的旧行, 每块一个
        BEGIN IMMEDIATE 事务, 返回删除总行数。"""
        total = 0
        with self._get_conn() as conn:
            while True:
                with self._immediate_txn(conn):
                    cur = conn.execute(
                        f"DELETE FROM {table} WHERE rowid IN "
                        f"(SELECT rowid FROM {table} WHERE {where_col} < ? "
                        f"LIMIT {self._PRUNE_CHUNK_SIZE})",
                        (cutoff,),
                    )
                    n = cur.rowcount
                total += n
                if n < self._PRUNE_CHUNK_SIZE:
                    return total

    def prune_probe_history(self, keep_days: int = 7) -> int:
        """Deletes probe_history rows older than keep_days to bound table growth.

        分块删除: probe_history 是最大的 event 表 (每节点每轮一条), 一次性
        DELETE 可能达百万行级, 单事务会造成 WAL 尖峰 + 探活写入阻塞。
        """
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
        return self._chunked_prune("probe_history", "probed_at", cutoff)
