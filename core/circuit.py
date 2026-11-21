"""
core/circuit.py - Industrial-grade Circuit Breaker with Exponential Backoff & Jitter
"""

import os
import random
from typing import Optional, Tuple
from datetime import datetime, timedelta
from core.models import ProxyNode, HealthStatus

BASE_COOLDOWN_SECONDS = 60.0
MAX_COOLDOWN_SECONDS = 1800.0
JITTER_MAX_SECONDS = 10.0

# Canary flap suppression (2026-11-21): half_open 复核失败重开时, 若节点
# fail_count 显示长期慢性死亡, 在 cap 之上按重开深度继续指数升级退避。
# 背景: teleapisub 等上游真死透的节点, canary 复核注定失败, 但 host_dampen
# 的 1/4 折扣把它们钉在 ~13min 一轮的必失败复核循环 (单节点 248 事件/天,
# 全池 63k canary 事件/天)。升级后的退避绕过 host_dampen ( dampen 只服务
# "共同原因可能修复" 的场景, chronic flapper 不满足该前提)。
FLAP_ESCALATION_BASE = 2.0        # 每档翻倍
FLAP_ESCALATION_MAX_SECONDS = 7200.0  # chronic flapper 复核上限 2h
# 本轮连续失败达到此值视为 chronic (反复证伪, 而非瞬时抖动)
FLAP_CHRONIC_FAIL_COUNT = 30
# Host-level failure dampening (2026-11-19 warp 隧道僵死事故): 同一宿主机
# 隧道层抖动 (sing-box 重启握手窗口 / mesh 抖动) 会让 sweep 把主机级事件
# 分散成 N 个独立断路, 每个都按指数退避各自叠加 → 恢复比底层慢 ~30min。
# 规则: 同主机短窗口内 dead 越多, 新触发的 DEAD 退避越短 (主机事件是
# 共同原因, 底层一修全体同时恢复, 长退避只会延迟拉活)。
HOST_FAILURE_WINDOW_MINUTES = 15
HOST_DEAD_CAP_FOR_SHORT_BACKOFF = 5  # 同主机 15min 内 dead >= 此数 → 退避打 1/4


class CircuitBreaker:
    """
    Manages 4-state circuit transitions:
    - HEALTHY (Closed): Normal operation.
    - DEGRADED: 1 failure observed or elevated latency.
    - DEAD (Open): 2+ consecutive failures. Traffic isolated; cooldown timer active.
    - HALF_OPEN: Cooldown expired. Allows a single canary probe to test recovery.
    """

    @staticmethod
    def compute_cooldown(consecutive_failures: int) -> float:
        """
        Exponential backoff with jitter:
        min(BASE * 2^(failures - 2), MAX) + uniform(0, JITTER)

        The exponent is clamped before exponentiation: 2**1024 overflows float
        conversion and would crash the probe sweep after a multi-day outage.
        """
        exponent = min(max(0, consecutive_failures - 2), 16)
        backoff = min(BASE_COOLDOWN_SECONDS * float(2 ** exponent), MAX_COOLDOWN_SECONDS)
        jitter = random.uniform(0, JITTER_MAX_SECONDS)
        return round(backoff + jitter, 2)

    @staticmethod
    def compute_flap_retrip_cooldown(consecutive_failures: int) -> float:
        """Chronic flapper 的 canary 重开退避: 在 MAX_COOLDOWN 之上按本轮
        连续失败深度继续指数升级, 封顶 FLAP_ESCALATION_MAX_SECONDS。

        锚用 consecutive_failures 而非累计 fail_count (2026-11-22 review
        P2 纠正): fail_count 是跨成功归零不清的累计值, 会把"历史上死过
        30 次、后来自愈、今天新死"的节点误判 chronic; consecutive_failures
        失败 +1 / 成功归零, 才真正表达"这轮反复死没死透"。

        与 compute_cooldown 的区别: 不受 MAX_COOLDOWN_SECONDS 封顶, 也不应
        再过 host_dampen_cooldown (dampen 加速的是"可能修复的共同原因",
        而本轮反复证伪的节点不满足该前提)。consecutive_failures 低 (新死
        节点) 不走本路径, 保持原有快速复核语义。"""
        depth = max(0, int(consecutive_failures // FLAP_CHRONIC_FAIL_COUNT) - 1)
        escalated = MAX_COOLDOWN_SECONDS * (FLAP_ESCALATION_BASE ** min(depth, 8))
        backoff = min(escalated, FLAP_ESCALATION_MAX_SECONDS)
        jitter = random.uniform(0, JITTER_MAX_SECONDS)
        return round(backoff + jitter, 2)

    @staticmethod
    def host_dampen_cooldown(cooldown_sec: float, conn, host: str) -> float:
        """同主机短窗口 mass-failure → 新 DEAD 退避打 1/4 (2026-11-19 事故根修).

        sweep 在宿主机隧道层抖动 (重启握手窗口) 期间会把主机级事件分散成
        N 个独立断路; 各自走满指数退避会让池侧恢复远慢于底层修复。共同原因
        的证据 = 同主机窗口内已有多个 dead —— 此时缩短新触发节点的退避,
        让下一轮 sweep 尽快复核 (失败会再退避, 不会探活风暴)。
        """
        if not host:
            return cooldown_sec
        try:
            window_start = (datetime.now()
                            - timedelta(minutes=HOST_FAILURE_WINDOW_MINUTES)).isoformat()
            row = conn.execute(
                "SELECT COUNT(DISTINCT ph.node_name) FROM probe_history ph "
                "JOIN nodes n ON n.name = ph.node_name "
                "WHERE n.host = ? AND ph.success = 0 AND ph.probed_at >= ?",
                (host, window_start),
            ).fetchone()
            recent_failures = row[0] if row else 0
            if recent_failures >= HOST_DEAD_CAP_FOR_SHORT_BACKOFF:
                return round(cooldown_sec / 4.0, 2)
        except Exception:
            pass  # 查询失败按无主机信号处理, 不阻塞熔断主路径
        return cooldown_sec

    @classmethod
    def evaluate(
        cls,
        node: ProxyNode,
        success: bool,
        latency_ms: float = 0.0,
        error: Optional[str] = None,
        consecutive_successes: int = 0,
        conn=None,
    ) -> Tuple[str, str, Optional[datetime]]:
        """
        Evaluates a probe result and determines the next circuit state,
        reason, and next_probe_allowed_at (if tripped into OPEN/DEAD).
        Returns: (new_state, reason, next_allowed_at)

        Recovery gate (HAProxy rise=2 / Consul success_before_passing, 2026-11-15):
        a DEAD/HALF_OPEN node's first canary success only enters a DEGRADED
        observation period; a second consecutive success confirms HEALTHY.
        consecutive_successes is the post-increment value the caller computed.

        conn (optional, 2026-11-19): storage 事务内连接 —— 传入时 DEAD 退避
        经 host_dampen_cooldown 感知同主机短窗口 mass-failure (共同原因 →
        短退避快复核); 测试路径不传 conn 时行为与旧版完全一致。
        """
        current_state = node.status
        now = datetime.now()

        if success:
            if current_state in (HealthStatus.DEAD.value, HealthStatus.HALF_OPEN.value):
                # rise=2: canary success -> observation period, not full recovery.
                # DEGRADED is excluded from the normal allocation ladder, giving
                # a slowstart-style ramp before full traffic resumes.
                return (
                    HealthStatus.DEGRADED.value,
                    "half_open_probe_succeeded_entering_observation",
                    None
                )
            elif current_state == HealthStatus.DEGRADED.value:
                if consecutive_successes >= 2:
                    return (
                        HealthStatus.HEALTHY.value,
                        "recovery_confirmed_after_observation",
                        None
                    )
                return (
                    HealthStatus.DEGRADED.value,
                    "recovery_observation_continues",
                    None
                )
            else:
                return (
                    HealthStatus.HEALTHY.value,
                    "healthy_probe_ok",
                    None
                )
        else:
            err_summary = (error or "probe_failed")[:60]
            new_failures = node.consecutive_failures + 1

            if current_state in (HealthStatus.HEALTHY.value, HealthStatus.UNPROBED.value):
                # 1st failure -> DEGRADED
                return (
                    HealthStatus.DEGRADED.value,
                    f"first_probe_failure: {err_summary}",
                    None
                )
            elif current_state == HealthStatus.DEGRADED.value:
                # 2nd failure -> Tripped into DEAD (Open)
                cooldown_sec = cls.compute_cooldown(new_failures)
                if conn is not None:
                    cooldown_sec = cls.host_dampen_cooldown(cooldown_sec, conn, node.host)
                next_allowed = now + timedelta(seconds=cooldown_sec)
                return (
                    HealthStatus.DEAD.value,
                    f"circuit_tripped_open_after_{new_failures}_failures: {err_summary}",
                    next_allowed
                )
            elif current_state == HealthStatus.HALF_OPEN.value:
                # Half-open trial failed -> Retrip into DEAD (Open) with longer cooldown.
                # 2026-11-21 flap 抑制: 本轮连续失败深的 chronic flapper 用升级
                # 退避 (绕过 host_dampen), 新死节点保持原快速复核路径。
                # 锚是 consecutive_failures (new_failures 含本次), 不是累计
                # fail_count —— 恢复过的节点二次死亡不该继承历史退避档。
                if new_failures >= FLAP_CHRONIC_FAIL_COUNT:
                    cooldown_sec = cls.compute_flap_retrip_cooldown(new_failures)
                else:
                    cooldown_sec = cls.compute_cooldown(new_failures)
                    if conn is not None:
                        cooldown_sec = cls.host_dampen_cooldown(cooldown_sec, conn, node.host)
                next_allowed = now + timedelta(seconds=cooldown_sec)
                return (
                    HealthStatus.DEAD.value,
                    f"half_open_probe_failed_reopened: {err_summary}",
                    next_allowed
                )
            else:
                # Already DEAD
                cooldown_sec = cls.compute_cooldown(new_failures)
                if conn is not None:
                    cooldown_sec = cls.host_dampen_cooldown(cooldown_sec, conn, node.host)
                next_allowed = now + timedelta(seconds=cooldown_sec)
                return (
                    HealthStatus.DEAD.value,
                    f"consecutive_failure_while_dead: {err_summary}",
                    next_allowed
                )

    # Envoy outlier-detection safeguards (2026-11-15): cap how far a single
    # sweep may expand the DEAD set. Without this, one upstream-wide hiccup
    # (subscription edge case, CF trace outage) could mass-eject the pool and
    # starve the allocator. domain cap 50% fits our heterogeneous multi-source
    # pool (each source = its own fault domain); global 30% is the panic backstop.
    DOMAIN_DEAD_RATIO_CAP = 0.50
    GLOBAL_DEAD_RATIO_CAP = 0.30
    # Envoy success_rate_minimum_hosts 思想: 小样本不做统计性拒绝 —— 全池节点数
    # 低于该值时 (单测/极小部署) guard 直接放行, 否则 1 个节点 dead = 100% 必拦。
    MIN_POOL_FOR_GUARD = 20

    @classmethod
    def allow_trip(cls, node: ProxyNode, conn) -> Tuple[bool, str]:
        """Decide whether a NEW DEAD transition may proceed (called inside the
        update_probe_result transaction, conn = same sqlite connection).
        Returns (allowed, reason). Renewals of already-DEAD nodes are checked
        by the caller (this only guards the DEGRADED/HALF_OPEN -> DEAD edge).
        Env `IP_POOL_MASS_GUARD=0` disables the guard entirely (kill-switch).

        2026-11-15 review #7/#8:
        - domain key 用 effective_fault_domain (空 fault_domain 回退 host),
          与 FailureDomainIsolator 同一规则 —— 否则 host 派生域的节点完全
          不受 50% domain cap 保护。
        - 分母只数【活跃】节点: DISABLED (永不恢复) 与 frozen (探活已降级,
          事实上出局) 都不算 —— Envoy outlier denominator 是 active hosts。
          不排除的话墓碑堆积会把 guard 锁死在永久 panic (合法熔断全被拒)。"""
        if os.environ.get("IP_POOL_MASS_GUARD", "1") in ("0", "false", "no"):
            return True, "mass_guard_disabled_by_env"
        row = conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE status != 'disabled' "
            "AND tags NOT LIKE '%\"frozen\"%'",).fetchone()
        if (row[0] or 0) < cls.MIN_POOL_FOR_GUARD:
            return True, "pool_too_small_for_guard"
        domain = node.effective_fault_domain
        if domain:
            # 2026-11-15 二轮 review P1: 域查询必须实现 host fallback ——
            # host 派生域的节点 fault_domain 列是 '', 只按 fault_domain = ? 查
            # 会命中 0 行, 50% domain cap 对这类域 (mesh host 的常见布局) 完全失效。
            # 与 storage.list_nodes_by_domain / mark_domain_dead 同一规则。
            row = conn.execute(
                "SELECT COUNT(*), SUM(CASE WHEN status='dead' THEN 1 ELSE 0 END) "
                "FROM nodes WHERE (fault_domain = ? OR (fault_domain = '' AND host = ?)) "
                "AND status != 'disabled' AND tags NOT LIKE '%\"frozen\"%'",
                (domain, domain)).fetchone()
            total, dead = (row[0] or 0), (row[1] or 0)
            if total > 0 and (dead + 1) / total > cls.DOMAIN_DEAD_RATIO_CAP:
                return False, f"ejection_capped_domain_over_{int(cls.DOMAIN_DEAD_RATIO_CAP * 100)}pct"
        row = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN status='dead' THEN 1 ELSE 0 END) "
            "FROM nodes WHERE status != 'disabled' AND tags NOT LIKE '%\"frozen\"%'").fetchone()
        total, dead = (row[0] or 0), (row[1] or 0)
        if total > 0 and (dead + 1) / total > cls.GLOBAL_DEAD_RATIO_CAP:
            return False, f"panic_mode_global_dead_over_{int(cls.GLOBAL_DEAD_RATIO_CAP * 100)}pct"
        return True, "allowed"

    @staticmethod
    def is_cooldown_expired(node: ProxyNode) -> bool:
        """
        Check if a DEAD node has passed its cooldown period and is eligible for HALF_OPEN.
        """
        if node.status != HealthStatus.DEAD.value:
            return False
        if not node.next_probe_allowed_at:
            return True
        try:
            allowed_at = datetime.fromisoformat(node.next_probe_allowed_at)
            return datetime.now() >= allowed_at
        except Exception:
            return True
