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

    @classmethod
    def evaluate(
        cls,
        node: ProxyNode,
        success: bool,
        latency_ms: float = 0.0,
        error: Optional[str] = None,
        consecutive_successes: int = 0,
    ) -> Tuple[str, str, Optional[datetime]]:
        """
        Evaluates a probe result and determines the next circuit state,
        reason, and next_probe_allowed_at (if tripped into OPEN/DEAD).
        Returns: (new_state, reason, next_allowed_at)

        Recovery gate (HAProxy rise=2 / Consul success_before_passing, 2026-11-15):
        a DEAD/HALF_OPEN node's first canary success only enters a DEGRADED
        observation period; a second consecutive success confirms HEALTHY.
        consecutive_successes is the post-increment value the caller computed.
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
                next_allowed = now + timedelta(seconds=cooldown_sec)
                return (
                    HealthStatus.DEAD.value,
                    f"circuit_tripped_open_after_{new_failures}_failures: {err_summary}",
                    next_allowed
                )
            elif current_state == HealthStatus.HALF_OPEN.value:
                # Half-open trial failed -> Retrip into DEAD (Open) with longer cooldown
                cooldown_sec = cls.compute_cooldown(new_failures)
                next_allowed = now + timedelta(seconds=cooldown_sec)
                return (
                    HealthStatus.DEAD.value,
                    f"half_open_probe_failed_reopened: {err_summary}",
                    next_allowed
                )
            else:
                # Already DEAD
                cooldown_sec = cls.compute_cooldown(new_failures)
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
