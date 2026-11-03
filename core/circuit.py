"""
core/circuit.py - Industrial-grade Circuit Breaker with Exponential Backoff & Jitter
"""

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
        """
        exponent = max(0, consecutive_failures - 2)
        backoff = min(BASE_COOLDOWN_SECONDS * (2 ** exponent), MAX_COOLDOWN_SECONDS)
        jitter = random.uniform(0, JITTER_MAX_SECONDS)
        return round(backoff + jitter, 2)

    @classmethod
    def evaluate(
        cls,
        node: ProxyNode,
        success: bool,
        latency_ms: float = 0.0,
        error: Optional[str] = None
    ) -> Tuple[str, str, Optional[datetime]]:
        """
        Evaluates a probe result and determines the next circuit state,
        reason, and next_probe_allowed_at (if tripped into OPEN/DEAD).
        Returns: (new_state, reason, next_allowed_at)
        """
        current_state = node.status
        now = datetime.now()

        if success:
            if current_state in (HealthStatus.DEAD.value, HealthStatus.HALF_OPEN.value):
                return (
                    HealthStatus.HEALTHY.value,
                    "half_open_probe_succeeded_circuit_closed",
                    None
                )
            elif current_state == HealthStatus.DEGRADED.value:
                return (
                    HealthStatus.HEALTHY.value,
                    "degraded_recovered_to_healthy",
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
