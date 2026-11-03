"""
core/models.py - Domain models for IP Pool System
"""

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class EgressType(str, Enum):
    WARP_IPV6 = "warp_ipv6"
    NATIVE_IPV4 = "native_ipv4"
    NATIVE_IPV6 = "native_ipv6"


class HealthStatus(str, Enum):
    HEALTHY = "healthy"      # Closed circuit: fully operational
    DEGRADED = "degraded"    # Unstable: high latency or single failure
    DEAD = "dead"            # Open circuit: isolated / failing
    HALF_OPEN = "half_open"  # Trial probe circuit: cooldown elapsed
    UNPROBED = "unprobed"


class ProxyNode(BaseModel):
    name: str
    host: str
    port: int
    protocol: str = "socks5h"
    region: str
    egress_type: str
    tags: List[str] = Field(default_factory=list)
    weight: int = 100
    exit_ip: Optional[str] = None
    latency_ms: Optional[float] = None
    status: str = HealthStatus.UNPROBED.value
    fail_count: int = 0
    last_probed_at: Optional[str] = None
    circuit_state: str = HealthStatus.UNPROBED.value
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    next_probe_allowed_at: Optional[str] = None

    @property
    def proxy_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"


class CircuitEvent(BaseModel):
    id: Optional[int] = None
    node_name: str
    from_state: str
    to_state: str
    reason: str
    created_at: str


class ProbeResult(BaseModel):
    node_name: str
    success: bool
    exit_ip: Optional[str] = None
    latency_ms: float = 0.0
    error: Optional[str] = None


class LeaseInfo(BaseModel):
    account_id: str
    node_name: str
    proxy_url: str
    exit_ip: Optional[str] = None
    region: str
    egress_type: str
    assigned_at: str


class PoolStats(BaseModel):
    total_nodes: int
    healthy_nodes: int
    degraded_nodes: int
    dead_nodes: int
    unprobed_nodes: int
    warp_ipv6_count: int
    native_ipv4_count: int
    native_ipv6_count: int
    region_distribution: dict
