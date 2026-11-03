"""
core/models.py - Domain models for IP Pool System
"""

from enum import Enum
from typing import List, Optional, Dict
from pydantic import BaseModel, Field


class EgressType(str, Enum):
    WARP_IPV6 = "warp_ipv6"
    NATIVE_IPV4 = "native_ipv4"
    NATIVE_IPV6 = "native_ipv6"
    CLASH_PROXY = "clash_proxy"


class RelayMode(str, Enum):
    DIRECT_ONLY = "direct_only"          # 仅允许直连
    RELAY_PREFERRED = "relay_pref"       # 优先中继（适合跨境恶劣节点）
    RELAY_FALLBACK = "relay_fallback"    # 直连优先，超时/失败自动跳板中继 (默认)


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
    relay_mode: str = RelayMode.RELAY_FALLBACK.value
    relay_hop_node: Optional[str] = None

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
    clash_proxy_count: int = 0
    region_distribution: dict


class RegistrationLease(BaseModel):
    account_id: str
    target_service: str
    node_name: str
    proxy_url: str
    exit_ip: Optional[str] = None
    region: str
    egress_type: str
    cooldown_seconds: int = 1800
    leased_at: str
    cooled_until: str
    relay_mode: str = RelayMode.RELAY_FALLBACK.value
    relay_hop: Optional[str] = None


class IpCooldownRecord(BaseModel):
    id: Optional[int] = None
    exit_ip: str
    target_service: str
    node_name: str
    cooled_until: str
    created_at: str


class BatteryEventType(str, Enum):
    RATE_LIMITED_429 = "rate_limited_429"
    BANNED_401 = "banned_401"
    TURNSTILE_FAILED = "turnstile_failed"
    TIMEOUT = "timeout"
    SUCCESS = "success"


class BatteryEventReport(BaseModel):
    account_id: str
    node_name: Optional[str] = None
    event_type: str
    target_service: Optional[str] = None
    force_rebind: bool = True


class AllocationStrategy(str, Enum):
    BALANCED_ROUND_ROBIN = "balanced_round_robin"
    CONSISTENT_HASH = "consistent_hash"


class AccountAllocationItem(BaseModel):
    account_id: str
    node_name: str
    proxy_url: str
    gateway_proxy_url: str
    exit_ip: Optional[str] = None
    region: str
    egress_type: str
    service: str
    allocated_at: str


class BatchAllocationRequest(BaseModel):
    service: str = "default"
    accounts: List[str]
    strategy: str = AllocationStrategy.BALANCED_ROUND_ROBIN.value
    egress_preference: Optional[str] = EgressType.WARP_IPV6.value
    region: Optional[str] = None
    max_accounts_per_ip: int = 50
    force_rebalance: bool = False


class BatchAllocationResponse(BaseModel):
    service: str
    total_accounts: int
    allocated_nodes_count: int
    min_accounts_per_node: int
    max_accounts_per_node: int
    avg_accounts_per_node: float
    allocations: Dict[str, AccountAllocationItem]


class NodeAllocationStat(BaseModel):
    node_name: str
    region: str
    egress_type: str
    exit_ip: Optional[str] = None
    account_count: int


class AllocationOverviewResponse(BaseModel):
    total_allocations: int
    service_distribution: Dict[str, int]
    node_stats: List[NodeAllocationStat]


