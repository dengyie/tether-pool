"""
core/models.py - Domain models for IP Pool System
"""

from enum import Enum
from typing import List, Optional, Dict
from pydantic import BaseModel, Field, field_validator


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


class Tier(str, Enum):
    PREMIUM = "premium"
    STANDARD = "standard"


class TierPolicy(str, Enum):
    STANDARD_FIRST = "standard_first"    # 默认：保护优质自建资源，优先使用普通池
    PREMIUM_FIRST = "premium_first"      # 核心/敏感业务：优先优质池，不可用时降级普通池
    PREMIUM_ONLY = "premium_only"        # 严禁降级，只使用优质池
    STANDARD_ONLY = "standard_only"      # 严禁占用优质池，只使用普通池


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
    tier: str = Tier.STANDARD.value
    configured_tier: Optional[str] = None
    quality_score: int = 50
    # Fault-domain identifiers: a host failure must isolate every listener on the
    # same box in one sweep instead of 25 independent circuit trips. When
    # `fault_domain` is empty the storage layer derives it from `host`.
    fault_domain: str = ""
    provider_region: str = ""
    # Node-side last liveness heartbeat (e.g. EasyTier peer timestamp).
    self_reported_at: Optional[str] = None

    @property
    def proxy_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"

    @property
    def effective_fault_domain(self) -> str:
        """Failure-domain key: explicit fault_domain wins, else the connection host."""
        return self.fault_domain.strip() or self.host


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
    tier: Optional[str] = None
    rebind_count: int = 0
    last_rebind_reason: str = ""
    binding_epoch: int = 0


class BindingEpochInfo(BaseModel):
    service: str
    epoch: int
    changed_at: Optional[str] = None
    last_change_reason: Optional[str] = None


class BindingChangeRecord(BaseModel):
    id: Optional[int] = None
    service: str
    epoch: int
    account_id: str
    from_node: Optional[str] = None
    to_node: str
    reason: str
    created_at: str


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
    tier_distribution: Dict[str, int] = Field(default_factory=dict)
    average_quality_score: float = 50.0


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
    tier: Optional[str] = None


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
    tier: Optional[str] = None


class BatchAllocationRequest(BaseModel):
    service: str = "default"
    accounts: List[str]
    strategy: str = AllocationStrategy.BALANCED_ROUND_ROBIN.value
    egress_preference: Optional[str] = EgressType.WARP_IPV6.value
    region: Optional[str] = None
    tier: Optional[str] = None
    tier_policy: Optional[str] = None
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


class NodeCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    host: str = Field(..., min_length=1)
    port: int = Field(..., ge=1, le=65535, description="Port number must be between 1 and 65535")
    protocol: str = Field("socks5h", description="Proxy protocol (socks5h, socks5, http, https)")
    region: str = Field(..., min_length=1)
    egress_type: str = Field(..., min_length=1)
    tags: List[str] = Field(default_factory=list)
    weight: int = Field(100, ge=1, le=1000)
    tier: Optional[str] = None
    configured_tier: Optional[str] = None
    relay_mode: str = RelayMode.RELAY_FALLBACK.value
    relay_hop_node: Optional[str] = None
    fault_domain: str = ""
    provider_region: str = ""

    @field_validator("tier", "configured_tier")
    @classmethod
    def validate_tier(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v_clean = v.strip().lower()
            if v_clean not in (Tier.PREMIUM.value, Tier.STANDARD.value):
                raise ValueError(f"Invalid tier: '{v}'. Must be '{Tier.PREMIUM.value}' or '{Tier.STANDARD.value}'")
            return v_clean
        return None

    @field_validator("protocol")
    @classmethod
    def validate_protocol(cls, v: str) -> str:
        v_clean = v.strip().lower()
        if v_clean not in ("socks5h", "socks5", "http", "https"):
            raise ValueError(f"Unsupported protocol: '{v}'. Must be socks5h, socks5, http, or https")
        return v_clean


class NodeUpdateRequest(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = Field(None, ge=1, le=65535)
    protocol: Optional[str] = None
    region: Optional[str] = None
    egress_type: Optional[str] = None
    tags: Optional[List[str]] = None
    weight: Optional[int] = Field(None, ge=1, le=1000)
    status: Optional[str] = None
    tier: Optional[str] = None
    configured_tier: Optional[str] = None
    relay_mode: Optional[str] = None
    relay_hop_node: Optional[str] = None
    fault_domain: Optional[str] = None
    provider_region: Optional[str] = None
    self_reported_at: Optional[str] = None

    @field_validator("tier", "configured_tier")
    @classmethod
    def validate_tier(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v_clean = v.strip().lower()
            if v_clean not in (Tier.PREMIUM.value, Tier.STANDARD.value):
                raise ValueError(f"Invalid tier: '{v}'. Must be '{Tier.PREMIUM.value}' or '{Tier.STANDARD.value}'")
            return v_clean
        return None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v_clean = v.strip().lower()
            valid_statuses = {s.value for s in HealthStatus}
            if v_clean not in valid_statuses:
                raise ValueError(f"Invalid status: '{v}'. Must be one of {valid_statuses}")
            return v_clean
        return None

    @field_validator("protocol")
    @classmethod
    def validate_protocol(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v_clean = v.strip().lower()
            if v_clean not in ("socks5h", "socks5", "http", "https"):
                raise ValueError(f"Unsupported protocol: '{v}'. Must be socks5h, socks5, http, or https")
            return v_clean
        return None




