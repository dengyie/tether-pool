"""gateway/auth.py — 参数化凭据解析与路由上下文 (从 server.py 分层, 2026-11-19)。

parse_proxy_credentials: 4 种凭据格式的线性探测解析;
RoutingContext: 请求级路由上下文; INTERNAL_ROUTING_HEADERS: 内部头清单。
"""
import logging
import urllib.parse
import uuid
from typing import Optional

logger = logging.getLogger("tetherpool.gateway.auth")



INTERNAL_ROUTING_HEADERS = frozenset({
    "x-account-id",
    "x-region",
    "x-region-strict",
    "x-egress-type",
    "x-tier",
    "x-tier-policy",
    "x-service",
    "x-target-service",
    "x-rebind",
    "x-force-rebind",
    "x-bind-persist",
    "proxy-authorization",
    "proxy-connection",
    "connection",
})


def _is_internal_header(name: str) -> bool:
    lowered = name.strip().lower()
    return lowered in INTERNAL_ROUTING_HEADERS or lowered.startswith("x-tetherpool-")


class ParsedCredentials:
    """
    Parametric credential parse result. Supports 4-item tuple unpacking for full backward
    compatibility, as well as property access for tier, tier_policy, force_rebind,
    region_strict and bind_persist.
    """
    def __init__(
        self,
        account_id: str,
        region: Optional[str] = None,
        egress_type: Optional[str] = None,
        service: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebind: bool = False,
        region_strict: bool = False,
        bind_persist: bool = False
    ):
        self.account_id = account_id
        self.region = region
        self.egress_type = egress_type
        self.service = service
        self.tier = tier
        self.tier_policy = tier_policy
        self.force_rebind = force_rebind
        self.region_strict = region_strict
        self.bind_persist = bind_persist

    def __iter__(self):
        yield self.account_id
        yield self.region
        yield self.egress_type
        yield self.service

    def __getitem__(self, idx):
        items = [self.account_id, self.region, self.egress_type, self.service]
        return items[idx]

    def __len__(self):
        return 4


class RoutingContext:
    """
    Gateway request routing context. Supports 4-item unpacking for backward compatibility,
    as well as properties for tier, tier_policy, force_rebind, region_strict and bind_persist.
    """
    def __init__(
        self,
        account_id: str,
        region: Optional[str] = None,
        egress_type: Optional[str] = None,
        service: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebind: bool = False,
        region_strict: bool = False,
        bind_persist: bool = False
    ):
        self.account_id = account_id
        self.region = region
        self.egress_type = egress_type
        self.service = service
        self.tier = tier
        self.tier_policy = tier_policy
        self.force_rebind = force_rebind
        self.region_strict = region_strict
        self.bind_persist = bind_persist

    def __iter__(self):
        yield self.account_id
        yield self.region
        yield self.egress_type
        yield self.service

    def __getitem__(self, idx):
        items = [self.account_id, self.region, self.egress_type, self.service]
        return items[idx]

    def __len__(self):
        return 4


def parse_proxy_credentials(raw_user: str) -> ParsedCredentials:
    """
    Parses parametric proxy username into (account_id, region, egress_type, service, tier, tier_policy, ...).
    Supported formats:
    1. Plain account ID: 'bot_user_01'
    2. Double-underscore tags: 'bot_01__type_warp_ipv6__region_tokyo__tier_premium__service_grok2api__rebind_true'
    3. Query-string format: 'bot_01?region=tokyo&type=warp_ipv6&tier=premium&service=grok2api&rebind=true'
    4. Key-value dash format: 'account=bot_01-region=tokyo-type=warp_ipv6-tier=premium-service=grok2api-rebind=true'
    """
    if not raw_user:
        return ParsedCredentials(f"anon_{uuid.uuid4().hex[:8]}")

    raw_user = raw_user.strip()
    account_id = raw_user
    region = None
    egress_type = None
    service = None
    tier = None
    tier_policy = None
    force_rebind = False
    region_strict = False
    bind_persist = False

    # Format 3: Query-string format
    if "?" in raw_user:
        base, query_str = raw_user.split("?", 1)
        account_id = base
        for part in query_str.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                k = k.strip().lower()
                v = v.strip()
                if k in ("region", "country", "geo"):
                    region = v
                elif k in ("type", "egress", "egress_type"):
                    egress_type = v
                elif k in ("service", "app", "target"):
                    service = v
                elif k in ("tier", "pool", "quality"):
                    tier = v.lower()
                elif k in ("tier_policy", "policy", "routing_policy"):
                    tier_policy = v.lower()
                elif k in ("rebind", "force_rebind", "rebalance"):
                    force_rebind = v.lower() in ("true", "1", "yes")
                elif k in ("region_strict", "strict_region"):
                    region_strict = v.lower() in ("true", "1", "yes")
                elif k in ("bind_persist", "bind_persist_true"):
                    bind_persist = v.lower() in ("true", "1", "yes")

    # Format 2: Double-underscore format (e.g. acc_001__type_warp_ipv6__region_tokyo__tier_premium__rebind_true)
    elif "__" in raw_user:
        parts = raw_user.split("__")
        account_id = parts[0]
        for p in parts[1:]:
            p_lower = p.lower()
            # Flag tokens must be matched BEFORE the `region_` value prefix, otherwise
            # `region_strict_true` is swallowed as region="strict_true".
            if p_lower in ("region_strict_true", "strict_region_true", "region_strict",
                           "strict_region", "region_strict=true", "strict_region=true"):
                region_strict = True
            elif p_lower in ("bind_persist_true", "bind_persist", "bind_persist=true"):
                bind_persist = True
            elif p_lower.startswith("type_"):
                egress_type = p[5:]
            elif p_lower.startswith("egress_"):
                egress_type = p[7:]
            elif p_lower.startswith("region_"):
                region = p[7:]
            elif p_lower.startswith("country_"):
                region = p[8:]
            elif p_lower.startswith("service_"):
                service = p[8:]
            elif p_lower.startswith("app_"):
                service = p[4:]
            elif p_lower.startswith("tier_"):
                tier = p_lower[5:]
            elif p_lower.startswith("policy_"):
                tier_policy = p_lower[7:]
            elif p_lower.startswith("tierpolicy_"):
                tier_policy = p_lower[11:]
            elif p_lower in ("rebind_true", "force_rebind", "rebind", "rebind=true"):
                force_rebind = True
            elif p_lower.startswith("rebind_"):
                force_rebind = p_lower[7:] in ("true", "1", "yes")

    # Format 4: Dash-separated key-value (e.g. account=bot_01-region=tokyo-tier=premium-rebind=true)
    elif "account=" in raw_user.lower() or "region=" in raw_user.lower() or "type=" in raw_user.lower() or "service=" in raw_user.lower() or "tier=" in raw_user.lower() or "rebind=" in raw_user.lower():
        parts = raw_user.split("-")
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                k = k.strip().lower()
                v = v.strip()
                if k in ("account", "user", "acc"):
                    account_id = v
                elif k in ("region", "country", "geo"):
                    region = v
                elif k in ("type", "egress", "egress_type"):
                    egress_type = v
                elif k in ("service", "app", "target"):
                    service = v
                elif k in ("tier", "pool", "quality"):
                    tier = v.lower()
                elif k in ("policy", "tier_policy"):
                    tier_policy = v.lower()
                elif k in ("rebind", "force_rebind"):
                    force_rebind = v.lower() in ("true", "1", "yes")
                elif k in ("region_strict", "strict_region"):
                    region_strict = v.lower() in ("true", "1", "yes")
                elif k in ("bind_persist",):
                    bind_persist = v.lower() in ("true", "1", "yes")

    # Sanitize and unquote account_id
    account_id = urllib.parse.unquote(account_id.strip()) if account_id else f"anon_{uuid.uuid4().hex[:8]}"
    region = region.strip() if region and region.strip() else None
    egress_type = egress_type.strip() if egress_type and egress_type.strip() else None
    service = service.strip() if service and service.strip() else None
    tier = tier.strip() if tier and tier.strip() else None
    tier_policy = tier_policy.strip() if tier_policy and tier_policy.strip() else None

    # 格式命中可观察 (2026-11-18 review F4): 四种格式共享一条线性探测链, 字面
    # 账号名撞上 `?` / `__` / `k=v` 关键词会被静默改写成别的格式解析 (绑定落到
    # 非预期名字下), 无日志时这类误触发无法排查。DEBUG 级: 稳态消费方一行/请求。
    fmt = ("query" if "?" in raw_user else "dunder" if "__" in raw_user else
           "dashkv" if any(t in raw_user.lower() for t in ("account=", "region=", "type=", "service=", "tier=", "rebind=")) else "plain")
    if fmt != "plain":
        logger.debug(
            "credential format=%s account=%s region=%s egress=%s service=%s tier=%s policy=%s rebind=%s strict=%s persist=%s",
            fmt, account_id, region, egress_type, service, tier, tier_policy,
            force_rebind, region_strict, bind_persist,
        )

    return ParsedCredentials(
        account_id=account_id,
        region=region,
        egress_type=egress_type,
        service=service,
        tier=tier,
        tier_policy=tier_policy,
        force_rebind=force_rebind,
        region_strict=region_strict,
        bind_persist=bind_persist
    )
