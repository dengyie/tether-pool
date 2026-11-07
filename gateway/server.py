"""
gateway/server.py - High-Performance Local Unified Forward Proxy Gateway (:9101)
Transparently maps client requests to sticky EasyTier SOCKS5 exits based on headers.
"""

import asyncio
import base64
import logging
import os
import sys
import socket
import struct
import urllib.parse
import uuid
from typing import Optional, Tuple, Dict

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import ProxyNode, RelayMode, ProbeResult, HealthStatus
from core.storage import Storage
from core.router import Router
from core.failover import FailureDomainIsolator, EasytierPeerMonitor

DEFAULT_GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1")
DEFAULT_GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "9101"))
BUFFER_SIZE = 65536
CONNECT_TIMEOUT = 10.0

# R1: bounded failover budget. The old code hardcoded `range(2)`, which meant a dead node
# was retried exactly once and then the request died — even though the pool had dozens of
# healthy exits. 3 attempts matches the acceptance target (3 x <=2s < 6s worst case).
GATEWAY_MAX_ATTEMPTS = max(1, int(os.environ.get("GATEWAY_MAX_ATTEMPTS", "3")))
# No wait by default: an all-healthy pool almost never retries, and adding sleep here
# stacks directly onto the user-visible request latency.
GATEWAY_RETRY_BACKOFF_MS = max(0, int(os.environ.get("GATEWAY_RETRY_BACKOFF_MS", "0")))
# R4: hard ceiling on a *single* attempt, including any relay fallback chaining. Without
# this the RELAY_FALLBACK path could spend 2s direct + 2s relay per attempt, so three
# attempts would blow the 6s budget on a host that is simply gone.
GATEWAY_ATTEMPT_TIMEOUT = float(os.environ.get("GATEWAY_ATTEMPT_TIMEOUT", "2.0"))

# R4: EasyTier peer-table monitoring (sub-5s whole-host detection).
EASYTIER_MONITOR_ENABLED = os.environ.get("EASYTIER_MONITOR_ENABLED", "1").strip() not in ("0", "false", "no")
EASYTIER_SCAN_INTERVAL_SEC = float(os.environ.get("EASYTIER_SCAN_INTERVAL_SEC", "1.0"))

# How long the surviving direction may keep draining after the opposite side EOFs.
HALF_CLOSE_GRACE_SECONDS = 5.0

# R6 error semantics, consumed by downstreams to decide whether to retry.
ERR_POOL_EXHAUSTED = "pool_exhausted"
ERR_BINDING_REHOMED = "binding_rehomed"
ERR_NO_HEALTHY_TARGET = "no_healthy_target"

# Routing metadata consumed by the gateway itself; never forwarded to origins.
logger = logging.getLogger("tetherpool.gateway")

INTERNAL_ROUTING_HEADERS = frozenset({
    "x-account-id",
    "x-region",
    "x-egress-type",
    "x-tier",
    "x-tier-policy",
    "x-service",
    "x-target-service",
    "x-rebind",
    "x-force-rebind",
    "proxy-authorization",
    "proxy-connection",
    "connection",
})


class NoPoolNodesError(RuntimeError):
    """No usable node in the pool: a capacity condition (503), not an upstream fault."""


class UpstreamFailoverError(Exception):
    """
    Retry budget exhausted for this request (R6).

    Carries the machine-readable `code` the gateway puts in `X-TetherPool-Error` so a
    downstream can distinguish "the whole pool is full" (shed load) from "your account's
    binding was dead and has now moved, retry once" (retry immediately) without parsing
    prose out of the response body.
    """

    def __init__(self, code: str, message: str, retry: Optional[int] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retry = retry

    @property
    def http_status(self) -> int:
        if self.code == ERR_BINDING_REHOMED:
            return 502
        return 503

    @property
    def http_reason(self) -> str:
        return "Bad Gateway" if self.http_status == 502 else "Service Unavailable"


def _is_internal_header(name: str) -> bool:
    lowered = name.strip().lower()
    return lowered in INTERNAL_ROUTING_HEADERS or lowered.startswith("x-tetherpool-")


async def socks5_handshake_on_stream(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
    timeout: float = CONNECT_TIMEOUT
) -> None:
    """Performs SOCKS5 auth negotiation and CONNECT command on an existing open stream."""
    # 1. Auth negotiation (No authentication: 0x00)
    writer.write(b"\x05\x01\x00")
    await writer.drain()

    auth_resp = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
    if auth_resp != b"\x05\x00":
        raise ConnectionError(f"Upstream SOCKS5 handshake rejected: {auth_resp.hex()}")

    # 2. Connect request
    try:
        ip_v4 = socket.inet_aton(target_host)
        addr_bytes = b"\x01" + ip_v4
    except OSError:
        try:
            ip_v6 = socket.inet_pton(socket.AF_INET6, target_host)
            addr_bytes = b"\x04" + ip_v6
        except OSError:
            host_bytes = target_host.encode("utf-8")
            if len(host_bytes) > 255:
                raise ValueError(f"Domain name exceeds SOCKS5 limit (255 bytes): {len(host_bytes)}")
            addr_bytes = b"\x03" + bytes([len(host_bytes)]) + host_bytes

    port_bytes = struct.pack("!H", target_port)
    req = b"\x05\x01\x00" + addr_bytes + port_bytes
    writer.write(req)
    await writer.drain()

    # 3. Read reply
    reply_head = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
    ver, rep, rsv, atyp = reply_head
    if rep != 0x00:
        raise ConnectionError(f"Upstream SOCKS5 connect failed with code: {rep}")

    # Consume bound address
    if atyp == 0x01:  # IPv4
        await reader.readexactly(4 + 2)
    elif atyp == 0x03:  # Domain
        dlen = (await reader.readexactly(1))[0]
        await reader.readexactly(dlen + 2)
    elif atyp == 0x04:  # IPv6
        await reader.readexactly(16 + 2)


async def socks5_connect(
    proxy_host: str,
    proxy_port: int,
    target_host: str,
    target_port: int,
    timeout: float = CONNECT_TIMEOUT
) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Establishes a single-hop connection to an upstream SOCKS5 proxy."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(proxy_host, proxy_port),
        timeout=timeout
    )
    try:
        await socks5_handshake_on_stream(reader, writer, target_host, target_port, timeout=timeout)
        return reader, writer
    except Exception:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        raise


async def socks5_relay_connect(
    relay_host: str,
    relay_port: int,
    target_proxy_host: str,
    target_proxy_port: int,
    dest_host: str,
    dest_port: int,
    timeout: float = CONNECT_TIMEOUT
) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """
    Two-hop chained SOCKS5 connection:
    Client/Gateway -> Relay Hop -> Target Proxy Node -> Destination Host:Port
    """
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(relay_host, relay_port),
        timeout=timeout
    )
    try:
        # Hop 1: Connect through Relay Hop to Target Proxy Node
        await socks5_handshake_on_stream(reader, writer, target_proxy_host, target_proxy_port, timeout=timeout)
        # Hop 2: Connect through Target Proxy Node to Destination
        await socks5_handshake_on_stream(reader, writer, dest_host, dest_port, timeout=timeout)
        return reader, writer
    except Exception:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        raise


def parse_host_port(raw_target: str, default_port: int = 80) -> Tuple[str, int]:
    """
    Safely parses host and port from a target string or Host header.
    Properly handles IPv4, IPv6 (with or without brackets), and domain names.
    Raises ValueError on invalid host or port numbers.
    """
    raw_target = raw_target.strip()
    if not raw_target:
        raise ValueError("Empty host target")

    # Fast-path / fallback for bare unbracketed IPv6 addresses (e.g., 2001:db8::1 or ::1)
    if ":" in raw_target and not raw_target.startswith("["):
        try:
            socket.inet_pton(socket.AF_INET6, raw_target)
            return raw_target, default_port
        except OSError:
            pass

    if not raw_target.startswith("//"):
        split_target = "//" + raw_target
    else:
        split_target = raw_target

    try:
        parsed = urllib.parse.urlsplit(split_target)
        host = parsed.hostname
        if not host:
            raise ValueError(f"Could not parse host from: {raw_target}")
        port = parsed.port if parsed.port is not None else default_port
        if not (1 <= port <= 65535):
            raise ValueError(f"Port out of range: {port}")
        return host, port
    except ValueError as e:
        raise ValueError(f"Invalid host or port: {raw_target}") from e


async def pump_streams(r1: asyncio.StreamReader, w1: asyncio.StreamWriter, r2: asyncio.StreamReader, w2: asyncio.StreamWriter):
    """
    Bidirectionally pumps data between client and upstream proxy.

    Uses half-close semantics: when one direction reaches EOF the peer is signalled
    with write_eof() and the opposite direction gets a bounded grace period to
    finish. Cancelling both directions on first completion truncated HTTP CONNECT
    responses whenever the client half-closed early.
    """
    async def _forward(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                data = await reader.read(BUFFER_SIZE)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            try:
                if writer.can_write_eof():
                    writer.write_eof()
            except (NotImplementedError, RuntimeError, OSError):
                pass

    t1 = asyncio.create_task(_forward(r1, w2))
    t2 = asyncio.create_task(_forward(r2, w1))
    try:
        _, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=HALF_CLOSE_GRACE_SECONDS)
            except asyncio.TimeoutError:
                pass
            except Exception:
                pass
    finally:
        for task in (t1, t2):
            if not task.done():
                task.cancel()
        await asyncio.gather(t1, t2, return_exceptions=True)
        for w in (w1, w2):
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass


class ParsedCredentials:
    """
    Parametric credential parse result. Supports 4-item tuple unpacking for full backward
    compatibility, as well as property access for tier, tier_policy, and force_rebind.
    """
    def __init__(
        self,
        account_id: str,
        region: Optional[str] = None,
        egress_type: Optional[str] = None,
        service: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebind: bool = False
    ):
        self.account_id = account_id
        self.region = region
        self.egress_type = egress_type
        self.service = service
        self.tier = tier
        self.tier_policy = tier_policy
        self.force_rebind = force_rebind

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

    def as_tuple(self) -> Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], bool]:
        return (self.account_id, self.region, self.egress_type, self.service, self.tier, self.tier_policy, self.force_rebind)


class RoutingContext:
    """
    Gateway request routing context. Supports 4-item unpacking for backward compatibility,
    as well as properties for tier, tier_policy, and force_rebind.
    """
    def __init__(
        self,
        account_id: str,
        region: Optional[str] = None,
        egress_type: Optional[str] = None,
        service: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebind: bool = False
    ):
        self.account_id = account_id
        self.region = region
        self.egress_type = egress_type
        self.service = service
        self.tier = tier
        self.tier_policy = tier_policy
        self.force_rebind = force_rebind

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
    Parses parametric proxy username into (account_id, region, egress_type, service, tier, tier_policy, force_rebind).
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

    # Format 2: Double-underscore format (e.g. acc_001__type_warp_ipv6__region_tokyo__tier_premium__rebind_true)
    elif "__" in raw_user:
        parts = raw_user.split("__")
        account_id = parts[0]
        for p in parts[1:]:
            p_lower = p.lower()
            if p_lower.startswith("type_"):
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

    # Sanitize and unquote account_id
    account_id = urllib.parse.unquote(account_id.strip()) if account_id else f"anon_{uuid.uuid4().hex[:8]}"
    region = region.strip() if region and region.strip() else None
    egress_type = egress_type.strip() if egress_type and egress_type.strip() else None
    service = service.strip() if service and service.strip() else None
    tier = tier.strip() if tier and tier.strip() else None
    tier_policy = tier_policy.strip() if tier_policy and tier_policy.strip() else None

    return ParsedCredentials(
        account_id=account_id,
        region=region,
        egress_type=egress_type,
        service=service,
        tier=tier,
        tier_policy=tier_policy,
        force_rebind=force_rebind
    )


class ForwardProxyGateway:
    def __init__(
        self,
        storage: Storage,
        router: Router,
        host: str = DEFAULT_GATEWAY_HOST,
        port: int = DEFAULT_GATEWAY_PORT,
        max_attempts: int = GATEWAY_MAX_ATTEMPTS,
        retry_backoff_ms: int = GATEWAY_RETRY_BACKOFF_MS,
        attempt_timeout: float = GATEWAY_ATTEMPT_TIMEOUT,
        isolator: Optional[FailureDomainIsolator] = None,
        peer_monitor: Optional[EasytierPeerMonitor] = None
    ):
        self.storage = storage
        self.router = router
        self.host = host
        self.port = port
        self.server = None
        self.max_attempts = max(1, max_attempts)
        self.retry_backoff = max(0, retry_backoff_ms) / 1000.0
        self.attempt_timeout = max(0.1, attempt_timeout)
        # R3: one isolator per gateway; it degrades to single-node reporting for
        # domains that contain only one listener.
        self.isolator = isolator or FailureDomainIsolator(storage, router)
        # R4: optional peer-table watcher, started by `start()` when enabled.
        self.peer_monitor = peer_monitor
        self._peer_task = None
        # Per-account relay counters (prod hotfix): attribute unified-gateway traffic to
        # downstream accounts. anon_* ids collapse into one label to bound cardinality.
        self.relay_total: Dict[str, int] = {}

    def _record_relay(self, account_id: str, node_name: str, target: str) -> None:
        key = account_id if not account_id.startswith("anon_") else "anon"
        if key not in self.relay_total and len(self.relay_total) >= 2000:
            key = "other"
        self.relay_total[key] = self.relay_total.get(key, 0) + 1
        logger.info("relay established account=%s node=%s target=%s", key, node_name, target)

    def render_metrics(self) -> bytes:
        lines = [
            "# HELP tetherpool_gateway_relay_total Relay connections established per account.",
            "# TYPE tetherpool_gateway_relay_total counter",
        ]
        for acct, count in sorted(self.relay_total.items()):
            lines.append('tetherpool_gateway_relay_total{account="%s"} %d' % (acct, count))
        return ("\n".join(lines) + "\n").encode()

    def _extract_routing_headers(self, headers_dict: Dict[str, str], client_ip: str) -> RoutingContext:
        """
        Extracts account_id, region, egress_type, service, tier, and tier_policy from headers or Proxy-Authorization.
        Supports:
        - X-Account-Id
        - Proxy-Authorization: Basic <base64(parametric_user:password)>
        - X-Region
        - X-Egress-Type
        - X-Tier: premium / standard
        - X-Tier-Policy: standard_first / premium_first / premium_only / standard_only
        - X-Service / X-Target-Service
        """
        account_id = headers_dict.get("x-account-id")
        auth_region = None
        auth_egress = None
        auth_service = None
        auth_tier = None
        auth_policy = None
        auth_force_rebind = False

        if not account_id and "proxy-authorization" in headers_dict:
            auth_val = headers_dict["proxy-authorization"].strip()
            if auth_val.lower().startswith("basic "):
                try:
                    decoded = base64.b64decode(auth_val[6:].strip()).decode("utf-8")
                    user_part = decoded.split(":", 1)[0] if ":" in decoded else decoded
                    parsed = parse_proxy_credentials(user_part)
                    account_id = parsed.account_id
                    auth_region = parsed.region
                    auth_egress = parsed.egress_type
                    auth_service = parsed.service
                    auth_tier = parsed.tier
                    auth_policy = parsed.tier_policy
                    auth_force_rebind = parsed.force_rebind
                except Exception:
                    pass

        if not account_id:
            # Ephemeral ID for anonymous requests so pool naturally distributes without pinning all localhost requests
            account_id = f"anon_{uuid.uuid4().hex[:8]}"

        region = headers_dict.get("x-region") or auth_region
        egress_type = headers_dict.get("x-egress-type") or auth_egress
        service = headers_dict.get("x-service") or headers_dict.get("x-target-service") or auth_service
        tier = headers_dict.get("x-tier") or auth_tier
        if tier:
            tier = tier.strip().lower()
        tier_policy = headers_dict.get("x-tier-policy") or auth_policy
        if tier_policy:
            tier_policy = tier_policy.strip().lower()

        rebind_hdr = headers_dict.get("x-rebind") or headers_dict.get("x-force-rebind")
        force_rebind = auth_force_rebind
        if rebind_hdr:
            force_rebind = rebind_hdr.strip().lower() in ("true", "1", "yes")

        return RoutingContext(
            account_id=account_id,
            region=region,
            egress_type=egress_type,
            service=service,
            tier=tier,
            tier_policy=tier_policy,
            force_rebind=force_rebind
        )

    async def connect_upstream_adaptive(
        self,
        node: ProxyNode,
        target_host: str,
        target_port: int,
        timeout: float = CONNECT_TIMEOUT
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """
        Adaptive upstream connection supporting Direct, Relay Cascade, and Fallback.
        Handles unreachable mesh nodes or sandboxed environments without IPv6.
        """
        relay_hop = self.router.resolve_relay_hop(node)

        # 1. DIRECT_ONLY mode
        if node.relay_mode == RelayMode.DIRECT_ONLY.value or not relay_hop:
            return await socks5_connect(node.host, node.port, target_host, target_port, timeout=timeout)

        # 2. RELAY_PREFERRED mode
        if node.relay_mode == RelayMode.RELAY_PREFERRED.value:
            try:
                return await socks5_relay_connect(
                    relay_host=relay_hop.host,
                    relay_port=relay_hop.port,
                    target_proxy_host=node.host,
                    target_proxy_port=node.port,
                    dest_host=target_host,
                    dest_port=target_port,
                    timeout=timeout
                )
            except Exception:
                return await socks5_connect(node.host, node.port, target_host, target_port, timeout=timeout)

        # 3. RELAY_FALLBACK mode (Default)
        direct_timeout = min(timeout, 2.0)
        try:
            return await socks5_connect(node.host, node.port, target_host, target_port, timeout=direct_timeout)
        except Exception:
            # Direct connection failed (e.g. host unreachable, UDP blocked, no IPv6).
            # Fallback to relay cascade!
            try:
                return await socks5_relay_connect(
                    relay_host=relay_hop.host,
                    relay_port=relay_hop.port,
                    target_proxy_host=node.host,
                    target_proxy_port=node.port,
                    dest_host=target_host,
                    dest_port=target_port,
                    timeout=timeout
                )
            except Exception:
                # Both direct and chained relay failed; emergency bridge via relay hop directly as egress
                return await socks5_connect(relay_hop.host, relay_hop.port, target_host, target_port, timeout=timeout)

    def _record_attempts(self, attempts: int):
        """Feeds the R7 per-request attempt histogram (rendered by MetricsCollector)."""
        entries = []
        for bound in (1, 2, 3, 4):
            if attempts <= bound:
                entries.append((
                    "ip_pool_gateway_attempts_per_request_bucket",
                    {"le": str(bound)}, 1.0,
                ))
        entries.append((
            "ip_pool_gateway_attempts_per_request_bucket", {"le": "+Inf"}, 1.0
        ))
        entries.append((
            "ip_pool_gateway_attempts_per_request_sum", {}, float(attempts)
        ))
        entries.append((
            "ip_pool_gateway_attempts_per_request_count", {}, 1.0
        ))
        # One transaction: the old per-bucket writes put seven SQLite commits on the
        # success path of every proxied request. Metrics must never fail a request.
        try:
            self.storage.incr_metrics(entries)
        except Exception as exc:
            print(f"[-] Failed to record gateway attempt metrics: {exc}")

    async def _open_upstream(
        self,
        account_id: str,
        region: Optional[str],
        egress_type: Optional[str],
        service: Optional[str],
        target_host: str,
        target_port: int,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebind: bool = False,
    ):
        """
        Opens the upstream SOCKS5 tunnel for an account, bounded by `max_attempts` (R1).

        Only the CONNECT/dial phase happens here; once streams are returned the caller
        starts forwarding, and no retry can happen after that — a forwarded request must
        never be replayed (R1 constraint, and the exact reason grok2api's incident was
        expensive: it retried at the wrong layer).

        On each failure the node is (a) excluded for the rest of THIS request, (b) fed to
        the circuit breaker, and (c) reported to the failure-domain isolator so a dead
        host takes all of its listeners down in one sweep (R3). When the budget is spent,
        the account's durable binding is moved to a healthy node (R2) so the *next* request
        succeeds even if this one cannot.
        """
        excluded: set = set()
        last_err: Optional[Exception] = None
        attempts = 0
        last_node_name: Optional[str] = None

        for attempt in range(self.max_attempts):
            if attempt > 0 and self.retry_backoff > 0:
                await asyncio.sleep(self.retry_backoff)

            lease = self.router.acquire_for_account(
                account_id=account_id,
                region=region,
                egress_type=egress_type,
                tier=tier,
                tier_policy=tier_policy,
                service=service,
                force_rebind=force_rebind or (attempt > 0),
                exclude_nodes=excluded,
            )
            if not lease:
                # The pool itself cannot serve this request; retrying cannot help.
                self._record_attempts(max(attempts, 1))
                raise UpstreamFailoverError(
                    ERR_POOL_EXHAUSTED,
                    "No available proxy nodes in IP pool"
                )

            node = self.storage.get_node(lease.node_name)
            if not node:
                excluded.add(lease.node_name)
                continue

            last_node_name = node.name
            attempts += 1
            try:
                streams = await asyncio.wait_for(
                    self.connect_upstream_adaptive(
                        node=node, target_host=target_host, target_port=target_port
                    ),
                    timeout=self.attempt_timeout,
                )
                self._record_attempts(attempts)
                # A listener answered: re-arm its domain so a genuine second outage is not
                # swallowed by the trip cooldown left over from the previous one.
                try:
                    self.isolator.note_success(node.name)
                except Exception as note_err:
                    print(f"[-] Isolator note_success failed for {node.name}: {note_err}")
                # Per-account attribution for the unified gateway (ops hotfix carried over
                # from production: without it there is no way to see which downstream
                # account generated the traffic).
                self._record_relay(account_id, node.name, f"{target_host}:{target_port}")
                return streams
            except Exception as exc:
                last_err = exc
                excluded.add(node.name)

                _, event = self.storage.update_probe_result(ProbeResult(
                    node_name=node.name,
                    success=False,
                    error=f"gateway_connect_failed: {exc}"[:120],
                ))

                # R3: a connection-layer failure is a host-level signal, not a port-level
                # one. The isolator decides whether this collapses the whole domain, but
                # only once the node's own circuit breaker has confirmed a DEAD transition
                # (a single failed dial is not a host failure).
                domain_result: Dict[str, object] = {"action": "no_isolator"}
                if self.isolator is not None:
                    try:
                        domain_result = self.isolator.report_unreachable(
                            node.name,
                            reason="gateway_connect_failed",
                            confirmed_dead=bool(
                                event and event.to_state == HealthStatus.DEAD.value
                            ),
                        )
                    except Exception as isolation_err:
                        print(f"[-] Failure-domain isolation error for {node.name}: {isolation_err}")

                # Per-node circuit trip still drains that node's own bindings when the
                # domain did not trip (e.g. a genuinely single broken listener).
                if (
                    domain_result.get("action") != "isolated"
                    and event
                    and event.to_state == HealthStatus.DEAD.value
                    and self.router
                ):
                    try:
                        self.router.rehome_leases_for_node(node.name)
                    except Exception as rehome_err:
                        print(f"[-] Error rehoming leases for {node.name} on gateway failure: {rehome_err}")

        self._record_attempts(max(attempts, 1))
        try:
            self.storage.incr_metric(
                "ip_pool_retry_exhausted_total",
                {"service": service or "default", "reason": "connect_failed"}
            )
        except Exception as metric_err:
            print(f"[-] Failed to record retry-exhausted metric: {metric_err}")

        # R2: the request is lost, but the ACCOUNT must not be. Move its durable binding
        # onto a healthy node so the next request does not hit the same dead host.
        rehome = {"rehomed": False, "reason": "no_attempts"}
        if last_node_name and self.router:
            try:
                rehome = self.router.rehome_account_binding(
                    account_id=account_id,
                    failed_node_name=last_node_name,
                    service=service,
                    trigger="retry_exhausted",
                )
            except Exception as rebind_err:
                print(f"[-] Account rebind after retry exhaustion failed for {account_id}: {rebind_err}")

        if rehome.get("rehomed"):
            raise UpstreamFailoverError(
                ERR_BINDING_REHOMED,
                f"Retry budget exhausted on {last_node_name}; binding moved to "
                f"{rehome.get('to_node')}. Retry this request once.",
                retry=1,
            )
        if rehome.get("reason") == "already_rehomed":
            # A concurrent request for the same account already won the rebind. The durable
            # binding is now off the failed node, so this caller should also retry once
            # (reporting `no_healthy_target` here would wrongly tell the downstream to stop).
            raise UpstreamFailoverError(
                ERR_BINDING_REHOMED,
                f"Binding for {account_id} was already moved to {rehome.get('node')}. "
                f"Retry this request once.",
                retry=1,
            )

        reason_detail = "no_healthy_target" if rehome.get("reason") == "no_healthy_target" else "unavailable"
        raise UpstreamFailoverError(
            ERR_NO_HEALTHY_TARGET,
            f"No healthy target available after {attempts} attempt(s): {reason_detail}. "
            f"Last error: {last_err}",
        )

    async def handle_client(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        peer = client_writer.get_extra_info("peername")
        client_ip = peer[0] if peer else "127.0.0.1"

        try:
            # 1. Read HTTP request line and headers
            header_data = await asyncio.wait_for(client_reader.readuntil(b"\r\n\r\n"), timeout=15.0)
            header_text = header_data.decode("iso-8859-1")
            lines = header_text.split("\r\n")
            if not lines or not lines[0]:
                client_writer.close()
                return

            req_line = lines[0]
            parts = req_line.split(" ")
            if len(parts) < 3:
                client_writer.close()
                return

            method, target, version = parts[0], parts[1], parts[2]

            # Parse headers
            headers_dict = {}
            for line in lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers_dict[k.strip().lower()] = v.strip()

            # 2. Extract routing constraints
            route_ctx = self._extract_routing_headers(headers_dict, client_ip)

            # Internal Prometheus endpoint: GET /metrics never proxies; it reports the
            # gateway's own per-account relay counters (ops hotfix carried over from prod).
            if method.upper() == "GET" and target.split("?")[0] == "/metrics":
                body = self.render_metrics()
                resp = ("HTTP/1.1 200 OK" + "\r\n"
                        + "Content-Type: text/plain; version=0.0.4\r\n"
                        + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body
                client_writer.write(resp)
                await client_writer.drain()
                client_writer.close()
                return

            # 3. Handle HTTP CONNECT (HTTPS)
            if method.upper() == "CONNECT":
                try:
                    target_host, target_port = parse_host_port(target, default_port=443)
                except ValueError as ve:
                    err_resp = f"HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nInvalid CONNECT target: {ve}\r\n".encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return

                try:
                    remote_reader, remote_writer = await self._open_upstream(
                        account_id=route_ctx.account_id,
                        region=route_ctx.region,
                        egress_type=route_ctx.egress_type,
                        service=route_ctx.service,
                        target_host=target_host,
                        target_port=target_port,
                        tier=route_ctx.tier,
                        tier_policy=route_ctx.tier_policy,
                        force_rebind=route_ctx.force_rebind,
                    )
                except NoPoolNodesError as pool_err:
                    err_resp = f"HTTP/1.1 503 Service Unavailable\r\nX-TetherPool-Error: {ERR_POOL_EXHAUSTED}\r\nContent-Type: text/plain\r\n\r\n{pool_err}\r\n".encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return
                except UpstreamFailoverError as failover_err:
                    retry_hdr = f", retry={failover_err.retry}" if failover_err.retry else ""
                    err_resp = (
                        f"HTTP/1.1 {failover_err.http_status} {failover_err.http_reason}\r\n"
                        f"X-TetherPool-Error: {failover_err.code}{retry_hdr}\r\n"
                        f"Content-Type: text/plain\r\n\r\n{failover_err.message}\r\n"
                    ).encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return
                except Exception as conn_err:
                    err_resp = f"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\n\r\nFailed to connect upstream SOCKS5 node: {conn_err}\r\n".encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return

                # The upstream socket belongs to this request from the moment it is open:
                # close it on every exit path, including a client disconnect in the window
                # before the 200 response has been written.
                try:
                    client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await client_writer.drain()
                    await pump_streams(client_reader, client_writer, remote_reader, remote_writer)
                finally:
                    remote_writer.close()

            # 4. Handle standard HTTP GET/POST/etc.
            else:
                parsed_url = urllib.parse.urlsplit(target)
                raw_host = parsed_url.netloc or headers_dict.get("host", "")
                try:
                    target_host, target_port = parse_host_port(raw_host, default_port=80)
                except ValueError as ve:
                    err_resp = f"HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nInvalid Host: {ve}\r\n".encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return

                path_and_query = parsed_url.path or "/"
                if parsed_url.query:
                    path_and_query += f"?{parsed_url.query}"

                try:
                    remote_reader, remote_writer = await self._open_upstream(
                        account_id=route_ctx.account_id,
                        region=route_ctx.region,
                        egress_type=route_ctx.egress_type,
                        service=route_ctx.service,
                        target_host=target_host,
                        target_port=target_port,
                        tier=route_ctx.tier,
                        tier_policy=route_ctx.tier_policy,
                        force_rebind=route_ctx.force_rebind,
                    )
                except NoPoolNodesError as pool_err:
                    err_resp = f"HTTP/1.1 503 Service Unavailable\r\nX-TetherPool-Error: {ERR_POOL_EXHAUSTED}\r\nContent-Type: text/plain\r\n\r\n{pool_err}\r\n".encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return
                except UpstreamFailoverError as failover_err:
                    retry_hdr = f", retry={failover_err.retry}" if failover_err.retry else ""
                    err_resp = (
                        f"HTTP/1.1 {failover_err.http_status} {failover_err.http_reason}\r\n"
                        f"X-TetherPool-Error: {failover_err.code}{retry_hdr}\r\n"
                        f"Content-Type: text/plain\r\n\r\n{failover_err.message}\r\n"
                    ).encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return
                except Exception as conn_err:
                    err_resp = f"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\n\r\nFailed to connect upstream SOCKS5 node: {conn_err}\r\n".encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return

                try:
                    # Reconstruct clean HTTP request without internal headers
                    new_req_lines = [f"{method} {path_and_query} {version}"]
                    for line in lines[1:]:
                        if ":" in line:
                            k, _ = line.split(":", 1)
                            if not _is_internal_header(k):
                                new_req_lines.append(line)
                    new_req_lines.append("Connection: close")
                    new_req_payload = "\r\n".join(new_req_lines) + "\r\n\r\n"

                    remote_writer.write(new_req_payload.encode("iso-8859-1"))
                    await remote_writer.drain()

                    await pump_streams(client_reader, client_writer, remote_reader, remote_writer)
                finally:
                    remote_writer.close()

        except Exception as e:
            print(f"[-] Gateway client error ({client_ip}): {e}")
        finally:
            # Every exit path (early returns included) must release the client socket.
            try:
                if not client_writer.is_closing():
                    client_writer.close()
                await client_writer.wait_closed()
            except Exception:
                pass

    async def _peer_monitor_loop(self):
        """
        R4: scans the EasyTier peer table once per interval. A missing peer means the whole
        host is gone, which the isolator converts into one domain trip — measured in
        seconds rather than the 15s HTTP-probe timeout that made the incident expensive.
        """
        assert self.peer_monitor is not None
        while True:
            try:
                await asyncio.to_thread(self.peer_monitor.scan_once)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[-] EasyTier peer monitor error: {exc}")
            await asyncio.sleep(EASYTIER_SCAN_INTERVAL_SEC)

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"[+] Unified Forward Proxy Gateway listening on {self.host}:{self.port}")
        if self.peer_monitor is not None and EASYTIER_MONITOR_ENABLED:
            self._peer_task = asyncio.create_task(self._peer_monitor_loop())
            print("[+] EasyTier peer monitor started (whole-host fast detection)")

    async def stop(self):
        if self._peer_task:
            self._peer_task.cancel()
            try:
                await self._peer_task
            except (asyncio.CancelledError, Exception):
                pass
            self._peer_task = None
        if self.server:
            self.server.close()
            await self.server.wait_closed()


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    storage = Storage()
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router)

    mesh_overrides: Dict[str, str] = {}
    raw_overrides = os.environ.get("EASYTIER_HOST_MAP", "").strip()
    if raw_overrides:
        # Format: "10.144.144.12=mesh-nl,10.144.144.21=aws-tokyo"
        for pair in raw_overrides.split(","):
            if "=" in pair:
                mesh_ip, host_label = pair.split("=", 1)
                mesh_overrides[mesh_ip.strip()] = host_label.strip()

    peer_monitor = EasytierPeerMonitor(
        storage=storage,
        isolator=isolator,
        cli_path=os.environ.get("EASYTIER_CLI", "easytier-cli"),
        host_by_mesh_ip=mesh_overrides,
    )

    gateway = ForwardProxyGateway(storage, router, isolator=isolator, peer_monitor=peer_monitor)
    await gateway.start()
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        await gateway.stop()


if __name__ == "__main__":
    asyncio.run(main())
