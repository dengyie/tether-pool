"""
gateway/server.py - High-Performance Local Unified Forward Proxy Gateway (:9101)
Transparently maps client requests to sticky EasyTier SOCKS5 exits based on headers.
"""

import asyncio
import base64
import collections
import hmac
import logging
import os
import sys
import socket
import struct
import time
import urllib.parse
import uuid
from typing import Optional, Tuple, Dict, Any

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import ProxyNode, RelayMode, ProbeResult, HealthStatus
from core.storage import Storage
from core.router import Router
from core.failover import FailureDomainIsolator, EasytierPeerMonitor
from core.prober import Prober
from core.target_health import TargetHealthTracker, TARGET_CLASS_CANARIES, classify_target

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
    """Establishes a single-hop connection to the upstream SOCKS5 proxy."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(proxy_host, proxy_port),
        timeout=timeout
    )
    try:
        await socks5_handshake_on_stream(reader, writer, target_host, target_port, timeout=timeout)
        return reader, writer
    except BaseException:
        # BaseException 而非 Exception: attempt 超时时 wait_for 取消内层任务,
        # CancelledError(BaseException 子类) 也必须关掉已建立的 socket, 否则泄漏 fd。
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
    except BaseException:
        # 同 socks5_connect: 取消路径 (CancelledError) 也必须释放已建立的 socket
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


async def pump_streams(r1: asyncio.StreamReader, w1: asyncio.StreamWriter, r2: asyncio.StreamReader, w2: asyncio.StreamWriter) -> str:
    """
    Bidirectionally pumps data between client and upstream proxy.

    Uses half-close semantics: when one direction reaches EOF the peer is signalled
    with write_eof() and the opposite direction gets a bounded grace period to
    finish. Cancelling both directions on first completion truncated HTTP CONNECT
    responses whenever the client half-closed early.

    Returns the relay outcome (2026-11-18 流中级 EOF 反馈, review 二次根修):
    - 'upstream_reset': 上游在客户端仍活跃时异常重置 (RST/error) —— 唯一计入
      节点反馈的信号 (线路故障与上游崩溃的公共形态)。
    - 'upstream_eof'  : 上游 clean FIN 先到 —— 不计入。keep-alive 的常规生命周期
      (服务端 idle timeout / Connection: close 响应后关连接), 计入会以每分钟
      数十次的频率误杀健康节点 (review P0 根修)。
    - 'client_first' / 'client_error': 客户端先收尾/中止 —— 不计入 (客户端
      行为; 客户端中止后上游跟着 RST 属正常传播)。
    - 'both_error'    : 双侧异常 —— 不计入 (歧义)。
    """
    async def _forward(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                data = await reader.read(BUFFER_SIZE)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
            return "eof"
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            return "error"
        except asyncio.CancelledError:
            raise
        except Exception:
            return "error"
        finally:
            try:
                if writer.can_write_eof():
                    writer.write_eof()
            except (NotImplementedError, RuntimeError, OSError):
                pass

    t1 = asyncio.create_task(_forward(r1, w2))  # 读客户端
    t2 = asyncio.create_task(_forward(r2, w1))  # 读上游
    try:
        done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        t1_done, t1_res = t1 in done, None
        t2_done, t2_res = t2 in done, None
        # FIRST_COMPLETED 阶段任务不可能被 cancel, result() 安全
        if t1_done:
            t1_res = t1.result()
        if t2_done:
            t2_res = t2.result()
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
    # 分类 (review 二次根修): 只有「客户端仍活跃 + 上游异常重置」才反馈。
    # 宽限期内对端行为受先断侧影响 (客户端中止 -> 上游跟着 RST 属正常传播),
    # 故只看 FIRST_COMPLETED 阶段, 不看宽限期; finally cancel 后 result() 会抛
    # CancelledError, 未完成任务一律视作 client 侧延续。
    if t2_done and t2_res == "error" and not t1_done:
        return "upstream_reset"
    if t1_done:
        return "client_first" if t1_res == "eof" else "client_error"
    if t2_done:
        return "upstream_eof"
    return "both_error"


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
        # 流中级 EOF 反馈 (2026-11-18): pump_streams "上游过早断开" 的滚动窗口
        # 计数, 达阈值写入一次探活失败 (断路器照常 degrade/exclude)。保守语义:
        # 上游站自身断流同样计入, 故阈值取多事件而非单发, 写入后清零重新累积 ——
        # 单渠道偶发上游抖动不会踢掉对其他消费方正常的节点。
        self._midstream_eof: dict = {}
        self.host = host
        self.port = port
        self.server = None
        self.max_attempts = max(1, max_attempts)
        self.retry_backoff = max(0, retry_backoff_ms) / 1000.0
        self.attempt_timeout = max(0.1, attempt_timeout)
        # 网关共享密码（2026-11-14 深度 review 加固）：历史上 Basic password 只是文档
        # 约定的 "tetherpool"，网关从不校验 —— mesh 内任意进程可用任意/空密码消费全池
        # 出口。设 GATEWAY_PASSWORD 后强制校验（空值=维持旧行为，测试/本地零成本）。
        self.password: Optional[str] = os.environ.get("GATEWAY_PASSWORD", "").strip() or None
        # R3: one isolator per gateway; it degrades to single-node reporting for
        # domains that contain only one listener.
        self.isolator = isolator or FailureDomainIsolator(storage, router)
        # R4: optional peer-table watcher, started by `start()` when enabled.
        self.peer_monitor = peer_monitor
        self._peer_task = None
        # Relay attribution: per-account and per-exit counters let ops see which downstream
        # and which exit actually carry traffic. Both maps are capped so a hostile or buggy
        # downstream can never grow gateway memory without bound.
        self.relay_total: Dict[str, int] = {}
        self.relay_nodes: Dict[str, int] = {}
        self.relay_count: int = 0
        self._relay_reported_count: int = 0
        # 0 disables the periodic reporter (per-connection DEBUG detail still exists).
        self.relay_report_interval = max(
            0.0, float(os.environ.get("GATEWAY_RELAY_SUMMARY_INTERVAL_SEC", "60"))
        )
        self._relay_task = None
        # 目标级健康兜底 (2026-11-14 宝可梦机场故障): 探活只测 ipify/CF, 机场出口对
        # Google/OpenAI 网段选择性故障时池仍报 healthy, 拨号又必然成功 (桥在本机),
        # R1 排除重试永远不触发 —— 业务请求会在中继建立后挂死。按目标类周期探活 +
        # 选路软排除 (临时 lease, 不迁移绑定), 候选被排除空时降级回绑定节点。
        self.target_health = TargetHealthTracker(
            bad_streak=max(1, int(os.environ.get("IP_POOL_TARGET_BAD_STREAK", "2"))),
            bad_ttl_seconds=float(os.environ.get("IP_POOL_TARGET_BAD_TTL", "300")),
        )
        self.target_probe_interval = max(
            0.0, float(os.environ.get("IP_POOL_TARGET_PROBE_INTERVAL", "120"))
        )
        self.target_probe_concurrency = max(
            1, int(os.environ.get("IP_POOL_TARGET_PROBE_CONCURRENCY", "40"))
        )
        self.target_probe_timeout = max(
            2.0, float(os.environ.get("IP_POOL_TARGET_PROBE_TIMEOUT", "8"))
        )
        self._target_probe_task = None
        self._target_last_bad: Dict[str, int] = {}
        # steer 计量 (按目标类累计): 兜底实际改道了多少请求, /metrics 与 relay
        # summary 都能看到 —— "探活绿但业务死"期间的救流量直接可查。
        self.target_steers: Dict[str, int] = {}
        # metrics 缓冲 (数据流优化 2026-11-14): 旧实现每个请求往 SQLite 写一次
        # 直方图事务 —— 成功路径上唯一的写操作。改为内存累计 + 5s 批量刷盘,
        # 崩溃最多丢 5s 的 ops 计数 (非计费数据, 可接受)。
        self._metrics_buffer: list = []
        self._metrics_flush_interval = max(
            1.0, float(os.environ.get("GATEWAY_METRICS_FLUSH_SEC", "5"))
        )
        self._metrics_task = None

    # Cap on distinct labels per relay map; extra keys collapse into "other".
    RELAY_LABEL_CAP = 2000

    @classmethod
    def _relay_key(cls, raw: str, table: Dict[str, int], cap: int = None) -> str:
        """Collapses anonymous ids and overflow into bounded labels."""
        key = raw if not raw.startswith("anon_") else "anon"
        limit = cls.RELAY_LABEL_CAP if cap is None else cap
        if key not in table and len(table) >= limit:
            return "other"
        return key

    MIDSTREAM_EOF_THRESHOLD = 4       # 窗口内过早断开次数达到即写探活失败
    MIDSTREAM_EOF_WINDOW = 600.0      # 滚动窗口秒数

    def _handle_midstream_eol(self, node_name: str, outcome: str) -> None:
        """pump_streams 上游过早断开 (upstream_first/both_error) 的节点级反馈。

        滚动窗口计数达 MIDSTREAM_EOF_THRESHOLD -> 通过既有探活失败通道写一次
        (断路器 degrade/exclude), 写入后清零重新累积。与 CONNECT 失败反馈互补:
        CONNECT 失败管建连层, 这里管隧道建立后的流中断层 —— 坏线路在两个层面
        都会被自动退出分配, 不再依赖消费方日志 scraping 才能发现。"""
        if outcome != "upstream_reset":
            return  # 防御纵深: 非 reset 语义一律不计 (调用点已过滤, 双保险)
        try:
            dq = self._midstream_eof.setdefault(node_name, collections.deque())
            now = time.time()
            dq.append(now)
            while dq and now - dq[0] > self.MIDSTREAM_EOF_WINDOW:
                dq.popleft()
            if len(dq) < self.MIDSTREAM_EOF_THRESHOLD:
                return
            dq.clear()
            print(f"[!] Midstream premature close threshold reached for {node_name} "
                  f"({outcome}) — recording probe failure (circuit breaker will degrade/exclude)")
            asyncio.get_running_loop().create_task(self._record_midstream_failure(node_name, outcome))
        except Exception as e:
            print(f"[-] Midstream EOF feedback failed for {node_name}: {e}")

    async def _record_midstream_failure(self, node_name: str, outcome: str) -> None:
        try:
            _, _event = await asyncio.to_thread(
                self.storage.update_probe_result, ProbeResult(
                    node_name=node_name,
                    success=False,
                    error=f"gateway_midstream_eof: {outcome} (upstream closed before client)"[:120],
                )
            )
        except Exception as probe_err:
            # 与 CONNECT 失败反馈同口径: 写失败只丢观测, 不影响请求
            print(f"[-] Midstream probe-result write failed for {node_name}: {probe_err}")

    def _record_relay(self, account_id: str, node_name: str, target: str) -> None:
        acct = self._relay_key(account_id, self.relay_total, self.RELAY_LABEL_CAP)
        node = self._relay_key(node_name, self.relay_nodes, self.RELAY_LABEL_CAP)
        self.relay_total[acct] = self.relay_total.get(acct, 0) + 1
        self.relay_nodes[node] = self.relay_nodes.get(node, 0) + 1
        self.relay_count += 1
        # Per-connection detail stays at DEBUG: at one INFO line per relayed request the
        # production journal grew without bound. The periodic `relay_summary` reports the
        # same information once per interval, and /metrics keeps the exact counters, so
        # nothing is lost by demoting this line.
        logger.debug("relay established account=%s node=%s target=%s", acct, node, target)

    def relay_summary(self) -> Dict[str, Any]:
        """
        Bounded snapshot of relay activity since the previous report.

        Called only by the periodic reporter: a year of traffic costs one journald line
        per interval instead of one per connection. Returns the interval delta plus the
        top accounts and exits — which is what actually answers "who is using this".
        """
        delta = self.relay_count - self._relay_reported_count
        self._relay_reported_count = self.relay_count
        top_accounts = sorted(self.relay_total.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
        top_nodes = sorted(self.relay_nodes.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
        return {
            "relays": delta,
            "total": self.relay_count,
            "accounts": len(self.relay_total),
            "nodes": len(self.relay_nodes),
            "top_accounts": top_accounts,
            "top_nodes": top_nodes,
            "target_bad": self.target_health.bad_counts(),
            "target_steers": dict(self.target_steers),
        }

    @staticmethod
    def _escape_label(value: str) -> str:
        """Prometheus exposition-format label escaping (review #5): account_id
        来自客户端头, 未转义的 \" 与 \\n 可注入任意 metric 行毒化抓取。"""
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    def render_metrics(self) -> bytes:
        lines = [
            "# HELP tetherpool_gateway_relay_total Relay connections established per account.",
            "# TYPE tetherpool_gateway_relay_total counter",
        ]
        for acct, count in sorted(self.relay_total.items()):
            lines.append('tetherpool_gateway_relay_total{account="%s"} %d'
                         % (self._escape_label(acct), count))
        lines.append("# HELP tetherpool_gateway_relay_by_node_total Relay connections established per exit node.")
        lines.append("# TYPE tetherpool_gateway_relay_by_node_total counter")
        for node, count in sorted(self.relay_nodes.items()):
            lines.append('tetherpool_gateway_relay_by_node_total{node="%s"} %d'
                         % (self._escape_label(node), count))
        lines.append("# HELP tetherpool_gateway_target_steers_total Requests steered away from target-class-bad exits (cumulative).")
        lines.append("# TYPE tetherpool_gateway_target_steers_total counter")
        for cls, count in sorted(self.target_steers.items()):
            lines.append('tetherpool_gateway_target_steers_total{class="%s"} %d'
                         % (self._escape_label(cls), count))
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
        auth_region_strict = False
        auth_bind_persist = False

        if not account_id and "proxy-authorization" in headers_dict:
            auth_val = headers_dict["proxy-authorization"].strip()
            if auth_val.lower().startswith("basic "):
                try:
                    # 解码容错与 _password_ok 同规 (errors="replace"): 密码门用
                    # replace 解码可放行非 UTF-8 用户名, 这里若用严格解码会把
                    # 已过 407 的请求静默打成 anon (不落库/丢粘性/无日志)。
                    decoded = base64.b64decode(auth_val[6:].strip()).decode("utf-8", "replace")
                    user_part = decoded.split(":", 1)[0] if ":" in decoded else decoded
                    parsed = parse_proxy_credentials(user_part)
                    account_id = parsed.account_id
                    auth_region = parsed.region
                    auth_egress = parsed.egress_type
                    auth_service = parsed.service
                    auth_tier = parsed.tier
                    auth_policy = parsed.tier_policy
                    auth_force_rebind = parsed.force_rebind
                    auth_region_strict = parsed.region_strict
                    auth_bind_persist = parsed.bind_persist
                except Exception as parse_err:
                    # 唯一剩余来源: b64 非法 (密码门已用同一 b64decode 放行,
                    # 理论不可达, 防御性兜底)。必须可观察, 不再静默吞。
                    logger.warning(
                        "credential parse failed (routing as anonymous): peer=%s err=%s",
                        client_ip, parse_err,
                    )

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

        strict_hdr = headers_dict.get("x-region-strict")
        region_strict = auth_region_strict
        if strict_hdr:
            region_strict = strict_hdr.strip().lower() in ("true", "1", "yes")

        persist_hdr = headers_dict.get("x-bind-persist")
        bind_persist = auth_bind_persist
        if persist_hdr:
            bind_persist = persist_hdr.strip().lower() in ("true", "1", "yes")

        return RoutingContext(
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
        # 数据流优化: 成功路径上不再有 SQLite 写事务 —— 直方图计入内存缓冲,
        # 由 _metrics_flush_loop 每 5s 批量落库 (崩一个 flush 周期至多丢 5s 计数)。
        # 惰性初始化: 测试里的 __new__ 裸实例与主流程都安全。
        buffer = getattr(self, "_metrics_buffer", None)
        if buffer is None:
            buffer = []
            self._metrics_buffer = buffer
        buffer.extend(entries)

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
        region_strict: bool = False,
        bind_persist: bool = False,
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

        # 目标级软排除集 (兜底): 该目标类下探活连续失败的节点。所有 attempt 都携带,
        # attempt 0 用临时 lease 语义 (不迁移绑定); attempt >0 已是真实失败路径,
        # R2 持久重绑语义不变。
        target_cls = classify_target(target_host)
        # 热路径读预计算缓存 (二轮 review): bad_nodes() 每请求 O(N) 全表扫描,
        # 缓存由探活循环每轮 refresh, 轮内新鲜度足够 (TTL 自适应覆盖轮长)。
        steer_bad: set = self.target_health.cached_bad_nodes(target_cls) if target_cls else set()

        for attempt in range(self.max_attempts):
            if attempt > 0 and self.retry_backoff > 0:
                await asyncio.sleep(self.retry_backoff)

            lease = await asyncio.to_thread(
                self.router.acquire_for_account,
                account_id=account_id,
                region=region,
                egress_type=egress_type,
                tier=tier,
                tier_policy=tier_policy,
                service=service,
                force_rebind=force_rebind or (attempt > 0),
                exclude_nodes=excluded | steer_bad,
                region_strict=region_strict,
                bind_persist=bind_persist,
                exclude_transient=(attempt == 0),
            )
            if not lease and attempt == 0 and steer_bad:
                # 降级: 目标类坏节点占满候选 (如整个 us-west 池对该类失联) ->
                # 无引导重试。宁可走坏出口, 绝不 503 —— 坏出口上目标类之外的
                # 流量仍可用, 且探活恢复后自动回到绑定节点。
                lease = await asyncio.to_thread(
                    self.router.acquire_for_account,
                    account_id=account_id,
                    region=region,
                    egress_type=egress_type,
                    tier=tier,
                    tier_policy=tier_policy,
                    service=service,
                    force_rebind=force_rebind,
                    exclude_nodes=excluded,
                    region_strict=region_strict,
                    bind_persist=bind_persist,
                )
            if lease and attempt == 0 and target_cls and lease.steered:
                self.target_steers[target_cls] = self.target_steers.get(target_cls, 0) + 1
            if not lease:
                # The pool itself cannot serve this request; retrying cannot help.
                self._record_attempts(max(attempts, 1))
                raise UpstreamFailoverError(
                    ERR_POOL_EXHAUSTED,
                    "No available proxy nodes in IP pool"
                )

            # legacy 镜像 lease 无拨号字段时回退查库 (阻塞 sqlite, 同样移出事件环)
            node = lease if lease.host is not None else \
                await asyncio.to_thread(self.storage.get_node, lease.node_name)
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
                # 流中级 EOF 反馈 (2026-11-18): 调用方需要 node 身份才能把
                # "上游过早断开" 归因到具体节点 (原只返回 streams 无法归因)。
                return node, streams[0], streams[1]
            except Exception as exc:
                last_err = exc
                excluded.add(node.name)

                # 阻塞 sqlite 写移出事件环 (二轮 review #2): 失败路径在 3000 并发下
                # 每次写库都是一次全事件环停顿; rehome 内部含写锁与多表事务, 同理。
                # 三轮 review #2: 此写裸露时一次 sqlite 抖动会从 except 块直接炸出
                # retry 循环 -> 请求 502 而不是换下一节点。失败路径的观测写必须
                # 与业务重试解耦: 写失败只丢观测, 不杀 failover。
                try:
                    _, event = await asyncio.to_thread(
                        self.storage.update_probe_result, ProbeResult(
                            node_name=node.name,
                            success=False,
                            error=f"gateway_connect_failed: {exc}"[:120],
                        )
                    )
                except Exception as probe_err:
                    print(f"[-] Probe-result write failed for {node.name} (failover continues): {probe_err}")
                    event = None

                # R3: a connection-layer failure is a host-level signal, not a port-level
                # one. The isolator decides whether this collapses the whole domain, but
                # only once the node's own circuit breaker has confirmed a DEAD transition
                # (a single failed dial is not a host failure).
                domain_result: Dict[str, object] = {"action": "no_isolator"}
                if self.isolator is not None:
                    try:
                        # report_unreachable 内部含 isolator 锁 + 多表事务 (可能触发
                        # 全域 mark_dead + rehome), 同为阻塞调用, 移出事件环。
                        domain_result = await asyncio.to_thread(
                            self.isolator.report_unreachable,
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
                        # 同 #2: rehome 含写锁事务, 移出事件环
                        await asyncio.to_thread(self.router.rehome_leases_for_node, node.name)
                    except Exception as rehome_err:
                        print(f"[-] Error rehoming leases for {node.name} on gateway failure: {rehome_err}")

        self._record_attempts(max(attempts, 1))
        try:
            await asyncio.to_thread(
                self.storage.incr_metric,
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
                # rehome 含 BINDING_WRITE_LOCK + 多表写事务, 同样移出事件环
                rehome = await asyncio.to_thread(
                    self.router.rehome_account_binding,
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

    async def _relay_and_monitor(self, client_reader, client_writer, upstream_node,
                                 remote_reader, remote_writer) -> None:
        """双分支共用的转发尾段: 泵流 (CONNECT 已写 200 / HTTP 已写请求行) +
        流中断反馈 + 确保上游 socket 关闭。upstream_reset 是唯一计入节点反馈
        的 outcome (P0 语义: 客户端先断/正常 EOF 不算上游之过)。
        """
        try:
            outcome = await pump_streams(client_reader, client_writer, remote_reader, remote_writer)
            if outcome == "upstream_reset":
                self._handle_midstream_eol(upstream_node.name, outcome)
        finally:
            remote_writer.close()

    def _metrics_exempt(self, method: str, target: str) -> bool:
        """GET /metrics (含 query 串) 免密; 路径变形 (/metrics/, //metrics) 不豁免。"""
        return method.upper() == "GET" and target.split("?")[0] == "/metrics"

    async def _write_and_close(self, client_writer: asyncio.StreamWriter, resp: bytes) -> None:
        """写一个完整 HTTP 响应并关闭连接 (每个 early-return 错误路径共用)。"""
        client_writer.write(resp)
        try:
            await client_writer.drain()
        finally:
            client_writer.close()

    @staticmethod
    def _error_response(status_line: str, headers_and_body: str) -> bytes:
        """拼一个 status line + 完整头部/正文块 的错误响应 (全部 early-return 路径共用)。"""
        return f"HTTP/1.1 {status_line}\r\n{headers_and_body}".encode()

    async def _open_upstream_or_respond(
        self, client_writer: asyncio.StreamWriter, route_ctx: "RoutingContext",
        target_host: str, target_port: int,
    ):
        """_open_upstream + 三类异常 -> 直接向客户端写响应并返回 None。

        成功时返回 (node, reader, writer)。错误分类与语义与原 handle_client 内联
        版本逐字一致: 503 池空 / failover 码透传 (+retry) / 其余一律 502。
        """
        try:
            return await self._open_upstream(
                account_id=route_ctx.account_id,
                region=route_ctx.region,
                egress_type=route_ctx.egress_type,
                service=route_ctx.service,
                target_host=target_host,
                target_port=target_port,
                tier=route_ctx.tier,
                tier_policy=route_ctx.tier_policy,
                force_rebind=route_ctx.force_rebind,
                region_strict=route_ctx.region_strict,
                bind_persist=route_ctx.bind_persist,
            )
        except NoPoolNodesError as pool_err:
            await self._write_and_close(client_writer, self._error_response(
                "503 Service Unavailable",
                f"X-TetherPool-Error: {ERR_POOL_EXHAUSTED}\r\nContent-Type: text/plain\r\n\r\n{pool_err}\r\n"))
            return None
        except UpstreamFailoverError as failover_err:
            retry_hdr = f", retry={failover_err.retry}" if failover_err.retry else ""
            await self._write_and_close(client_writer, self._error_response(
                f"{failover_err.http_status} {failover_err.http_reason}",
                f"X-TetherPool-Error: {failover_err.code}{retry_hdr}\r\n"
                f"Content-Type: text/plain\r\n\r\n{failover_err.message}\r\n"))
            return None
        except Exception as conn_err:
            await self._write_and_close(client_writer, self._error_response(
                "502 Bad Gateway",
                f"Content-Type: text/plain\r\n\r\nFailed to connect upstream SOCKS5 node: {conn_err}\r\n"))
            return None

    def _password_ok(self, auth_header: str) -> bool:
        """Constant-shape check of the Basic password part against GATEWAY_PASSWORD.

        The username half is routing metadata (account/region/...) and stays free-form;
        only the password half is the shared secret. Header absent -> reject.
        """
        if not auth_header or not auth_header.lower().startswith("basic "):
            return False
        try:
            decoded = base64.b64decode(auth_header[6:].strip()).decode("utf-8", "replace")
        except Exception:
            return False
        presented = decoded.split(":", 1)[1] if ":" in decoded else ""
        # bytes 比较: compare_digest 对含非 ASCII (U+FFFD 替换符) 的 str 抛 TypeError,
        # 会让畸形 auth 头绕过 407 路径静默断连。
        return hmac.compare_digest(
            presented.encode("utf-8", "replace"), (self.password or "").encode("utf-8")
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

            # 2a. Shared-password gate (2026-11-14 review hardening): when
            # GATEWAY_PASSWORD is set, every request (CONNECT / plain HTTP / headers-only
            # routing) must present it in Proxy-Authorization. /metrics exempt so
            # Prometheus scrapers need no proxy credentials.
            if self.password and not self._metrics_exempt(method, target):
                if not self._password_ok(headers_dict.get("proxy-authorization", "")):
                    # 提取 presented username 做观测 (与 F1 同规: utf-8/replace 解码,
                    # 只留用户名前 40 字符, 不落密码), 用于定位无凭据/坏凭据的调用方。
                    presented_user = ""
                    raw_auth = headers_dict.get("proxy-authorization", "")
                    if raw_auth.lower().startswith("basic "):
                        try:
                            decoded = base64.b64decode(raw_auth[6:].strip()).decode("utf-8", "replace")
                            presented_user = decoded.split(":", 1)[0][:40]
                        except Exception:
                            presented_user = "<b64-error>"
                    logger.warning(
                        "gateway auth rejected: peer=%s target=%s user=%s",
                        client_ip, target[:80], presented_user or "<none>",
                    )
                    if method.upper() == "CONNECT":
                        resp = b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"tetherpool\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    else:
                        resp = b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Type: text/plain\r\nContent-Length: 16\r\nConnection: close\r\n\r\nauth required\r\n\r\n"
                    client_writer.write(resp)
                    await client_writer.drain()
                    client_writer.close()
                    return

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

                upstream = await self._open_upstream_or_respond(
                    client_writer, route_ctx, target_host, target_port,
                )
                if upstream is None:
                    return
                upstream_node, remote_reader, remote_writer = upstream

                # The upstream socket belongs to this request from the moment it is open:
                # close it on every exit path, including a client disconnect in the window
                # before the 200 response has been written.
                client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await client_writer.drain()
                await self._relay_and_monitor(
                    client_reader, client_writer, upstream_node, remote_reader, remote_writer)

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

                upstream = await self._open_upstream_or_respond(
                    client_writer, route_ctx, target_host, target_port,
                )
                if upstream is None:
                    return
                upstream_node, remote_reader, remote_writer = upstream

                # Reconstruct clean HTTP request without internal headers.
                # 2026-11-15 review #8: 除逐名过滤外, 还必须剥 Connection 头
                # 点名的 hop-by-hop 头 + 无条件剥 transfer-encoding/te/upgrade/
                # keep-alive/trailer —— 网关把 body 当不透明字节流泵, 转发
                # 分帧头会产生冲突框架 (request-smuggling 面)。
                connection_named: set = set()
                for line in lines[1:]:
                    if ":" in line:
                        k, v = line.split(":", 1)
                        if k.strip().lower() == "connection":
                            connection_named.update(
                                t.strip().lower() for t in v.split(",") if t.strip())
                HOP_BY_DROP = {"transfer-encoding", "te", "upgrade",
                               "keep-alive", "trailer", "proxy-connection"}
                new_req_lines = [f"{method} {path_and_query} {version}"]
                for line in lines[1:]:
                    if ":" in line:
                        k, _ = line.split(":", 1)
                        kl = k.strip().lower()
                        if not _is_internal_header(k) and kl not in connection_named \
                                and kl not in HOP_BY_DROP:
                            new_req_lines.append(line)
                new_req_lines.append("Connection: close")
                new_req_payload = "\r\n".join(new_req_lines) + "\r\n\r\n"

                remote_writer.write(new_req_payload.encode("iso-8859-1"))
                await remote_writer.drain()
                await self._relay_and_monitor(
                    client_reader, client_writer, upstream_node, remote_reader, remote_writer)

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
                logger.warning("EasyTier peer monitor error: %s", exc)
            await asyncio.sleep(EASYTIER_SCAN_INTERVAL_SEC)

    async def _relay_report_loop(self):
        """
        Emits the bounded relay summary once per interval.

        This is what keeps per-connection attribution observable without writing a line
        per relayed request: the counters in /metrics stay exact, and the journal gets a
        single aggregated line per interval with the top accounts and exits.
        """
        while True:
            await asyncio.sleep(self.relay_report_interval)
            try:
                summary = self.relay_summary()
                if summary["relays"]:
                    logger.info(
                        "relay summary %ss: relays=%d total=%d accounts=%d nodes=%d "
                        "top_accounts=%s top_nodes=%s target_bad=%s target_steers=%s",
                        int(self.relay_report_interval), summary["relays"],
                        summary["total"], summary["accounts"], summary["nodes"],
                        summary["top_accounts"], summary["top_nodes"],
                        summary.get("target_bad") or {},
                        summary.get("target_steers") or {},
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("relay summary error: %s", exc)

    async def _metrics_flush_loop(self):
        """把内存里的直方图缓冲批量落库 (5s 一批); 失败时回灌并封顶防膨胀。"""
        while True:
            await asyncio.sleep(self._metrics_flush_interval)
            await self._flush_metrics()

    async def _flush_metrics(self):
        if not self._metrics_buffer:
            return
        entries, self._metrics_buffer = self._metrics_buffer, []
        try:
            self.storage.incr_metrics(entries)
        except Exception as exc:
            print(f"[-] Failed to flush gateway metrics: {exc}")
            self._metrics_buffer = entries + self._metrics_buffer
            if len(self._metrics_buffer) > 50000:
                del self._metrics_buffer[:len(self._metrics_buffer) - 50000]

    async def _target_probe_loop(self):
        """
        目标类健康探活循环 (兜底): 每轮对每个目标类的 canary URL 探测全池健康节点。

        判坏需连续 bad_streak 次失败 (tracker 内建), 一次成功立即清除。只在坏计数
        变化时打日志 —— 稳态零输出, 故障时一条摘要, 恢复时一条摘要。
        """
        prober = Prober(self.storage, timeout=self.target_probe_timeout)
        sem = asyncio.Semaphore(self.target_probe_concurrency)
        while True:
            await asyncio.sleep(self.target_probe_interval)
            round_start = time.monotonic()
            try:
                healthy = [n for n in self.storage.list_nodes()
                           if n.status == HealthStatus.HEALTHY.value]
                # 内存卫生: 已删/退役节点的 (node, class) 条目永远不会再被
                # record/TTL 触达, 每轮裁剪一次 (数据流 review 2026-11-14)。
                self.target_health.prune({n.name for n in healthy})
                for cls, url in TARGET_CLASS_CANARIES.items():
                    async def _one(node: ProxyNode):
                        async with sem:
                            res = await prober.probe_target_service(node, url)
                        self.target_health.record(node.name, cls, res.success, res.error or "")

                    # return_exceptions: 单个探测异常不得吞掉本轮其余结果 (un-awaited 孤儿任务教训)
                    await asyncio.gather(*[_one(n) for n in healthy], return_exceptions=True)
                counts = self.target_health.bad_counts()
                # TTL 自适应 (二轮 review): 轮长 > TTL 时 bad 记录在下一轮判定前就
                # 过期, steering 恒空。按实测轮长放宽 TTL, 并重算热路径坏集缓存。
                self.target_health.note_round_duration(time.monotonic() - round_start)
                self.target_health.refresh_bad_cache()
                if counts != self._target_last_bad:
                    logger.info(
                        "target-health transition: bad=%s (was=%s) "
                        "[classes: google/openai/claude, interval=%ss, round=%.0fs]",
                        counts, self._target_last_bad, int(self.target_probe_interval),
                        time.monotonic() - round_start,
                    )
                    self._target_last_bad = counts
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("target probe sweep error: %s", exc)

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logger.info("Unified Forward Proxy Gateway listening on %s:%s", self.host, self.port)

        def _task_done_callback(name: str):
            def _cb(task: asyncio.Task):
                # 后台循环死亡必须炸响而不是静默 (2026-11-15 review: target probe
                # loop 因缺 import time 首轮 NameError 静默全灭, steer 子系统
                # 上线即失效无人知晓)。CancelledError 是正常停机, 不算事。
                if task.cancelled():
                    return
                exc = task.exception()
                if exc:
                    logger.critical("Background loop %s DIED: %r — feature is now inert!", name, exc)
            return _cb

        if self.peer_monitor is not None and EASYTIER_MONITOR_ENABLED:
            self._peer_task = asyncio.create_task(self._peer_monitor_loop())
            self._peer_task.add_done_callback(_task_done_callback("peer_monitor"))
            logger.info("EasyTier peer monitor started (whole-host fast detection)")
        if self.relay_report_interval > 0:
            self._relay_task = asyncio.create_task(self._relay_report_loop())
            self._relay_task.add_done_callback(_task_done_callback("relay_report"))
            logger.info("Relay summary every %ss (per-connection detail at DEBUG)",
                        int(self.relay_report_interval))
        if self.target_probe_interval > 0:
            self._target_probe_task = asyncio.create_task(self._target_probe_loop())
            self._target_probe_task.add_done_callback(_task_done_callback("target_probe"))
            logger.info("Target-health probing every %ss (classes=%s, concurrency=%d)",
                        int(self.target_probe_interval),
                        ",".join(TARGET_CLASS_CANARIES), self.target_probe_concurrency)
        self._metrics_task = asyncio.create_task(self._metrics_flush_loop())
        self._metrics_task.add_done_callback(_task_done_callback("metrics_flush"))
        logger.info("Gateway metrics flush every %ss (in-memory accumulation)",
                    int(self._metrics_flush_interval))

    async def stop(self):
        # 最终刷盘: 进程退出前把残余计数落库 (尽力而为, 失败不阻塞停机)
        try:
            await asyncio.wait_for(self._flush_metrics(), timeout=5.0)
        except Exception:
            pass
        for task_attr in ("_peer_task", "_relay_task", "_target_probe_task", "_metrics_task"):
            task = getattr(self, task_attr)
            if task:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
                setattr(self, task_attr, None)
        if self.server:
            self.server.close()
            await self.server.wait_closed()


async def main():
    logging.basicConfig(
        level=os.environ.get("GATEWAY_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
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
