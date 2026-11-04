"""
gateway/server.py - High-Performance Local Unified Forward Proxy Gateway (:9101)
Transparently maps client requests to sticky EasyTier SOCKS5 exits based on headers.
"""

import asyncio
import base64
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

DEFAULT_GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1")
DEFAULT_GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "9101"))
BUFFER_SIZE = 65536
CONNECT_TIMEOUT = 10.0
# How long the surviving direction may keep draining after the opposite side EOFs.
HALF_CLOSE_GRACE_SECONDS = 5.0

# Routing metadata consumed by the gateway itself; never forwarded to origins.
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
    def __init__(self, storage: Storage, router: Router, host: str = DEFAULT_GATEWAY_HOST, port: int = DEFAULT_GATEWAY_PORT):
        self.storage = storage
        self.router = router
        self.host = host
        self.port = port
        self.server = None

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
        Opens the upstream SOCKS5 tunnel for an account, retrying once on a different exit.

        A request-time connect failure is the fastest signal that a node is broken, so it
        is recorded as a probe failure (letting the circuit breaker react immediately
        instead of waiting up to one full sweep) and the request is retried through
        another node rather than returning 502 for the rest of the probe interval.
        """
        excluded = None
        last_err: Optional[Exception] = None

        for attempt in range(2):
            lease = self.router.acquire_for_account(
                account_id=account_id,
                region=region,
                egress_type=egress_type,
                tier=tier,
                tier_policy=tier_policy,
                service=service,
                force_rebind=force_rebind or (attempt > 0),
                exclude_node=excluded,
            )
            if not lease:
                raise NoPoolNodesError("No available proxy nodes in IP pool")

            node = self.storage.get_node(lease.node_name)
            if not node:
                raise NoPoolNodesError("Target proxy node not found in registry")

            try:
                return await self.connect_upstream_adaptive(
                    node=node, target_host=target_host, target_port=target_port
                )
            except Exception as exc:
                last_err = exc
                excluded = node.name
                _, event = self.storage.update_probe_result(ProbeResult(
                    node_name=node.name,
                    success=False,
                    error=f"gateway_connect_failed: {exc}"[:120],
                ))
                if event and event.to_state == HealthStatus.DEAD.value and self.router:
                    try:
                        self.router.rehome_leases_for_node(node.name)
                    except Exception as rehome_err:
                        print(f"[-] Error rehoming leases for {node.name} on gateway failure: {rehome_err}")

        raise last_err if last_err else RuntimeError("Upstream connection failed")

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
                    err_resp = f"HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\n{pool_err}\r\n".encode()
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
                    err_resp = f"HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\n{pool_err}\r\n".encode()
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

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"[+] Unified Forward Proxy Gateway listening on {self.host}:{self.port}")

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


async def main():
    storage = Storage()
    router = Router(storage)
    gateway = ForwardProxyGateway(storage, router)
    await gateway.start()
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        await gateway.stop()


if __name__ == "__main__":
    asyncio.run(main())
