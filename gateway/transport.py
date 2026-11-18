"""gateway/transport.py — 上游 SOCKS5 握手与目标解析 (从 server.py 分层, 2026-11-19)。

纯传输原语: 不含路由/熔断/凭据语义。ForwardProxyGateway 通过 server.py 的
re-export 使用, 既有 `from gateway.server import parse_host_port` 等导入不受影响。
"""
import asyncio
import os
import socket
import struct
import urllib.parse
from typing import Tuple



CONNECT_TIMEOUT = 10.0


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
