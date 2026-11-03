"""
gateway/server.py - High-Performance Local Unified Forward Proxy Gateway (:9101)
Transparently maps client requests to sticky EasyTier SOCKS5 exits based on headers.
"""

import asyncio
import base64
import os
import sys
import re
import socket
import struct
import urllib.parse
import uuid
from typing import Optional, Tuple, Dict

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import ProxyNode, LeaseInfo, EgressType
from core.storage import Storage
from core.router import Router

DEFAULT_GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1")
DEFAULT_GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "9101"))
BUFFER_SIZE = 65536
CONNECT_TIMEOUT = 10.0


async def socks5_connect(proxy_host: str, proxy_port: int, target_host: str, target_port: int, timeout: float = CONNECT_TIMEOUT) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """
    Establishes connection to an upstream SOCKS5 proxy and asks it to connect to target_host:target_port.
    Uses SOCKS5 domain name resolution (0x03) to ensure remote DNS resolution.
    Guarantees writer socket closure on any error or timeout.
    """
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(proxy_host, proxy_port),
        timeout=timeout
    )

    try:
        # 1. Auth negotiation (No authentication: 0x00)
        writer.write(b"\x05\x01\x00")
        await writer.drain()

        auth_resp = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
        if auth_resp != b"\x05\x00":
            raise ConnectionError(f"Upstream SOCKS5 handshake rejected: {auth_resp.hex()}")

        # 2. Connect request
        # Detect IP vs Domain
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
    """Bidirectionally pumps data between client and upstream proxy with cooperative task cancellation."""
    async def _forward(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                data = await reader.read(BUFFER_SIZE)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (asyncio.CancelledError, ConnectionError, OSError):
            pass
        except Exception:
            pass

    t1 = asyncio.create_task(_forward(r1, w2))
    t2 = asyncio.create_task(_forward(r2, w1))
    try:
        done, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    finally:
        for w in (w1, w2):
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass


class ForwardProxyGateway:
    def __init__(self, storage: Storage, router: Router, host: str = DEFAULT_GATEWAY_HOST, port: int = DEFAULT_GATEWAY_PORT):
        self.storage = storage
        self.router = router
        self.host = host
        self.port = port
        self.server = None

    def _extract_routing_headers(self, headers_dict: Dict[str, str], client_ip: str) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Extracts account_id, region, and egress_type from headers.
        Checks:
        - X-Account-Id
        - Proxy-Authorization: Basic <base64(account_id:password)>
        - X-Region
        - X-Egress-Type
        """
        account_id = headers_dict.get("x-account-id")
        if not account_id and "proxy-authorization" in headers_dict:
            auth_val = headers_dict["proxy-authorization"].strip()
            if auth_val.lower().startswith("basic "):
                try:
                    decoded = base64.b64decode(auth_val[6:].strip()).decode("utf-8")
                    if ":" in decoded:
                        account_id = decoded.split(":", 1)[0]
                    else:
                        account_id = decoded
                except Exception:
                    pass

        if not account_id:
            # Ephemeral ID for anonymous requests so pool naturally distributes without pinning all localhost requests
            account_id = f"anon_{uuid.uuid4().hex[:8]}"

        region = headers_dict.get("x-region")
        egress_type = headers_dict.get("x-egress-type", EgressType.WARP_IPV6.value)

        return account_id, region, egress_type

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
            account_id, region, egress_type = self._extract_routing_headers(headers_dict, client_ip)

            # 3. Resolve sticky lease
            lease = self.router.acquire_for_account(
                account_id=account_id,
                region=region,
                egress_type=egress_type
            )
            if not lease:
                err_resp = b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\nNo available proxy nodes in IP pool\r\n"
                client_writer.write(err_resp)
                await client_writer.drain()
                client_writer.close()
                return

            # Extract upstream node host and port
            node = self.storage.get_node(lease.node_name)
            if not node:
                err_resp = b"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\n\r\nTarget proxy node not found in registry\r\n"
                client_writer.write(err_resp)
                await client_writer.drain()
                client_writer.close()
                return

            upstream_host = node.host
            upstream_port = node.port

            # 4. Handle HTTP CONNECT (HTTPS)
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
                    remote_reader, remote_writer = await socks5_connect(
                        upstream_host, upstream_port, target_host, target_port
                    )
                except Exception as conn_err:
                    err_resp = f"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\n\r\nFailed to connect upstream SOCKS5 node: {conn_err}\r\n".encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return

                # Send 200 Connection Established to client
                client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await client_writer.drain()

                # Pump streams
                await pump_streams(client_reader, client_writer, remote_reader, remote_writer)

            # 5. Handle standard HTTP GET/POST/etc.
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
                    remote_reader, remote_writer = await socks5_connect(
                        upstream_host, upstream_port, target_host, target_port
                    )
                except Exception as conn_err:
                    err_resp = f"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\n\r\nFailed to connect upstream SOCKS5 node: {conn_err}\r\n".encode()
                    client_writer.write(err_resp)
                    await client_writer.drain()
                    client_writer.close()
                    return

                # Reconstruct clean HTTP request without internal headers
                new_req_lines = [f"{method} {path_and_query} {version}"]
                internal_headers = {"x-account-id", "x-region", "x-egress-type", "proxy-authorization", "proxy-connection", "connection"}
                for line in lines[1:]:
                    if ":" in line:
                        k, _ = line.split(":", 1)
                        if k.strip().lower() not in internal_headers:
                            new_req_lines.append(line)
                new_req_lines.append("Connection: close")
                new_req_payload = "\r\n".join(new_req_lines) + "\r\n\r\n"

                remote_writer.write(new_req_payload.encode("iso-8859-1"))
                await remote_writer.drain()

                # Pump streams
                await pump_streams(client_reader, client_writer, remote_reader, remote_writer)

        except Exception:
            try:
                client_writer.close()
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
