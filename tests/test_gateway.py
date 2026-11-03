"""
tests/test_gateway.py - Unit tests for Local Unified Forward Proxy Gateway (:9101)
"""

import asyncio
import base64
import tempfile
import os
import pytest
from core.models import ProxyNode, EgressType, HealthStatus
from core.storage import Storage
from core.router import Router
from gateway.server import ForwardProxyGateway, parse_host_port


@pytest.fixture
def temp_gateway():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    router = Router(storage)
    gateway = ForwardProxyGateway(storage, router, host="127.0.0.1", port=0)
    yield gateway, storage, router
    if os.path.exists(db_path):
        os.remove(db_path)


def test_extract_routing_headers(temp_gateway):
    gateway, storage, router = temp_gateway

    # 1. Custom headers
    h1 = {
        "x-account-id": "alice_test",
        "x-region": "tokyo",
        "x-egress-type": "warp_ipv6"
    }
    acc, reg, egr = gateway._extract_routing_headers(h1, "127.0.0.1")
    assert acc == "alice_test"
    assert reg == "tokyo"
    assert egr == "warp_ipv6"

    # 2. Proxy-Authorization Basic header
    auth_str = base64.b64encode(b"bob_user:password123").decode("utf-8")
    h2 = {
        "proxy-authorization": f"Basic {auth_str}",
        "x-region": "singapore"
    }
    acc2, reg2, egr2 = gateway._extract_routing_headers(h2, "127.0.0.1")
    assert acc2 == "bob_user"
    assert reg2 == "singapore"
    assert egr2 == "warp_ipv6"

    # 3. Anonymous fallback generates distributed ephemeral id
    h3 = {}
    acc3, reg3, egr3 = gateway._extract_routing_headers(h3, "192.168.1.50")
    assert acc3.startswith("anon_")
    assert len(acc3) > 5


@pytest.mark.asyncio
async def test_gateway_connect_no_nodes(temp_gateway):
    """When pool has no nodes, gateway should reply with 503 Service Unavailable"""
    gateway, storage, router = temp_gateway

    # Start gateway on a free port
    server = await asyncio.start_server(gateway.handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
    await writer.drain()

    resp = await reader.read(1024)
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()

    assert b"503 Service Unavailable" in resp


def test_parse_host_port():
    # 1. Standard host:port
    h, p = parse_host_port("example.com:443")
    assert h == "example.com"
    assert p == 443

    # 2. Host without port (default fallback)
    h, p = parse_host_port("example.com", default_port=80)
    assert h == "example.com"
    assert p == 80

    # 3. IPv6 with port
    h, p = parse_host_port("[2001:db8::1]:8443")
    assert h == "2001:db8::1"
    assert p == 8443

    # 4. IPv6 without port (bracketed)
    h, p = parse_host_port("[2001:db8::1]", default_port=80)
    assert h == "2001:db8::1"
    assert p == 80

    # 5. IPv6 without port (unbracketed)
    h, p = parse_host_port("2001:db8::1", default_port=80)
    assert h == "2001:db8::1"
    assert p == 80

    # 6. Invalid port
    with pytest.raises(ValueError):
        parse_host_port("example.com:invalid_port")


@pytest.mark.asyncio
async def test_gateway_connect_malformed_target(temp_gateway):
    """When client sends a malformed CONNECT target, gateway replies with 400 Bad Request"""
    gateway, storage, router = temp_gateway

    # Insert a dummy healthy node so 503 is not triggered
    storage.upsert_node(ProxyNode(
        name="gw-test-node",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    ))

    server = await asyncio.start_server(gateway.handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"CONNECT example.com:invalid_port HTTP/1.1\r\nHost: example.com\r\n\r\n")
    await writer.drain()

    resp = await reader.read(1024)
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()

    assert b"400 Bad Request" in resp

