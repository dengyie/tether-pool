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
        "x-egress-type": "warp_ipv6",
        "x-service": "grok2api"
    }
    acc, reg, egr, svc = gateway._extract_routing_headers(h1, "127.0.0.1")
    assert acc == "alice_test"
    assert reg == "tokyo"
    assert egr == "warp_ipv6"
    assert svc == "grok2api"

    # 2. Proxy-Authorization Basic header
    auth_str = base64.b64encode(b"bob_user:password123").decode("utf-8")
    h2 = {
        "proxy-authorization": f"Basic {auth_str}",
        "x-region": "singapore"
    }
    acc2, reg2, egr2, svc2 = gateway._extract_routing_headers(h2, "127.0.0.1")
    assert acc2 == "bob_user"
    assert reg2 == "singapore"
    # No implicit egress default: an omitted type must not re-point an existing binding
    assert egr2 is None
    assert svc2 is None

    # 3. Anonymous fallback generates distributed ephemeral id
    h3 = {}
    acc3, reg3, egr3, svc3 = gateway._extract_routing_headers(h3, "192.168.1.50")
    assert acc3.startswith("anon_")
    assert len(acc3) > 5
    assert svc3 is None


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


@pytest.mark.asyncio
async def test_gateway_retries_on_upstream_connect_failure(temp_gateway, monkeypatch):
    """A request-time connect failure must be recorded as a probe failure and the
    request retried on a different node instead of returning 502 for a whole interval."""
    gateway, storage, router = temp_gateway

    for i in range(2):
        storage.upsert_node(ProxyNode(
            name=f"gw-retry-{i}",
            host="10.144.144.3",
            port=10881 + i,
            region="tokyo",
            egress_type=EgressType.WARP_IPV6.value,
            status=HealthStatus.HEALTHY.value,
            exit_ip=f"2a09:bac5:9::{i}",
        ))

    # A local stand-in for the tunnelled origin
    async def echo_handler(reader, writer):
        while True:
            data = await reader.read(4096)
            if not data:
                break
            writer.write(data)
            await writer.drain()
        writer.close()

    echo_server = await asyncio.start_server(echo_handler, "127.0.0.1", 0)
    echo_port = echo_server.sockets[0].getsockname()[1]

    attempts = []
    captured = {}
    real_connect = gateway.connect_upstream_adaptive

    async def flaky_connect(node, target_host, target_port, timeout=15.0):
        attempts.append(node.name)
        if len(attempts) == 1:
            raise OSError("simulated mesh failure")
        reader, writer = await asyncio.open_connection("127.0.0.1", echo_port)
        captured["writer"] = writer
        return reader, writer

    monkeypatch.setattr(gateway, "connect_upstream_adaptive", flaky_connect)

    server = await asyncio.start_server(gateway.handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(
        b"CONNECT example.com:443 HTTP/1.1\r\n"
        b"Host: example.com:443\r\n"
        b"X-Account-Id: retry-acct\r\n\r\n"
    )
    await writer.drain()

    resp = await reader.readuntil(b"\r\n\r\n")
    assert b"200 Connection Established" in resp
    assert attempts == ["gw-retry-0", "gw-retry-1"] or (
        len(attempts) == 2 and attempts[0] != attempts[1]
    )

    # The failing node was reported to the circuit breaker immediately
    failed = storage.get_node(attempts[0])
    assert failed.consecutive_failures >= 1
    assert failed.status == HealthStatus.DEGRADED.value

    # Tunnel is functional end-to-end
    writer.write(b"ping-through-tunnel")
    await writer.drain()
    assert await reader.read(64) == b"ping-through-tunnel"

    # Disconnect: the upstream socket must be released, not leaked
    writer.close()
    await writer.wait_closed()
    await asyncio.sleep(0.1)
    assert captured["writer"].is_closing()

    server.close()
    await server.wait_closed()
    echo_server.close()
    await echo_server.wait_closed()
    assert real_connect is not None


def test_extract_routing_headers_no_implicit_egress_default(temp_gateway):
    """An omitted egress type must not silently re-point a stored binding."""
    gateway, _storage, _router = temp_gateway
    acc, region, egress, service = gateway._extract_routing_headers({}, "127.0.0.1")
    assert egress is None
    assert service is None


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


def test_relay_counters_and_metrics_rendering(temp_gateway):
    """Per-account relay counters aggregate anon ids under one label and the
    /metrics rendering is valid Prometheus text (ops attribution feature)."""
    gateway, storage, router = temp_gateway

    gateway._record_relay("oc2api_Fk8KFb", "sg-warp-01", "example.com:443")
    gateway._record_relay("oc2api_Fk8KFb", "sg-warp-01", "example.com:443")
    gateway._record_relay("anon_abcdef12", "sg-warp-01", "example.com:443")

    assert gateway.relay_total["oc2api_Fk8KFb"] == 2
    assert gateway.relay_total["anon"] == 1

    body = gateway.render_metrics().decode()
    assert '# TYPE tetherpool_gateway_relay_total counter' in body
    assert 'tetherpool_gateway_relay_total{account="oc2api_Fk8KFb"} 2' in body
    assert 'tetherpool_gateway_relay_total{account="anon"} 1' in body
    # anon uuid ids must never leak as individual labels
    assert "anon_abcdef12" not in body
