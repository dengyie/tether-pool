"""
tests/test_relay_cascade.py - Unit and Integration Tests for SOCKS5 Chained Relay and Adaptive Fallback
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from core.models import ProxyNode, RelayMode, HealthStatus, EgressType
from core.storage import Storage
from core.router import Router
from gateway.server import (
    ForwardProxyGateway, socks5_handshake_on_stream,
    socks5_connect, socks5_relay_connect
)


@pytest.fixture
def temp_storage(tmp_path):
    db_file = str(tmp_path / "test_relay.db")
    storage = Storage(db_path=db_file)
    return storage


@pytest.fixture
def sample_nodes(temp_storage):
    # Relay hub in Hong Kong
    relay_node = ProxyNode(
        name="clash-004-vless-azure-reality",
        host="127.0.0.1",
        port=11004,
        region="hongkong",
        egress_type=EgressType.CLASH_PROXY.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip="104.208.65.233"
    )
    # Target node in Tokyo (configured with fallback relay)
    tokyo_node = ProxyNode(
        name="aws-tokyo-warp-01",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip="2606:4700::1",
        relay_mode=RelayMode.RELAY_FALLBACK.value,
        relay_hop_node="clash-004-vless-azure-reality"
    )
    temp_storage.upsert_node(relay_node)
    temp_storage.upsert_node(tokyo_node)
    return relay_node, tokyo_node


@pytest.mark.asyncio
async def test_resolve_relay_hop_explicit_and_fallback(temp_storage, sample_nodes):
    relay_node, tokyo_node = sample_nodes
    router = Router(temp_storage)

    # 1. Explicit relay hop configured
    hop = router.resolve_relay_hop(tokyo_node)
    assert hop is not None
    assert hop.name == "clash-004-vless-azure-reality"

    # 2. Node without explicit relay hop falls back to regional hub
    unconfigured_node = ProxyNode(
        name="random-mesh-node",
        host="10.144.144.50",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    )
    temp_storage.upsert_node(unconfigured_node)
    fallback_hop = router.resolve_relay_hop(unconfigured_node)
    assert fallback_hop is not None
    assert fallback_hop.name == "clash-004-vless-azure-reality"


@pytest.mark.asyncio
async def test_socks5_handshake_on_stream_ipv4_and_domain():
    # Mock stream reader and writer
    mock_reader = AsyncMock()
    mock_writer = MagicMock()
    mock_writer.write = MagicMock()
    mock_writer.drain = AsyncMock()

    # Step 1: Handshake response (0x05, 0x00)
    # Step 2: Reply head (0x05, 0x00, 0x00, 0x01) -> IPv4 address
    # Step 3: Bound address (4 bytes IP + 2 bytes port)
    mock_reader.readexactly = AsyncMock(side_effect=[
        b"\x05\x00",
        b"\x05\x00\x00\x01",
        b"\x7f\x00\x00\x01\x1f\x90"
    ])

    await socks5_handshake_on_stream(
        mock_reader, mock_writer,
        target_host="1.1.1.1",
        target_port=443,
        timeout=2.0
    )

    assert mock_writer.write.call_count == 2
    # First write was auth request
    assert mock_writer.write.call_args_list[0][0][0] == b"\x05\x01\x00"
    # Second write was connect request
    req_bytes = mock_writer.write.call_args_list[1][0][0]
    assert req_bytes.startswith(b"\x05\x01\x00\x01")


@pytest.mark.asyncio
async def test_connect_upstream_adaptive_direct_success(temp_storage, sample_nodes):
    relay_node, tokyo_node = sample_nodes
    router = Router(temp_storage)
    gw = ForwardProxyGateway(temp_storage, router)

    fake_reader = AsyncMock()
    fake_writer = MagicMock()

    with patch("gateway.server.socks5_connect", new=AsyncMock(return_value=(fake_reader, fake_writer))) as mock_conn:
        r, w = await gw.connect_upstream_adaptive(tokyo_node, "cloudflare.com", 443)
        assert r == fake_reader
        assert w == fake_writer
        # Direct connect was called
        mock_conn.assert_awaited_once_with(
            tokyo_node.host, tokyo_node.port, "cloudflare.com", 443, timeout=2.0
        )


@pytest.mark.asyncio
async def test_connect_upstream_adaptive_fallback_to_relay_cascade(temp_storage, sample_nodes):
    relay_node, tokyo_node = sample_nodes
    router = Router(temp_storage)
    gw = ForwardProxyGateway(temp_storage, router)

    fake_relay_reader = AsyncMock()
    fake_relay_writer = MagicMock()

    # Simulate direct connection timeout/failure
    async def fake_direct_fail(*args, **kwargs):
        raise asyncio.TimeoutError("Direct mesh connect timed out")

    async def fake_relay_success(*args, **kwargs):
        return (fake_relay_reader, fake_relay_writer)

    with patch("gateway.server.socks5_connect", side_effect=fake_direct_fail):
        with patch("gateway.server.socks5_relay_connect", side_effect=fake_relay_success) as mock_relay:
            r, w = await gw.connect_upstream_adaptive(tokyo_node, "cloudflare.com", 443)
            assert r == fake_relay_reader
            assert w == fake_relay_writer

            mock_relay.assert_awaited_once_with(
                relay_host=relay_node.host,
                relay_port=relay_node.port,
                target_proxy_host=tokyo_node.host,
                target_proxy_port=tokyo_node.port,
                dest_host="cloudflare.com",
                dest_port=443,
                timeout=10.0
            )
