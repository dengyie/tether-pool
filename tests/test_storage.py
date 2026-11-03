"""
tests/test_storage.py - Unit tests for Storage engine
"""

import os
import tempfile
import pytest
from core.models import ProxyNode, ProbeResult, EgressType, HealthStatus
from core.storage import Storage


@pytest.fixture
def temp_storage():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    yield storage
    if os.path.exists(db_path):
        os.remove(db_path)


def test_upsert_and_get_node(temp_storage):
    node = ProxyNode(
        name="test-tokyo-01",
        host="10.144.144.3",
        port=10881,
        protocol="socks5h",
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        tags=["warp", "ipv6"],
        weight=100
    )
    temp_storage.upsert_node(node)

    retrieved = temp_storage.get_node("test-tokyo-01")
    assert retrieved is not None
    assert retrieved.name == "test-tokyo-01"
    assert retrieved.host == "10.144.144.3"
    assert retrieved.port == 10881
    assert retrieved.region == "tokyo"
    assert retrieved.status == HealthStatus.UNPROBED.value


def test_probe_success_updates_node(temp_storage):
    node = ProxyNode(
        name="test-sg-01",
        host="10.144.144.13",
        port=10881,
        region="singapore",
        egress_type=EgressType.WARP_IPV6.value
    )
    temp_storage.upsert_node(node)

    res = ProbeResult(
        node_name="test-sg-01",
        success=True,
        exit_ip="2a09:bac5::1",
        latency_ms=120.5
    )
    temp_storage.update_probe_result(res)

    updated = temp_storage.get_node("test-sg-01")
    assert updated.status == HealthStatus.HEALTHY.value
    assert updated.exit_ip == "2a09:bac5::1"
    assert updated.latency_ms == 120.5
    assert updated.fail_count == 0


def test_probe_failure_degrades_and_kills_node(temp_storage):
    node = ProxyNode(
        name="test-nl-01",
        host="10.144.144.12",
        port=10881,
        region="netherlands",
        egress_type=EgressType.WARP_IPV6.value
    )
    temp_storage.upsert_node(node)

    # First fail -> degraded
    temp_storage.update_probe_result(ProbeResult(node_name="test-nl-01", success=False, error="timeout"))
    updated = temp_storage.get_node("test-nl-01")
    assert updated.status == HealthStatus.DEGRADED.value
    assert updated.fail_count == 1

    # Second fail -> dead
    temp_storage.update_probe_result(ProbeResult(node_name="test-nl-01", success=False, error="timeout"))
    updated2 = temp_storage.get_node("test-nl-01")
    assert updated2.status == HealthStatus.DEAD.value
    assert updated2.fail_count == 2


def test_leases_persistence(temp_storage):
    node = ProxyNode(
        name="test-india-01",
        host="10.144.144.5",
        port=10881,
        region="india",
        egress_type=EgressType.WARP_IPV6.value
    )
    temp_storage.upsert_node(node)

    lease = temp_storage.set_lease("account_alpha", "test-india-01")
    assert lease.account_id == "account_alpha"
    assert lease.node_name == "test-india-01"

    fetched = temp_storage.get_lease("account_alpha")
    assert fetched is not None
    assert fetched.node_name == "test-india-01"
    assert fetched.proxy_url == "socks5h://10.144.144.5:10881"


def test_pool_stats(temp_storage):
    temp_storage.upsert_node(ProxyNode(name="n1", host="1.1.1.1", port=1, region="tokyo", egress_type=EgressType.WARP_IPV6.value))
    temp_storage.upsert_node(ProxyNode(name="n2", host="1.1.1.1", port=2, region="tokyo", egress_type=EgressType.NATIVE_IPV4.value))
    temp_storage.upsert_node(ProxyNode(name="n3", host="1.1.1.1", port=3, region="singapore", egress_type=EgressType.WARP_IPV6.value))

    stats = temp_storage.get_stats()
    assert stats.total_nodes == 3
    assert stats.unprobed_nodes == 3
    assert stats.warp_ipv6_count == 2
    assert stats.native_ipv4_count == 1
    assert stats.region_distribution["tokyo"] == 2
    assert stats.region_distribution["singapore"] == 1
