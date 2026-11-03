"""
tests/test_router.py - Unit tests for Router & Sticky Hashing
"""

import os
import tempfile
import pytest
from core.models import ProxyNode, EgressType, HealthStatus
from core.storage import Storage
from core.router import Router


@pytest.fixture
def temp_router():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    router = Router(storage)

    # Insert 4 nodes in 2 regions
    nodes = [
        ProxyNode(name="tokyo-warp-01", host="10.144.144.3", port=10881, region="tokyo", egress_type=EgressType.WARP_IPV6.value, status=HealthStatus.HEALTHY.value),
        ProxyNode(name="tokyo-warp-02", host="10.144.144.3", port=10882, region="tokyo", egress_type=EgressType.WARP_IPV6.value, status=HealthStatus.HEALTHY.value),
        ProxyNode(name="sg-warp-01", host="10.144.144.13", port=10881, region="singapore", egress_type=EgressType.WARP_IPV6.value, status=HealthStatus.HEALTHY.value),
        ProxyNode(name="sg-native-01", host="10.144.144.13", port=10801, region="singapore", egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value),
    ]
    for n in nodes:
        storage.upsert_node(n)

    yield router, storage

    if os.path.exists(db_path):
        os.remove(db_path)


def test_sticky_hash_assignment(temp_router):
    router, storage = temp_router

    lease1 = router.acquire_for_account("user_123")
    assert lease1 is not None

    # Call again for same user -> should return same node
    lease2 = router.acquire_for_account("user_123")
    assert lease2.node_name == lease1.node_name

    # Force rebind
    lease3 = router.acquire_for_account("user_123", force_rebind=True)
    assert lease3 is not None


def test_filter_by_region(temp_router):
    router, storage = temp_router

    lease = router.acquire_for_account("user_tokyo_only", region="tokyo")
    assert lease is not None
    assert "tokyo" in lease.node_name


def test_filter_by_egress_type(temp_router):
    router, storage = temp_router

    lease = router.acquire_for_account("user_native_only", egress_type=EgressType.NATIVE_IPV4.value)
    assert lease is not None
    assert lease.node_name == "sg-native-01"
