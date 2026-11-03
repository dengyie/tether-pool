"""
tests/test_universal_allocator_and_sdk.py - Tests for Universal Allocation Engine,
Parametric Proxy Auth, and Client SDK
"""

import base64
import pytest
from starlette.testclient import TestClient

from core.models import (
    ProxyNode, HealthStatus, EgressType,
    BatchAllocationRequest, AllocationStrategy
)
from core.storage import Storage
from core.allocator import AllocationEngine
from gateway.server import ForwardProxyGateway, parse_proxy_credentials
from core.router import Router
import api.server
from adapters.tetherpool_client import TetherPoolClient


@pytest.fixture
def temp_storage(tmp_path):
    db_file = str(tmp_path / "test_universal.db")
    storage = Storage(db_path=db_file)
    return storage


@pytest.fixture
def warp_100_nodes(temp_storage):
    """Generates 100 WARP IPv6 nodes across 4 regions (25 per region)"""
    regions = ["tokyo", "netherlands", "singapore", "india"]
    nodes = []
    for i in range(1, 101):
        r = regions[(i - 1) % len(regions)]
        node = ProxyNode(
            name=f"test-warp-{i:03d}",
            host=f"10.144.144.{10 + (i // 25)}",
            port=10800 + i,
            region=r,
            egress_type=EgressType.WARP_IPV6.value,
            status=HealthStatus.HEALTHY.value,
            exit_ip=f"2a09:bac5:3b{i:02d}::1"
        )
        temp_storage.upsert_node(node)
        nodes.append(node)
    return nodes


def test_batch_allocate_2100_accounts_balanced_sharding(temp_storage, warp_100_nodes):
    allocator = AllocationEngine(temp_storage)

    # 2,100 accounts (typical grok2api scale)
    accounts = [f"grok_acc_{i:04d}@domain.com" for i in range(1, 2101)]

    req = BatchAllocationRequest(
        service="grok2api",
        accounts=accounts,
        strategy=AllocationStrategy.BALANCED_ROUND_ROBIN.value,
        egress_preference=EgressType.WARP_IPV6.value,
        max_accounts_per_ip=30
    )

    resp = allocator.batch_allocate(req)

    assert resp.total_accounts == 2100
    assert resp.allocated_nodes_count == 100
    # Perfect balance across 100 nodes: 2100 / 100 = 21 accounts per node!
    assert resp.min_accounts_per_node == 21
    assert resp.max_accounts_per_node == 21
    assert resp.avg_accounts_per_node == 21.0

    # Verify allocations persistence & formatting
    assert len(resp.allocations) == 2100
    first_acc = resp.allocations["grok_acc_0001@domain.com"]
    assert first_acc.service == "grok2api"
    assert first_acc.egress_type == "warp_ipv6"
    assert first_acc.proxy_url.startswith("socks5")
    assert "tetherpool@" in first_acc.gateway_proxy_url
    assert first_acc.exit_ip is not None


def test_incremental_allocation_idempotence(temp_storage, warp_100_nodes):
    allocator = AllocationEngine(temp_storage)

    # Batch 1: 50 accounts
    accounts_1 = [f"bot_{i:03d}" for i in range(1, 51)]
    req1 = BatchAllocationRequest(service="test_svc", accounts=accounts_1)
    res1 = allocator.batch_allocate(req1)
    first_binding = res1.allocations["bot_001"].node_name

    # Batch 2: Same 50 accounts + 10 new accounts with force_rebalance=False
    accounts_2 = accounts_1 + [f"bot_{i:03d}" for i in range(51, 61)]
    req2 = BatchAllocationRequest(service="test_svc", accounts=accounts_2, force_rebalance=False)
    res2 = allocator.batch_allocate(req2)

    assert res2.total_accounts == 60
    # Original binding must be preserved
    assert res2.allocations["bot_001"].node_name == first_binding


def test_consistent_hash_sharding(temp_storage, warp_100_nodes):
    allocator = AllocationEngine(temp_storage)

    accounts = [f"ch_bot_{i:03d}" for i in range(1, 101)]
    req = BatchAllocationRequest(
        service="hash_svc",
        accounts=accounts,
        strategy=AllocationStrategy.CONSISTENT_HASH.value
    )
    res = allocator.batch_allocate(req)
    assert res.total_accounts == 100
    assert res.allocated_nodes_count > 0


def test_parametric_proxy_credentials_parser():
    # 1. Plain
    user, r, eg, svc = parse_proxy_credentials("plain_user")
    assert user == "plain_user"
    assert r is None
    assert eg is None
    assert svc is None

    # 2. Double-underscore format with service
    user, r, eg, svc = parse_proxy_credentials("acc_123__type_warp_ipv6__region_tokyo__service_grok2api")
    assert user == "acc_123"
    assert r == "tokyo"
    assert eg == "warp_ipv6"
    assert svc == "grok2api"

    # 3. Query string format
    user, r, eg, svc = parse_proxy_credentials("acc_456?region=singapore&type=native_ipv4&service=cpa")
    assert user == "acc_456"
    assert r == "singapore"
    assert eg == "native_ipv4"
    assert svc == "cpa"

    # 4. Key-value dash format
    user, r, eg, svc = parse_proxy_credentials("account=acc_789-region=netherlands-type=clash_proxy-service=test")
    assert user == "acc_789"
    assert r == "netherlands"
    assert eg == "clash_proxy"
    assert svc == "test"


def test_gateway_routing_headers_with_parametric_auth(temp_storage):
    router = Router(temp_storage)
    gw = ForwardProxyGateway(temp_storage, router)

    # Encode Basic Auth with parametric credentials
    raw_cred = "bot_hero__type_clash_proxy__region_hongkong__service_botcorp:mypassword"
    b64_val = base64.b64encode(raw_cred.encode()).decode()

    headers = {
        "proxy-authorization": f"Basic {b64_val}"
    }

    acc_id, region, egress, service = gw._extract_routing_headers(headers, "127.0.0.1")
    assert acc_id == "bot_hero"
    assert region == "hongkong"
    assert egress == "clash_proxy"
    assert service == "botcorp"


def test_api_allocation_endpoints():
    # Seed node into api.server.storage
    api.server.storage.upsert_node(ProxyNode(
        name="api-alloc-warp-node",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip="2a09:bac5::1"
    ))

    client = TestClient(api.server.app)

    # 1. POST /v1/allocation/batch
    batch_payload = {
        "service": "grok2api_test",
        "accounts": ["grok_1", "grok_2", "grok_3"],
        "strategy": "balanced_round_robin",
        "egress_preference": "warp_ipv6"
    }
    resp = client.post("/v1/allocation/batch", json=batch_payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_accounts"] == 3
    assert "grok_1" in data["allocations"]

    # 2. POST /v1/allocation/acquire (single account)
    acq_resp = client.post(
        "/v1/allocation/acquire",
        params={"service": "grok2api_test", "account_id": "grok_single"}
    )
    assert acq_resp.status_code == 200
    acq_data = acq_resp.json()
    assert acq_data["account_id"] == "grok_single"
    assert acq_data["service"] == "grok2api_test"
    assert acq_data["node_name"] is not None
    assert acq_data["proxy_url"].startswith("socks5")

    # 3. GET /v1/allocation/status
    stat_resp = client.get("/v1/allocation/status")
    assert stat_resp.status_code == 200
    assert stat_resp.json()["total_allocations"] >= 4

    # 4. POST /v1/allocation/release
    rel_resp = client.post(
        "/v1/allocation/release",
        params={"service": "grok2api_test", "account_id": "grok_single"}
    )
    assert rel_resp.status_code == 200
    assert rel_resp.json()["success"] is True


def test_tetherpool_client_sdk_url_generation():
    sdk = TetherPoolClient(gateway_base="http://127.0.0.1:9101")
    url = sdk.get_gateway_proxy_url("my_acc", egress_type="warp_ipv6", region="tokyo", service="grok2api")
    assert url == "http://my_acc__type_warp_ipv6__region_tokyo__service_grok2api:tetherpool@127.0.0.1:9101"


def test_gateway_router_reuses_universal_allocation(temp_storage, warp_100_nodes):
    allocator = AllocationEngine(temp_storage)
    router = Router(temp_storage)

    # 1. Allocate acc_123 via universal batch allocation
    req = BatchAllocationRequest(
        service="grok2api",
        accounts=["acc_123"],
        strategy="balanced_round_robin",
        egress_preference="warp_ipv6"
    )
    res = allocator.batch_allocate(req)
    assigned_node_name = res.allocations["acc_123"].node_name

    # 2. Query router with service="grok2api" - MUST return the EXACT same node
    lease = router.acquire_for_account("acc_123", service="grok2api")
    assert lease is not None
    assert lease.node_name == assigned_node_name


def test_battery_event_updates_account_allocations_and_rebinds(temp_storage, warp_100_nodes):
    allocator = AllocationEngine(temp_storage)
    router = Router(temp_storage)

    # 1. Allocate acc_swap via allocator
    item = allocator.acquire_single(service="grok2api", account_id="acc_swap")
    old_node_name = item.node_name

    # 2. Report 429 battery event
    from core.models import BatteryEventReport
    report = BatteryEventReport(
        account_id="acc_swap",
        target_service="grok2api",
        event_type="rate_limited_429",
        force_rebind=True
    )
    result = router.handle_battery_event(report)
    assert result["battery_swapped"] is True
    new_node_name = result["new_lease"]["node_name"]
    assert new_node_name is not None

    # 3. Verify account_allocations was updated to the new node
    updated_alloc = temp_storage.get_allocation("grok2api", "acc_swap")
    assert updated_alloc["node_name"] == new_node_name


def test_max_accounts_per_ip_capacity_exhaustion(temp_storage):
    # Only 2 nodes available
    temp_storage.upsert_node(ProxyNode(
        name="cap-node-1", host="10.0.0.1", port=1080, region="tokyo",
        egress_type="warp_ipv6", status="healthy"
    ))
    temp_storage.upsert_node(ProxyNode(
        name="cap-node-2", host="10.0.0.2", port=1080, region="tokyo",
        egress_type="warp_ipv6", status="healthy"
    ))

    allocator = AllocationEngine(temp_storage)
    # 2 nodes * max 5 per ip = 10 capacity. Requesting 15 accounts must raise ValueError!
    req = BatchAllocationRequest(
        service="test_cap",
        accounts=[f"user_{i}" for i in range(15)],
        max_accounts_per_ip=5
    )

    with pytest.raises(ValueError) as excinfo:
        allocator.batch_allocate(req)
    assert "Pool capacity exceeded" in str(excinfo.value)


def test_heterogeneous_nodes_heap_balance_fill(temp_storage):
    # 2 nodes: Node A has 5 existing accounts, Node B has 0
    node_a = ProxyNode(name="heap-a", host="10.0.0.1", port=1080, region="tokyo", egress_type="warp_ipv6", status="healthy")
    node_b = ProxyNode(name="heap-b", host="10.0.0.2", port=1080, region="tokyo", egress_type="warp_ipv6", status="healthy")
    temp_storage.upsert_node(node_a)
    temp_storage.upsert_node(node_b)

    # Seed 5 accounts on node_a
    seed_records = [("test_svc", f"seed_{i}", "heap-a", "2026-11-04T00:00:00") for i in range(5)]
    temp_storage.batch_save_allocations(seed_records)

    allocator = AllocationEngine(temp_storage)
    # Assign 2 new accounts with force_rebalance=False
    req = BatchAllocationRequest(
        service="test_svc",
        accounts=[f"seed_{i}" for i in range(5)] + ["new_1", "new_2"],
        force_rebalance=False,
        max_accounts_per_ip=10
    )
    res = allocator.batch_allocate(req)

    # Both new accounts MUST be placed on heap-b (lowest loaded)
    assert res.allocations["new_1"].node_name == "heap-b"
    assert res.allocations["new_2"].node_name == "heap-b"


def test_anonymous_request_does_not_persist_lease(temp_storage, warp_100_nodes):
    router = Router(temp_storage)
    lease = router.acquire_for_account("anon_a1b2c3d4")
    assert lease is not None
    assert lease.account_id == "anon_a1b2c3d4"

    # Must NOT exist in database leases table
    db_lease = temp_storage.get_lease("anon_a1b2c3d4")
    assert db_lease is None
