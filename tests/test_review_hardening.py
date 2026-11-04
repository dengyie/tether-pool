"""
tests/test_review_hardening.py - Regression tests for deep code review hardening:
1. Model field validators & port bounds
2. Static vs API-managed dynamic node coexistence on reload/prune
3. DELETE node critical section & pre-mark DEAD
4. Status and circuit_state bi-directional sync & audit logging
5. Gateway parametric credentials & X-Rebind header propagation
6. Allocator single acquire constraint matching
"""

import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from core.models import (
    ProxyNode, HealthStatus, Tier, TierPolicy, EgressType,
    NodeCreateRequest, NodeUpdateRequest
)
from core.storage import Storage
from core.router import Router
from core.allocator import AllocationEngine
from gateway.server import (
    parse_proxy_credentials, ForwardProxyGateway, RoutingContext
)
from api.server import app


def test_model_port_and_enum_validators():
    # 1. Valid create request
    req = NodeCreateRequest(
        name="test-node-01",
        host="1.1.1.1",
        port=1080,
        protocol="socks5h",
        region="tokyo",
        egress_type="warp_ipv6",
        tier="premium"
    )
    assert req.port == 1080
    assert req.tier == "premium"

    # 2. Port out of bounds
    with pytest.raises(ValidationError):
        NodeCreateRequest(
            name="bad-port-node",
            host="1.1.1.1",
            port=0,
            region="tokyo",
            egress_type="warp_ipv6"
        )
    with pytest.raises(ValidationError):
        NodeCreateRequest(
            name="bad-port-node-2",
            host="1.1.1.1",
            port=65536,
            region="tokyo",
            egress_type="warp_ipv6"
        )

    # 3. Invalid tier
    with pytest.raises(ValidationError):
        NodeCreateRequest(
            name="bad-tier-node",
            host="1.1.1.1",
            port=1080,
            region="tokyo",
            egress_type="warp_ipv6",
            tier="super_tier"
        )

    # 4. Invalid protocol
    with pytest.raises(ValidationError):
        NodeCreateRequest(
            name="bad-proto-node",
            host="1.1.1.1",
            port=1080,
            protocol="wireguard",
            region="tokyo",
            egress_type="warp_ipv6"
        )

    # 5. NodeUpdateRequest validations
    with pytest.raises(ValidationError):
        NodeUpdateRequest(port=70000)
    with pytest.raises(ValidationError):
        NodeUpdateRequest(status="exploded")
    with pytest.raises(ValidationError):
        NodeUpdateRequest(tier="invalid_tier")


def test_api_managed_node_protection(storage):
    # Setup: static nodes and one dynamic node tagged api_managed
    static_node = ProxyNode(
        name="static-01",
        host="10.0.0.1",
        port=1080,
        region="tokyo",
        egress_type="warp_ipv6",
        status=HealthStatus.HEALTHY.value,
        tags=["static_config"]
    )
    api_node = ProxyNode(
        name="api-dyn-01",
        host="10.0.0.2",
        port=1080,
        region="singapore",
        egress_type="native_ipv4",
        status=HealthStatus.HEALTHY.value,
        tags=["api_managed", "customer_added"]
    )
    storage.upsert_node(static_node)
    storage.upsert_node(api_node)

    # When config reloads with only static-02, static-01 is missing, but api-dyn-01 must not be marked dead
    stale = storage.mark_nodes_missing_as_dead(keep_names=["static-02"])
    assert "static-01" in stale
    assert "api-dyn-01" not in stale

    node_dyn = storage.get_node("api-dyn-01")
    assert node_dyn.status == HealthStatus.HEALTHY.value

    # Prune nodes except static-02: static-01 is pruned, but api-dyn-01 must remain intact
    pruned = storage.prune_nodes_except(keep_names=["static-02"])
    assert "static-01" in pruned
    assert "api-dyn-01" not in pruned

    assert storage.get_node("static-01") is None
    assert storage.get_node("api-dyn-01") is not None


def test_status_and_circuit_state_sync_with_audit_trail(storage):
    node = ProxyNode(
        name="sync-node-01",
        host="10.0.0.10",
        port=1080,
        region="tokyo",
        egress_type="warp_ipv6",
        status=HealthStatus.HEALTHY.value,
        circuit_state=HealthStatus.HEALTHY.value
    )
    storage.upsert_node(node)

    # Patch status to dead via update_node
    updated = storage.update_node("sync-node-01", {"status": HealthStatus.DEAD.value})
    assert updated.status == HealthStatus.DEAD.value
    assert updated.circuit_state == HealthStatus.DEAD.value

    # Verify audit trail in circuit_events
    events = storage.list_circuit_events(limit=5)
    matching = [e for e in events if e.node_name == "sync-node-01"]
    assert len(matching) > 0
    assert matching[0].from_state == HealthStatus.HEALTHY.value
    assert matching[0].to_state == HealthStatus.DEAD.value
    assert matching[0].reason == "api_manual_patch"


def test_gateway_parametric_credentials_rebind_parsing():
    # 1. Double-underscore format with rebind_true
    creds1 = parse_proxy_credentials("bot_101__type_warp_ipv6__region_tokyo__rebind_true")
    assert creds1.account_id == "bot_101"
    assert creds1.region == "tokyo"
    assert creds1.egress_type == "warp_ipv6"
    assert creds1.force_rebind is True

    # 2. Query string format with rebind=true
    creds2 = parse_proxy_credentials("bot_102?region=singapore&tier=premium&rebind=true")
    assert creds2.account_id == "bot_102"
    assert creds2.region == "singapore"
    assert creds2.tier == "premium"
    assert creds2.force_rebind is True

    # 3. Dash format with rebind=true
    creds3 = parse_proxy_credentials("account=bot_103-region=netherlands-tier=standard-rebind=true")
    assert creds3.account_id == "bot_103"
    assert creds3.region == "netherlands"
    assert creds3.tier == "standard"
    assert creds3.force_rebind is True

    # 4. Normal credentials without rebind
    creds4 = parse_proxy_credentials("bot_104__region_tokyo")
    assert creds4.force_rebind is False


def test_gateway_extract_routing_headers(storage, router):
    gw = ForwardProxyGateway(storage, router)

    # Test X-Rebind header
    headers = {
        "x-account-id": "acc_x_1",
        "x-rebind": "true",
        "x-tier": "premium"
    }
    ctx = gw._extract_routing_headers(headers, "127.0.0.1")
    assert ctx.account_id == "acc_x_1"
    assert ctx.force_rebind is True
    assert ctx.tier == "premium"

    # Test X-Force-Rebind header
    headers2 = {
        "x-account-id": "acc_x_2",
        "x-force-rebind": "1"
    }
    ctx2 = gw._extract_routing_headers(headers2, "127.0.0.1")
    assert ctx2.force_rebind is True


def test_allocator_acquire_single_constraint_matching(storage):
    engine = AllocationEngine(storage)

    # Register two healthy nodes: one tokyo standard, one singapore premium
    n_tokyo = ProxyNode(
        name="tokyo-std-01",
        host="10.0.0.1",
        port=1080,
        region="tokyo",
        egress_type="warp_ipv6",
        tier=Tier.STANDARD.value,
        status=HealthStatus.HEALTHY.value
    )
    n_sg = ProxyNode(
        name="sg-prem-01",
        host="10.0.0.2",
        port=1080,
        region="singapore",
        egress_type="native_ipv4",
        tier=Tier.PREMIUM.value,
        status=HealthStatus.HEALTHY.value
    )
    storage.upsert_node(n_tokyo)
    storage.upsert_node(n_sg)

    # Allocate initially to Tokyo standard
    item1 = engine.acquire_single(
        service="test_svc",
        account_id="user_cons_1",
        region="tokyo",
        tier=Tier.STANDARD.value
    )
    assert item1.node_name == "tokyo-std-01"

    # Subsequent acquire without constraints reuses Tokyo
    item2 = engine.acquire_single(service="test_svc", account_id="user_cons_1")
    assert item2.node_name == "tokyo-std-01"

    # Subsequent acquire requesting Singapore should NOT return Tokyo
    item3 = engine.acquire_single(service="test_svc", account_id="user_cons_1", region="singapore")
    assert item3.node_name == "sg-prem-01"

    # Subsequent acquire requesting Premium tier only should return Singapore
    item4 = engine.acquire_single(
        service="test_svc",
        account_id="user_cons_1",
        tier=Tier.PREMIUM.value,
        tier_policy=TierPolicy.PREMIUM_ONLY.value
    )
    assert item4.node_name == "sg-prem-01"


def test_api_node_create_auto_tag_and_delete_with_rehome(storage, router):
    client = TestClient(app)

    # 1. Create a node through API without passing "api_managed" in tags
    create_payload = {
        "name": "api-autotag-01",
        "host": "10.144.144.20",
        "port": 10881,
        "protocol": "socks5h",
        "region": "tokyo",
        "egress_type": "warp_ipv6",
        "tags": ["custom_tag"],
        "weight": 100,
        "tier": "standard"
    }
    resp = client.post("/v1/nodes", json=create_payload)
    assert resp.status_code == 200
    data = resp.json()
    assert "api_managed" in data["tags"]
    assert "custom_tag" in data["tags"]

    # Add a healthy fallback node in same region
    fallback = ProxyNode(
        name="tokyo-fallback",
        host="10.144.144.21",
        port=10882,
        protocol="socks5h",
        region="tokyo",
        egress_type="warp_ipv6",
        status=HealthStatus.HEALTHY.value
    )
    storage.upsert_node(fallback)

    # Set the api node healthy and bind an account
    storage.update_node("api-autotag-01", {"status": HealthStatus.HEALTHY.value})
    lease = router.acquire_for_account(account_id="rehome_acc_01", region="tokyo")
    assert lease is not None

    # Delete the node with rehome=True
    del_resp = client.delete("/v1/nodes/api-autotag-01?rehome=true")
    assert del_resp.status_code == 200
    del_data = del_resp.json()
    assert del_data["deleted"] is True
    assert del_data["node_name"] == "api-autotag-01"

    # Node is deleted from registry
    assert storage.get_node("api-autotag-01") is None

    # Account lease is rehomed to the fallback node
    new_lease = router.acquire_for_account(account_id="rehome_acc_01", region="tokyo")
    assert new_lease.node_name == "tokyo-fallback"

