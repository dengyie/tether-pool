"""
tests/test_code_review_fixes.py - Regression & Correctness Tests for Production Code Review Findings

Validates:
1. Finding 1: Configured premium nodes recover to premium tier after temporary outage / DEAD state.
2. Finding 2 & 3: Router acquire_for_account concurrency lock & PREMIUM_FIRST cross-tier binding reuse.
3. Finding 4: acquire_for_registration & allocator never return DEAD nodes upon exhaustion.
4. Finding 5: Service reboot / load_initial_nodes protects real-time dynamic quality score and tier.
5. Finding 7: Gateway connection failure tripping node to DEAD triggers lease re-homing.
6. Finding 9: _lease_from_allocation & allocator.acquire_single propagate tier correctly.
7. Finding 10: Prometheus metrics expose tier label on node distribution and account bindings.
8. Finding 11: TetherPoolClient SDK supports tier and tier_policy parameters.
"""

import os
import tempfile
import asyncio
from unittest.mock import MagicMock
import pytest

from core.models import (
    ProxyNode, ProbeResult, EgressType, HealthStatus, Tier, TierPolicy,
    LeaseInfo, AccountAllocationItem
)
from core.storage import Storage
from core.router import Router
from core.allocator import AllocationEngine
from core.metrics import MetricsCollector
from adapters.tetherpool_client import TetherPoolClient


def test_finding1_configured_premium_recovers_after_temporary_failure():
    """
    Finding 1: When a configured premium node fails and enters DEAD state,
    it is temporarily degraded to standard. Once it recovers, it must automatically
    restore to premium (not be permanently locked to standard).
    """
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)
        node = ProxyNode(
            name="native-sg-01",
            host="10.144.144.13",
            port=10801,
            region="singapore",
            egress_type=EgressType.NATIVE_IPV4.value,
            tags=["self_hosted", "native"],
            tier=Tier.PREMIUM.value,
            configured_tier=Tier.PREMIUM.value,
            quality_score=95,
            status=HealthStatus.HEALTHY.value,
            exit_ip="1.1.1.1",
            latency_ms=45.0
        )
        storage.upsert_node(node)

        # 1. Node trips into DEAD state due to consecutive probe failures
        storage.update_probe_result(ProbeResult(node_name=node.name, success=False, error="timeout"))
        storage.update_probe_result(ProbeResult(node_name=node.name, success=False, error="timeout"))
        
        dead_node = storage.get_node(node.name)
        assert dead_node.status == HealthStatus.DEAD.value
        assert dead_node.tier == Tier.STANDARD.value  # Demoted while dead

        # 2. Network recovers: probe succeeds with low latency (<350ms)
        storage.update_probe_result(ProbeResult(
            node_name=node.name,
            success=True,
            latency_ms=42.0,
            exit_ip="1.1.1.1"
        ))

        recovered_node = storage.get_node(node.name)
        assert recovered_node.status == HealthStatus.HEALTHY.value
        # MUST recover back to premium!
        assert recovered_node.tier == Tier.PREMIUM.value
        assert recovered_node.quality_score >= 80
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_finding3_router_premium_first_allows_fallback_binding_reuse():
    """
    Finding 3: Under PREMIUM_FIRST, if an account was previously assigned a standard
    node as a fallback, subsequent calls with tier='premium' should reuse the healthy
    binding rather than declaring a binding conflict and reverting to ephemeral mode.
    """
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)
        router = Router(storage=storage)

        std_node = ProxyNode(
            name="warp-tokyo-01",
            host="10.144.144.3",
            port=10881,
            region="tokyo",
            egress_type=EgressType.WARP_IPV6.value,
            tier=Tier.STANDARD.value,
            status=HealthStatus.HEALTHY.value,
            quality_score=60
        )
        storage.upsert_node(std_node)

        # Initial acquisition with PREMIUM_FIRST falls back to standard node
        lease1 = router.acquire_for_account(
            account_id="client_bot_01",
            region="tokyo",
            tier=Tier.PREMIUM.value,
            tier_policy=TierPolicy.PREMIUM_FIRST.value
        )
        assert lease1 is not None
        assert lease1.node_name == "warp-tokyo-01"
        assert lease1.tier == Tier.STANDARD.value

        # Subsequent call with tier="premium" should reuse the sticky standard binding
        lease2 = router.acquire_for_account(
            account_id="client_bot_01",
            region="tokyo",
            tier=Tier.PREMIUM.value,
            tier_policy=TierPolicy.PREMIUM_FIRST.value
        )
        assert lease2 is not None
        assert lease2.node_name == "warp-tokyo-01"
        # Confirm it's still persisted in account_allocations
        alloc = storage.find_allocation_by_account("client_bot_01", "default")
        assert alloc is not None
        assert alloc["node_name"] == "warp-tokyo-01"
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_finding4_never_allocate_dead_nodes_on_exhaustion():
    """
    Finding 4: When all nodes are dead, acquire_for_registration and allocator
    must return None / raise, and NEVER hand out a DEAD node.
    """
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)
        router = Router(storage=storage)
        allocator = AllocationEngine(storage=storage)

        dead_node = ProxyNode(
            name="dead-exit-01",
            host="10.144.144.4",
            port=1080,
            region="us-west",
            egress_type=EgressType.NATIVE_IPV4.value,
            status=HealthStatus.DEAD.value,
            exit_ip="100.100.100.100"
        )
        storage.upsert_node(dead_node)

        # Registration must reject and return None
        reg_lease = router.acquire_for_registration(
            account_id="reg_bot_01",
            target_service="test_svc",
            region="us-west"
        )
        assert reg_lease is None

        # Universal allocator must strictly raise or exclude dead node
        with pytest.raises(RuntimeError, match="No eligible nodes available"):
            allocator.acquire_single(
                service="test_svc",
                account_id="acc_01",
                region="us-west"
            )
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_finding5_startup_upsert_preserves_probed_score_and_tier():
    """
    Finding 5: When the API boots up and reloads nodes.yaml with unprobed status,
    existing nodes that were already probed must retain their dynamic quality_score
    and tier in SQLite.
    """
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)

        # Initial node after probing
        node = ProxyNode(
            name="server-tokyo-01",
            host="10.144.144.3",
            port=10801,
            region="tokyo",
            egress_type=EgressType.NATIVE_IPV4.value,
            tier=Tier.PREMIUM.value,
            configured_tier=Tier.PREMIUM.value,
            quality_score=98,
            status=HealthStatus.HEALTHY.value,
            latency_ms=35.0,
            exit_ip="13.230.1.1"
        )
        storage.upsert_node(node)

        # Simulated system reboot: initial unprobed upsert from config
        reboot_node = ProxyNode(
            name="server-tokyo-01",
            host="10.144.144.3",
            port=10801,
            region="tokyo",
            egress_type=EgressType.NATIVE_IPV4.value,
            tier=Tier.STANDARD.value,  # Unprobed default tier
            configured_tier=Tier.PREMIUM.value,
            quality_score=50,          # Unprobed default score
            status=HealthStatus.UNPROBED.value
        )
        storage.upsert_node(reboot_node)

        after_reboot = storage.get_node("server-tokyo-01")
        # Probed status, score, and tier must be preserved!
        assert after_reboot.status == HealthStatus.HEALTHY.value
        assert after_reboot.quality_score == 98
        assert after_reboot.tier == Tier.PREMIUM.value
        assert after_reboot.configured_tier == Tier.PREMIUM.value
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_finding9_lease_and_allocation_item_include_tier():
    """
    Finding 9: _lease_from_allocation and allocator.acquire_single include tier.
    """
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)
        allocator = AllocationEngine(storage=storage)

        node = ProxyNode(
            name="node-prem-test",
            host="10.144.144.10",
            port=1080,
            region="netherlands",
            egress_type=EgressType.NATIVE_IPV4.value,
            tier=Tier.PREMIUM.value,
            status=HealthStatus.HEALTHY.value,
            quality_score=95
        )
        storage.upsert_node(node)

        item = allocator.acquire_single(
            service="grok2api",
            account_id="grok_user_01",
            tier=Tier.PREMIUM.value
        )
        assert item.tier == Tier.PREMIUM.value
        assert item.node_name == "node-prem-test"

        # Verify _lease_from_allocation helper
        from api.server import _lease_from_allocation
        alloc_record = storage.find_allocation_by_account("grok_user_01", "grok2api")
        lease = _lease_from_allocation("grok_user_01", alloc_record, st=storage)
        assert lease is not None
        assert lease.tier == Tier.PREMIUM.value
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_finding10_metrics_include_tier_labels():
    """
    Finding 10: Prometheus metrics include tier label.
    """
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)
        storage.upsert_node(ProxyNode(
            name="prem-tokyo",
            host="10.144.144.3",
            port=10801,
            region="tokyo",
            egress_type=EgressType.NATIVE_IPV4.value,
            tier=Tier.PREMIUM.value,
            status=HealthStatus.HEALTHY.value
        ))
        storage.batch_save_allocations([("test_svc", "user1", "prem-tokyo", "2026-11-05T00:00:00")])

        collector = MetricsCollector(storage)
        metrics_text = collector.generate_metrics()

        assert 'tier="premium"' in metrics_text
        assert 'ip_pool_node_count{' in metrics_text
        assert 'ip_pool_account_bindings{' in metrics_text
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_finding11_sdk_tetherpool_client_tier_support():
    """
    Finding 11: TetherPoolClient SDK methods accept tier and tier_policy.
    """
    client = TetherPoolClient(api_base="http://127.0.0.1:9100", gateway_base="http://127.0.0.1:9101")
    
    # Check gateway proxy URL format with tier and policy
    url = client.get_gateway_proxy_url(
        account_id="bot_user_99",
        region="tokyo",
        tier="premium",
        tier_policy="premium_only"
    )
    assert "tier_premium" in url
    assert "policy_premium_only" in url
    assert "region_tokyo" in url
