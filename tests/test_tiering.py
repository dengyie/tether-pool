"""
tests/test_tiering.py - Unit and Regression Tests for Quality Pool Separation (优质/劣质代理分池)

Validates:
1. Multi-factor IP quality scoring engine (core/quality.py)
2. Storage layer tiering & quality score persistence (core/storage.py)
3. Router tier ladder selection & default standard_first protection (core/router.py)
4. Strict tier isolation policies (premium_only, standard_only)
5. Registration anti-sybil default to premium pool
6. Gateway parameter parsing & internal header stripping (gateway/server.py)
"""

import os
import tempfile
import pytest
from core.quality import IPQualityEvaluator, QualityTier
from core.models import ProxyNode, ProbeResult, EgressType, Tier, TierPolicy
from core.storage import Storage
from core.router import Router
from core.allocator import AllocationEngine, BatchAllocationRequest
from gateway.server import parse_proxy_credentials


def test_ip_quality_evaluator_scoring_breakdown():
    # 1. Native dedicated node with low latency (<150ms), healthy, verified exit IP
    score, tier, factors = IPQualityEvaluator.evaluate_node(
        tags=["self_hosted", "native", "aws"],
        egress_type=EgressType.NATIVE_IPV4.value,
        latency_ms=85.0,
        status="healthy",
        consecutive_failures=0,
        exit_ip="13.230.10.20",
    )
    assert score == 40 + 30 + 20 + 10  # 100
    assert tier == QualityTier.PREMIUM
    assert factors["provenance"]["score"] == 40
    assert factors["latency"]["score"] == 30
    assert factors["stability"]["score"] == 20
    assert factors["exit_ip"]["score"] == 10

    # 2. Third-party aggregated node with moderate latency, degraded
    score2, tier2, factors2 = IPQualityEvaluator.evaluate_node(
        tags=["clash_sub"],
        egress_type=EgressType.CLASH_PROXY.value,
        latency_ms=450.0,
        status="degraded",
        consecutive_failures=1,
        exit_ip="198.51.100.1",
    )
    # Provenance: 15, Latency (350-700ms): 10, Stability: 5, Exit IP: 10 -> Total: 40
    assert score2 == 15 + 10 + 5 + 10
    assert tier2 == QualityTier.STANDARD

    # 3. Dead node with consecutive failures -> score penalty
    score3, tier3, _ = IPQualityEvaluator.evaluate_node(
        tags=["self_hosted"],
        egress_type=EgressType.NATIVE_IPV4.value,
        latency_ms=100.0,
        status="dead",
        consecutive_failures=3,
        exit_ip="13.230.10.20",
    )
    # 40 (prov) + 30 (rtt) - 20 (stability) + 10 (exit) = 60
    assert score3 == 60
    # DEAD node cannot be premium
    assert tier3 == QualityTier.STANDARD

    # 4. External threat penalty reduction
    score4, _, factors4 = IPQualityEvaluator.evaluate_node(
        tags=["self_hosted"],
        egress_type=EgressType.NATIVE_IPV4.value,
        latency_ms=100.0,
        status="healthy",
        consecutive_failures=0,
        exit_ip="1.2.3.4",
        external_fraud_score=80,  # Penalty = 80 * 0.3 = 24
    )
    assert score4 == (40 + 30 + 20 + 10) - 24
    assert factors4["fraud_penalty"]["penalty"] == 24


def test_ip_quality_evaluator_configured_tier_precedence():
    # Explicitly configured premium node keeps premium unless dead
    score, tier, _ = IPQualityEvaluator.evaluate_node(
        tags=[],
        egress_type="warp_ipv6",
        latency_ms=500.0,
        status="healthy",
        configured_tier="premium",
    )
    assert tier == QualityTier.PREMIUM

    # If dead, explicit premium is demoted to standard
    score_dead, tier_dead, _ = IPQualityEvaluator.evaluate_node(
        tags=[],
        egress_type="warp_ipv6",
        latency_ms=500.0,
        status="dead",
        configured_tier="premium",
    )
    assert tier_dead == QualityTier.STANDARD

    # Explicitly configured standard node is strictly locked to standard
    # even if it has high score, low latency, and native dedicated tags
    score_std, tier_std, _ = IPQualityEvaluator.evaluate_node(
        tags=["self_hosted", "native"],
        egress_type="native_ipv4",
        latency_ms=50.0,
        status="healthy",
        configured_tier="standard",
    )
    assert score_std >= 80
    assert tier_std == QualityTier.STANDARD


def test_storage_tier_persistence_and_stats():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)

        node_prem = ProxyNode(
            name="node-tokyo-prem",
            host="10.144.144.10",
            port=1080,
            region="tokyo",
            egress_type=EgressType.NATIVE_IPV4.value,
            tags=["self_hosted", "native"],
            tier=Tier.PREMIUM.value,
            quality_score=95,
            status="healthy",
            exit_ip="13.230.10.20",
            latency_ms=80.0,
        )
        node_std = ProxyNode(
            name="node-tokyo-std",
            host="10.144.144.20",
            port=1080,
            region="tokyo",
            egress_type=EgressType.WARP_IPV6.value,
            tags=["warp"],
            tier=Tier.STANDARD.value,
            quality_score=60,
            status="healthy",
            exit_ip="2606:4700::1",
            latency_ms=250.0,
        )

        storage.upsert_node(node_prem)
        storage.upsert_node(node_std)

        # Verify list_nodes filtering by tier
        prem_nodes = storage.list_nodes(tier=Tier.PREMIUM.value)
        assert len(prem_nodes) == 1
        assert prem_nodes[0].name == "node-tokyo-prem"
        assert prem_nodes[0].tier == Tier.PREMIUM.value
        assert prem_nodes[0].quality_score == 95

        std_nodes = storage.list_nodes(tier=Tier.STANDARD.value)
        assert len(std_nodes) == 1
        assert std_nodes[0].name == "node-tokyo-std"

        # Verify get_stats returns tier distribution & average quality score
        stats = storage.get_stats()
        assert stats.tier_distribution.get("premium") == 1
        assert stats.tier_distribution.get("standard") == 1
        assert stats.average_quality_score == 77.5

        # Update probe result and dynamic score update
        storage.update_probe_result(
            ProbeResult(
                node_name="node-tokyo-std",
                success=True,
                latency_ms=120.0,
                exit_ip="2606:4700::2",
            )
        )
        updated_std = storage.get_node("node-tokyo-std")
        assert updated_std.status == "healthy"
        assert updated_std.latency_ms == 120.0
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_router_default_routing_protects_premium_pool():
    """
    Default routing policy must be 'standard_first':
    When no tier is specified, requests MUST route to standard nodes
    to protect expensive self-hosted premium nodes from exhaustion.
    """
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)
        router = Router(storage=storage)

        # Setup 1 premium node and 1 standard node in same region
        prem_node = ProxyNode(
            name="node-prem-01",
            host="10.144.144.1",
            port=1080,
            region="hongkong",
            egress_type=EgressType.NATIVE_IPV4.value,
            tier=Tier.PREMIUM.value,
            status="healthy",
            quality_score=95,
        )
        std_node = ProxyNode(
            name="node-std-01",
            host="10.144.144.2",
            port=1080,
            region="hongkong",
            egress_type=EgressType.WARP_IPV6.value,
            tier=Tier.STANDARD.value,
            status="healthy",
            quality_score=55,
        )
        storage.upsert_node(prem_node)
        storage.upsert_node(std_node)

        # 1. Unspecified request (no tier) -> should acquire standard node
        lease = router.acquire_for_account(
            account_id="bot_unspecified_001",
            region="hongkong",
            egress_type=None,
        )
        assert lease is not None
        assert lease.node_name == "node-std-01"
        assert lease.tier == Tier.STANDARD.value

        # 2. When standard pool is completely dead/exhausted -> standard_first falls back to premium
        storage.update_probe_result(ProbeResult(node_name="node-std-01", success=False, error="timeout"))
        storage.update_probe_result(ProbeResult(node_name="node-std-01", success=False, error="timeout"))

        fallback_lease = router.acquire_for_account(
            account_id="bot_fallback_002",
            region="hongkong",
            egress_type=None,
        )
        assert fallback_lease is not None
        assert fallback_lease.node_name == "node-prem-01"
        assert fallback_lease.tier == Tier.PREMIUM.value
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_router_explicit_premium_and_strict_isolation():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)
        router = Router(storage=storage)

        prem_node = ProxyNode(
            name="node-prem-01",
            host="10.144.144.1",
            port=1080,
            region="tokyo",
            egress_type=EgressType.NATIVE_IPV4.value,
            tier=Tier.PREMIUM.value,
            status="healthy",
            quality_score=98,
        )
        std_node = ProxyNode(
            name="node-std-01",
            host="10.144.144.2",
            port=1080,
            region="tokyo",
            egress_type=EgressType.WARP_IPV6.value,
            tier=Tier.STANDARD.value,
            status="healthy",
            quality_score=50,
        )
        storage.upsert_node(prem_node)
        storage.upsert_node(std_node)

        # 1. Explicit premium request
        lease_prem = router.acquire_for_account(
            account_id="high_value_bot_01",
            region="tokyo",
            tier=Tier.PREMIUM.value,
        )
        assert lease_prem is not None
        assert lease_prem.node_name == "node-prem-01"
        assert lease_prem.tier == Tier.PREMIUM.value

        # 2. Strict premium isolation (premium_only) when premium node dies
        storage.update_probe_result(ProbeResult(node_name="node-prem-01", success=False, error="timeout"))
        storage.update_probe_result(ProbeResult(node_name="node-prem-01", success=False, error="timeout"))

        # With premium_first: can fall back to standard
        lease_fallback = router.acquire_for_account(
            account_id="high_value_bot_02",
            region="tokyo",
            tier=Tier.PREMIUM.value,
            tier_policy=TierPolicy.PREMIUM_FIRST.value,
        )
        assert lease_fallback is not None
        assert lease_fallback.node_name == "node-std-01"

        # With premium_only: strictly isolated, never falls back to standard
        lease_strict = router.acquire_for_account(
            account_id="high_value_bot_03",
            region="tokyo",
            tier=Tier.PREMIUM.value,
            tier_policy=TierPolicy.PREMIUM_ONLY.value,
        )
        assert lease_strict is None
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_registration_anti_sybil_defaults_to_premium():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = Storage(db_path=db_path)
        router = Router(storage=storage)

        prem_node = ProxyNode(
            name="node-reg-prem",
            host="10.144.144.10",
            port=1080,
            region="singapore",
            egress_type=EgressType.NATIVE_IPV4.value,
            tier=Tier.PREMIUM.value,
            status="healthy",
            exit_ip="1.1.1.1",
            quality_score=95,
        )
        std_node = ProxyNode(
            name="node-reg-std",
            host="10.144.144.20",
            port=1080,
            region="singapore",
            egress_type=EgressType.WARP_IPV6.value,
            tier=Tier.STANDARD.value,
            status="healthy",
            exit_ip="2.2.2.2",
            quality_score=50,
        )
        storage.upsert_node(prem_node)
        storage.upsert_node(std_node)

        reg_lease = router.acquire_for_registration(
            account_id="signup_user_001",
            target_service="openai_signup",
            region="singapore",
        )
        assert reg_lease is not None
        assert reg_lease.node_name == "node-reg-prem"
        assert reg_lease.tier == Tier.PREMIUM.value
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_gateway_credential_and_header_parsing():
    # 1. Double-underscore format
    creds1 = parse_proxy_credentials("bot_101__tier_premium__region_tokyo")
    assert creds1.account_id == "bot_101"
    assert creds1.region == "tokyo"
    assert creds1.tier == "premium"
    # Backward compatible unpacking into 4 elements
    user, reg, egress, svc = creds1
    assert user == "bot_101"
    assert reg == "tokyo"

    # 2. Query string format
    creds2 = parse_proxy_credentials("worker_02?tier=standard&tier_policy=standard_only")
    assert creds2.account_id == "worker_02"
    assert creds2.tier == "standard"
    assert creds2.tier_policy == "standard_only"

    # 3. Gateway instance header extraction and stripping
    from gateway.server import ForwardProxyGateway
    gateway = ForwardProxyGateway(storage=None, router=None, host="127.0.0.1", port=0)
    headers = {
        "x-account-id": "header_user_01",
        "x-region": "us-west",
        "x-tier": "premium",
        "x-tier-policy": "premium_only",
    }
    ctx = gateway._extract_routing_headers(headers, "127.0.0.1")
    assert ctx.account_id == "header_user_01"
    assert ctx.region == "us-west"
    assert ctx.tier == "premium"
    assert ctx.tier_policy == "premium_only"

    # Verify internal headers are completely in INTERNAL_ROUTING_HEADERS
    from gateway.server import INTERNAL_ROUTING_HEADERS
    assert "x-tier" in INTERNAL_ROUTING_HEADERS
    assert "x-tier-policy" in INTERNAL_ROUTING_HEADERS
    assert "x-account-id" in INTERNAL_ROUTING_HEADERS
