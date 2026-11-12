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

def test_force_rebind_without_exclude_moves_off_the_current_node(temp_router):
    """A bare __rebind_true (no exclude_node) must not re-land on the same
    sticky node: the deterministic CRC32 hash below would otherwise re-select
    the very node the caller is trying to leave (observed 2026-11-05 with the
    opencode2api per-key egress integration)."""
    router, storage = temp_router

    first = router.acquire_for_account("acc_rebind_user", region="tokyo",
                                       egress_type=EgressType.WARP_IPV6.value)
    assert first is not None
    second = router.acquire_for_account("acc_rebind_user", region="tokyo",
                                        egress_type=EgressType.WARP_IPV6.value,
                                        force_rebind=True)
    assert second is not None
    assert second.node_name != first.node_name
    # The rebind persists the new binding
    alloc = storage.find_allocation_by_account("acc_rebind_user", service="default")
    assert alloc["node_name"] == second.node_name


# ---------------------------------------------------------------- weighted LB

def test_weighted_pick_deterministic_and_covers_pool(temp_router):
    """Same key + same candidate set => same node; distinct keys spread over
    every candidate (no dead zones) when weights are uniform."""
    router, _ = temp_router
    from core.storage import Storage  # noqa: F401  (import guard for runners)
    nodes = router.storage.list_nodes(status=HealthStatus.HEALTHY.value)
    nodes.sort(key=lambda x: x.name)

    picked = {router._weighted_deterministic_pick(nodes, f"acc_{i}").name
              for i in range(2000)}
    assert picked == {n.name for n in nodes}, "uniform weights must cover the whole pool"

    for i in range(0, 2000, 97):
        assert (router._weighted_deterministic_pick(nodes, f"acc_{i}").name
                == router._weighted_deterministic_pick(nodes, f"acc_{i}").name)


def test_weighted_pick_share_follows_weight():
    """A node with weight 300 vs three nodes at 100 owns ~3/6 of the hash space."""
    from core.router import Router as R
    mk = lambda name, w: ProxyNode(name=name, host="127.0.0.1", port=1,
                                   region="us-west", egress_type="clash_proxy",
                                   weight=w)
    nodes = [mk("a", 300), mk("b", 100), mk("c", 100), mk("d", 100)]
    nodes.sort(key=lambda x: x.name)
    counts = {"a": 0, "b": 0, "c": 0, "d": 0}
    n = 60000
    for i in range(n):
        counts[R._weighted_deterministic_pick(nodes, f"u{i}").name] += 1
    assert abs(counts["a"] / n - 0.5) < 0.02, counts
    for k in ("b", "c", "d"):
        assert abs(counts[k] / n - 1 / 6) < 0.01, (k, counts)


def test_weighted_pick_clamps_bad_weights():
    """weight<=0 (手编 nodes.yaml 可能写出) 不得崩溃或把节点清出池."""
    from core.router import Router as R
    mk = lambda name, w: ProxyNode(name=name, host="127.0.0.1", port=1,
                                   region="us-west", egress_type="clash_proxy",
                                   weight=w)
    nodes = sorted([mk("a", 0), mk("b", -5), mk("c", 100)], key=lambda x: x.name)
    seen = {R._weighted_deterministic_pick(nodes, f"u{i}").name for i in range(3000)}
    assert seen == {"a", "b", "c"}


def test_relay_hop_early_return_for_direct_only(temp_router, monkeypatch):
    """direct_only 节点不得触发 hub 扫描: resolve_relay_hop 必须零查询返回 None
    (热路径上 129 个家宽桥每个连接省 4 次 storage 查询)。"""
    router, storage = temp_router
    calls = []
    orig = storage.list_nodes
    monkeypatch.setattr(storage, "list_nodes",
                        lambda *a, **kw: calls.append(1) or orig(*a, **kw))

    node = storage.get_node("tokyo-warp-01")
    node.relay_mode = "direct_only"
    assert router.resolve_relay_hop(node) is None
    assert calls == [], "direct_only must not scan relay hubs"

    node.relay_mode = "relay_fallback"
    node.relay_hop_node = None
    router.resolve_relay_hop(node)  # 默认行为不变 (可能命中 hub 兜底)
    assert len(calls) >= 1


def test_untyped_requests_never_pick_residential(temp_router):
    """无 egress_type 的请求 (匿名/未配置消费方) 不得落在 metered 家宽上 —
    residential 是显式 opt-in 资源 (type=clash_proxy), 成本保护 2026-11-12。"""
    router, storage = temp_router
    resi = ProxyNode(name="us-resi-01", host="127.0.0.1", port=11101,
                     region="us-west", egress_type="clash_proxy",
                     tags=["clash", "bridge", "residential"])
    warp = ProxyNode(name="us-warp-01", host="10.0.0.1", port=10881,
                     region="us-west", egress_type=EgressType.WARP_IPV6.value)
    for n in (resi, warp):
        n.status = HealthStatus.HEALTHY.value
        storage.upsert_node(n)

    for i in range(200):
        lease = router.acquire_for_account(f"anon_{i}")  # 无 region/type
        assert lease.node_name != "us-resi-01", "untyped request landed on residential"

    typed = router.acquire_for_account("typed_consumer", region="us-west",
                                       egress_type="clash_proxy")
    assert typed.node_name == "us-resi-01", "typed clash_proxy must still reach residential"


def test_explicit_fault_domain_wins_over_host(temp_router):
    """显式 fault_domain 覆盖 host 派生: 桥核域崩溃不得连带 Verge 域 (根修②)。"""
    core_node = ProxyNode(name="teleapi-a", host="127.0.0.1", port=11101,
                          region="us-west", egress_type="clash_proxy",
                          fault_domain="mac-bridge-core")
    verge_node = ProxyNode(name="pigeonw-a", host="127.0.0.1", port=11053,
                           region="us-west", egress_type="clash_proxy")
    for n in (core_node, verge_node):
        storage_nodes = temp_router[1]
        n.status = HealthStatus.HEALTHY.value
        storage_nodes.upsert_node(n)
    assert core_node.effective_fault_domain == "mac-bridge-core"
    assert verge_node.effective_fault_domain == "127.0.0.1"


def test_weighted_pick_raises_on_empty():
    """空候选列表必须显式报错, 而不是ZeroDivisionError (防御未来调用点)."""
    import pytest as _pytest
    from core.router import Router as R
    with _pytest.raises(ValueError):
        R._weighted_deterministic_pick([], "acc")


def test_weighted_acquisition_skews_share_e2e(temp_router):
    """端到端: 经 acquire_for_account 的非均匀权重必须反映份额 (300:100:100:100 ≈ 50/16.7)."""
    router, storage = temp_router
    mk = lambda name, w: ProxyNode(name=name, host="10.0.0.1", port=10881,
                                   region="us-west",
                                   egress_type=EgressType.WARP_IPV6.value, weight=w)
    for n in (mk("us-a-heavy", 300), mk("us-b", 100), mk("us-c", 100), mk("us-d", 100)):
        n.status = HealthStatus.HEALTHY.value
        storage.upsert_node(n)

    counts = {}
    n = 4000
    for i in range(n):
        lease = router.acquire_for_account(f"wt_user_{i}", region="us-west",
                                           egress_type=EgressType.WARP_IPV6.value)
        counts[lease.node_name] = counts.get(lease.node_name, 0) + 1
    assert abs(counts["us-a-heavy"] / n - 0.5) < 0.03, counts
    for k in ("us-b", "us-c", "us-d"):
        assert abs(counts[k] / n - 1 / 6) < 0.02, (k, counts)
    # 粘性: 同一账号重复获取必须命中同一节点
    a1 = router.acquire_for_account("wt_sticky", region="us-west",
                                    egress_type=EgressType.WARP_IPV6.value)
    a2 = router.acquire_for_account("wt_sticky", region="us-west",
                                    egress_type=EgressType.WARP_IPV6.value)
    assert a1.node_name == a2.node_name
