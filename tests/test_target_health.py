"""
tests/test_target_health.py - 目标级健康兜底 (2026-11-14 宝可梦机场故障)

覆盖三件事:
1. TargetHealthTracker: 连续失败判坏 / 一次成功即恢复 / TTL 过期视为未知(不排除)
2. classify_target: 域名后缀分类, IP 与未知域名无类
3. Router.exclude_transient: 绑定节点被软排除时改道候选节点但不落库 ——
   绑定原地保留; 不开 exclude_transient 时保持既有持久重绑语义
"""

import os
import tempfile
import pytest

from core.models import ProxyNode, EgressType, HealthStatus
from core.storage import Storage
from core.router import Router
from core.target_health import TargetHealthTracker, classify_target


# ---------------------------------------------------------------------------
# 1. Tracker
# ---------------------------------------------------------------------------

def test_tracker_requires_streak_before_bad():
    t = TargetHealthTracker(bad_streak=2, bad_ttl_seconds=300)
    t.record("pigeonw-01", "google", False, "timeout")
    assert not t.is_bad("pigeonw-01", "google"), "单次失败不得判坏 (防抖动假阳性)"
    t.record("pigeonw-01", "google", False, "timeout")
    assert t.is_bad("pigeonw-01", "google")
    assert t.bad_nodes("google") == {"pigeonw-01"}
    assert t.bad_nodes("openai") == set(), "按目标类隔离"


def test_tracker_single_success_clears():
    t = TargetHealthTracker(bad_streak=2, bad_ttl_seconds=300)
    t.record("n1", "google", False)
    t.record("n1", "google", False)
    assert t.is_bad("n1", "google")
    t.record("n1", "google", True)
    assert not t.is_bad("n1", "google"), "一次成功立即恢复"
    assert t.bad_nodes("google") == set()


def test_tracker_bad_entry_expires():
    t = TargetHealthTracker(bad_streak=2, bad_ttl_seconds=300)
    t.record("n1", "google", False)
    t.record("n1", "google", False)
    # TTL 过后 (探活循环若崩溃) 排除不得永久化
    later = __import__("time").time() + 301
    assert not t.is_bad("n1", "google", now=later)
    assert t.bad_nodes("google", now=later) == set()


# ---------------------------------------------------------------------------
# 2. classify_target
# ---------------------------------------------------------------------------

def test_classify_target_domains():
    assert classify_target("www.googleapis.com") == "google"
    assert classify_target("generativelanguage.googleapis.com") == "google"
    assert classify_target("google.com") == "google"
    assert classify_target("aigateway.googleapis.com") == "google"
    assert classify_target("api.openai.com") == "openai"
    assert classify_target("chatgpt.com") == "openai"
    assert classify_target("api.anthropic.com") == "claude"
    assert classify_target("claude.ai") == "claude"
    # 前缀陷阱: notgoogle.com 不得命中 google.com
    assert classify_target("notgoogle.com") is None
    assert classify_target("example.com") is None
    # 裸 IP / IPv6 / 空
    assert classify_target("23.132.180.188") is None
    assert classify_target("2a09:bac5::1") is None
    assert classify_target("") is None


# ---------------------------------------------------------------------------
# 3. Router.exclude_transient
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_router():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    router = Router(storage)
    for n in [
        ProxyNode(name="pigeonw-01", host="127.0.0.1", port=12053, region="us-west",
                  egress_type=EgressType.WARP_IPV6.value, status=HealthStatus.HEALTHY.value),
        ProxyNode(name="warp-alt-01", host="10.144.144.3", port=10881, region="tokyo",
                  egress_type=EgressType.WARP_IPV6.value, status=HealthStatus.HEALTHY.value),
        ProxyNode(name="warp-alt-02", host="10.144.144.3", port=10882, region="tokyo",
                  egress_type=EgressType.WARP_IPV6.value, status=HealthStatus.HEALTHY.value),
    ]:
        storage.upsert_node(n)
    yield router, storage
    if os.path.exists(db_path):
        os.remove(db_path)


def _bind(router, storage, account, node):
    # 确定性 CRC32 哈希下 force_rebind 也会稳定落在同一备选节点上, 直接经
    # storage 预置绑定才能确定性地把账号钉到指定节点。
    from datetime import datetime
    storage.batch_save_allocations([("default", account, node, datetime.now().isoformat())])
    lease = router.acquire_for_account(account)
    assert lease.node_name == node, f"预置绑定失败: {lease.node_name}"
    return lease


def test_exclude_transient_steers_without_persisting(temp_router):
    router, storage = temp_router
    _bind(router, storage, "acc_google", "pigeonw-01")

    lease = router.acquire_for_account(
        "acc_google", exclude_nodes={"pigeonw-01"}, exclude_transient=True
    )
    assert lease is not None
    assert lease.node_name in ("warp-alt-01", "warp-alt-02"), "改道到了候选节点"

    # 关键: 绑定原地保留 —— 本次改道不落库
    alloc = storage.find_allocation_by_account("acc_google")
    assert alloc["node_name"] == "pigeonw-01"

    # 下一请求 (无排除) 自动回到绑定节点
    lease2 = router.acquire_for_account("acc_google")
    assert lease2.node_name == "pigeonw-01"


def test_exclude_transient_degrades_to_bound_node_when_alone(temp_router):
    router, storage = temp_router
    _bind(router, storage, "acc_solo", "pigeonw-01")
    # 把候选全部干掉: 剩余两个节点标 DEAD
    for n in ("warp-alt-01", "warp-alt-02"):
        storage.update_node_status(n, HealthStatus.DEAD.value)

    lease = router.acquire_for_account(
        "acc_solo", exclude_nodes={"pigeonw-01"}, exclude_transient=True
    )
    # 候选被排除空 -> 降级回绑定节点 (宁走坏出口, 不 None/503)
    assert lease is not None
    assert lease.node_name == "pigeonw-01"


def test_without_transient_flag_persists_rebind(temp_router):
    router, storage = temp_router
    _bind(router, storage, "acc_legacy", "pigeonw-01")

    lease = router.acquire_for_account("acc_legacy", exclude_nodes={"pigeonw-01"})
    assert lease is not None
    assert lease.node_name in ("warp-alt-01", "warp-alt-02")

    # 既有语义保持: 无 exclude_transient 的排除重试走 R2 持久重绑
    alloc = storage.find_allocation_by_account("acc_legacy")
    assert alloc["node_name"] == lease.node_name


def test_degrade_respects_call_constraints(temp_router):
    """降级不得违约: region 钉定的调用, 即使候选被排除空, 也绝不跨区返回
    绑定节点 —— 宁可 None/503 (与 binding_conflict 路径同语义)。"""
    router, storage = temp_router
    _bind(router, storage, "acc_pinned", "pigeonw-01")  # us-west 绑定
    for n in ("warp-alt-01", "warp-alt-02"):
        storage.update_node_status(n, HealthStatus.DEAD.value)

    lease = router.acquire_for_account(
        "acc_pinned",
        region="tokyo",  # 调用钉定 tokyo; pigeonw 是 us-west
        exclude_nodes={"pigeonw-01"},
        exclude_transient=True,
    )
    assert lease is None, "跨区降级违约被拒绝"

    # 对照: 无 region 钉定时同样的局面正常降级到绑定节点
    lease2 = router.acquire_for_account(
        "acc_pinned", exclude_nodes={"pigeonw-01"}, exclude_transient=True
    )
    assert lease2 is not None and lease2.node_name == "pigeonw-01"


# ---------------------------------------------------------------------------
# 4. 滑窗判定 (治振颤偏坏) — review P3 修复
# ---------------------------------------------------------------------------

def test_tracker_pure_oscillation_never_bad():
    """f/o/f/o 50% 振颤: 连败路不触发, 满窗失败率 50% < 75% —— 不判坏。"""
    t = TargetHealthTracker(bad_streak=2, bad_ttl_seconds=300)
    for ok in (False, True, False, True):
        t.record("n1", "google", ok)
    assert not t.is_bad("n1", "google")


def test_tracker_rate_path_catches_flaky_bad():
    """f/f/o/f: 连败路失效 (尾对不是双双失败), 滑窗 75% 失败 -> 判坏。"""
    t = TargetHealthTracker(bad_streak=2, bad_ttl_seconds=300)
    for ok in (False, False, True, False):
        t.record("n1", "google", ok)
    assert t.is_bad("n1", "google"), "滑窗路应捕获振颤偏坏"


def test_tracker_latest_success_always_recovers():
    """恢复永远优先于惩罚: 满窗 3 败但最新一次成功 -> 不坏。"""
    t = TargetHealthTracker(bad_streak=2, bad_ttl_seconds=300)
    for ok in (False, False, False, True):
        t.record("n1", "google", ok)
    assert not t.is_bad("n1", "google")


# ---------------------------------------------------------------------------
# 5. review P3 修复: legacy lease 分支 steer 同语义 + steered 旗标
# ---------------------------------------------------------------------------

def test_exclude_transient_legacy_lease_steers_without_persist(temp_router):
    router, storage = temp_router
    # 仅 lease 镜像, 无 universal allocation -> 走 1b 分支
    storage.set_lease("acc_legacy", "pigeonw-01")
    assert storage.find_allocation_by_account("acc_legacy") is None

    lease = router.acquire_for_account(
        "acc_legacy", exclude_nodes={"pigeonw-01"}, exclude_transient=True
    )
    assert lease is not None
    assert lease.node_name in ("warp-alt-01", "warp-alt-02")
    assert lease.steered is True, "steer lease 必须带旗标供网关计量"
    assert storage.get_lease("acc_legacy").node_name == "pigeonw-01", "lease 行不动"


def test_steer_flag_on_universal_path(temp_router):
    router, storage = temp_router
    _bind(router, storage, "acc_flag", "pigeonw-01")
    lease = router.acquire_for_account(
        "acc_flag", exclude_nodes={"pigeonw-01"}, exclude_transient=True
    )
    assert lease.steered is True
    # 非 steer 路径旗标保持 False (sticky / R2 重绑)
    lease2 = router.acquire_for_account("acc_flag")
    assert lease2.steered is False


def test_gateway_counts_and_renders_steers():
    """网关 steer 计量: /metrics 渲染 + relay summary 携带 (review P3 修复)。"""
    import tempfile
    from gateway.server import ForwardProxyGateway

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        storage = Storage(db_path=f.name)
    try:
        gateway = ForwardProxyGateway(storage, Router(storage), host="127.0.0.1", port=0)
        gateway.target_steers["google"] = 3
        gateway.target_steers["openai"] = 1
        body = gateway.render_metrics().decode()
        assert 'tetherpool_gateway_target_steers_total{class="google"} 3' in body
        assert 'tetherpool_gateway_target_steers_total{class="openai"} 1' in body
        summary = gateway.relay_summary()
        assert summary["target_steers"] == {"google": 3, "openai": 1}
    finally:
        os.remove(f.name)


# ---------------------------------------------------------------------------
# 6. 数据流优化 (2026-11-14): 连接复用 / hub 缓存 / lease 拨号字段 / metrics 缓冲
# ---------------------------------------------------------------------------

def test_storage_connection_reuse_semantics():
    """复用连接后每次操作仍具独立事务语义: 成功 commit、异常 rollback。"""
    import tempfile
    from datetime import datetime as _dt
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        storage = Storage(db_path=f.name)
    try:
        node = ProxyNode(name="reuse-01", host="10.0.0.1", port=1, region="tokyo",
                         egress_type=EgressType.WARP_IPV6.value)
        storage.upsert_node(node)
        assert storage.get_node("reuse-01") is not None
        # 异常路径: 事务中途抛错 -> rollback -> 半截写入不存在, 连接仍可用
        with pytest.raises(RuntimeError):
            with storage._get_conn() as conn:
                conn.execute(
                    "INSERT INTO nodes (name, host, port, region, egress_type, status) "
                    "VALUES ('half-txn', '10.9.9.9', 9, 'tokyo', 'warp_ipv6', 'healthy')"
                )
                raise RuntimeError("boom")
        assert storage.get_node("half-txn") is None, "rollback 生效"
        # 同线程第二次操作复用同一连接
        storage.upsert_node(ProxyNode(name="reuse-02", host="10.0.0.2", port=2,
                                      region="tokyo", egress_type=EgressType.WARP_IPV6.value))
        assert storage.get_node("reuse-02") is not None
        # 写入确实落盘 (commit 生效)
        assert storage.get_node("reuse-01").name == "reuse-01"
        _ = _dt.now()
    finally:
        os.remove(f.name)


def test_hub_cache_serves_within_ttl(temp_router):
    """hub 候选缓存: TTL 内不再重复扫库, 节点变化在下个 TTL 窗口生效。"""
    import time
    router, storage = temp_router
    # 造一个 healthy clash hub 节点
    storage.upsert_node(ProxyNode(name="hub-hk-01", host="127.0.0.1", port=9,
                                  region="hongkong", egress_type=EgressType.CLASH_PROXY.value,
                                  status=HealthStatus.HEALTHY.value))
    node = ProxyNode(name="warp-fallback-01", host="10.0.0.3", port=3, region="tokyo",
                     egress_type=EgressType.WARP_IPV6.value,
                     status=HealthStatus.HEALTHY.value)
    storage.upsert_node(node)
    router._hub_cache_ttl = 10

    hop1 = router.resolve_relay_hop(node)
    assert hop1 is not None and hop1.name == "hub-hk-01"
    # 缓存窗口内节点死亡, resolve 仍返回旧 hop (10s 新鲜度对兜底路径够用)
    storage.update_node_status("hub-hk-01", HealthStatus.DEAD.value)
    hop2 = router.resolve_relay_hop(node)
    assert hop2 is not None and hop2.name == "hub-hk-01"
    # TTL 过期后重新扫描, 死节点出局
    router._hub_cache_ts = 0.0
    assert router.resolve_relay_hop(node) is None


def test_lease_carries_dial_fields(temp_router):
    """lease 携带拨号四元组: 网关可免 get_node; steered/degrade 路径同样携带。"""
    router, storage = temp_router
    _bind(router, storage, "acc_dial", "pigeonw-01")
    lease = router.acquire_for_account("acc_dial")
    assert lease.host == "127.0.0.1" and lease.port == 12053
    assert lease.relay_mode is not None and lease.name == "pigeonw-01"
    # steer 路径同样携带
    sleave = router.acquire_for_account(
        "acc_dial", exclude_nodes={"pigeonw-01"}, exclude_transient=True
    )
    assert sleave.host is not None and sleave.steered is True


def test_gateway_metrics_buffered_and_flushed():
    """_record_attempts 只进内存缓冲; flush 批量落库; 失败回灌。"""
    import tempfile
    from gateway.server import ForwardProxyGateway
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        storage = Storage(db_path=f.name)
    try:
        gw = ForwardProxyGateway(storage, Router(storage), host="127.0.0.1", port=0)
        gw._record_attempts(2)
        assert gw._metrics_buffer, "请求路径只入缓冲"
        # 落库前 DB 里没有直方图计数
        names = [r["name"] for r in storage.get_runtime_metrics()]
        assert not any("attempts_per_request" in n for n in names)
        import asyncio
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(gw._flush_metrics())
        names = [r["name"] for r in storage.get_runtime_metrics()]
        assert any("attempts_per_request" in n for n in names), "flush 后落库"
        assert gw._metrics_buffer == []
    finally:
        os.remove(f.name)


def test_tracker_prunes_deleted_nodes():
    """内存卫生: 已删节点的坏条目被 prune 清掉, 活跃节点条目保留。"""
    t = TargetHealthTracker(bad_streak=2, bad_ttl_seconds=300)
    t.record("dead-node", "google", False)
    t.record("dead-node", "google", False)
    t.record("alive-node", "google", False)
    t.record("alive-node", "google", False)
    assert t.bad_nodes("google") == {"dead-node", "alive-node"}
    removed = t.prune({"alive-node"})
    assert removed == 1
    assert t.bad_nodes("google") == {"alive-node"}
