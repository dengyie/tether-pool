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
