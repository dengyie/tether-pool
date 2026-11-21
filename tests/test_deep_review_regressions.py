"""Regression coverage for the deep review findings."""

import api.server
from starlette.testclient import TestClient
from core.models import HealthStatus, ProbeResult, ProxyNode
from core.quality import IPQualityEvaluator, QualityTier
from core.storage import Storage


def _node(storage, name):
    storage.upsert_node(ProxyNode(
        name=name, host="127.0.0.1", port=1080, region="test",
        egress_type="warp_ipv6", status=HealthStatus.HEALTHY.value,
    ))


def test_incremental_export_returns_tombstones_and_deduped_nodes(storage):
    _node(storage, "export-node")
    storage.batch_save_allocations([
        ("grok2api", "export-a", "export-node", "2026-11-20T00:00:00"),
        ("grok2api", "export-b", "export-node", "2026-11-20T00:00:00"),
    ])
    start = storage.get_binding_epoch("grok2api")["epoch"]
    storage.delete_allocation("grok2api", "export-a")

    result = api.server.export_grok2api(since_epoch=start)

    assert result["deletions"] == ["export-a"]
    assert set(result["assignments"]) == set()
    assert [node["name"] for node in result["nodes"]] == []
    assert result["epoch"] == storage.get_binding_epoch("grok2api")["epoch"]
    assert result["has_more"] is False


def test_incremental_export_cap_does_not_advance_past_unreturned_changes(storage, monkeypatch):
    _node(storage, "cap-node")
    records = [
        ("grok2api", f"cap-{i:05d}", "cap-node", "2026-11-20T00:00:00")
        for i in range(20002)
    ]
    storage.batch_save_allocations(records)
    monkeypatch.setattr(api.server, "_EXPORT_CHANGES_CAP", 20000, raising=False)

    result = api.server.export_grok2api(since_epoch=0)

    assert result["truncated"] is True
    assert result["has_more"] is True
    assert result["epoch"] < storage.get_binding_epoch("grok2api")["epoch"]
    assert result["epoch"] == 40


def test_service_token_cannot_authorize_admin_write(monkeypatch):
    monkeypatch.setattr(api.server, "API_TOKEN", "")
    monkeypatch.setattr(api.server, "SERVICE_TOKENS", {"zcode-hub": "tok-zh-123"})
    response = TestClient(api.server.app).post(
        "/v1/config/reload?service=zcode-hub",
        headers={"Authorization": "Bearer tok-zh-123"},
    )
    assert response.status_code == 401


def test_allocator_exclusive_mutex_skips_occupied_nodes(storage):
    """一号一节点硬互斥 (exclusive 2026-11-21, 生产移植回填): exclusive=true
    的 acquire 不共占 —— 已被本 service 其它账号占用的节点剔除, 占满即 503
    (zcode-hub 风控红线: 同一出口 IP 多账号 = 405 封 IP)。"""
    import pytest as _pytest
    from core.allocator import AllocationEngine

    _node(storage, "ex-node-1")
    _node(storage, "ex-node-2")
    _node(storage, "ex-node-3")
    engine = AllocationEngine(storage)
    # exclusive 是 opt-in (与生产 API 默认 False 一致, zcode-hub 显式传参)
    engine.acquire_single(service="zcode-hub", account_id="acc-a",
                          egress_preference=None, exclusive=True)
    second = engine.acquire_single(service="zcode-hub", account_id="acc-b",
                                   egress_preference=None, exclusive=True)
    third = engine.acquire_single(service="zcode-hub", account_id="acc-c",
                                  egress_preference=None, exclusive=True)
    # 三个账号三台节点, 零共占
    occupied = {a["node_name"] for a in storage.list_allocations_by_service("zcode-hub")}
    assert len(occupied) == 3
    # 占满 -> RuntimeError 且短语带 exclusive exhausted (hub 侧据此区分排障)
    with _pytest.raises(RuntimeError, match="exclusive exhausted"):
        engine.acquire_single(service="zcode-hub", account_id="acc-d",
                              egress_preference=None, exclusive=True)


def test_router_service_exclusive_blocks_sharing(storage):
    """数据面同语义 (SERVICE_EXCLUSIVE_SERVICES): gateway/auto_rehome 路径
    的 acquire_for_account 对 exclusive service 不返回已被本 service 其它
    账号占用的候选; 全部占用时宁 blocked 不共占。"""
    from core.router import Router, SERVICE_EXCLUSIVE_SERVICES
    assert "zcode-hub" in SERVICE_EXCLUSIVE_SERVICES

    _node(storage, "svc-occ-1")
    _node(storage, "svc-free")
    storage.batch_save_allocations([
        ("zcode-hub", "other-acc", "svc-occ-1", "2026-11-21T00:00:00"),
    ])
    router = Router(storage)
    lease = router.acquire_for_account(
        account_id="new-acc", service="zcode-hub",
        egress_type=None, bind_persist=True,
    )
    assert lease is not None
    assert lease.node_name == "svc-free"  # 绝不落 svc-occ-1


def test_target_probe_rejects_unallowlisted_url(storage):
    _node(storage, "probe-node")
    response = TestClient(api.server.app).post("/v1/probe/target", params={
        "node_name": "probe-node",
        "target_url": "http://127.0.0.1/secret",
    })
    assert response.status_code == 422


def test_storage_rejects_allocation_to_unknown_node(storage):
    import pytest
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError):
        storage.batch_save_allocations([
            ("test", "dangling", "missing-node", "2026-11-20T00:00:00")
        ])


def test_leases_are_default_namespace_mirror_only(storage):
    _node(storage, "svc-node")
    _node(storage, "default-node")
    storage.batch_save_allocations([
        ("service-a", "shared-account", "svc-node", "2026-11-20T00:00:00"),
    ])
    assert storage.get_lease("shared-account") is None

    storage.batch_save_allocations([
        ("default", "shared-account", "default-node", "2026-11-20T00:00:01"),
    ])
    assert storage.get_lease("shared-account").node_name == "default-node"


def test_batch_duplicate_across_chunks_logs_actual_previous_node(storage):
    _node(storage, "duplicate-a")
    _node(storage, "duplicate-b")
    _node(storage, "duplicate-c")
    storage.batch_save_allocations([
        ("svc", "repeated", "duplicate-a", "2026-11-20T00:00:00"),
    ])
    records = [
        ("svc", f"filler-{i:04d}", "duplicate-a", "2026-11-20T00:00:01")
        for i in range(499)
    ]
    records.extend([
        ("svc", "repeated", "duplicate-b", "2026-11-20T00:00:02"),
        ("svc", "repeated", "duplicate-c", "2026-11-20T00:00:03"),
    ])
    storage.batch_save_allocations(records)

    changes = [c for c in storage.list_binding_changes_since("svc", 0)
               if c.account_id == "repeated"]
    assert storage.get_allocation("svc", "repeated")["node_name"] == "duplicate-c"
    assert len(changes) == 2
    assert changes[-1].from_node == "duplicate-a"
    assert changes[-1].to_node == "duplicate-c"


def test_dynamic_premium_allows_sub_5s_latency():
    """动态分级门槛: 延迟上限放宽到 5s 内 (跨国住宅/机房探测 RTT),
    score>=75 且 healthy 即可动态晋级 premium, 不再卡 <350ms。"""
    # 5s 边界内: 动态晋级 (native + exit_ip + healthy)
    score, tier, _ = IPQualityEvaluator.evaluate_node(
        tags=["self_hosted", "native"],
        egress_type="native_ipv4",
        latency_ms=4999.0,
        status="healthy",
        exit_ip="13.230.10.20",
    )
    assert score >= 75
    assert tier == QualityTier.PREMIUM

    # 5s 边界外: 仍为 standard
    _, tier_slow, _ = IPQualityEvaluator.evaluate_node(
        tags=["self_hosted", "native"],
        egress_type="native_ipv4",
        latency_ms=5001.0,
        status="healthy",
        exit_ip="13.230.10.20",
    )
    assert tier_slow == QualityTier.STANDARD

    # 350ms~5s 区间 (旧版会被卡): dynamic 晋级
    _, tier_mid, _ = IPQualityEvaluator.evaluate_node(
        tags=["self_hosted", "native"],
        egress_type="native_ipv4",
        latency_ms=2000.0,
        status="healthy",
        exit_ip="13.230.10.20",
    )
    assert tier_mid == QualityTier.PREMIUM


def test_dynamic_premium_requires_score_and_alive():
    """放宽延迟不放宽质量: score<75 或 dead 不得动态晋级。"""
    # 高分不足 (third_party provenance 15 + lat 10 + stability 20 + exit 10 = 55)
    _, tier_low, _ = IPQualityEvaluator.evaluate_node(
        tags=["clash_sub"],
        egress_type="clash_proxy",
        latency_ms=1000.0,
        status="healthy",
        exit_ip="1.2.3.4",
    )
    assert tier_low == QualityTier.STANDARD

    # dead 原生机永不动态晋级
    _, tier_dead, _ = IPQualityEvaluator.evaluate_node(
        tags=["self_hosted", "native"],
        egress_type="native_ipv4",
        latency_ms=100.0,
        status="dead",
        exit_ip="13.230.10.20",
    )
    assert tier_dead == QualityTier.STANDARD


def test_probe_recompute_uses_relaxed_premium_gate(storage):
    """探活重算链路 (store_nodes.update_probe_result) 使用放宽后的门槛:
    原生机 5s 内动态晋级 premium; warp 节点分数不足保持 standard。"""
    storage.upsert_node(ProxyNode(
        name="relax-native", host="10.144.144.30", port=1080, region="test",
        egress_type="native_ipv4", tags=["self_hosted", "native"],
        status=HealthStatus.HEALTHY.value,
    ))
    res = ProbeResult(node_name="relax-native", success=True,
                      latency_ms=2800.0, exit_ip="203.0.113.9")
    storage.update_probe_result(res)
    updated = storage.get_node("relax-native")
    assert updated.latency_ms == 2800.0
    # unprobed baseline 时注册为 standard; 探活后动态重算晋级 (5s 内)
    assert updated.tier == "premium"
    assert updated.quality_score >= 75

    # warp 节点 provenance 低 (25 分), 5s 内也够不到 75 晋级线 -> standard
    _node(storage, "relax-warp")
    storage.update_probe_result(ProbeResult(
        node_name="relax-warp", success=True,
        latency_ms=2800.0, exit_ip="203.0.113.10"))
    warp = storage.get_node("relax-warp")
    assert warp.tier == "standard"


def test_half_open_flap_suppression_escalates_retrip_cooldown():
    """canary 风暴根修 (2026-11-21): half_open 失败重开时, 退避按本轮连续
    失败深度在 cap 之上继续升级 —— 真死透的节点 (上游已宕 7 天) 收敛到小时级
    复核, 不再被 host_dampen 1/4 拉回 13 分钟一轮的必失败复核循环。"""
    from core.circuit import CircuitBreaker
    from datetime import datetime

    node = ProxyNode(
        name="flap-node", host="10.0.0.9", port=1080, region="test",
        egress_type="warp_ipv6", status=HealthStatus.HALF_OPEN.value,
        consecutive_failures=8, fail_count=500,
    )
    # 第 1 次重开: 与旧行为同量级 (cap 内)
    state1, reason1, na1 = CircuitBreaker.evaluate(node, success=False, error="timeout")
    assert state1 == HealthStatus.DEAD.value
    assert "reopened" in reason1
    assert na1 is not None
    from datetime import datetime as _dt
    cd1 = (na1 - _dt.now()).total_seconds() if na1 else 0.0

    # 连续第 31+ 次失败 (本轮反复证伪): 新语义 = 连续失败深度参与升级,
    # 退避显著超过旧 cap 1800s
    flappy = ProxyNode(
        name="flap-node2", host="10.0.0.10", port=1080, region="test",
        egress_type="warp_ipv6", status=HealthStatus.HALF_OPEN.value,
        consecutive_failures=100, fail_count=3000,
    )
    _, _, na_esc = CircuitBreaker.evaluate(flappy, success=False, error="timeout")
    assert na_esc is not None
    cd_esc = (na_esc - _dt.now()).total_seconds()
    # consecutive_failures=100 的 chronic flapper: 退避必须突破 1800s 旧上限
    assert cd_esc > 1800.0

    # 新死节点 (连续失败低): 首次 canary 失败不做激进升级 (保护快速复核)
    fresh = ProxyNode(
        name="fresh-dead", host="10.0.0.11", port=1080, region="test",
        egress_type="warp_ipv6", status=HealthStatus.HALF_OPEN.value,
        consecutive_failures=2, fail_count=3,
    )
    _, _, na_fresh = CircuitBreaker.evaluate(fresh, success=False, error="timeout")
    cd_fresh = (na_fresh - _dt.now()).total_seconds() if na_fresh else 0.0
    assert cd_fresh <= 1800.0 + 10.0  # cap + jitter, 无 flap 升级


def test_flap_anchor_uses_consecutive_not_cumulative_fail_count():
    """2026-11-22 review P2 纠正: 退避锚必须是本轮 consecutive_failures,
    不能是跨成功不清零的累计 fail_count —— 历史上死过 30+ 次后自愈、今天
    新死的节点, 首场 canary 失败不应被历史误判成 chronic。"""
    from core.circuit import CircuitBreaker
    from datetime import datetime as _dt

    recovered_then_redead = ProxyNode(
        name="recovered-redead", host="10.0.0.12", port=1080, region="test",
        egress_type="warp_ipv6", status=HealthStatus.HALF_OPEN.value,
        consecutive_failures=2,   # 本轮才刚开始失败
        fail_count=5000,          # 但历史累计很高
    )
    _, _, na = CircuitBreaker.evaluate(recovered_then_redead, success=False, error="timeout")
    cd = (na - _dt.now()).total_seconds() if na else 0.0
    # 累计值锚会把这节点钉到 >1800s; 连续值锚保持快速复核
    assert cd <= 1800.0 + 10.0


def test_incremental_export_move_then_delete_yields_deletion_only(storage):
    """2026-11-22 review P1: 同账号同页 move→delete (先 rebind 后配额回收)
    必须按末条 delete 定归属 —— first-wins 去重会把 delete 丢掉,
    下游保留幽灵绑定继续打已回收的出口。"""
    _node(storage, "md-a")
    _node(storage, "md-b")
    storage.batch_save_allocations([
        ("grok2api", "md-acc", "md-a", "2026-11-20T00:00:00"),
    ])
    start = storage.get_binding_epoch("grok2api")["epoch"]
    storage.batch_save_allocations([
        ("grok2api", "md-acc", "md-b", "2026-11-20T00:00:01"),
    ])
    storage.delete_allocation("grok2api", "md-acc")

    result = api.server.export_grok2api(since_epoch=start)
    assert "md-acc" in result["deletions"]
    assert "md-acc" not in result["assignments"]


def test_incremental_export_delete_then_rebind_yields_assignment_only(storage):
    """同页 delete→re-bind (配额回收后重新分配) 必须按末条 move 定归属,
    只出现在 assignments 且指向新节点 —— 反过来只认 delete 会让下游
    错误摘掉活账号。"""
    _node(storage, "dr-a")
    _node(storage, "dr-b")
    storage.batch_save_allocations([
        ("grok2api", "dr-acc", "dr-a", "2026-11-20T00:00:00"),
    ])
    start = storage.get_binding_epoch("grok2api")["epoch"]
    storage.delete_allocation("grok2api", "dr-acc")
    storage.batch_save_allocations([
        ("grok2api", "dr-acc", "dr-b", "2026-11-20T00:00:01"),
    ])

    result = api.server.export_grok2api(since_epoch=start)
    assert result["assignments"]["dr-acc"]["name"] == "dr-b"
    assert "dr-acc" not in result["deletions"]


def test_probe_target_allows_allowlisted_path(storage, monkeypatch):
    """白名单校验只拦协议/host/端口 (review P3 确认), 不拦业务 path ——
    allowlisted URL 带路径必须放行进入实际探测, 422 只留给黑名单形态。"""
    from core.target_health import validate_probe_target
    assert validate_probe_target("https://api.openai.com/v1/models") == "api.openai.com"
    assert validate_probe_target("https://chatgpt.com/backend-api/models") == "chatgpt.com"

    async def _stub_probe(node, target_url):
        return ProbeResult(node_name=node.name, success=True, latency_ms=1.0)

    monkeypatch.setattr(api.server.prober, "probe_target_service", _stub_probe)
    _node(storage, "probe-ok")
    response = TestClient(api.server.app).post("/v1/probe/target", params={
        "node_name": "probe-ok",
        "target_url": "https://api.openai.com/v1/models",
    })
    assert response.status_code == 200
    assert response.json()["success"] is True
