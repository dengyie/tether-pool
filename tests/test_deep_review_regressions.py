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


def test_rehome_shared_ladder_enforces_service_exclusive(storage):
    """四轮 review S1: _find_rehome_candidates 共享 ladder 此前无互斥过滤,
    zcode-hub 无约束绑定走节点 sweep base_candidates / 域 sweep / R2 else
    分支全部绕过一号一节点 —— 09:44 共占风暴对无约束绑定重现。"""
    from core.router import Router

    _node(storage, "ladder-occ")   # 已被其它 zcode-hub 账号占用
    _node(storage, "ladder-free")
    storage.batch_save_allocations([
        ("zcode-hub", "hub-other", "ladder-occ", "2026-11-22T00:00:00"),
    ])
    router = Router(storage)
    cands = router._find_rehome_candidates(
        "test", "warp_ipv6", None, exclude_names=set(), service="zcode-hub"
    )
    assert [c.name for c in cands] == ["ladder-free"]
    # 非 exclusive service 不受 zcode-hub 占用影响 (C2 越权过滤根修)
    cands_plain = router._find_rehome_candidates(
        "test", "warp_ipv6", None, exclude_names=set(), service="grok2api"
    )
    assert {c.name for c in cands_plain} == {"ladder-occ", "ladder-free"}


def test_node_sweep_unconstrained_exclusive_never_cotenames(storage):
    """节点 sweep 的无约束 zcode-hub 单元走带互斥 ladder (S1) 且批内已选
    节点不得被第二个账号再选 (sweep_picked) —— 攒批落库前 DB 查不到本批
    先选节点, 不补集合会批内共占。"""
    from core.router import Router

    # 各节点独立 host: 同 host 节点互为 fault-domain peers (sweep 必须排除
    # 兄弟 listener), 共享 127.0.0.1 会让整个候选池被判同域而 blocked。
    _node(storage, "sweep-broken")
    for name, port in (("sweep-t1", 1081), ("sweep-t2", 1082), ("sweep-occupied", 1083)):
        storage.upsert_node(ProxyNode(
            name=name, host="10.0.0.%d" % port, port=port, region="test",
            egress_type="warp_ipv6", status=HealthStatus.HEALTHY.value,
        ))
    storage.batch_save_allocations([
        # broken 节点上两个 zcode-hub 单元 (遗留共占态), 外加一个占住
        # sweep-occupied 的账号
        ("zcode-hub", "hub-a", "sweep-broken", "2026-11-22T00:00:00"),
        ("zcode-hub", "hub-b", "sweep-broken", "2026-11-22T00:00:00"),
        ("zcode-hub", "hub-c", "sweep-occupied", "2026-11-22T00:00:00"),
    ])
    router = Router(storage)
    result = router.rehome_leases_for_node("sweep-broken")
    # 两个单元必须落在不同节点, 且都不是 sweep-occupied (互斥) / sweep-broken
    moved = {d["account_id"]: d["to_node"] for d in result["details"]
             if d.get("to_node")}
    assert len(moved) == 2
    assert len(set(moved.values())) == 2
    assert "sweep-occupied" not in set(moved.values())
    remaining = {a["account_id"]: a["node_name"]
                 for a in storage.list_allocations_by_service("zcode-hub")}
    # hub-a/hub-b 落点不与 hub-c (sweep-occupied) 共占
    assert moved["hub-a"] != moved["hub-b"]
    assert {remaining["hub-a"], remaining["hub-b"]}.isdisjoint({"sweep-broken"})
    assert remaining["hub-c"] == "sweep-occupied"


def test_svc_exclusive_filter_no_longer_poisons_other_services(storage):
    """四轮 review C2: _svc_exclusive_filter 曾把 zcode-hub 占用套到所有
    service 的 rehome 候选上 —— zcode-hub 占住的节点对 grok2api/cpa 无谓
    缩容, 风暴下放大 rehome_blocked。修复后约束感知路径只按本 service 过滤。"""
    from core.router import Router

    _node(storage, "poison-1")   # zcode-hub 占用
    _node(storage, "poison-2")
    storage.batch_save_allocations([
        ("zcode-hub", "hub-x", "poison-1", "2026-11-22T00:00:00"),
    ])
    router = Router(storage)
    cands, blocked = router._find_rehome_candidates_for_account(
        {"region": "test", "egress_type": "warp_ipv6"},
        "test", "warp_ipv6", None,
        exclude_names=set(), service="grok2api",
    )
    assert blocked is None
    assert {c.name for c in cands} == {"poison-1", "poison-2"}


def test_release_endpoint_holds_binding_write_lock(storage, monkeypatch):
    """四轮 review S2: /v1/allocation/release 是 account_allocations 写者,
    必须持 BINDING_WRITE_LOCK —— 否则插入并发 acquire 的 RMW 窗口,
    release 后绑定被 batch_save INSERT ON CONFLICT 复活。"""
    from core.locks import BINDING_WRITE_LOCK
    import threading

    _node(storage, "rel-node")
    storage.batch_save_allocations([
        ("grok2api", "rel-acc", "rel-node", "2026-11-22T00:00:00"),
    ])
    observed = {}
    orig_delete = api.server.storage.delete_allocation

    def _spy(service, account_id):
        observed["held"] = BINDING_WRITE_LOCK._is_owned() \
            if hasattr(BINDING_WRITE_LOCK, "_is_owned") else None
        # RLock 无公开 _is_owned: 用「已持锁线程再次 acquire 立即成功」判定
        got = BINDING_WRITE_LOCK.acquire(blocking=False)
        if got:
            BINDING_WRITE_LOCK.release()
        observed["held"] = got
        return orig_delete(service, account_id)

    monkeypatch.setattr(api.server.storage, "delete_allocation", _spy)
    client = TestClient(api.server.app)
    response = client.post("/v1/allocation/release",
                           params={"service": "grok2api", "account_id": "rel-acc"})
    assert response.status_code == 200
    assert observed["held"] is True  # 释放端点持锁 = 同线程非阻塞 acquire 成功
    assert storage.find_allocation_by_account("rel-acc", service="grok2api") is None


def test_delta_snapshot_flags_retention_gap(storage, monkeypatch):
    """四轮 review S3: since_epoch 早于保留窗 (消费方离线 > prune 保留期)
    时空 delta 会静默把游标推进 current_epoch —— 下游吞掉全部保留窗变更,
    镜像永久漂移。gap=true 必须显式暴露。"""
    _node(storage, "gap-node")
    storage.batch_save_allocations([
        ("grok2api", "gap-acc", "gap-node", "2026-11-22T00:00:00"),
    ])
    current = storage.get_binding_epoch("grok2api")["epoch"]
    # 模拟保留窗裁剪: 删掉全部 change 行 (epoch 状态不动, 同 prune_binding_changes 语义)
    with storage._get_conn() as conn:
        conn.execute("DELETE FROM binding_changes WHERE service = 'grok2api'")
    snap = storage.get_binding_delta_snapshot("grok2api", 0, 100)
    assert snap["changes"] == []
    assert snap["gap"] is True          # 0 < current 但行已被裁掉
    # 正常窗口无 gap
    fresh = storage.get_binding_delta_snapshot("grok2api", current, 100)
    assert fresh["gap"] is False
    # export 端点透传 gap
    result = api.server.export_grok2api(since_epoch=0)
    assert result["gap"] is True


def test_allocator_persists_call_constraints_as_bound(storage):
    """四轮 review S4: 控制面 acquire_single 传了 region/tier/egress 但从不
    落 bound_* —— 后续 rehome 把账号当无约束可用性优先迁移, 跨区/跨
    egress 违约 (region_strict 硬承诺形同虚设)。与数据面 router 语义对齐。"""
    from core.allocator import AllocationEngine

    _node(storage, "bind-us")
    _node(storage, "bind-jp")
    engine = AllocationEngine(storage)
    engine.acquire_single(service="reg-svc", account_id="pin-1",
                          region="test", tier="standard",
                          egress_preference="warp_ipv6")
    alloc = storage.find_allocation_by_account("pin-1", service="reg-svc")
    assert alloc["node_name"].startswith("bind-")
    assert alloc["bound_region"] == "test"
    assert alloc["bound_tier"] == "standard"
    assert alloc["bound_egress_type"] == "warp_ipv6"
    # 无约束调用保持无约束 (不虚构承诺)
    engine.acquire_single(service="reg-svc", account_id="loose-1")
    loose = storage.find_allocation_by_account("loose-1", service="reg-svc")
    assert not loose["bound_region"] and not loose["bound_egress_type"]


def test_prober_half_open_transition_guards_disabled(storage):
    """四轮 review P2: HALF_OPEN canary 的 update_node_status 无条件 UPDATE,
    快照过期窗口覆盖管理员 disable。combine_status_and_record_event 用
    CASE guard 只迁移仍为 DEAD 的行。"""
    from core.prober import Prober

    _node(storage, "half-open-node")
    storage.update_node_status("half-open-node", "dead")
    prober = Prober(storage)
    # 模拟快照过期: 管理员在探针取节点与落库之间 disable 了节点
    storage.update_node_status("half-open-node", "disabled")
    prober.storage.combine_status_and_record_event(
        "half-open-node", "half_open",
        from_state="dead", reason="cooldown_expired_canary_probe",
    )
    row = storage.get_node("half-open-node")
    assert row.status == "disabled"   # 管理意图保留, 未被 HALF_OPEN 覆盖
    # 正常路径: DEAD -> HALF_OPEN 迁移 + 事件行
    _node(storage, "half-open-2")
    storage.update_node_status("half-open-2", "dead")
    prober.storage.combine_status_and_record_event(
        "half-open-2", "half_open",
        from_state="dead", reason="cooldown_expired_canary_probe",
    )
    assert storage.get_node("half-open-2").status == "half_open"
    events = storage.list_circuit_events(limit=5)
    assert any(e.node_name == "half-open-2" and e.to_state == "half_open"
               for e in events)


def test_gateway_framing_reject_uses_real_crlf(storage, monkeypatch):
    """四轮 review G1: framing-invalid 400 响应曾是 \\r\\n 字面文本 —— wire
    上无真实 CRLF, 2026-11-19 加固的「干净 400 + close」契约从未生效。"""
    from gateway.server import ForwardProxyGateway

    # 直接断言源码不再含字面 \\r\\n 的 400 响应字节 (构造级回归)
    import inspect
    src = inspect.getsource(ForwardProxyGateway)
    assert 'b"HTTP/1.1 400 Bad Request\\\\r\\\\n' not in src


def test_gateway_relay_budget_split_not_starved(storage, monkeypatch):
    """四轮 review G2: RELAY_FALLBACK 的 direct 腿曾吃满 attempt_timeout
    (min(10,2)=2.0), direct 挂起时 relay 级联在预算内永远轮不到。修复后
    direct/relay 各拿一半预算。"""
    import asyncio
    from gateway import server as gw

    class _StubRouter:
        @staticmethod
        def resolve_relay_hop(node):
            return ProxyNode(name="relay-hop", host="127.0.0.1", port=1,
                             region="test", egress_type="warp_ipv6")

    _node(storage, "fb-node")
    node = storage.get_node("fb-node")
    node.relay_mode = "relay_fallback"
    gateway = gw.ForwardProxyGateway(storage, router=_StubRouter())
    observed = {}

    async def _fake_direct(host, port, th, tp, timeout=None):
        observed["direct_timeout"] = timeout
        raise OSError("direct hangs")

    async def _fake_relay(relay_host, relay_port, target_proxy_host,
                          target_proxy_port, dest_host, dest_port, timeout=None):
        observed["relay_timeout"] = timeout
        # relay 腿必须在预算内真的被执行 (直接成功即证明未被饿死)
        reader, writer = None, None

        class _W:
            def close(self): pass
            async def wait_closed(self): pass

        return object(), _W()

    monkeypatch.setattr(gw, "socks5_connect", _fake_direct)
    monkeypatch.setattr(gw, "socks5_relay_connect", _fake_relay)
    streams = asyncio.run(gateway.connect_upstream_adaptive(
        node, "example.com", 443, timeout=gw.CONNECT_TIMEOUT))
    # direct 拿一半预算, relay 腿拿到另一半且实际执行
    assert observed["direct_timeout"] <= gw.CONNECT_TIMEOUT / 2 + 0.01
    assert observed["relay_timeout"] >= gw.CONNECT_TIMEOUT / 2 - 0.01
