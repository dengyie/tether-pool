"""
tests/test_ban_aware.py - Ban-aware allocation (2026-11-14 需求单, zcode-hub 消费方)

覆盖: ban_events 存储 (record/幂等/release/prune)、Router 与 Allocator 的
avoid_banned/avoid_cidr/avoid_same_isp 硬过滤、判封门 (3012 不隔离)、
限流、ban/bans/release API、per-account status、metrics 渲染。
"""

import ipaddress
import pytest
from starlette.testclient import TestClient

import api.server
from core.models import ProxyNode, EgressType, HealthStatus
from core.storage import Storage
from core.router import Router
from core.ban import node_isp, ip_in_excluded_nets, build_ban_excluded_networks
from core.allocator import AllocationEngine
from core.metrics import MetricsCollector


# --- fixtures ---

@pytest.fixture
def storage():
    import tempfile, os
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    s = Storage(db_path=f.name)
    yield s
    os.unlink(f.name)


def _mk_node(s, name, exit_ip=None, isp=None, region="us-west", egress=EgressType.CLASH_PROXY.value, tier="standard"):
    tags = ["clash", "bridge", "residential"]
    if isp:
        tags.append(f"isp={isp}")
    s.upsert_node(ProxyNode(
        name=name, host="127.0.0.1", port=11000 + (abs(hash(name)) % 900),
        region=region, egress_type=egress, tags=tags,
        status=HealthStatus.HEALTHY.value, exit_ip=exit_ip, tier=tier,
    ))


@pytest.fixture
def seeded(storage):
    _mk_node(storage, "clash-a", exit_ip="23.1.1.5", isp="comcast")
    _mk_node(storage, "clash-b", exit_ip="23.1.1.9", isp="comcast")   # 同 /24
    _mk_node(storage, "clash-c", exit_ip="45.6.7.8", isp="att")       # 跨 ISP 跨段
    _mk_node(storage, "clash-d", exit_ip="99.8.7.6")                  # 独立段, 无 isp tag
    return storage


# --- helpers unit ---

def test_build_ban_excluded_networks_slash24():
    # /24 归并键经 build_ban_excluded_networks 生效 (原 _ipv4_slash24 已删, review F6)
    rec = type("Q", (), {"exit_ip": "23.1.1.5"})()
    nets = build_ban_excluded_networks([rec], ["23.2.0.0/16"])
    assert ipaddress.ip_address("23.1.1.99") in nets[0]
    assert ipaddress.ip_address("23.2.5.1") in nets[1]
    # IPv6 exit_ip 产出 v6 /24 网段 (v4 家宽 /24 语义之外的一致处理); 非法串跳过
    v6 = type("Q", (), {"exit_ip": "2001:db8::1"})()
    v6_nets = build_ban_excluded_networks([v6], [])
    assert len(v6_nets) == 1 and ipaddress.ip_address("2001:db8::ffff") in v6_nets[0]


def test_ip_in_excluded_nets():
    nets = build_ban_excluded_networks([], ["23.1.1.0/24"])
    assert ip_in_excluded_nets("23.1.1.99", nets)
    assert not ip_in_excluded_nets("45.6.7.8", nets)
    assert not ip_in_excluded_nets(None, nets)
    # 非法 CIDR 跳过不炸
    assert build_ban_excluded_networks([], ["garbage"]) == []


def test_node_isp_tag():
    n = ProxyNode(name="x", host="h", port=1, region="r", egress_type="clash_proxy", tags=["clash", "ISP=Comcast"])
    assert node_isp(n) == "comcast"
    n2 = ProxyNode(name="y", host="h", port=1, region="r", egress_type="clash_proxy", tags=["clash"])
    assert node_isp(n2) == ""


# --- storage layer ---

def test_record_ban_and_idempotency(storage):
    rec, idem = storage.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {"http_status": 405}, 24)
    assert not idem and rec.quarantined_until
    rec2, idem2 = storage.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    assert idem2 and rec2.ban_id == rec.ban_id
    # 不同 reason 不共享幂等
    rec3, idem3 = storage.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "rate_limited_429_burst", {}, 24)
    assert not idem3 and rec3.ban_id != rec.ban_id
    # 已解除后重复上报 -> 新 ban
    storage.release_quarantine("zcode-hub", "clash-a")
    storage.release_quarantine("zcode-hub", "clash-a")  # 已无 active 行, 返回 0
    rec4, idem4 = storage.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    assert not idem4


def test_record_ban_zero_hours_records_only(storage):
    rec, _ = storage.record_ban("zcode-hub", "acc", "clash-a", None, None, "unusual_activity_3012", {}, 0)
    assert rec.quarantined_until is None
    assert storage.get_active_quarantines() == []


def test_get_active_quarantines_service_scoped(storage):
    storage.record_ban("svc1", "a", "n1", "1.1.1.1", None, "edge_405_html", {}, 24)
    storage.record_ban("svc2", "a", "n2", "2.2.2.2", None, "edge_405_html", {}, 24)
    assert {q.node_name for q in storage.get_active_quarantines(service="svc1")} == {"n1"}
    assert {q.node_name for q in storage.get_active_quarantines()} == {"n1", "n2"}


def test_list_bans_and_prune(storage):
    storage.record_ban("svc", "a", "n1", "1.1.1.1", None, "edge_405_html", {}, 0)
    storage.record_ban("svc", "a", "n2", "1.1.1.2", None, "edge_405_html", {}, 0)
    assert len(storage.list_bans(service="svc")) == 2
    assert len(storage.list_bans(limit=1)) == 1
    # prune: keep_days 窗口外的行删除; 刚创建的行保留
    assert storage.prune_ban_events(keep_days=30) == 0
    assert len(storage.list_bans()) == 2


def test_release_quarantine(storage):
    storage.record_ban("svc", "a", "n1", "1.1.1.1", None, "edge_405_html", {}, 24)
    assert storage.release_quarantine("svc", "n1") == 1
    assert storage.get_active_quarantines() == []
    assert storage.release_quarantine("svc", "n1") == 0


# --- Router layer ---

def test_router_avoid_banned_excludes_node(seeded):
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    r = Router(seeded)
    lease = r.acquire_for_account("acc", egress_type=None, service="zcode-hub", avoid_banned=True)
    assert lease is not None
    assert lease.node_name != "clash-a"


def test_router_avoid_banned_slash24(seeded):
    # clash-b 与被封 clash-a 同 /24, 也必须被排除
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    r = Router(seeded)
    for _ in range(5):
        lease = r.acquire_for_account(f"acc{_:d}", egress_type=None, service="zcode-hub", avoid_banned=True)
        assert lease is not None
        assert lease.node_name not in ("clash-a", "clash-b"), f"{lease.node_name} 同 /24 未被排除"


def test_router_avoid_same_isp(seeded):
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    r = Router(seeded)
    for i in range(6):
        lease = r.acquire_for_account(f"acc{i}", egress_type=None, service="zcode-hub", avoid_banned=True, avoid_same_isp=True)
        assert lease is not None
        assert lease.node_name not in ("clash-a", "clash-b"), "同 ISP 未被排除"


def test_router_no_isp_tag_not_excluded_by_same_isp(seeded):
    # clash-d 无 isp tag: 不参与 avoid_same_isp 判定、不因此被排除 (需求 §5.3)
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    # 封掉 clash-c (att), 只剩 clash-d 可用 (comcast 的 a/b 被节点名排除)
    seeded.record_ban("zcode-hub", "acc2", "clash-c", "45.6.7.8", "att", "edge_405_html", {}, 24)
    r = Router(seeded)
    lease = r.acquire_for_account("acc3", egress_type=None, service="zcode-hub", avoid_banned=True, avoid_same_isp=True)
    assert lease is not None and lease.node_name == "clash-d"


def test_router_avoid_cidr_explicit(seeded):
    r = Router(seeded)
    lease = r.acquire_for_account("acc", egress_type=None, service="zcode-hub", avoid_cidrs=["45.6.7.0/24"])
    assert lease is not None
    assert not ipaddress.ip_address(lease.exit_ip) in ipaddress.ip_network("45.6.7.0/24")


def test_router_banned_binding_not_sticky(seeded):
    # 绑定节点被封: avoid_banned 时 fast path 必须拒绝, 返回别的节点
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    r = Router(seeded)
    first = r.acquire_for_account("acc", egress_type=None, service="zcode-hub")
    # 人为把绑定指到 clash-a (batch_save_allocations 直写, 不依赖 CRC 碰运气)
    seeded.batch_save_allocations([("zcode-hub", "acc", "clash-a", first.assigned_at)])
    lease = r.acquire_for_account("acc", egress_type=None, service="zcode-hub", avoid_banned=True)
    assert lease is not None
    assert lease.node_name != "clash-a"
    # 绑定未被迁移 (opt-in 语义: 只影响本次选路)
    assert seeded.get_allocation("zcode-hub", "acc")["node_name"] == "clash-a"


def test_router_avoid_banned_exhausted_returns_none(storage):
    _mk_node(storage, "only-1", exit_ip="9.9.9.9")
    storage.record_ban("svc", "acc", "only-1", "9.9.9.9", None, "edge_405_html", {}, 24)
    r = Router(storage)
    assert r.acquire_for_account("acc", egress_type=None, service="svc", avoid_banned=True) is None


def test_router_without_avoid_banned_ignores_quarantine(seeded):
    # opt-in 语义: 不传 avoid_banned 的调用完全不受 quarantine 影响
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    r = Router(seeded)
    for i in range(6):
        lease = r.acquire_for_account(f"acc{i}", egress_type=None, service="zcode-hub")
        assert lease is not None  # 不炸不排除


def test_router_banned_binding_sticky_when_no_avoid_param(seeded):
    # 网关数据面不变量 (review F9): 有 active quarantine 但不传 avoid 参数时,
    # 被封绑定节点照常 sticky 返回 —— quarantine 只影响 opt-in 的调用。
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    seeded.batch_save_allocations([("zcode-hub", "acc", "clash-a", "2026-11-14T00:00:00")])
    r = Router(seeded)
    lease = r.acquire_for_account("acc", egress_type=None, service="zcode-hub")
    assert lease is not None and lease.node_name == "clash-a"


def test_router_force_rebind_with_avoid_banned_persists(seeded):
    # zcode-hub Mode B 主流程 (review F9): force_rebind + avoid_banned =
    # 显式强制换绑, 新节点必须落库 (区别于普通 avoid 调用的临时改道)。
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    seeded.batch_save_allocations([("zcode-hub", "acc", "clash-a", "2026-11-14T00:00:00")])
    r = Router(seeded)
    lease = r.acquire_for_account("acc", egress_type=None, service="zcode-hub", avoid_banned=True, force_rebind=True)
    assert lease is not None and lease.node_name != "clash-a"
    assert seeded.get_allocation("zcode-hub", "acc")["node_name"] == lease.node_name


def test_router_degraded_banned_binding_rehomes(seeded):
    # review F5: DEGRADED 且被封的绑定沿用既有降级 rehome 语义 —— 迁绑落库
    # (与"绑定节点降级时迁走"同形, ban 不改变该行为)。锁定语义防回归。
    # 注: 必须带 egress_type (网关数据面真实形态) —— egress_type=None 时
    # ladder 前 1/2 级剔除 residential tag 节点, 候选只剩被封节点, 按硬边界
    # 语义正确地回 None (test_router_avoid_banned_exhausted_returns_none 已锁)。
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    seeded.upsert_node(ProxyNode(
        name="clash-a", host="127.0.0.1", port=11001, region="us-west",
        egress_type=EgressType.CLASH_PROXY.value, tags=["clash", "isp=comcast"],
        status=HealthStatus.DEGRADED.value, exit_ip="23.1.1.5", tier="standard",
    ))
    seeded.batch_save_allocations([("zcode-hub", "acc", "clash-a", "2026-11-14T00:00:00")])
    r = Router(seeded)
    lease = r.acquire_for_account("acc", egress_type=EgressType.CLASH_PROXY.value, service="zcode-hub", avoid_banned=True)
    assert lease is not None and lease.node_name != "clash-a"
    assert seeded.get_allocation("zcode-hub", "acc")["node_name"] != "clash-a"


# --- Allocator layer ---

def test_allocator_avoid_banned_fast_path_skip(seeded):
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    seeded.batch_save_allocations([("zcode-hub", "acc", "clash-a", "2026-11-14T00:00:00")])
    eng = AllocationEngine(seeded)
    item = eng.acquire_single("zcode-hub", "acc", egress_preference=None, avoid_banned=True)
    assert item.node_name != "clash-a"
    # 不传 avoid_banned: fast path 照常返回绑定节点
    item2 = eng.acquire_single("zcode-hub", "acc", egress_preference=None)
    assert item2.node_name == "clash-a"


def test_allocator_avoid_cidr_and_isp(seeded):
    eng = AllocationEngine(seeded)
    item = eng.acquire_single("zcode-hub", "acc", egress_preference=None, avoid_cidrs=["45.6.7.0/24"], avoid_same_isp=True)
    assert item.exit_ip and ipaddress.ip_address(item.exit_ip) not in ipaddress.ip_network("45.6.7.0/24")
    # 选中节点若带 isp tag, 不得是 comcast/att 中被封的那个场景 (此处只验字段回带)
    assert item.isp in (None, "comcast", "att")


def test_allocator_exhausted_raises(seeded):
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    for nm in ("clash-b", "clash-c", "clash-d"):
        seeded.record_ban("zcode-hub", "acc", nm, None, None, "edge_405_html", {}, 24)
    eng = AllocationEngine(seeded)
    with pytest.raises(RuntimeError):
        eng.acquire_single("zcode-hub", "acc", egress_preference=None, avoid_banned=True)


def test_allocator_force_rebalance_with_avoid_banned_persists(seeded):
    # zcode-hub Mode B 主流程 (review F9): force_rebalance + avoid_banned =
    # 显式强制换绑, 新节点必须落库。
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    seeded.batch_save_allocations([("zcode-hub", "acc", "clash-a", "2026-11-14T00:00:00")])
    eng = AllocationEngine(seeded)
    item = eng.acquire_single("zcode-hub", "acc", egress_preference=None, avoid_banned=True, force_rebalance=True)
    assert item.node_name != "clash-a"
    assert seeded.get_allocation("zcode-hub", "acc")["node_name"] == item.node_name


def test_allocator_region_strict_no_cross_region_fallback(seeded):
    # review F1 根因修复: region 候选耗尽时 region_strict=true 必须空手而归
    # (RuntimeError -> 503), 绝不跨区兜底; 不带 strict 维持既有跨区行为。
    eng = AllocationEngine(seeded)
    with pytest.raises(RuntimeError):
        eng.acquire_single("zcode-hub", "acc-strict", egress_preference=None, region="eu-west", region_strict=True)
    item = eng.acquire_single("zcode-hub", "acc-relaxed", egress_preference=None, region="eu-west")
    assert item is not None  # 非 strict: 跨区兜底 (既有语义)


def test_record_ban_concurrent_idempotency(storage):
    # review F3 根因修复: BEGIN IMMEDIATE 让幂等 SELECT 与 INSERT 同写事务,
    # 并发上报同 (service,node,reason) 只能落一行。
    import threading
    errors: list = []

    def report():
        try:
            storage.record_ban("svc", "a", "n1", "1.1.1.1", None, "edge_405_html", {}, 24)
        except Exception as e:  # pragma: no cover - 锁超时才算失败
            errors.append(e)

    threads = [threading.Thread(target=report) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert errors == []
    assert len(storage.get_active_quarantines(service="svc")) == 1


# --- API layer ---

@pytest.fixture
def client():
    """API 测试直接用 conftest 隔离出来的 api.server.storage (autouse fixture)。"""
    s = api.server.storage
    _mk_node(s, "clash-a", exit_ip="23.1.1.5", isp="comcast")
    _mk_node(s, "clash-b", exit_ip="23.1.1.9", isp="comcast")   # 同 /24
    _mk_node(s, "clash-c", exit_ip="45.6.7.8", isp="att")       # 跨 ISP 跨段
    _mk_node(s, "clash-d", exit_ip="45.6.7.9")                  # 跨段, 无 isp tag
    return TestClient(api.server.app)


def test_ban_report_202_and_idempotent(client, seeded):
    body = {"service": "zcode-hub", "account": "acc", "node_id": "clash-a",
            "reason": "edge_405_html", "evidence": {"http_status": 405, "body_kind": "html"}}
    r1 = client.post("/v1/feedback/ban", json=body)
    assert r1.status_code == 202
    d1 = r1.json()
    assert d1["node_id"] == "clash-a" and d1["exit_ip"] == "23.1.1.5" and d1["isp"] == "comcast"
    assert d1["quarantine_until"] and not d1["idempotent"]
    r2 = client.post("/v1/feedback/ban", json=body)
    assert r2.status_code == 202
    d2 = r2.json()
    assert d2["idempotent"] and d2["ban_id"] == d1["ban_id"]


def test_ban_report_unusual_3012_no_quarantine(client, seeded):
    body = {"service": "zcode-hub", "account": "acc", "node_id": "clash-a",
            "reason": "unusual_activity_3012", "evidence": {"http_status": 200, "body_kind": "json"}}
    r = client.post("/v1/feedback/ban", json=body)
    assert r.status_code == 202
    assert r.json()["quarantine_until"] is None
    assert seeded.get_active_quarantines() == []
    # body_kind=html 的 3012 例外: 消费方确认是边缘 HTML -> 隔离
    body["evidence"]["body_kind"] = "html"
    r2 = client.post("/v1/feedback/ban", json=body)
    assert r2.json()["quarantine_until"] is not None


def test_ban_report_400_no_binding(client):
    r = client.post("/v1/feedback/ban", json={"service": "s", "account": "ghost", "reason": "edge_405_html"})
    assert r.status_code == 400


def test_ban_report_invalid_reason_422(client, seeded):
    r = client.post("/v1/feedback/ban", json={"service": "zcode-hub", "account": "acc", "node_id": "clash-a", "reason": "made_up"})
    assert r.status_code == 422


def test_ban_rate_limit_429(client, monkeypatch):
    monkeypatch.setattr(api.server, "BAN_RATE_LIMIT_PER_HOUR", 2)
    # 幂等会折叠同 node+reason, 限流计数靠不同节点制造独立行
    for i, nid in enumerate(("clash-a", "clash-b")):
        ok = client.post("/v1/feedback/ban", json={"service": "zcode-hub", "account": f"a{i}", "node_id": nid, "reason": "edge_405_html"})
        assert ok.status_code == 202
    r = client.post("/v1/feedback/ban", json={"service": "zcode-hub", "account": "a9", "node_id": "clash-c", "reason": "edge_405_html"})
    assert r.status_code == 429


def test_bans_list_and_release(client, seeded):
    client.post("/v1/feedback/ban", json={"service": "zcode-hub", "account": "acc", "node_id": "clash-a", "reason": "edge_405_html"})
    hist = client.get("/v1/feedback/bans?service=zcode-hub").json()
    assert len(hist) == 1 and hist[0]["node_name"] == "clash-a"
    rel = client.post("/v1/feedback/ban/release?service=zcode-hub&node_id=clash-a")
    assert rel.status_code == 200 and rel.json()["success"]
    rel2 = client.post("/v1/feedback/ban/release?service=zcode-hub&node_id=clash-a")
    assert rel2.json()["success"] is False


def test_acquire_route_ban_aware_passthrough(client):
    s = api.server.storage
    s.record_ban("zcode-hub", "acc-x", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    r = client.post("/v1/allocation/acquire?service=zcode-hub&account_id=acc-x&egress_preference=&avoid_banned=true&avoid_same_isp=true")
    assert r.status_code == 200
    d = r.json()
    assert d["node_name"] not in ("clash-a", "clash-b")
    # 既有语义不破: 不带 avoid 参数照常
    r2 = client.post("/v1/allocation/acquire?service=zcode-hub&account_id=acc-y&egress_preference=")
    assert r2.status_code == 200


def test_account_status_endpoint(client, seeded):
    client.post("/v1/allocation/acquire?service=zcode-hub&account_id=acc-z&egress_preference=")
    r = client.get("/v1/allocation/status/account?service=zcode-hub&account=acc-z")
    assert r.status_code == 200
    d = r.json()
    assert d["node_id"] and d["exit_ip"]
    assert d["pool_health"]["healthy"] >= 1
    assert isinstance(d["quarantined"], list)
    # 未绑定账号
    r2 = client.get("/v1/allocation/status/account?service=zcode-hub&account=ghost")
    assert r2.status_code == 200 and r2.json()["node_id"] is None


def test_metrics_render_ban_metrics(client, seeded):
    seeded.record_ban("zcode-hub", "acc", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    seeded.incr_metric("ip_pool_ban_events_total", {"service": "zcode-hub", "reason": "edge_405_html"})
    mc = MetricsCollector(seeded)
    text = mc.generate_metrics()
    assert "ip_pool_ban_events_total" in text
    assert 'ip_pool_quarantine_active{service="zcode-hub"} 1' in text


def test_prune_event_tables_includes_bans(seeded):
    seeded.record_ban("svc", "a", "n", "1.1.1.1", None, "edge_405_html", {}, 0)
    res = seeded.prune_event_tables(keep_days=7)
    assert "ban_events" in res and res["ban_events"] == 0


# --- API layer: review follow-up fixes (2026-11-14) ---

def test_ban_report_lease_fallback_cross_service_guard(client):
    # review F2 根因修复: account 在别的 service 有 allocation 时, lease 归属
    # 不明, 不得猜测塞进本 service 的 quarantine -> 400。
    s = api.server.storage
    s.batch_save_allocations([("other-svc", "ghost-cross", "clash-a", "2026-11-14T00:00:00")])
    s.set_lease("ghost-cross", "clash-b")
    r = client.post("/v1/feedback/ban", json={"service": "zcode-hub", "account": "ghost-cross", "reason": "edge_405_html"})
    assert r.status_code == 400


def test_ban_report_legacy_lease_fallback_still_works(client):
    # F2 的另一面: 纯 legacy 账户 (全库无任何 allocation 镜像) 的 lease 兜底
    # 必须仍然可用 —— 与 Router 1b 同守卫, 不误伤迁移前账户。
    s = api.server.storage
    s.set_lease("legacy-acc", "clash-a")
    r = client.post("/v1/feedback/ban", json={"service": "zcode-hub", "account": "legacy-acc", "reason": "edge_405_html"})
    assert r.status_code == 202 and r.json()["node_id"] == "clash-a"


def test_ban_report_scope_global_422(client, seeded):
    # review F8: scope 契约诚实 —— P0 只实现 service, 其他值显式 422 而非静默。
    r = client.post("/v1/feedback/ban", json={"service": "zcode-hub", "account": "acc", "node_id": "clash-a", "reason": "edge_405_html", "scope": "global"})
    assert r.status_code == 422


def test_bans_list_evidence_is_object(client, seeded):
    # review F8: evidence 出参反序列化为对象, 消费方不必二次 parse。
    client.post("/v1/feedback/ban", json={"service": "zcode-hub", "account": "acc", "node_id": "clash-a", "reason": "edge_405_html", "evidence": {"http_status": 405, "body_kind": "html"}})
    hist = client.get("/v1/feedback/bans?service=zcode-hub").json()
    assert hist[0]["evidence"] == {"http_status": 405, "body_kind": "html"}


def test_acquire_route_region_strict(client):
    # review F1 根因修复: /v1/allocation/acquire 的 region_strict=true 硬边界 ——
    # region 池空 -> 503 (绝不静默跨区); 不带 strict 维持既有跨区兜底。
    r = client.post("/v1/allocation/acquire?service=zcode-hub&account_id=acc-strict&egress_preference=&region=eu-west&region_strict=true")
    assert r.status_code == 503
    r2 = client.post("/v1/allocation/acquire?service=zcode-hub&account_id=acc-relaxed&egress_preference=&region=eu-west")
    assert r2.status_code == 200


def test_acquire_route_no_warning_without_avoid_params(client, seeded):
    # review F4: 防汛检查只在 ban-aware 调用上跑 (热路径不付额外查询)。
    s = api.server.storage
    s.record_ban("zcode-hub", "a1", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    r = client.post("/v1/allocation/acquire?service=zcode-hub&account_id=acc&egress_preference=")
    assert r.status_code == 200
    # pydantic 序列化 optional 字段恒在, warning 应为 null 而非 quarantine_flood
    assert r.json().get("warning") is None


def _extra_nodes(s, *specs):
    """client fixture 的 4 节点池不够摆防汛阈值, 测试内加独立 /24 节点。"""
    for name, ip in specs:
        _mk_node(s, name, exit_ip=ip)


def test_acquire_route_flood_warning(client, seeded):
    # review F4: 防汛告警只在 avoid 参数存在时计算 —— 2/4 节点被封 (50% > 30%)
    # 且 avoid_banned=true -> warning=quarantine_flood (clash-e 供候选, 200 不 503)。
    s = api.server.storage
    _extra_nodes(s, ("clash-e", "77.7.7.7"))
    s.record_ban("zcode-hub", "a1", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    s.record_ban("zcode-hub", "a3", "clash-c", "45.6.7.8", "att", "edge_405_html", {}, 24)
    r = client.post("/v1/allocation/acquire?service=zcode-hub&account_id=acc&egress_preference=&avoid_banned=true")
    assert r.status_code == 200 and r.json().get("warning") == "quarantine_flood"


def test_acquire_route_flood_dedupe_no_false_positive(client, seeded):
    # review F7 根因修复: 防汛计数按节点去重 —— 同节点多 reason 的多行只算一个。
    # 场景: 8 节点池, clash-a 封 2 行 (2 reason) + clash-c 封 1 行 = 3 行 / 2 节点。
    # 阈值 0.3*8 = 2.4: 按行数 3 > 2.4 会误报, 按节点 2 <= 2.4 正确无告警。
    s = api.server.storage
    _extra_nodes(s, ("clash-e", "77.7.7.7"), ("clash-f", "78.8.8.8"), ("clash-g", "79.9.9.9"), ("clash-h", "80.1.1.1"))
    s.record_ban("zcode-hub", "a1", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    s.record_ban("zcode-hub", "a2", "clash-a", "23.1.1.5", "comcast", "unusual_activity_3012", {"body_kind": "html"}, 24)
    s.record_ban("zcode-hub", "a3", "clash-c", "45.6.7.8", "att", "edge_405_html", {}, 24)
    r = client.post("/v1/allocation/acquire?service=zcode-hub&account_id=acc&egress_preference=&avoid_banned=true")
    assert r.status_code == 200 and r.json().get("warning") is None


def test_metrics_gauge_dedupes_nodes(client, seeded):
    # review F7: gauge 语义 = 被隔离节点数, 同节点多 reason 只计一次。
    seeded.record_ban("zcode-hub", "a1", "clash-a", "23.1.1.5", "comcast", "edge_405_html", {}, 24)
    seeded.record_ban("zcode-hub", "a2", "clash-a", "23.1.1.5", "comcast", "unusual_activity_3012", {"body_kind": "html"}, 24)
    mc = MetricsCollector(seeded)
    text = mc.generate_metrics()
    assert 'ip_pool_quarantine_active{service="zcode-hub"} 1' in text
