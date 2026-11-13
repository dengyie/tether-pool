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
from core.router import Router, node_isp, _ipv4_slash24, _ip_in_excluded_nets, _build_ban_excluded_networks
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

def test_ipv4_slash24():
    assert _ipv4_slash24("23.1.1.5") == "23.1.1.5/24"
    assert _ipv4_slash24(None) is None
    assert _ipv4_slash24("not-an-ip") is None
    # IPv6 不做 /24 归并 (家宽 /24 语义只对 v4 有意义); v6 节点仅按节点名排除
    assert _ipv4_slash24("2001:db8::1") is None


def test_ip_in_excluded_nets():
    nets = _build_ban_excluded_networks([], ["23.1.1.0/24"])
    assert _ip_in_excluded_nets("23.1.1.99", nets)
    assert not _ip_in_excluded_nets("45.6.7.8", nets)
    assert not _ip_in_excluded_nets(None, nets)
    # 非法 CIDR 跳过不炸
    assert _build_ban_excluded_networks([], ["garbage"]) == []


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
    seeded.set_lease(type("L", (), {})) if False else None  # noop, 保持可读
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
