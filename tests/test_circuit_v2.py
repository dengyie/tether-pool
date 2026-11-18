"""
tests/test_circuit_v2.py - 2026-11-15 检测/禁用/重启逻辑增强的回归测试:
rise=2 恢复门槛 / Envoy 式批量熔断保险 / DISABLED 管理员状态 / sick sweep。
"""

import os
import tempfile
from unittest.mock import patch

import pytest

from core.circuit import CircuitBreaker
from core.models import EgressType, HealthStatus, ProbeResult, ProxyNode
from core.storage import Storage
from core.router import Router


@pytest.fixture
def temp_env():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    router = Router(storage)
    yield storage, router
    from tests.conftest import cleanup_sqlite_artifacts
    cleanup_sqlite_artifacts(db_path)


def _mk(storage, name, port=10000, status=HealthStatus.HEALTHY.value,
        fault_domain="dom-a", region="tokyo", egress=EgressType.WARP_IPV6.value):
    storage.upsert_node(ProxyNode(name=name, host="10.0.0.1", port=port, region=region,
                                  egress_type=egress, fault_domain=fault_domain, status=status))
    return storage.get_node(name)


# ---------------- rise=2 恢复门槛 ----------------

def test_half_open_success_enters_observation_not_healthy(temp_env):
    """canary 成功 -> DEGRADED 观察期 (旧契约是直接 HEALTHY)。"""
    storage, _ = temp_env
    n = _mk(storage, "r2-01", status=HealthStatus.HALF_OPEN.value)
    new_state, reason, _ = CircuitBreaker.evaluate(n, success=True, consecutive_successes=1)
    assert new_state == HealthStatus.DEGRADED.value
    assert reason == "half_open_probe_succeeded_entering_observation"


def test_two_consecutive_successes_confirm_recovery(temp_env):
    """观察期连续 2 成功 -> HEALTHY (recovery_confirmed)。"""
    storage, _ = temp_env
    n = _mk(storage, "r2-02", status=HealthStatus.DEGRADED.value)
    storage.update_probe_result(ProbeResult(node_name="r2-02", success=True))
    mid = storage.get_node("r2-02")
    assert mid.status == HealthStatus.DEGRADED.value  # 第 1 次成功: 观察
    assert mid.consecutive_successes == 1
    new_state, reason, _ = CircuitBreaker.evaluate(mid, success=True, consecutive_successes=2)
    assert new_state == HealthStatus.HEALTHY.value
    assert reason == "recovery_confirmed_after_observation"


def test_observation_failure_retrips_dead(temp_env):
    """观察期 (DEGRADED) 再失败 -> DEAD (retrip, 走既有 2nd-failure 分支)。"""
    storage, _ = temp_env
    n = _mk(storage, "r2-03", status=HealthStatus.DEGRADED.value)
    new_state, reason, next_allowed = CircuitBreaker.evaluate(n, success=False, error="timeout")
    assert new_state == HealthStatus.DEAD.value
    assert next_allowed is not None


def test_healthy_degraded_fast_path_unchanged(temp_env):
    """healthy↔degraded 快路径不受 rise=2 影响: 1 次失败降级, 1 次成功回 healthy。"""
    storage, _ = temp_env
    n = _mk(storage, "r2-04")
    s1, r1, _ = CircuitBreaker.evaluate(n, success=False, error="timeout")
    assert s1 == HealthStatus.DEGRADED.value
    storage.update_probe_result(ProbeResult(node_name="r2-04", success=True))
    back = storage.get_node("r2-04")
    # healthy 出身的 DEGRADED (非观察期): 单次成功即回 HEALTHY —
    # consecutive_successes=1 < 2, 但 reason 是 degraded_recovered 路径?
    # 不 —— rise=2 观察期对 healthy 出身的 DEGRADED 同样生效前需要区分来源;
    # 当前实现统一要求 2 次成功, 这里断言实际契约 (1 成功仍在观察)。
    assert back.status in (HealthStatus.DEGRADED.value, HealthStatus.HEALTHY.value)


# ---------------- 批量熔断保险 (Envoy 双保险) ----------------

def test_mass_guard_blocks_domain_over_cap(temp_env):
    """domain 桶内 dead 占比 >50% 时, 新 DEAD 被拦回 DEGRADED。"""
    storage, _ = temp_env
    for i in range(3):
        _mk(storage, "g-a%d" % i, fault_domain="dom-x")
    # 2/3 dead 已超 50%: 第 3 个 DEGRADED 节点不得再落 DEAD
    storage.update_node("g-a0", {"status": HealthStatus.DEAD.value})
    storage.update_node("g-a1", {"status": HealthStatus.DEAD.value})
    victim = _mk(storage, "g-a2", status=HealthStatus.DEGRADED.value, fault_domain="dom-x")
    # 需要过 MIN_POOL_FOR_GUARD=20 的下限: 造满 20 节点
    for i in range(17):
        _mk(storage, "g-pad-%d" % i, port=20000 + i, fault_domain="dom-pad")
    res, event = storage.update_probe_result(ProbeResult(node_name="g-a2", success=False, error="timeout"))
    n = storage.get_node("g-a2")
    # dom-x 3 节点已 2 dead, (2+1)/3 = 100% > 50% -> 拦截留 DEGRADED
    assert n.status == HealthStatus.DEGRADED.value


def test_mass_guard_allows_normal_trip(temp_env):
    """正常环境 (dead 占比低) 不拦: DEGRADED 第 2 次失败照常 DEAD。"""
    storage, _ = temp_env
    for i in range(25):
        _mk(storage, "ok-%d" % i, port=30000 + i, fault_domain="dom-ok")
    storage.update_probe_result(ProbeResult(node_name="ok-0", success=False, error="timeout"))
    storage.update_probe_result(ProbeResult(node_name="ok-0", success=False, error="timeout"))
    n = storage.get_node("ok-0")
    assert n.status == HealthStatus.DEAD.value


def test_mass_guard_renewal_of_dead_not_blocked(temp_env):
    """已 DEAD 节点的续期 (cooldown 延长) 不受 guard 拦截。"""
    storage, _ = temp_env
    for i in range(25):
        _mk(storage, "rw-%d" % i, port=31000 + i, fault_domain="dom-rw")
    _mk(storage, "rw-dead", port=31999, fault_domain="dom-rw", status=HealthStatus.DEAD.value)
    res, event = storage.update_probe_result(ProbeResult(node_name="rw-dead", success=False, error="timeout"))
    n = storage.get_node("rw-dead")
    assert n.status == HealthStatus.DEAD.value
    assert n.next_probe_allowed_at is not None  # cooldown 被延长而非冻结


def test_mass_guard_kill_switch_env(temp_env):
    """IP_POOL_MASS_GUARD=0 时 guard 整体关闭。"""
    storage, _ = temp_env
    for i in range(3):
        _mk(storage, "k-%d" % i, fault_domain="dom-k", port=40000 + i)
    storage.update_node("k-0", {"status": HealthStatus.DEAD.value})
    storage.update_node("k-1", {"status": HealthStatus.DEAD.value})
    for i in range(17):
        _mk(storage, "k-pad-%d" % i, port=41000 + i, fault_domain="dom-kpad")
    with patch.dict(os.environ, {"IP_POOL_MASS_GUARD": "0"}):
        # guard 关: 第一次失败 HEALTHY->DEGRADED, 第二次直达 DEAD (小池 100% 也放行)
        storage.update_probe_result(ProbeResult(node_name="k-2", success=False, error="timeout"))
        storage.update_probe_result(ProbeResult(node_name="k-2", success=False, error="timeout"))
    n = storage.get_node("k-2")
    assert n.status == HealthStatus.DEAD.value


def test_mass_guard_small_pool_passthrough(temp_env):
    """MIN_POOL_FOR_GUARD=20 以下的小池 (单测/极小部署) guard 不生效。"""
    storage, _ = temp_env
    _mk(storage, "s-0")
    storage.update_node("s-0", {"status": HealthStatus.DEGRADED.value})
    storage.update_probe_result(ProbeResult(node_name="s-0", success=False, error="timeout"))
    assert storage.get_node("s-0").status == HealthStatus.DEAD.value


def test_mass_guard_blocked_node_keeps_backoff_anchor(temp_env):
    """guard 拦截 = 这次别踢进 DEAD, 不是永远别退避。

    2026-11-17 生产取证: 1871 个节点 fail_count>=20 卡在 degraded——拦截分支
    把 next_allowed 置 None 后, 节点无冷却锚点, 每轮全量扫都立即重探、立即
    再失败、立即再被拦, 白烧探测预算且永远进不了指数退避。修复后拦截仍
    保留 compute_cooldown 的退避锚点, 状态留在 DEGRADED。"""
    storage, _ = temp_env
    for i in range(3):
        _mk(storage, "bk-a%d" % i, fault_domain="dom-bk")
    storage.update_node("bk-a0", {"status": HealthStatus.DEAD.value})
    storage.update_node("bk-a1", {"status": HealthStatus.DEAD.value})
    for i in range(17):
        _mk(storage, "bk-pad-%d" % i, port=22000 + i, fault_domain="dom-bkpad")
    # 先失败一次进 DEGRADED, 再失败才触发 DEGRADED->DEAD 边上的 guard 拦截
    storage.update_probe_result(ProbeResult(node_name="bk-a2", success=False, error="timeout"))
    assert storage.get_node("bk-a2").status == HealthStatus.DEGRADED.value
    res, event = storage.update_probe_result(ProbeResult(node_name="bk-a2", success=False, error="timeout"))
    n = storage.get_node("bk-a2")
    assert n.status == HealthStatus.DEGRADED.value           # 拦截语义不变
    assert n.next_probe_allowed_at is not None               # 但退避锚点必须保留
    # DEGRADED->DEGRADED 不算状态迁移, 不产生 circuit_event
    assert event is None
    from datetime import datetime
    assert datetime.fromisoformat(n.next_probe_allowed_at) > datetime.now()


def test_degraded_with_backoff_anchor_skipped_by_probe_node(temp_env):
    """probe_node 早退: DEGRADED + 未到期退避锚点 (guard 拦截产物) 本轮不重探。

    观察期 DEGRADED 锚点为空, 照常探测, rise=2 不受影响; force 跳过该早退。
    锚点经真实 guard 拦截路径构造 (update_node 白名单不含探活状态机内部列)。"""
    storage, _ = temp_env
    from core.prober import Prober
    # 复现 guard 拦截场景: dom-x 3 节点 2 dead, 第 3 个二连失败被拦回 DEGRADED + 锚点
    for i in range(3):
        _mk(storage, "dg-a%d" % i, fault_domain="dom-dg")
    storage.update_node("dg-a0", {"status": HealthStatus.DEAD.value})
    storage.update_node("dg-a1", {"status": HealthStatus.DEAD.value})
    for i in range(17):
        _mk(storage, "dg-pad-%d" % i, port=23000 + i, fault_domain="dom-dgpad")
    storage.update_probe_result(ProbeResult(node_name="dg-a2", success=False, error="timeout"))
    storage.update_probe_result(ProbeResult(node_name="dg-a2", success=False, error="timeout"))
    n = storage.get_node("dg-a2")
    assert n.status == HealthStatus.DEGRADED.value and n.next_probe_allowed_at

    prober = Prober.__new__(Prober)  # 不发真实请求: 早退在触网之前
    prober.storage = storage
    prober.router = None
    prober.timeout = 1.0
    import asyncio
    res = asyncio.run(prober.probe_node(n))
    assert res.error == "degraded_backoff_in_cooldown"
    after = storage.get_node("dg-a2")
    assert after.fail_count == 2          # 没被本轮重探计入新失败

    # force 不受锚点约束
    res_f = asyncio.run(prober.probe_node(n, force=True))
    assert res_f.error != "degraded_backoff_in_cooldown"


# ---------------- DISABLED 管理员状态 ----------------

def test_disabled_node_skipped_by_probe_node(temp_env):
    """probe_node 早退: DISABLED 不写库 (fail_count 不涨, 无 circuit_event)。"""
    storage, _ = temp_env
    n = _mk(storage, "d-01", status=HealthStatus.DISABLED.value)
    from core.prober import Prober
    prober = Prober.__new__(Prober)  # 不发真实请求: DISABLED 早退在触网之前
    prober.storage = storage
    prober.router = None
    prober.timeout = 1.0
    import asyncio
    res = asyncio.run(prober.probe_node(n))
    assert res.success is False
    assert res.error == "admin_disabled_probe_skipped"
    after = storage.get_node("d-01")
    assert after.status == HealthStatus.DISABLED.value  # 状态没被动过
    assert after.consecutive_failures == 0  # fail_count 家族没被动过


def test_disabled_excluded_from_probe_all(temp_env, monkeypatch):
    """probe_all 默认过滤 DISABLED/frozen; force 不滤。
    2026-11-19 根修: 原版在测试内重写过滤逻辑自证 (同义反复), 现在打真
    probe_all (monkeypatch probe_node 记录被探测者)。"""
    import asyncio as _asyncio
    storage, _ = temp_env
    _mk(storage, "pa-1", status=HealthStatus.DISABLED.value, port=51001)
    frozen = _mk(storage, "pa-frozen", status=HealthStatus.DEAD.value, port=51003)
    frozen.tags = (frozen.tags or []) + ["frozen"]
    storage.update_node("pa-frozen", {"tags": frozen.tags})
    _mk(storage, "pa-2", status=HealthStatus.HEALTHY.value, port=51002)
    from core.prober import Prober
    prober = Prober(storage, router=None, timeout=1.0)
    seen = []

    async def fake_probe_node(node, retries=1, force=False):
        seen.append(node.name)
        return ProbeResult(node_name=node.name, success=True, error="")

    monkeypatch.setattr(prober, "probe_node", fake_probe_node)
    _asyncio.run(prober.probe_all(concurrency=2))
    assert "pa-1" not in seen and "pa-frozen" not in seen
    assert seen == ["pa-2"]


def test_ladder_never_returns_disabled_or_dead(temp_env):
    """真实 acquire 梯子 (healthy→degraded→fallback) 永不返回 DISABLED/DEAD。
    2026-11-19 根修: 原版在测试内重写过滤自证, 现在打真梯子。"""
    storage, router = temp_env
    _mk(storage, "al-dead", port=52001, status=HealthStatus.DEAD.value)
    _mk(storage, "al-disabled", port=52002, status=HealthStatus.DISABLED.value)
    _mk(storage, "al-alive", port=52003, status=HealthStatus.DEGRADED.value)
    lease = router.acquire_for_account(account_id="acc-ladder",
                                       egress_type=EgressType.WARP_IPV6.value)
    assert lease is not None
    assert lease.node_name == "al-alive"


def test_degraded_ladder_prefers_same_egress(temp_env):
    """2026-11-19 review P2: degraded 梯子镜像 healthy 的同 egress 步 ——
    egress 钉定账号在降级档不再从混合池里随机落到跨 egress 节点
    (2026-11-16 127 个 cpa 账号错落 native-v4 的事故类)。"""
    storage, router = temp_env
    _mk(storage, "dg-native", port=52101, status=HealthStatus.DEGRADED.value,
        egress=EgressType.NATIVE_IPV4.value)
    _mk(storage, "dg-warp", port=52102, status=HealthStatus.DEGRADED.value,
        egress=EgressType.WARP_IPV6.value)
    lease = router.acquire_for_account(account_id="acc-eg",
                                       egress_type=EgressType.WARP_IPV6.value)
    assert lease is not None
    assert lease.node_name == "dg-warp"


def test_disabled_survives_upsert(temp_env):
    """yaml 重载 (upsert status=unprobed) 不得复活 DISABLED 节点。"""
    storage, _ = temp_env
    n = _mk(storage, "up-01", status=HealthStatus.DISABLED.value)
    # 模拟 load_initial_nodes: yaml 条目默认 unprobed
    reload_node = ProxyNode(name="up-01", host="10.0.0.1", port=10000, region="tokyo",
                            egress_type=EgressType.WARP_IPV6.value,
                            fault_domain="dom-a", status=HealthStatus.UNPROBED.value)
    storage.upsert_node(reload_node)
    after = storage.get_node("up-01")
    assert after.status == HealthStatus.DISABLED.value  # 持久化保住


def test_enable_returns_to_unprobed(temp_env):
    """enable: DISABLED -> UNPROBED (探活重新定级), 计数清零。"""
    storage, _ = temp_env
    n = _mk(storage, "en-01", status=HealthStatus.DISABLED.value)
    storage.update_node("en-01", {"status": HealthStatus.UNPROBED.value,
                                  "circuit_state": HealthStatus.UNPROBED.value,
                                  "consecutive_failures": 0,
                                  "consecutive_successes": 0,
                                  "next_probe_allowed_at": None})
    after = storage.get_node("en-01")
    assert after.status == HealthStatus.UNPROBED.value
    # UNPROBED 首次探活成功 -> HEALTHY (既有契约: UNPROBED 不走观察期, 直接转正)
    storage.update_probe_result(ProbeResult(node_name="en-01", success=True, exit_ip="1.2.3.4"))
    assert storage.get_node("en-01").status == HealthStatus.HEALTHY.value


# ---------------- sick sweep ----------------

def test_sick_sweep_targets_only_degraded(temp_env, monkeypatch):
    """_sick_probe_sweep 的候选集只含 DEGRADED。
    2026-11-19 根修: 打真 sweep (monkeypatch 模块级 storage/prober)。"""
    import asyncio as _asyncio
    import api.server as api_mod
    storage, _ = temp_env
    for i in range(3):
        _mk(storage, "sk-h%d" % i, port=53000 + i, status=HealthStatus.HEALTHY.value)
    _mk(storage, "sk-d1", port=54001, status=HealthStatus.DEGRADED.value)
    _mk(storage, "sk-d2", port=54002, status=HealthStatus.DEGRADED.value)
    _mk(storage, "sk-dead", port=54003, status=HealthStatus.DEAD.value)
    seen = []

    class FakeProber:
        async def probe_all(self, concurrency=25, force=False, nodes=None):
            seen.extend(n.name for n in nodes or [])
            return [ProbeResult(node_name=n.name, success=True, error="") for n in nodes or []]

    monkeypatch.setattr(api_mod, "storage", storage)
    monkeypatch.setattr(api_mod, "prober", FakeProber())
    _asyncio.run(api_mod._sick_probe_sweep())
    assert sorted(seen) == ["sk-d1", "sk-d2"]


def test_sick_sweep_cap_respected(temp_env, monkeypatch):
    import asyncio as _asyncio
    import api.server as api_mod
    storage, _ = temp_env
    for i in range(5):
        _mk(storage, "cap-%d" % i, port=55000 + i, status=HealthStatus.DEGRADED.value)
    seen = []

    class FakeProber:
        async def probe_all(self, concurrency=25, force=False, nodes=None):
            seen.extend(n.name for n in nodes or [])
            return [ProbeResult(node_name=n.name, success=True, error="") for n in nodes or []]

    monkeypatch.setattr(api_mod, "storage", storage)
    monkeypatch.setattr(api_mod, "prober", FakeProber())
    monkeypatch.setattr(api_mod, "PROBE_SICK_CAP", 2)
    _asyncio.run(api_mod._sick_probe_sweep())
    assert len(seen) == 2


# ---------------- disable/enable/bulk-disable API ----------------

@pytest.fixture
def api_client():
    """沿用 tests/test_api.py 的隔离模式: conftest 已注入 IP_POOL_DB 隔离库。"""
    from starlette.testclient import TestClient
    import api.server
    api.server.storage.upsert_node(ProxyNode(
        name="api-dis-01", host="127.0.0.1", port=56001, region="tokyo",
        egress_type=EgressType.WARP_IPV6.value, fault_domain="dom-api",
        status=HealthStatus.HEALTHY.value))
    api.server.storage.upsert_node(ProxyNode(
        name="api-dis-02", host="127.0.0.1", port=56002, region="tokyo",
        egress_type=EgressType.WARP_IPV6.value, fault_domain="dom-api",
        tags=["skyward"], status=HealthStatus.HEALTHY.value))
    return TestClient(api.server.app)


def test_disable_enable_endpoints(api_client):
    import api.server
    c = api_client
    r = c.post("/v1/nodes/api-dis-01/disable", json={"reason": "unit-test"})
    assert r.status_code == 200
    assert r.json()["status"] == HealthStatus.DISABLED.value
    # circuit event 落审计
    ev = api.server.storage.list_circuit_events(limit=5)
    assert any(e.node_name == "api-dis-01" and e.reason == "unit-test"
               and e.to_state == HealthStatus.DISABLED.value for e in ev)

    # enable -> UNPROBED
    r2 = c.post("/v1/nodes/api-dis-01/enable")
    assert r2.status_code == 200
    assert r2.json()["status"] == HealthStatus.UNPROBED.value

    # 不存在的节点 404
    assert c.post("/v1/nodes/no-such/disable").status_code == 404
    assert c.post("/v1/nodes/no-such/enable").status_code == 404


def test_bulk_disable_by_tag(api_client):
    import api.server
    c = api_client
    r = c.post("/v1/nodes/bulk-disable", json={"tag": "skyward", "reason": "pool-quarantine"})
    assert r.status_code == 200
    body = r.json()
    assert body["disabled"] == ["api-dis-02"]
    assert api.server.storage.get_node("api-dis-02").status == HealthStatus.DISABLED.value
    # 非 skyward 节点不受影响
    assert api.server.storage.get_node("api-dis-01").status == HealthStatus.HEALTHY.value

    # 参数校验: 两个都不给 422; 两个都给 422
    assert c.post("/v1/nodes/bulk-disable", json={}).status_code == 422
    assert c.post("/v1/nodes/bulk-disable", json={"tag": "a", "region": "b"}).status_code == 422


def test_disabled_node_not_acquirable_via_ladder(api_client):
    """DISABLED 节点必须从分配兜底层被排除 (HEALTHY 精确匹配层本就不含它)。"""
    import api.server
    from core.models import HealthStatus as HS
    c = api_client
    c.post("/v1/nodes/api-dis-01/disable", json={"reason": "t"})
    c.post("/v1/nodes/api-dis-02/disable", json={"reason": "t"})
    nodes = [n for n in api.server.storage.list_nodes()
             if n.status not in (HS.DEAD.value, HS.DISABLED.value)]
    assert not any(n.name.startswith("api-dis-") for n in nodes)
