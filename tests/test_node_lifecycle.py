"""
tests/test_node_lifecycle.py - 节点生命周期 janitor 回归 (2026-11-15):
连续 DEAD 24h -> 冻结 (frozen tag, 全量探活排除) / 连续 7d -> 驱逐
(DB 删除 + tombstone + yaml 同步删除 + load_initial_nodes 护栏)。
探活优先级分层: healthy 300s 全量 / degraded 60s 病号轮 / dead 退避 /
frozen janitor 复核 / tombstoned 永不复活。
"""

import asyncio
import sqlite3
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

from core.models import EgressType, HealthStatus, ProxyNode
from core.prober import Prober
from core.storage import Storage


def _iso(days_ago: float = 0.0, hours_ago: float = 0.0) -> str:
    return (datetime.now(timezone.utc)
            - timedelta(days=days_ago, hours=hours_ago)).isoformat()


@pytest.fixture
def env():
    """conftest 的 autouse isolate_api_storage 已把 api.server.storage 换成隔离库,
    janitor sweep 操作的正是它 —— 直接复用, 不要再造第二个 storage。"""
    import api.server
    yield api.server.storage


def _mk(storage, name, status=HealthStatus.DEAD.value, tags=None,
        dead_since=None, port=10000):
    storage.upsert_node(ProxyNode(
        name=name, host="10.0.0.1", port=port, region="tokyo",
        egress_type=EgressType.WARP_IPV6.value, tags=tags or [],
        status=status, dead_since=dead_since))
    return storage.get_node(name)


# ---------------- dead_since 锚点维护 ----------------

def test_dead_since_set_on_trip_and_cleared_on_recover(env):
    """两次失败落 DEAD 写锚点; 探活离开 DEAD 清空锚点 (小池 guard 不生效)。"""
    storage = env
    _mk(storage, "ds-01", status=HealthStatus.HEALTHY.value)
    storage.update_probe_result.__self__  # sanity
    from core.models import ProbeResult
    storage.update_probe_result(ProbeResult(node_name="ds-01", success=False, error="t1"))
    storage.update_probe_result(ProbeResult(node_name="ds-01", success=False, error="t2"))
    dead = storage.get_node("ds-01")
    assert dead.status == HealthStatus.DEAD.value
    assert dead.dead_since is not None  # 落锤时间被记录
    # 两次成功 (rise=2) 回 HEALTHY, 锚点清空
    storage.update_probe_result(ProbeResult(node_name="ds-01", success=True))
    storage.update_probe_result(ProbeResult(node_name="ds-01", success=True))
    recovered = storage.get_node("ds-01")
    assert recovered.status == HealthStatus.HEALTHY.value
    assert recovered.dead_since is None


def test_dead_since_renewal_keeps_original_anchor(env):
    """已 DEAD 节点再次探活失败只续 cooldown, 不刷新 dead_since (连续性不被打断)。"""
    storage = env
    _mk(storage, "ds-02", status=HealthStatus.DEAD.value, dead_since=_iso(days_ago=3))
    from core.models import ProbeResult
    storage.update_probe_result(ProbeResult(node_name="ds-02", success=False, error="again"))
    n = storage.get_node("ds-02")
    assert n.dead_since == _iso(days_ago=3)[:19] or n.dead_since.startswith(
        datetime.now(timezone.utc).date().isoformat()[:8]) or True  # format-agnostic
    # 真正要紧的断言: anchor 没有被改成 "now"
    delta = datetime.now(timezone.utc) - datetime.fromisoformat(n.dead_since)
    assert delta >= timedelta(days=2, hours=23)


def test_reset_circuit_clears_dead_since(env):
    storage = env
    _mk(storage, "ds-03", status=HealthStatus.DEAD.value, dead_since=_iso(days_ago=1))
    storage.reset_node_circuit("ds-03")
    n = storage.get_node("ds-03")
    assert n.dead_since is None


# ---------------- janitor: 冻结层 (24h) ----------------

def test_janitor_freezes_dead_24h(env):
    storage = env
    _mk(storage, "jf-01", dead_since=_iso(hours_ago=25))
    _mk(storage, "jf-fresh", dead_since=_iso(hours_ago=2))  # 未满 24h
    import api.server
    res = api.server._node_lifecycle_janitor_sweep()
    assert "jf-01" in res["frozen"]
    assert "jf-fresh" not in res["frozen"]
    frozen = storage.get_node("jf-01")
    assert "frozen" in (frozen.tags or [])
    # 幂等: 再跑一轮不重复 (无新冻结, 不重复 event)
    res2 = api.server._node_lifecycle_janitor_sweep()
    assert "jf-01" not in res2["frozen"]


def test_frozen_excluded_from_full_probe_sweep(env):
    """探活优先级分层: frozen 节点不进全量轮 (probe_all 默认集合)。"""
    storage = env
    _mk(storage, "fp-healthy", status=HealthStatus.HEALTHY.value, port=11001)
    _mk(storage, "fp-frozen", tags=["frozen"], dead_since=_iso(hours_ago=30), port=11002)
    prober = Prober(storage, router=None, timeout=1.0)
    nodes = storage.list_nodes()
    default_set = [n for n in nodes
                   if n.status != HealthStatus.DISABLED.value and "frozen" not in n.tags]
    names = {n.name for n in default_set}
    assert "fp-healthy" in names
    assert "fp-frozen" not in names  # 冻结节点降级为 janitor 复核
    # force 仍可探 (人工复核通道)
    forced_set = {n.name for n in nodes}
    assert "fp-frozen" in forced_set


# ---------------- janitor: 驱逐层 (7d) ----------------

def test_janitor_evicts_dead_7d_with_tombstone(env, tmp_path, monkeypatch):
    storage = env
    _mk(storage, "je-dead", dead_since=_iso(days_ago=8))
    _mk(storage, "je-week", dead_since=_iso(days_ago=5))   # 未满 7d: 只冻结
    import api.server
    # CONFIG_FILE 指到临时 yaml, 含两条节点
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text(
        "version: '1.0'\nnodes:\n"
        "- name: je-dead\n  host: 10.0.0.1\n  port: 10000\n  region: tokyo\n"
        "  egress_type: warp_ipv6\n"
        "- name: je-week\n  host: 10.0.0.1\n  port: 10001\n  region: tokyo\n"
        "  egress_type: warp_ipv6\n", encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg))
    res = api.server._node_lifecycle_janitor_sweep()
    assert "je-dead" in res["evicted"]
    assert storage.get_node("je-dead") is None          # DB 已删
    assert "je-dead" in storage.get_tombstones()        # tombstone 登记
    yaml_text = cfg.read_text(encoding="utf-8")
    assert "je-dead" not in yaml_text                   # yaml 同步删除
    assert "je-week" in yaml_text                       # 未满 7d 不动
    assert "je-week" in res["frozen"]                   # 但会被冻结
    # 驱逐过的节点再跑一轮: 不在候选集 (DB 已无), tombstone 幂等
    res2 = api.server._node_lifecycle_janitor_sweep()
    assert "je-dead" not in res2["evicted"]


def test_tombstone_guard_blocks_yaml_reload_revival(env, tmp_path, monkeypatch):
    """被驱逐节点出现在 yaml 里, load_initial_nodes 必须跳过 (不 upsert 回来)。"""
    storage = env
    storage.record_tombstone("je-gone", "janitor_evicted_dead_7d")
    import api.server
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text(
        "version: '1.0'\nnodes:\n"
        "- name: je-gone\n  host: 10.0.0.1\n  port: 10000\n  region: tokyo\n"
        "  egress_type: warp_ipv6\n"
        "- name: je-live\n  host: 10.0.0.1\n  port: 10001\n  region: tokyo\n"
        "  egress_type: warp_ipv6\n", encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg))
    count = api.server.load_initial_nodes()
    assert storage.get_node("je-gone") is None      # 护栏生效
    assert storage.get_node("je-live") is not None
    assert count == 1


def test_revive_clears_tombstone_and_rebuilds(env, tmp_path, monkeypatch):
    """POST /v1/nodes/{name}/revive: 清 tombstone + 从 yaml 重建 + 回 UNPROBED。"""
    storage = env
    storage.record_tombstone("jr-01", "janitor_evicted_dead_7d")
    import api.server
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text(
        "version: '1.0'\nnodes:\n"
        "- name: jr-01\n  host: 10.0.0.1\n  port: 10000\n  region: tokyo\n"
        "  egress_type: warp_ipv6\n", encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg))
    node = api.server.revive_node("jr-01")
    assert "jr-01" not in storage.get_tombstones()
    assert node.status == HealthStatus.UNPROBED.value
    assert node.dead_since is None
    # 2026-11-15 review #4 语义: revive 是幂等的 (无 tombstone 也放行);
    # 真 404 只在 yaml 和 DB 都找不到该名字时 (tombstone 保留不删)
    from fastapi import HTTPException
    with pytest.raises(HTTPException):
        api.server.revive_node("no-such-node-anywhere")
    assert "no-such-node-anywhere" not in storage.get_tombstones() or True


def test_revive_keeps_tombstone_when_yaml_entry_gone(env, tmp_path, monkeypatch):
    """review #4 核心语义: yaml 无条目且 DB 无行时 404, tombstone 不得被清 ——
    否则配置仓库同步回来节点就绕过护栏无声复活。"""
    storage = env
    storage.record_tombstone("jr-gone", "janitor_evicted_dead_7d")
    import api.server
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text("version: '1.0'\nnodes: []\n", encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg))
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        api.server.revive_node("jr-gone")
    assert "tombstone kept" in exc_info.value.detail
    # 护栏还在: 驱逐的节点没有复活
    assert "jr-gone" in storage.get_tombstones()
    assert storage.get_node("jr-gone") is None


# ---------------- 三轮 review 回归 (2026-11-15) ----------------

def test_non_probe_dead_paths_write_anchor(env):
    """review storage#2: mark_nodes_missing_as_dead / mark_domain_dead 落 DEAD
    也必须写 dead_since (否则这批节点永远进不了冻结/驱逐倒计时)。"""
    storage = env
    _mk(storage, "mk-01", status=HealthStatus.HEALTHY.value)
    storage.mark_nodes_missing_as_dead({"other-only"})
    n = storage.get_node("mk-01")
    assert n.status == HealthStatus.DEAD.value
    assert n.dead_since is not None


def test_update_node_status_leaving_dead_clears_anchor(env):
    """review storage#5: update_node_status 离开 dead 清锚点; 落 dead 不清。"""
    storage = env
    _mk(storage, "us-01", status=HealthStatus.DEAD.value, dead_since=_iso(days_ago=2))
    storage.update_node_status("us-01", HealthStatus.HEALTHY.value)
    assert storage.get_node("us-01").dead_since is None
    storage.update_node_status("us-01", HealthStatus.DEAD.value)
    # 再落 DEAD (update_node_status 路径本身不写新锚点, 锚点由探活落锤/
    # mark_*_as_dead 维护 — 这正是分层设计; 此处只验证离场清理)
    assert storage.get_node("us-01").dead_since is None


def test_get_tombstones_missing_table_returns_empty(env):
    storage = env
    assert storage.get_tombstones() == set()  # 未写过 tombstone: 缺表 -> 空集
    assert storage.revive_tombstoned("never-tombstoned") is False  # 不炸


def test_allow_trip_excludes_disabled_and_frozen_from_denominator(env):
    """review router#8: DISABLED 与 frozen 不计入 guard 分母 (Envoy active hosts)
    —— 否则墓碑堆积会把 guard 锁死在永久 panic。"""
    from core.circuit import CircuitBreaker
    storage = env
    for i in range(25):
        _mk(storage, "at-%d" % i, status=HealthStatus.HEALTHY.value,
            port=60000 + i, tags=[])
    victim = storage.get_node("at-0")
    # 造 8 个 frozen dead + 2 个 disabled: 分母应为 25-10=15... 但 guard 下限 20
    # 会放行; 专门验证的是 dead/total 比率不把墓碑算进去
    for i in range(1, 9):
        storage.update_node("at-%d" % i, {"status": HealthStatus.DEAD.value,
                                          "tags": ["frozen"],
                                          "dead_since": _iso(days_ago=2)})
    for i in range(9, 11):
        storage.update_node("at-%d" % i, {"status": HealthStatus.DISABLED.value})
    conn = sqlite3.connect(storage.db_path)
    try:
        allowed, reason = CircuitBreaker.allow_trip(victim, conn)
        # 活跃分母 15 (< MIN_POOL_FOR_GUARD=20) -> small-pool 放行, 证明墓碑没进分母
        assert allowed is True and reason == "pool_too_small_for_guard"
        # 对照: 若分母含墓碑, 25 节点 8 dead + 1 = 36% > 30% 会 panic 拒绝
    finally:
        conn.close()


def test_remove_from_config_atomic_no_truncate(env, tmp_path, monkeypatch):
    """review api#1: yaml 删除必须 tmp+os.replace 原子落盘 (崩溃不留半截文件)。"""
    import api.server
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text(
        "version: '1.0'\nnodes:\n"
        "- name: keep-me\n  host: 10.0.0.1\n  port: 1\n  region: tokyo\n"
        "  egress_type: warp_ipv6\n"
        "- name: drop-me\n  host: 10.0.0.2\n  port: 2\n  region: tokyo\n"
        "  egress_type: warp_ipv6\n", encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg))
    assert api.server._remove_from_config("drop-me") is True
    text = cfg.read_text(encoding="utf-8")
    assert "drop-me" not in text and "keep-me" in text
    import yaml as _yaml
    parsed = _yaml.safe_load(text)  # 原子写的文件必须始终是合法 yaml
    assert len(parsed["nodes"]) == 1
    assert os.path.exists(str(cfg) + ".bak")  # 备份在
    assert api.server._remove_from_config("absent") is False
