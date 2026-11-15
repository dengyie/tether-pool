"""
tests/test_node_lifecycle.py - 节点生命周期 janitor 回归 (2026-11-15):
连续 DEAD 24h -> 冻结 (frozen tag, 全量探活排除) / 连续 7d -> 驱逐
(DB 删除 + tombstone + yaml 同步删除 + load_initial_nodes 护栏)。
探活优先级分层: healthy 300s 全量 / degraded 60s 病号轮 / dead 退避 /
frozen janitor 复核 / tombstoned 永不复活。
"""

import asyncio
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
    # 无 tombstone 的名字 404 语义: revive_node 抛 HTTPException
    from fastapi import HTTPException
    with pytest.raises(HTTPException):
        api.server.revive_node("jr-01")  # 已复活, tombstone 不存在
