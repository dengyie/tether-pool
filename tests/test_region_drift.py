"""
tests/test_region_drift.py - gen 节点区域标签纠偏 (R1/R2) 回归

R2 核心: update_probe_result 成功事务按 CF trace loc= 校正 gen 池 region
(us->us-west, 其余小写 ISO), 与 exit_ip 同事务; 仅限 tags 含 genproxy 的节点。
R1/R3 辅助: region_from_country 映射 + ProbeResult 向后兼容。
"""
import os
import sqlite3
import tempfile

import pytest

from core.models import EgressType, HealthStatus, ProbeResult, ProxyNode
from core.storage import Storage, region_from_country


@pytest.fixture
def storage():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    s = Storage(db_path=db_path)
    yield s
    if os.path.exists(db_path):
        from tests.conftest import cleanup_sqlite_artifacts
        cleanup_sqlite_artifacts(db_path)


def make_gen(name="clash-701-gen-0001", region="us-west"):
    return ProxyNode(
        name=name, host="127.0.0.1", port=21001, protocol="socks5h",
        region=region, egress_type=EgressType.CLASH_PROXY.value,
        tags=["clash", "bridge", "http", "genproxy"],
        relay_mode="direct_only", fault_domain="huawei-gen-bridge",
        provider_region="us-west",
    )


# ---- region_from_country 映射 ----

def test_region_from_country_mapping():
    assert region_from_country("us") == "us-west"
    assert region_from_country("US") == "us-west"
    assert region_from_country("de") == "de"
    assert region_from_country("DE") == "de"
    assert region_from_country("ca") == "ca"


def test_region_from_country_invalid_is_none():
    assert region_from_country(None) is None
    assert region_from_country("") is None
    assert region_from_country("   ") is None
    assert region_from_country("usa") is None      # 非两字母
    assert region_from_country("123") is None      # 非字母
    assert region_from_country(42) is None         # 非字符串


# ---- ProbeResult 向后兼容 ----

def test_probe_result_backward_compatible():
    r = ProbeResult(node_name="x", success=True, exit_ip="1.2.3.4", latency_ms=10.0)
    assert r.country_code is None
    r2 = ProbeResult(node_name="x", success=True, country_code="DE")
    assert r2.country_code == "DE"


# ---- R2: update_probe_result 成功事务写 region ----

def test_gen_node_region_corrected_on_probe(storage):
    storage.upsert_node(make_gen())
    res = ProbeResult(node_name="clash-701-gen-0001", success=True,
                      exit_ip="5.1.2.3", latency_ms=100.0, country_code="DE")
    storage.update_probe_result(res)

    n = storage.get_node("clash-701-gen-0001")
    assert n.region == "de"
    assert n.provider_region == "de"
    assert "geo-fixed" in n.tags
    assert n.exit_ip == "5.1.2.3"
    assert n.status == HealthStatus.HEALTHY.value


def test_gen_node_us_keeps_us_west(storage):
    storage.upsert_node(make_gen())
    res = ProbeResult(node_name="clash-701-gen-0001", success=True,
                      exit_ip="23.1.2.3", latency_ms=100.0, country_code="US")
    storage.update_probe_result(res)

    n = storage.get_node("clash-701-gen-0001")
    assert n.region == "us-west"
    assert "geo-fixed" not in n.tags


def test_gen_node_no_country_code_untouched(storage):
    """探活失败回填/无 loc (旧格式) -> 不动 region。"""
    storage.upsert_node(make_gen())
    res = ProbeResult(node_name="clash-701-gen-0001", success=True,
                      exit_ip="5.1.2.3", latency_ms=100.0, country_code=None)
    storage.update_probe_result(res)

    n = storage.get_node("clash-701-gen-0001")
    assert n.region == "us-west"
    assert n.provider_region == "us-west"
    assert "geo-fixed" not in n.tags


def test_non_gen_node_region_not_touched(storage):
    """非 gen 池 (sub/pigeonw/teleapi) region 不在探活事务里动 —— sub 池每日
    刷新自带 region_of, pigeonw premium / teleapi 出界。"""
    node = ProxyNode(
        name="clash-501-sub-001", host="127.0.0.1", port=25001,
        protocol="socks5h", region="de", egress_type=EgressType.CLASH_PROXY.value,
        tags=["clash", "bridge", "sub"],
    )
    storage.upsert_node(node)
    res = ProbeResult(node_name="clash-501-sub-001", success=True,
                      exit_ip="5.1.2.3", latency_ms=100.0, country_code="US")
    storage.update_probe_result(res)

    n = storage.get_node("clash-501-sub-001")
    assert n.region == "de"
    assert "geo-fixed" not in n.tags


def test_pigeonw_node_region_not_touched(storage):
    node = ProxyNode(
        name="clash-304-pigeonw-04", host="127.0.0.1", port=12056,
        protocol="socks5h", region="us-west", egress_type=EgressType.CLASH_PROXY.value,
        tags=["clash", "bridge", "pigeonw"],
    )
    storage.upsert_node(node)
    res = ProbeResult(node_name="clash-304-pigeonw-04", success=True,
                      exit_ip="5.1.2.3", latency_ms=100.0, country_code="CA")
    storage.update_probe_result(res)

    n = storage.get_node("clash-304-pigeonw-04")
    assert n.region == "us-west"


def test_region_drift_metric_recorded(storage):
    storage.upsert_node(make_gen())
    res = ProbeResult(node_name="clash-701-gen-0001", success=True,
                      exit_ip="5.1.2.3", latency_ms=100.0, country_code="DE")
    storage.update_probe_result(res)

    conn = sqlite3.connect(storage.db_path)
    row = conn.execute(
        "SELECT value FROM runtime_metrics WHERE name = 'ip_pool_region_drift_fixed_total'"
    ).fetchall()
    conn.close()
    assert row, "region drift metric not recorded"
    assert row[0][0] == 1.0


def test_drift_correction_is_stable_not_flapping(storage):
    """R2 保真语义: 校正一次后, 后续探活 loc 一致时不再触发 (metrics 不重复累加
    且 tags 只有一份 geo-fixed)。"""
    storage.upsert_node(make_gen())
    for _ in range(3):
        storage.update_probe_result(ProbeResult(
            node_name="clash-701-gen-0001", success=True,
            exit_ip="5.1.2.3", latency_ms=100.0, country_code="DE"))
    n = storage.get_node("clash-701-gen-0001")
    assert n.region == "de"
    assert n.tags.count("geo-fixed") == 1

    conn = sqlite3.connect(storage.db_path)
    total = conn.execute(
        "SELECT COALESCE(SUM(value),0) FROM runtime_metrics WHERE name = 'ip_pool_region_drift_fixed_total'"
    ).fetchone()[0]
    conn.close()
    assert total == 1.0


# ---- geo-fixed 重启护栏: load_initial_nodes 以 DB 权威值覆盖 yaml 滞后 region ----

def test_startup_geo_fixed_region_taken_from_db(tmp_path, monkeypatch, isolate_api_storage):
    """yaml 快照滞后于探活纠偏时, 重启不得把旧 region 写回 (upsert ON CONFLICT
    无条件覆盖 region, 滞后标签会产生错误路由窗口)。带 geo-fixed 标记的节点,
    load_initial_nodes 必须以 DB 权威值覆盖 yaml 条目。"""
    import yaml
    import api.server
    storage = isolate_api_storage

    # DB 事实源: gen 节点已被探活纠偏为 de 并带 geo-fixed
    db_node = ProxyNode(
        name="clash-701-gen-0001", host="127.0.0.1", port=21001,
        protocol="socks5h", region="de", egress_type=EgressType.CLASH_PROXY.value,
        tags=["clash", "bridge", "http", "genproxy", "geo-fixed"],
        relay_mode="direct_only", fault_domain="huawei-gen-bridge",
        provider_region="de",
    )
    storage.upsert_node(db_node)
    # yaml 配置源: 快照仍滞后为 us-west
    cfg_path = tmp_path / "nodes.yaml"
    cfg = {"nodes": [
        {"name": "clash-701-gen-0001", "host": "127.0.0.1", "port": 21001,
         "region": "us-west", "provider_region": "us-west",
         "egress_type": EgressType.CLASH_PROXY.value,
         "tags": ["clash", "bridge", "http", "genproxy"]},
    ]}
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg_path))
    monkeypatch.setattr(api.server, "PRUNE_MISSING_NODES", False)

    api.server.load_initial_nodes()

    n = storage.get_node("clash-701-gen-0001")
    assert n.region == "de", "startup must not overwrite geo-fixed region with stale yaml value"
    assert n.provider_region == "de"


def test_startup_non_geo_fixed_node_respects_yaml(tmp_path, monkeypatch, isolate_api_storage):
    """无 geo-fixed 标记的节点维持原语义: yaml 为权威 (运维改 region 生效)。"""
    import yaml
    import api.server
    storage = isolate_api_storage

    storage.upsert_node(ProxyNode(
        name="plain-node", host="127.0.0.1", port=21002,
        protocol="socks5h", region="us-west",
        egress_type=EgressType.CLASH_PROXY.value, tags=["clash"],
    ))
    cfg_path = tmp_path / "nodes.yaml"
    cfg = {"nodes": [
        {"name": "plain-node", "host": "127.0.0.1", "port": 21002,
         "region": "eu-west", "egress_type": EgressType.CLASH_PROXY.value,
         "tags": ["clash"]},
    ]}
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg_path))
    monkeypatch.setattr(api.server, "PRUNE_MISSING_NODES", False)

    api.server.load_initial_nodes()

    assert storage.get_node("plain-node").region == "eu-west"
