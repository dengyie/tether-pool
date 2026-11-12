"""tests/test_node_pipeline.py - 节点接入流水线单测 (纯离线, 不触网/不碰真实配置)."""
import argparse
import json
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import node_pipeline as np  # noqa: E402


def make_group(seq_start=52, count=3, protocol="http"):
    return {
        "source": "https://example.com/sub.yaml",
        "protocol": protocol,
        "clash_node_prefix": "🇺🇸T-",
        "tether_name_template": "clash-{seq:03d}-t-{idx:02d}",
        "tether_seq_start": seq_start,
        "region": "us-west",
        "tier": "standard",
        "egress_type": "clash_proxy",
        "tags": ["clash", "bridge", "t"],
        "relay_mode": "direct_only",
        "dialer_proxy": "[自建] AWS",
        "inject_into_groups": ["PROXY", "AUTO"],
        "legacy_groups_cleanup": [],
        "bridge_ports": {"declared": "11053-11055", "realized": "11053-11055"},
        "gateway_route": "region=us-west&type=clash_proxy",
        "nodes": [
            {"idx": i + 1, "name": f"🇺🇸T-{i+1:02d}", "type": protocol,
             "server": f"23.0.0.{i+1}", "port": 62000,
             "username": f"u{i}", "password": f"p{i}", "exit_ip": f"23.0.0.{i+1}"}
            for i in range(count)
        ],
    }


# ---------------------------------------------------------------- gen ↔ parse 往返

def test_gen_parse_roundtrip_full_fields():
    """gen_group_js 产物解析后须与 clean_nodes 全字段一致 (拦截字段清洗回归/type 缺失)."""
    group = make_group()
    script = "// anchor\n// ==================== Mihomo 节点兼容 ====================\n"
    script = np.upsert_js_block(script, "t", np.gen_group_js("t", group))
    deployed = np.parse_managed_nodes(script, "t")
    assert deployed is not None
    assert deployed == np.clean_nodes(group)
    # 白名单: 内部字段不得外泄, type/udp 必在
    for n in deployed:
        assert "idx" not in n and "exit_ip" not in n
        assert n["type"] == "http" and n["udp"] is False


def test_gen_uses_protocol_fallback_when_node_type_missing():
    """manifest 节点缺 type 时由 group.protocol 兜底 (mihomo 强校验, 缺 type 拒载)."""
    group = make_group()
    for n in group["nodes"]:
        del n["type"]
    script = "// x\n// ==================== Mihomo 节点兼容 ====================\n"
    script = np.upsert_js_block(script, "t", np.gen_group_js("t", group))
    deployed = np.parse_managed_nodes(script, "t")
    assert all(n["type"] == "http" for n in deployed)


# ---------------------------------------------------------------- upsert 幂等

def test_upsert_js_block_idempotent():
    group = make_group()
    script = "// anchor\n// ==================== Mihomo 节点兼容 ====================\n"
    once = np.upsert_js_block(script, "t", np.gen_group_js("t", group))
    twice = np.upsert_js_block(once, "t", np.gen_group_js("t", group))
    assert once == twice
    assert twice.count("NODE-PIPELINE:t:v1") == 2  # BEGIN + END 各一次


def test_upsert_js_block_missing_end_normalized():
    """历史嵌套/缺 END 的坏状态应被 strip 规范化为单一块 (不再抛错)."""
    group = make_group()
    script = np.upsert_js_block("// a\n", "t", np.gen_group_js("t", group))
    broken = script.replace(np.BLOCK_END.format(group="t") + "\n", "")  # 缺 END
    fixed = np.upsert_js_block(broken, "t", np.gen_group_js("t", group))
    assert fixed.count(np.BLOCK_BEGIN.format(group="t")) == 1
    assert fixed.count(np.BLOCK_END.format(group="t")) == 1
    assert np.parse_managed_nodes(fixed, "t") == np.clean_nodes(group)


def test_main_call_idempotent_and_lists_all_groups():
    manifest = {"groups": {"a": make_group(), "b": make_group()}}
    script = "function main(){\n  ensureHomeMeshNode(config);\n}"
    once = np.upsert_main_call(script, manifest)
    twice = np.upsert_main_call(once, manifest)
    assert once == twice
    assert "ensureNPGroup_a(config);" in twice and "ensureNPGroup_b(config);" in twice


# ---------------------------------------------------------------- 命名与端口

def test_tether_names_padding_and_seq_start():
    group = make_group(seq_start=52)
    assert np.tether_names(group) == ["clash-052-t-01", "clash-053-t-02", "clash-054-t-03"]


def test_tether_names_missing_seq_start_raises():
    group = make_group()
    del group["tether_seq_start"]
    with pytest.raises(SystemExit, match="tether_seq_start"):
        np.tether_names(group)


def test_next_free_ports_empty_and_overlap():
    assert np.next_free_ports({"groups": {}}, 3) == [11053, 11054, 11055]
    manifest = {"groups": {"g": {"bridge_ports": {"declared": "11073-11082", "realized": "11053-11072"}}}}
    assert np.next_free_ports(manifest, 2) == [11083, 11084]


def test_realized_ports_map_overrides_range():
    group = make_group()
    group["bridge_ports"]["realized_map"] = {"1": 11060, "2": 11061, "3": 11062}
    assert np.realized_ports(group) == [11060, 11061, 11062]
    group["bridge_ports"]["realized_map"] = {}
    assert np.realized_ports(group) == [11053, 11054, 11055]


# ---------------------------------------------------------------- 漂移判定

def test_js_block_drift_detects_field_change():
    group = make_group()
    script = "// a\n// ==================== Mihomo 节点兼容 ====================\n"
    script = np.upsert_js_block(script, "t", np.gen_group_js("t", group))
    assert np.js_block_drift("t", group, script) == []

    # 凭据轮换 → 漂移
    rotated = json.loads(json.dumps(group))
    rotated["nodes"][0]["password"] = "newpass"
    assert np.js_block_drift("t", rotated, script) == ["🇺🇸T-01"]

    # 块缺失 → 漂移
    assert np.js_block_drift("t", group, "// no block") == ["<block-missing:t>"]


def test_js_block_drift_detects_udp_and_type_regression():
    """重构覆盖清洗逻辑时 (丢 udp/type), 漂移判定必须仍能拦截."""
    group = make_group()
    script = "// a\n// ==================== Mihomo 节点兼容 ====================\n"
    script = np.upsert_js_block(script, "t", np.gen_group_js("t", group))
    # 模拟旧版生成物: 带 idx/exit_ip, 缺 udp
    dirty = json.loads(json.dumps(group))
    for n in dirty["nodes"]:
        n["idx"] = n["idx"]
        n["exit_ip"] = n["server"]
        n.pop("udp", None)
    raw = ",\n      ".join(json.dumps(n, ensure_ascii=False) for n in dirty["nodes"])
    begin = np.BLOCK_BEGIN.format(group="t")
    end = np.BLOCK_END.format(group="t")
    body = script.split(begin, 1)[1].split(end, 1)[1]
    script_dirty = script.split(begin, 1)[0] + begin + "\n  nodes: [\n      " + raw + "\n  ],\n" + end + body
    drift = np.js_block_drift("t", dirty, script_dirty)
    assert len(drift) == len(dirty["nodes"])  # 全部节点判漂移


# ---------------------------------------------------------------- 参数校验与容错 (review 二轮)

def test_import_rejects_invalid_group_name():
    """含连字符的组名会生成非法 JS 标识符, 必须在入口拒绝."""
    rc = np.cmd_import(argparse.Namespace(
        source="x.yaml", group="my-vendor", region="us-west", tier="standard",
        prefix=None, url_test_group=None, relay_group=None, relay_candidates=[],
        replace=False, dry_run=True, restart_verge=False))
    assert rc == 1


def test_heal_rebuilds_when_script_missing(tmp_path, monkeypatch):
    """Script.js 完全缺失 (Verge 重装场景) 时 heal 应从零重建而非崩溃."""
    group = make_group()
    manifest = {"groups": {"t": group}}
    mpath = tmp_path / "node_groups.yaml"
    mpath.write_text(yaml.dump(manifest))
    spath = tmp_path / "Script.js"  # 不创建
    mmerge = tmp_path / "Merge.yaml"
    mmerge.write_text(yaml.dump({"listeners": []}))
    npath = tmp_path / "nodes.yaml"
    tnames = np.tether_names(group)
    npath.write_text(yaml.dump({"nodes": [
        {"name": t, "host": "127.0.0.1", "port": p, "region": "us-west",
         "relay_mode": "direct_only"} for t, p in zip(tnames, [11053, 11054, 11055])]}))

    vdir = tmp_path / "verge" / "profiles"
    vdir.mkdir(parents=True)
    # 宿主脚本备份 (含 main/锚点) — heal 应自动恢复
    (vdir / "Script.js.bak-20261110").write_text(
        "function main(config = {}) {\n  ensureHomeMeshNode(config);\n  return config;\n}\n")
    monkeypatch.setattr(np, "VERGE_DIR", tmp_path / "verge")
    monkeypatch.setattr(np, "MANIFEST_PATH", mpath)
    monkeypatch.setattr(np, "SCRIPT_JS", spath)
    monkeypatch.setattr(np, "MERGE_YAML", mmerge)
    monkeypatch.setattr(np, "NODES_YAML", npath)
    live_px = {n["name"]: {"dialer-proxy": "[自建] AWS"} for n in group["nodes"]}
    members = ["[自建] AWS"] + [n["name"] for n in group["nodes"]]  # 模拟重组后: 注入生效
    live_px.update({"[自建] AWS": {}, "PROXY": {"all": list(members)}, "AUTO": {"all": list(members)}})
    monkeypatch.setattr(np, "mihomo_get", lambda p, timeout=12: (200, {"proxies": live_px}))
    monkeypatch.setattr(np, "pool_nodes", lambda: {
        t: {"port": p, "status": "healthy"} for t, p in zip(tnames, [11053, 11054, 11055])})
    monkeypatch.setattr(np, "listening_ports", lambda *a, **k: {11053, 11054, 11055})
    monkeypatch.setattr(np, "restart_tetherpool", lambda: True)

    rc = np.cmd_heal(argparse.Namespace(group="t", yes=True, force_js=False, restart_verge=False, realize=False))
    assert rc == 0
    assert spath.exists(), "Script.js 应被从零重建"
    rebuilt = spath.read_text()
    assert rebuilt.count("NODE-PIPELINE:t:v1") == 2  # BEGIN + END 各一次
    assert np.parse_managed_nodes(rebuilt, "t") == np.clean_nodes(group)


def test_realize_group_rewrites_on_drift(tmp_path, monkeypatch):
    """端口↔节点映射级联漂移时, realize 应回写 realized_map + nodes.yaml 端口."""
    group = make_group()
    manifest = {"groups": {"t": group}}
    mpath = tmp_path / "manifest.yaml"
    mpath.write_text(yaml.dump(manifest))
    npath = tmp_path / "nodes.yaml"
    tnames = np.tether_names(group)
    npath.write_text(yaml.dump({"nodes": [
        {"name": t, "host": "127.0.0.1", "port": p} for t, p in zip(tnames, [11053, 11054, 11055])]}))

    monkeypatch.setattr(np, "MANIFEST_PATH", mpath)
    monkeypatch.setattr(np, "NODES_YAML", npath)
    monkeypatch.setattr(np, "listening_ports", lambda *a, **k: {11053, 11054, 11055})
    exits = {11053: "23.0.0.2", 11054: "23.0.0.3", 11055: "23.0.0.1"}  # 级联漂移
    monkeypatch.setattr(np, "probe_bridge_exit", lambda p, timeout=18: exits[p])
    monkeypatch.setattr(np, "restart_tetherpool", lambda: True)

    rc, changed = np.realize_group(manifest, "t", [11053, 11054, 11055])
    assert rc == 0 and changed
    m = yaml.safe_load(open(mpath))
    assert m["groups"]["t"]["bridge_ports"]["realized_map"] == {"1": 11055, "2": 11053, "3": 11054}
    ports = {n["name"]: n["port"] for n in yaml.safe_load(open(npath))["nodes"]}
    assert [ports[t] for t in tnames] == [11055, 11053, 11054]


def test_realize_group_no_drift_is_noop(tmp_path, monkeypatch):
    group = make_group()
    manifest = {"groups": {"t": group}}
    mpath = tmp_path / "manifest.yaml"
    mpath.write_text(yaml.dump(manifest))
    monkeypatch.setattr(np, "MANIFEST_PATH", mpath)
    monkeypatch.setattr(np, "listening_ports", lambda *a, **k: {11053, 11054, 11055})
    monkeypatch.setattr(np, "probe_bridge_exit",
                        lambda p, timeout=18: f"23.0.0.{p - 11052}")  # 与声明一致
    rc, changed = np.realize_group(manifest, "t", [11053, 11054, 11055])
    assert rc == 0 and not changed
    assert "realized_map" not in yaml.safe_load(open(mpath))["groups"]["t"]["bridge_ports"]


# ---------------------------------------------------------------- check 容错

def _write_verge_group_env(tmp_path, monkeypatch, standalone_only=False):
    """搭建 check/heal 的离线环境: manifest + nodes.yaml + mock 池/活体."""
    group = make_group()
    if standalone_only:
        group["bridge_core"] = {"type": "standalone-mihomo"}
        group.pop("dialer_proxy", None)
        group.pop("inject_into_groups", None)
    manifest = {"groups": {"t": group}}
    mpath = tmp_path / "node_groups.yaml"
    mpath.write_text(yaml.dump(manifest))
    npath = tmp_path / "nodes.yaml"
    tnames = np.tether_names(group)
    ports = list(range(11053, 11053 + len(tnames)))
    npath.write_text(yaml.dump({"nodes": [
        {"name": t, "host": "127.0.0.1", "port": p, "region": "us-west",
         "relay_mode": "direct_only"} for t, p in zip(tnames, ports)]}))
    monkeypatch.setattr(np, "MANIFEST_PATH", mpath)
    monkeypatch.setattr(np, "NODES_YAML", npath)
    monkeypatch.setattr(np, "pool_nodes", lambda: {
        t: {"port": p, "status": "healthy"} for t, p in zip(tnames, ports)})
    monkeypatch.setattr(np, "listening_ports", lambda *a, **k: set(ports))
    return group, tnames, ports


def test_check_degrades_not_crashes_when_verge_broken(tmp_path, monkeypatch, capsys):
    """有受管组且 Verge 不可达: check 必须降级为 L2/L4 DRIFT (退出码 2), 不崩."""
    group, tnames, ports = _write_verge_group_env(tmp_path, monkeypatch)
    spath = tmp_path / "Script.js"
    spath.write_text("function main(config) { return config; }")

    def boom(*a, **k):
        raise RuntimeError("mihomo socket down")
    monkeypatch.setattr(np, "SCRIPT_JS", spath)
    monkeypatch.setattr(np, "MERGE_YAML", tmp_path / "Merge.yaml")
    monkeypatch.setattr(np, "read_script", boom)
    monkeypatch.setattr(np, "mihomo_get", boom)

    rc = np.cmd_check(argparse.Namespace())
    assert rc == 2, "受管组在 Verge 不可达时应报 DRIFT 而非崩溃"
    out = capsys.readouterr().out
    assert "Script.js 不可读" in out and "mihomo 活体不可达" in out
    assert "[DRIFT]" in out


def test_check_standalone_only_needs_no_verge(tmp_path, monkeypatch, capsys):
    """全 standalone 环境 (无 Verge): read_script/mihomo_get 都不可用, check 仍应全绿."""
    group, tnames, ports = _write_verge_group_env(tmp_path, monkeypatch, standalone_only=True)
    spath = tmp_path / "Script.js"  # 不创建

    def boom(*a, **k):
        raise RuntimeError("no verge here")
    monkeypatch.setattr(np, "SCRIPT_JS", spath)
    monkeypatch.setattr(np, "read_script", boom)
    monkeypatch.setattr(np, "mihomo_get", boom)

    rc = np.cmd_check(argparse.Namespace())
    assert rc == 0, "全 standalone 环境不依赖 Verge, check 应全绿"
    out = capsys.readouterr().out
    assert "[SKIP ] L2" in out and "自有桥核 listener" in out
