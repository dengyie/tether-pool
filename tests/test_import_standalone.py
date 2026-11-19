"""tests/test_import_standalone.py - standalone 桥核导入器单测 (纯离线, 不触网/不碰真实配置)."""
import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import import_standalone as isa  # noqa: E402


# ---------------------------------------------------------------- relay 凭据外置

def test_load_relay_definition_reads_local_file(tmp_path, monkeypatch):
    relay_file = tmp_path / "relay-nodes.local.yaml"
    relay_file.write_text(yaml.dump({"relay_nodes": [
        {"name": "[自建] Test-Relay", "type": "vless", "server": "1.2.3.4", "port": 443,
         "uuid": "test-uuid"},
    ]}))
    monkeypatch.setattr(isa, "RELAY_LOCAL_YAML", relay_file)

    got = isa.load_relay_definition("[自建] Test-Relay")
    assert got is not None and got["uuid"] == "test-uuid"
    assert isa.load_relay_definition("不存在") is None


def test_load_relay_definition_missing_or_bad_file_is_none(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(isa, "RELAY_LOCAL_YAML", tmp_path / "no-such-file.yaml")
    assert isa.load_relay_definition("anything") is None

    bad = tmp_path / "bad.yaml"
    bad.write_text("{ key: [unclosed")
    monkeypatch.setattr(isa, "RELAY_LOCAL_YAML", bad)
    assert isa.load_relay_definition("anything") is None  # 坏文件按缺失降级, 不抛异常
    assert "解析失败" in capsys.readouterr().out


def test_cmd_relay_missing_def_errors_without_guessing(tmp_path, monkeypatch, capsys):
    """桥核配置缺中转节点且本地凭据文件无同名定义: 必须报错退出, 不硬编码凭据."""
    (tmp_path / "config").mkdir()
    bridge = tmp_path / "config" / "g-bridge.yaml"
    bridge.write_text(yaml.dump({"proxies": [{"name": "node-1", "type": "http",
                                              "server": "s", "port": 1}]}))
    monkeypatch.setattr(isa, "REPO", tmp_path)
    monkeypatch.setattr(isa, "RELAY_LOCAL_YAML", tmp_path / "no-such.yaml")

    rc = isa.cmd_relay(argparse.Namespace(group="g", mode="on",
                                          relay_node="[自建] Missing-Relay"))
    assert rc == 2
    cfg = yaml.safe_load(open(bridge))
    assert all("dialer-proxy" not in p for p in cfg["proxies"]), "失败时不得半接线"


def test_cmd_relay_off_removes_dialer(tmp_path, monkeypatch):
    (tmp_path / "config").mkdir()
    bridge = tmp_path / "config" / "g-bridge.yaml"
    bridge.write_text(yaml.dump({"proxies": [
        {"name": "node-1", "type": "http", "server": "s", "port": 1, "dialer-proxy": "r"},
        {"name": "node-2", "type": "http", "server": "s", "port": 2, "dialer-proxy": "r"},
    ]}))
    monkeypatch.setattr(isa, "REPO", tmp_path)

    rc = isa.cmd_relay(argparse.Namespace(group="g", mode="off", relay_node="r"))
    assert rc == 0
    cfg = yaml.safe_load(open(bridge))
    assert all("dialer-proxy" not in p for p in cfg["proxies"])


# ---------------------------------------------------------------- import 失败清理

def test_cmd_import_cleans_bridge_yaml_on_validation_failure(tmp_path, monkeypatch):
    """mihomo -t 校验失败: 残留无效核配置会让 keeper 无限退避重启 → 必须清理."""
    import subprocess as sp

    monkeypatch.setattr(isa, "REPO", tmp_path)
    (tmp_path / "config").mkdir(exist_ok=True)

    # 只 mock np 侧文件路径与外部命令; 其余走真实函数
    manifest = {"groups": {}, "updated": "x"}
    monkeypatch.setattr(isa.np, "load_manifest", lambda: manifest)
    saved = {}
    monkeypatch.setattr(isa.np, "save_manifest", lambda m: saved.update(m))
    monkeypatch.setattr(isa.np, "next_free_ports", lambda m, n: list(range(11101, 11101 + n)))
    ny = {"nodes": []}
    monkeypatch.setattr(isa.np, "next_free_seq", lambda ny_, m: 300)
    monkeypatch.setattr(isa.np, "NODES_YAML", tmp_path / "nodes.yaml")
    (tmp_path / "nodes.yaml").write_text(yaml.dump(ny))

    monkeypatch.setattr(isa, "load_source", lambda s: {"proxies": [
        {"name": f"n{i}", "type": "http", "server": "brd.superproxy.io", "port": 44445,
         "username": f"u-ip-1.2.3.{i}", "password": "p"} for i in range(2)]})
    monkeypatch.setattr(sp, "run", lambda *a, **k: sp.CompletedProcess([], 1, stderr="boom"))

    rc = isa.cmd_import(argparse.Namespace(
        source="x", group="gtest", region="us-west", tier="standard", prefix=None,
        filter_type="http", filter_server="", tags="residential", port_base=None,
        fault_domain=None, replace=False, dry_run=False))
    assert rc == 2
    assert not (tmp_path / "config/gtest-bridge.yaml").exists(), "校验失败必须清理残留核配置"
    assert manifest["groups"] == {}, "校验失败不得写 manifest"


def test_cmd_relay_on_cleans_stale_dialer_on_relay_defs(tmp_path, monkeypatch):
    """2026-11-19 review P1 根修回归: 提拔新中继时, 中继定义 (新旧) 上的历史
    dialer 必须清掉 —— 残留会形成链式双跳, 且 watchdog current_relay() 读到
    多值判 AMBIGUOUS 静默失效 (今晚 Oregon→Tokyo 割接实测 140 节点含定义)。"""
    (tmp_path / "config").mkdir()
    bridge = tmp_path / "config" / "g-bridge.yaml"
    bridge.write_text(yaml.dump({"proxies": [
        {"name": "[自建] Old-Relay", "type": "vless", "server": "a", "port": 443,
         "dialer-proxy": "[自建] Old-Relay"},   # 上一次割接给旧中继留下的残值
        {"name": "[自建] New-Relay", "type": "vless", "server": "b", "port": 443,
         "dialer-proxy": "[自建] Old-Relay"},   # 新中继定义也带着旧 dialer
        {"name": "node-1", "type": "http", "server": "s", "port": 1},
    ]}))
    relay_file = tmp_path / "relay-nodes.local.yaml"
    relay_file.write_text(yaml.dump({"relay_nodes": [
        {"name": "[自建] Old-Relay", "type": "vless", "server": "a", "port": 443},
        {"name": "[自建] New-Relay", "type": "vless", "server": "b", "port": 443},
    ]}))
    monkeypatch.setattr(isa, "REPO", tmp_path)
    monkeypatch.setattr(isa, "RELAY_LOCAL_YAML", relay_file)

    rc = isa.cmd_relay(argparse.Namespace(group="g", mode="on",
                                          relay_node="[自建] New-Relay"))
    assert rc == 0
    cfg = yaml.safe_load(open(bridge))
    by_name = {p["name"]: p for p in cfg["proxies"]}
    assert by_name["node-1"]["dialer-proxy"] == "[自建] New-Relay"
    # 全部中继定义 (新旧) 均不得接线或残留 dialer —— 否则链式双跳 + watchdog AMBIGUOUS
    assert "dialer-proxy" not in by_name["[自建] New-Relay"]
    assert "dialer-proxy" not in by_name["[自建] Old-Relay"]


def test_cmd_relay_on_injects_from_local_file(tmp_path, monkeypatch):
    (tmp_path / "config").mkdir()
    bridge = tmp_path / "config" / "g-bridge.yaml"
    bridge.write_text(yaml.dump({"proxies": [
        {"name": "node-1", "type": "http", "server": "s", "port": 1},
    ]}))
    relay_file = tmp_path / "relay-nodes.local.yaml"
    relay_file.write_text(yaml.dump({"relay_nodes": [
        {"name": "[自建] R1", "type": "vless", "server": "1.2.3.4", "port": 443,
         "uuid": "u-1"},
    ]}))
    monkeypatch.setattr(isa, "REPO", tmp_path)
    monkeypatch.setattr(isa, "RELAY_LOCAL_YAML", relay_file)

    rc = isa.cmd_relay(argparse.Namespace(group="g", mode="on", relay_node="[自建] R1"))
    assert rc == 0
    cfg = yaml.safe_load(open(bridge))
    by_name = {p["name"]: p for p in cfg["proxies"]}
    assert by_name["node-1"]["dialer-proxy"] == "[自建] R1"
    assert by_name["[自建] R1"]["uuid"] == "u-1"      # 定义被完整注入
    assert "dialer-proxy" not in by_name["[自建] R1"]  # 且自身无 dialer


def test_import_region_auto_infer_mixed_group_falls_back_global(tmp_path, monkeypatch, capsys):
    """2026-11-19 newfree 事故回归: --region 未传时按节点名/服务器推导, 混合区
    组落 global——绝不默认 us-west 假标 (region 是调度硬约束, 假值=假承诺事故源:
    消费方 region=us-west 会拿到国内直连出口)。前缀同时去 🇺🇸/家宽 假语义。"""
    manifest = {"groups": {}, "updated": ""}
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "node_groups.yaml").write_text(yaml.dump(manifest))
    (tmp_path / "config" / "nodes.yaml").write_text(yaml.dump({"nodes": []}))
    bridge_yaml = tmp_path / "config" / "gtest-bridge.yaml"
    monkeypatch.setattr(isa.np, "load_manifest", lambda: {"groups": {}, "updated": ""})
    saved = {}
    monkeypatch.setattr(isa.np, "save_manifest", lambda m: saved.update(m))
    monkeypatch.setattr(isa.np, "next_free_ports", lambda m, n: list(range(11317, 11317 + n)))
    monkeypatch.setattr(isa.np, "next_free_seq", lambda ny, m: 3590)
    monkeypatch.setattr(isa.np, "NODES_YAML", tmp_path / "config" / "nodes.yaml")
    # 防止真写仓库文件: cmd_import 用 np.NODES_YAML 与 np.save_manifest, 但
    # manifest 组键写 saved 字典; nodes.yaml 用 np.NODES_YAML 路径 → 已指 tmp。
    monkeypatch.setattr(isa, "load_source", lambda s: {"proxies": [
        {"name": "HK-FREE", "type": "vless", "server": "2.26.204.46", "port": 31001},
        {"name": "Lightsail-SG-Reality", "type": "vless", "server": "18.140.145.188", "port": 443},
        {"name": "国内1-SOCKS", "type": "socks5", "server": "47.92.214.169", "port": 50001},
    ]})
    monkeypatch.setattr(isa.subprocess, "run",
                        lambda *a, **k: __import__("subprocess").CompletedProcess([], 0, "", ""))

    rc = isa.cmd_import(argparse.Namespace(
        source="x", group="gtest", region="", tier="standard", prefix=None,
        filter_type="", filter_server="", tags="datacenter", port_base=None,
        fault_domain=None, replace=False, dry_run=False))
    assert rc == 0
    assert saved["groups"]["gtest"]["region"] == "global", "混合区必须落 global, 不得假标 us-west"
    assert saved["groups"]["gtest"]["gateway_route"] == "region=global&type=clash_proxy"
    assert saved["groups"]["gtest"]["clash_node_prefix"] == "GTEST-", "前缀不得带 🇺🇸/家宽 假语义"
    ny = yaml.safe_load(open(tmp_path / "config" / "nodes.yaml"))
    regions = {n["region"] for n in ny["nodes"] if "gtest" in n["name"]}
    assert regions == {"global"}, f"nodes.yaml 同步 region=global, got {regions}"


def test_import_region_explicit_passthrough(tmp_path, monkeypatch):
    """显式 --region 仍然直通 (同区组如 teleapi 明确传值语义不变)。"""
    (tmp_path / "config").mkdir()
    monkeypatch.setattr(isa.np, "load_manifest", lambda: {"groups": {}, "updated": ""})
    saved = {}
    monkeypatch.setattr(isa.np, "save_manifest", lambda m: saved.update(m))
    monkeypatch.setattr(isa.np, "next_free_ports", lambda m, n: list(range(11400, 11400 + n)))
    monkeypatch.setattr(isa.np, "next_free_seq", lambda ny, m: 3600)
    monkeypatch.setattr(isa.np, "NODES_YAML", tmp_path / "config" / "nodes.yaml")
    (tmp_path / "config" / "nodes.yaml").write_text(yaml.dump({"nodes": []}))
    monkeypatch.setattr(isa, "load_source", lambda s: {"proxies": [
        {"name": "US-1", "type": "http", "server": "brd.superproxy.io", "port": 44445},
    ]})
    monkeypatch.setattr(isa.subprocess, "run",
                        lambda *a, **k: __import__("subprocess").CompletedProcess([], 0, "", ""))

    rc = isa.cmd_import(argparse.Namespace(
        source="x", group="gus", region="us-west", tier="standard", prefix=None,
        filter_type="", filter_server="", tags="", port_base=None,
        fault_domain=None, replace=False, dry_run=False))
    assert rc == 0
    assert saved["groups"]["gus"]["region"] == "us-west"
