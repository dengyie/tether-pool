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
        filter_type="http", filter_server="", port_base=None, fault_domain=None,
        replace=False, dry_run=False))
    assert rc == 2
    assert not (tmp_path / "config/gtest-bridge.yaml").exists(), "校验失败必须清理残留核配置"
    assert manifest["groups"] == {}, "校验失败不得写 manifest"
