"""tests/test_mesh_ingest.py - mesh 宿主机收编 (2026-11-18) 全链路单测.

覆盖: prober 节点级 probe_url 阶梯 / storage 持久化 / ban ISP 段 / loader 字段 /
node_pipeline mesh 组 (kind 分派 / 期望条目 / 远端 socks5 拨测 / check / heal)。
纯离线: 拨测用本机 socket mock, 不触网、不碰真实配置。
"""
import argparse
import socket
import sys
import threading
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import node_pipeline as np  # noqa: E402

from core.models import ProxyNode, EgressType  # noqa: E402
from core.prober import select_probe_target, PROBE_URL_V4, PROBE_URL_CF_TRACE  # noqa: E402
from core.ban import detect_isp_by_exit_ip  # noqa: E402


# ---------------------------------------------------------------- 基础制造器

def make_node(**kw) -> ProxyNode:
    base = dict(name="mesh-x", host="10.144.144.10", port=10801, protocol="socks5h",
                region="cn", egress_type="native_ipv4", tags=["self_hosted", "mesh"])
    base.update(kw)
    return ProxyNode(**base)


def make_mesh_group() -> dict:
    return {
        "kind": "mesh",
        "host_ip": "10.144.144.10",
        "fault_domain": "home-win",
        "region": "cn",
        "egress_type": "native_ipv4",
        "tier": "premium",
        "weight": 50,
        "tags": ["self_hosted", "mesh", "residential", "cmcc", "native"],
        "probe_url": "https://ipinfo.io/ip",
        "tether_name_template": "home-win-native-v{idx:02d}",
        "tether_seq_start": 500,
        "relay_mode": "direct_only",
        "nodes": [{"idx": 1, "port": 10801}],
    }


# ---------------------------------------------------------------- prober 阶梯

def test_probe_target_default_ladder():
    assert select_probe_target(make_node(egress_type="native_ipv4")) == (PROBE_URL_V4, False)
    assert select_probe_target(make_node(egress_type="warp_ipv6")) == (PROBE_URL_CF_TRACE, True)
    assert select_probe_target(make_node(egress_type="clash_proxy")) == (PROBE_URL_CF_TRACE, True)


def test_probe_target_node_override_plain_ip():
    """CN 家宽节点: GFW 拦 Cloudflare 段, probe_url 覆盖为纯 IP 文本目标."""
    n = make_node(probe_url="https://ipinfo.io/ip")
    assert select_probe_target(n) == ("https://ipinfo.io/ip", False)


def test_probe_target_node_override_cf_trace():
    n = make_node(egress_type="native_ipv4", probe_url="https://www.cloudflare.com/cdn-cgi/trace")
    assert select_probe_target(n) == ("https://www.cloudflare.com/cdn-cgi/trace", True)


# ---------------------------------------------------------------- storage 持久化

def test_storage_probe_url_roundtrip(tmp_path):
    from core.storage import Storage
    s = Storage(db_path=str(tmp_path / "t.db"))
    s.upsert_node(make_node(probe_url="https://ipinfo.io/ip"))
    assert s.get_node("mesh-x").probe_url == "https://ipinfo.io/ip"
    # 换新值可覆盖
    s.upsert_node(make_node(probe_url="http://members.3322.org/dyndns/getip"))
    assert s.get_node("mesh-x").probe_url == "http://members.3322.org/dyndns/getip"
    # 空值清除 (yaml/DTO 权威源, review 根修): 移除字段即回默认阶梯
    s.upsert_node(make_node(probe_url=""))
    assert s.get_node("mesh-x").probe_url == ""


def test_probe_url_validator_rejects_non_http():
    import pytest
    from core.models import ProxyNode
    with pytest.raises(Exception):
        ProxyNode(name="x", host="h", port=1, region="cn", egress_type="native_ipv4", probe_url="ftp://x")
    with pytest.raises(Exception):
        ProxyNode(name="x", host="h", port=1, region="cn", egress_type="native_ipv4", probe_url="just-text")
    n = ProxyNode(name="x", host="h", port=1, region="cn", egress_type="native_ipv4", probe_url=" https://ipinfo.io/ip ")
    assert n.probe_url == "https://ipinfo.io/ip"  # strip + 放行 http(s)


def test_loader_reads_probe_url(tmp_path, monkeypatch):
    """yaml 权威源 -> loader -> ProxyNode 全链路."""
    import api.server
    cfg = tmp_path / "nodes.yaml"
    cfg.write_text(
        "version: '1.0'\nnodes:\n"
        "- name: home-win-native-v01\n  host: 10.144.144.10\n  port: 10801\n"
        "  protocol: socks5h\n  region: cn\n  egress_type: native_ipv4\n"
        "  tags: [self_hosted, mesh, residential, cmcc]\n  weight: 50\n  tier: premium\n"
        "  fault_domain: home-win\n  probe_url: https://ipinfo.io/ip\n",
        encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg))
    # 直接调用 loader (min-10 护栏: 单节点 yaml 会跳过 mark_missing, 无副作用)
    api.server.load_initial_nodes()
    saved = api.server.storage.get_node("home-win-native-v01")
    assert saved.probe_url == "https://ipinfo.io/ip"
    assert saved.fault_domain == "home-win" and saved.weight == 50


# ---------------------------------------------------------------- ban ISP 段

def test_ban_cmcc_segment():
    assert detect_isp_by_exit_ip("183.193.163.187") == "cmcc"
    assert detect_isp_by_exit_ip("183.193.161.16") == "cmcc"
    assert detect_isp_by_exit_ip("23.230.200.152") == "comcast"
    assert detect_isp_by_exit_ip("32.189.158.231") is None  # AWS 段未登记不误标


# ---------------------------------------------------------------- 管线: 分派与期望条目

def test_group_kind_dispatch():
    assert np.group_kind({"bridge_core": {"type": "standalone-mihomo"}}) == "standalone"
    assert np.group_kind({"kind": "mesh"}) == "mesh"
    assert np.group_kind({}) == "verge"
    assert np.group_kind({"kind": "mesh", "bridge_core": {"type": "standalone-mihomo"}}) == "standalone"


def test_mesh_expected_entry_group_level():
    g = make_mesh_group()
    e = np.mesh_expected_entry(g, 0)
    assert e["name"] == "home-win-native-v01"
    assert e["host"] == "10.144.144.10" and e["port"] == 10801
    assert e["region"] == "cn" and e["egress_type"] == "native_ipv4"
    assert e["fault_domain"] == "home-win"
    assert e["probe_url"] == "https://ipinfo.io/ip"
    assert e["weight"] == 50 and e["tier"] == "premium"
    assert "mesh" in e["tags"] and "pipeline" in e["tags"]


def test_mesh_expected_entry_node_overrides():
    g = make_mesh_group()
    g["nodes"] = [{"idx": 1, "port": 10802, "host_ip": "10.144.144.11",
                   "fault_domain": "laptop-win", "weight": 30}]
    e = np.mesh_expected_entry(g, 0)
    assert e["host"] == "10.144.144.11" and e["port"] == 10802
    assert e["fault_domain"] == "laptop-win" and e["weight"] == 30
    assert e["region"] == "cn"  # 组级回填


def test_mesh_ports_and_names():
    g = make_mesh_group()
    g["nodes"] = [{"idx": 1, "port": 10801}, {"idx": 2, "port": 10802}]
    assert np.mesh_ports(g) == [10801, 10802]
    assert np.tether_names(g) == ["home-win-native-v01", "home-win-native-v02"]


# ---------------------------------------------------------------- 管线: 远端 socks5 拨测

def _socks5_server(sock: socket.socket, reply: bytes, accept_gate: threading.Event):
    """单连接 mock: 收问候, 回 reply, 立即关闭."""
    accept_gate.set()
    try:
        conn, _ = sock.accept()
        conn.settimeout(3)
        conn.recv(16)
        conn.sendall(reply)
        conn.close()
    except OSError:
        pass


def test_socks5_probe_dial_ok():
    srv, port = _free_port()
    gate = threading.Event()
    t = threading.Thread(target=_socks5_server, args=(srv, b"\x05\x00", gate), daemon=True)
    t.start()
    assert gate.wait(3)
    ok, why = np.socks5_probe_dial("127.0.0.1", port)
    assert ok and why == "ok"
    t.join(3)


def test_socks5_probe_dial_rejects_bad_method():
    srv, port = _free_port()
    gate = threading.Event()
    t = threading.Thread(target=_socks5_server, args=(srv, b"\x05\xff", gate), daemon=True)
    t.start()
    assert gate.wait(3)
    ok, why = np.socks5_probe_dial("127.0.0.1", port)
    assert not ok and "握手拒绝" in why
    t.join(3)


def test_socks5_probe_dial_refused():
    srv, port = _free_port()
    srv.close()
    ok, why = np.socks5_probe_dial("127.0.0.1", port, timeout=1.0)
    assert not ok and why


def _free_port():
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    return srv, srv.getsockname()[1]


# ---------------------------------------------------------------- 管线: check / heal

def _write_mesh_env(tmp_path, monkeypatch, dial_ok=True):
    """落 manifest + nodes.yaml + pool mock, 返回 (group, tname)."""
    group = make_mesh_group()
    manifest = {"groups": {"mesh-home-win": group}}
    mpath = tmp_path / "node_groups.yaml"
    mpath.write_text(yaml.dump(manifest))
    npath = tmp_path / "nodes.yaml"
    entry = np.mesh_expected_entry(group, 0)
    npath.write_text(yaml.dump({"nodes": [entry]}))
    monkeypatch.setattr(np, "MANIFEST_PATH", mpath)
    monkeypatch.setattr(np, "NODES_YAML", npath)
    monkeypatch.setattr(np, "pool_nodes", lambda: {entry["name"]: {"port": 10801, "status": "healthy"}})
    monkeypatch.setattr(np, "listening_ports", lambda *a, **k: set())
    monkeypatch.setattr(np, "mihomo_get", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no verge")))
    if dial_ok:
        monkeypatch.setattr(np, "socks5_probe_dial", lambda h, p, timeout=4.0: (True, "ok"))
    else:
        monkeypatch.setattr(np, "socks5_probe_dial", lambda h, p, timeout=4.0: (False, "refused"))
    return group, entry["name"]


def test_check_mesh_group_all_green(tmp_path, monkeypatch, capsys):
    """mesh 组全绿路径: L2 SKIP, L3 一致, L4 远端拨测 PASS, L5 注册一致."""
    _g, tname = _write_mesh_env(tmp_path, monkeypatch)
    rc = np.cmd_check(argparse.Namespace())
    assert rc == 0
    out = capsys.readouterr().out
    assert "[SKIP ] L2" in out and "mesh 宿主机组" in out
    assert "L3 nodes.yaml 注册" in out and "L4 mesh 宿主拨测" in out
    assert "✅ 五方一致" in out


def test_check_mesh_group_dial_failure_is_drift(tmp_path, monkeypatch, capsys):
    _g, _t = _write_mesh_env(tmp_path, monkeypatch, dial_ok=False)
    rc = np.cmd_check(argparse.Namespace())
    assert rc == 2
    out = capsys.readouterr().out
    assert "[DRIFT] L4 mesh 宿主拨测" in out


def test_check_mesh_group_yaml_drift(tmp_path, monkeypatch, capsys):
    """nodes.yaml 缺 fault_domain/probe_url/weight 时 L3 必须报漂移 (不全字段比对形同虚设)."""
    _g, tname = _write_mesh_env(tmp_path, monkeypatch)
    npath = np.NODES_YAML
    ny = yaml.safe_load(open(npath, encoding="utf-8"))
    ny["nodes"][0].pop("fault_domain")
    ny["nodes"][0]["weight"] = 100
    npath.write_text(yaml.dump(ny))
    rc = np.cmd_check(argparse.Namespace())
    assert rc == 2
    out = capsys.readouterr().out
    assert "fault_domain" in out and "weight" in out


def test_heal_mesh_group_writes_nodes_yaml(tmp_path, monkeypatch, capsys):
    """mesh 组 heal: 缺失/漂移条目按 mesh_expected_entry 补齐, 不碰 Script.js/Merge.yaml."""
    group, tname = _write_mesh_env(tmp_path, monkeypatch)
    # 抽走 nodes.yaml 条目制造漂移
    ny = yaml.safe_load(open(np.NODES_YAML, encoding="utf-8"))
    ny["nodes"] = []
    np.NODES_YAML.write_text(yaml.dump(ny))
    monkeypatch.setattr(np, "SCRIPT_JS", tmp_path / "Script.js")     # 不存在
    monkeypatch.setattr(np, "MERGE_YAML", tmp_path / "Merge.yaml")   # 不存在
    monkeypatch.setattr(np, "restart_tetherpool", lambda: None)      # 离线: 不真重启

    ns = argparse.Namespace(group="mesh-home-win", force_js=False, restart_verge=False,
                            realize=False, yes=True)
    rc = np.cmd_heal(ns)
    assert rc == 0
    healed = yaml.safe_load(open(np.NODES_YAML, encoding="utf-8"))["nodes"]
    assert len(healed) == 1 and healed[0]["name"] == tname
    assert healed[0]["host"] == "10.144.144.10"
    assert healed[0]["fault_domain"] == "home-win"
    assert healed[0]["probe_url"] == "https://ipinfo.io/ip"
    assert healed[0]["weight"] == 50 and healed[0]["tier"] == "premium"
    out = capsys.readouterr().out
    assert "跳过 Script.js/Merge.yaml" in out   # JS 跳过声明 (mesh 不进 Verge)


# ---------------------------------------------------------------- review 根修回归 (2026-11-18)

def test_check_l3_flags_handwritten_yaml_missing_pipeline_tag(tmp_path, monkeypatch, capsys):
    """负向: 手写 yaml 缺 pipeline tag (与 mesh_expected_entry 不同源) 时 L3 必须报漂移.

    回归 guard: 原 L3 手写字段清单跳过 tags, check 全绿但 heal 会改写生产配置。"""
    _g, tname = _write_mesh_env(tmp_path, monkeypatch)
    ny = yaml.safe_load(open(np.NODES_YAML, encoding="utf-8"))
    ny["nodes"][0]["tags"] = ["self_hosted", "mesh", "residential", "cmcc", "native"]  # 无 pipeline
    np.NODES_YAML.write_text(yaml.dump(ny))
    rc = np.cmd_check(argparse.Namespace())
    assert rc == 2
    out = capsys.readouterr().out
    assert "tags" in out


def test_heal_mesh_idempotent(tmp_path, monkeypatch, capsys):
    """heal 收敛后二次执行必须零变更 (L3/heal 同源契约的直接后果)."""
    _g, _t = _write_mesh_env(tmp_path, monkeypatch)
    monkeypatch.setattr(np, "SCRIPT_JS", tmp_path / "Script.js")
    monkeypatch.setattr(np, "MERGE_YAML", tmp_path / "Merge.yaml")
    monkeypatch.setattr(np, "restart_tetherpool", lambda: None)
    ns = argparse.Namespace(group="mesh-home-win", force_js=False, restart_verge=False,
                            realize=False, yes=True)
    assert np.cmd_heal(ns) == 0
    capsys.readouterr()
    assert np.cmd_heal(ns) == 0
    out2 = capsys.readouterr().out
    assert "无需修复" in out2  # 幂等: 第二次零变更


def test_heal_mesh_clears_removed_probe_url(tmp_path, monkeypatch, capsys):
    """manifest 移除 probe_url -> heal 必须从 yaml 清除 (无条件写入语义)."""
    _g, tname = _write_mesh_env(tmp_path, monkeypatch)
    monkeypatch.setattr(np, "SCRIPT_JS", tmp_path / "Script.js")
    monkeypatch.setattr(np, "MERGE_YAML", tmp_path / "Merge.yaml")
    monkeypatch.setattr(np, "restart_tetherpool", lambda: None)
    # 从 manifest 移除 probe_url (模拟运维回退默认探活阶梯)
    group_no_pu = {k: v for k, v in _g.items() if k != "probe_url"}
    np.MANIFEST_PATH.write_text(yaml.dump({"groups": {"mesh-home-win": group_no_pu}}))
    ns = argparse.Namespace(group="mesh-home-win", force_js=False, restart_verge=False,
                            realize=False, yes=True)
    assert np.cmd_heal(ns) == 0
    healed = yaml.safe_load(open(np.NODES_YAML, encoding="utf-8"))["nodes"]
    assert "probe_url" not in healed[0]  # 被清除而非保留旧值
    capsys.readouterr()


def test_loader_second_load_without_probe_url_clears_db(tmp_path, monkeypatch):
    """yaml 移除 probe_url -> 二次 reload 后 DB 清空 (权威源语义端到端)."""
    import api.server
    cfg = tmp_path / "nodes.yaml"
    with_probe = ("version: '1.0'\nnodes:\n"
                  "- name: home-win-native-v4\n  host: 10.144.144.10\n  port: 10801\n"
                  "  protocol: socks5h\n  region: cn\n  egress_type: native_ipv4\n"
                  "  tags: [self_hosted, mesh]\n  fault_domain: home-win\n"
                  "  probe_url: https://ipinfo.io/ip\n")
    without_probe = with_probe.replace("  probe_url: https://ipinfo.io/ip\n", "")
    cfg.write_text(with_probe, encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg))
    api.server.load_initial_nodes()
    assert api.server.storage.get_node("home-win-native-v4").probe_url == "https://ipinfo.io/ip"
    cfg.write_text(without_probe, encoding="utf-8")
    api.server.load_initial_nodes()
    assert api.server.storage.get_node("home-win-native-v4").probe_url == ""


def test_warp_exit_contract_enforced_regardless_of_probe_format():
    """warp_ipv6 出口契约与探活目标格式正交: 纯文本 probe_url 探得 v4 也必须判失败."""
    import pytest
    from core.prober import enforce_exit_contract
    warp = make_node(egress_type="warp_ipv6", probe_url="https://ipinfo.io/ip")
    enforce_exit_contract(warp, "2a09:bac1:76c1:62b0::39d:59")  # v6 放行
    with pytest.raises(ValueError):
        enforce_exit_contract(warp, "183.193.163.187")  # v4 出口 -> 契约违约
    native = make_node(egress_type="native_ipv4")
    enforce_exit_contract(native, "183.193.163.187")  # native 不受 v6 契约约束


def test_mesh_expected_entry_tags_dedup():
    """组 tags 已含 mesh 时不得产生重复标签 (heal 写入权威形态)."""
    g = make_mesh_group()  # tags 已含 'mesh'
    e = np.mesh_expected_entry(g, 0)
    assert e["tags"].count("mesh") == 1
    assert "pipeline" in e["tags"]
