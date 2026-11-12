#!/usr/bin/env python3
"""import_standalone.py — standalone 桥核组导入器 (仅入 TetherPool, 绝不写 Clash Verge).

沉淀 2026-11-12 teleapi 批次的手工流程 (docs/NODE_ONBOARDING.md §六), 供后续
"vendor 分享只要求入池" 批次复用。与 node_pipeline.py 的 Verge 注入型 import
互不影响; manifest 组带 bridge_core.type=standalone-mihomo, check 自动分流。

用法 (两阶段):
  # 阶段 1: 解析订阅/文件 → 写 manifest + nodes.yaml + 桥核配置 (不动运行时)
  import_standalone.py import <订阅url|本地clash.yaml> --group NAME [--region us-west]
      [--filter-type http] [--filter-server brd.superproxy.io] [--port-base 11101]
      [--fault-domain mac-NAME] [--dry-run]

  # 阶段 2: start.sh restart 拉起桥核后, 探活回写 realized_map + import_audit
  import_standalone.py realize --group NAME [--workers 10]

桥核生命周期: start.sh (keeper 守护) 自动接管 config/<group>-bridge.yaml。
"""
import argparse
import collections
import re
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
import node_pipeline as np  # noqa: E402  (复用 load_manifest/save_manifest/端口规划/探活)

REPO = Path(__file__).resolve().parent.parent
PINNED_IP_RE = re.compile(r"-ip-(\d+\.\d+\.\d+\.\d+)$")
CITY_RE = re.compile(r"「(.+?)」")
RELAY_LOCAL_YAML = REPO / "config/relay-nodes.local.yaml"


def load_relay_definition(relay_name: str) -> Optional[dict]:
    """从 gitignored 本地凭据文件读取中转节点定义 (name 必须精确匹配)."""
    if not RELAY_LOCAL_YAML.exists():
        return None
    try:
        data = yaml.safe_load(open(RELAY_LOCAL_YAML, encoding="utf-8")) or {}
    except Exception as e:  # noqa: BLE001 — 坏文件按缺失处理, 不打断 relay 流程
        log(f"[relay] ⚠️ {RELAY_LOCAL_YAML.name} 解析失败 ({e}), 按缺失处理")
        return None
    for p in data.get("relay_nodes", []):
        if p.get("name") == relay_name:
            return p
    return None


def log(msg):
    print(msg, flush=True)


def load_source(source: str) -> dict:
    if source.startswith("http://") or source.startswith("https://"):
        log(f"[1/6] 拉取订阅: {source}")
        with urllib.request.urlopen(source, timeout=60) as r:
            data = yaml.safe_load(r.read())
    else:
        log(f"[1/6] 读取本地文件: {source}")
        data = yaml.safe_load(open(source, encoding="utf-8"))
    return data or {}


def pick_proxies(proxies: list, filter_type: str, filter_server: str) -> list:
    picked = []
    for p in proxies:
        if not p.get("server") or not p.get("port"):
            continue
        if filter_type and str(p.get("type", "")).lower() != filter_type:
            continue
        if filter_server and filter_server not in str(p.get("server", "")):
            continue
        picked.append(p)
    return picked


def cmd_import(args):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.group):
        log("[-] --group 必须匹配 ^[a-z][a-z0-9_]*$")
        return 1
    manifest = np.load_manifest()
    if args.group in manifest["groups"] and not args.replace:
        log(f"[-] 组 {args.group} 已存在; 加 --replace 覆盖")
        return 1

    data = load_source(args.source)
    proxies = pick_proxies(data.get("proxies", []), args.filter_type, args.filter_server)
    log(f"[2/6] 过滤后节点: {len(proxies)} (type={args.filter_type or '任意'}, server~={args.filter_server or '任意'})")
    if not proxies:
        log("[-] 零节点命中过滤条件")
        return 2

    # 出口 IP / 城市尽力解析 (Bright Data username 内嵌 -ip-; 其他 vendor 留空交 realize 探测)
    parsed = []
    for p in proxies:
        m = PINNED_IP_RE.search(p.get("username") or "")
        city = CITY_RE.search(p.get("name") or "")
        parsed.append({**p, "_exit_ip": m.group(1) if m else "", "_city": city.group(1) if city else ""})
    pinned = sum(1 for p in parsed if p["_exit_ip"])
    log(f"      固定出口 IP 可解析: {pinned}/{len(parsed)}")

    prefix = args.prefix or f"🇺🇸{args.group.upper()}-家宽-"
    ports = args.port_base and list(range(args.port_base, args.port_base + len(parsed))) \
        or np.next_free_ports(manifest, len(parsed))
    ny_now = yaml.safe_load(open(np.NODES_YAML, encoding="utf-8")) or {}
    seq_start = np.next_free_seq(ny_now, manifest)
    width = max(2, len(str(len(parsed))))

    group = {
        "source": args.source,
        "protocol": str(parsed[0].get("type", "http")).lower(),
        "clash_node_prefix": prefix,
        "tether_name_template": f"clash-{{seq:03d}}-{args.group}-{{idx:0{width}d}}",
        "tether_seq_start": seq_start,
        "region": args.region,
        "tier": args.tier,
        "egress_type": "clash_proxy",
        "tags": ["clash", "bridge", str(parsed[0].get("type", "http")).lower(), args.group, "residential"],
        "relay_mode": "direct_only",
        "bridge_core": {
            "type": "standalone-mihomo",
            "binary": "bin/mihomo",
            "config": f"config/{args.group}-bridge.yaml",
            "home": f"config/{args.group}-bridge",
            "log": f"logs/{args.group}-bridge.log",
            "pid": f"data/{args.group}-bridge.pid",
            "fault_domain": args.fault_domain,
            "managed_by": "start.sh (bridge-core keeper)",
            "note": "仅入 TetherPool, 不写 Clash Verge/tebi/pxed (import_standalone.py)",
        },
        "bridge_ports": {"declared": f"{ports[0]}-{ports[-1]}", "realized": ""},
        "listener_prefix": f"{args.group}-bridge-",
        "gateway_route": f"region={args.region}&type=clash_proxy",
        "nodes": [
            {"idx": i + 1,
             "name": f"{prefix}{i + 1:0{width}d}",
             "server": p["server"], "port": p["port"],
             "username": p.get("username", ""), "password": p.get("password", ""),
             "exit_ip": p["_exit_ip"], "type": str(p.get("type", "http")).lower(),
             "city": p["_city"], "vendor_name": p.get("name", "")}
            for i, p in enumerate(parsed)
        ],
    }

    bridge_cfg = {
        "mode": "global", "log-level": "warning", "ipv6": False, "allow-lan": False,
        "external-controller": "", "unified-delay": False, "tcp-concurrent": False,
        "keep-alive-interval": 30, "disable-keep-alive": False,
        "proxies": [
            {"name": n["name"], "type": n["type"], "server": n["server"], "port": n["port"],
             "username": n["username"], "password": n["password"], "udp": False}
            for n in group["nodes"]
        ],
        "listeners": [
            {"name": f"{args.group}-bridge-{i + 1:03d}", "type": "socks",
             "port": ports[i], "listen": "127.0.0.1", "proxy": n["name"]}
            for i, n in enumerate(group["nodes"])
        ],
    }

    if args.dry_run:
        log(f"[dry-run] 将新增组 {args.group}: {len(parsed)} 节点, seq_start={seq_start}, "
            f"桥接段 {ports[0]}-{ports[-1]}, 故障域 {args.fault_domain}")
        return 0

    log(f"[3/6] 写桥核配置 config/{args.group}-bridge.yaml (mihomo -t 校验)")
    bridge_path = REPO / f"config/{args.group}-bridge.yaml"
    np.atomic_write_text(bridge_path, yaml.dump(bridge_cfg, allow_unicode=True, sort_keys=False))
    home = REPO / f"config/{args.group}-bridge"
    home.mkdir(exist_ok=True)
    r = subprocess.run([str(REPO / "bin/mihomo"), "-d", str(home), "-t", "-f", str(bridge_path)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        log(r.stderr[-400:])
        bridge_path.unlink(missing_ok=True)  # 残留无效核配置会让 keeper 无限退避重启
        log("[-] 桥核配置校验失败, 已清理残留配置, manifest/nodes 未写入")
        return 2

    log("[4/6] 写 manifest (bridge_core 组)")
    manifest["groups"][args.group] = group
    manifest["updated"] = time.strftime("%Y-%m-%d")
    np.save_manifest(manifest)

    log("[5/6] 写 nodes.yaml (含 fault_domain / direct_only)")
    ny = yaml.safe_load(open(np.NODES_YAML, encoding="utf-8")) or {}
    have = {n.get("name") for n in ny.get("nodes", [])}
    added = 0
    for i, n in enumerate(group["nodes"]):
        tname = group["tether_name_template"].format(seq=seq_start + i, idx=i + 1)
        if tname in have:
            continue
        ny["nodes"].append({
            "name": tname, "host": "127.0.0.1", "port": ports[i],
            "protocol": "socks5h", "region": args.region, "egress_type": "clash_proxy",
            "tags": group["tags"] + ["bridge-core"], "weight": 100, "tier": args.tier,
            "relay_mode": "direct_only", "fault_domain": args.fault_domain,
        })
        added += 1
    np.atomic_write_text(np.NODES_YAML, yaml.dump(ny, allow_unicode=True, sort_keys=False, width=200))

    log(f"[6/6] 完成: manifest 组 '{args.group}' ({len(parsed)} 节点) + nodes.yaml (+{added})")
    log(f"      下一步: ① start.sh restart (keeper 拉起桥核并自动 force probe)")
    log(f"               ② import_standalone.py realize --group {args.group}")
    return 0


def cmd_realize(args):
    manifest = np.load_manifest()
    group = manifest["groups"][args.group]
    count = len(group["nodes"])
    ports = np.realized_ports(group)
    log(f"[realize] 探活 {args.group}: {ports[0]}–{ports[-1]} ({count} 端口, workers={args.workers})")

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        exits = list(ex.map(np.probe_bridge_exit, ports))

    alive = [e for e in exits if e]
    ip_to_idx, ambiguous = {}, set()
    for n in group["nodes"]:
        eip = n.get("exit_ip", "")
        if eip in ip_to_idx:
            ambiguous.add(eip)
        ip_to_idx[eip] = n["idx"]

    mapping, unknown = {}, []
    for port, eip in zip(ports, exits):
        if not eip:
            continue
        if eip not in ip_to_idx or eip in ambiguous:
            unknown.append((port, eip))
            continue
        mapping[ip_to_idx[eip]] = port

    dead = [p for p, e in zip(ports, exits) if not e]
    log(f"[realize] alive={len(alive)}/{count} dead={len(dead)} mapped={len(mapping)} unknown_exit={len(unknown)}")
    if dead:
        log(f"[realize] dead ports: {dead} (vendor 407/超时 → 断路器自愈; 凭据轮换后重跑 realize)")

    identity = all(mapping.get(n["idx"]) == ports[i] for i, n in enumerate(group["nodes"]) if n["idx"] in mapping)
    if len(mapping) == count and identity:
        group["bridge_ports"]["realized"] = group["bridge_ports"]["declared"]
        log("[realize] ✓ 恒等映射成立, realized_map 不需要回写")
    else:
        group["bridge_ports"]["realized_map"] = {str(i): mapping[i] for i in sorted(mapping)}
        ny = yaml.safe_load(open(np.NODES_YAML, encoding="utf-8")) or {}
        by_name = {n.get("name"): n for n in ny.get("nodes", [])}
        for i, n in enumerate(group["nodes"]):
            tname = group["tether_name_template"].format(seq=group["tether_seq_start"] + i, idx=n["idx"])
            if n["idx"] in mapping and tname in by_name:
                by_name[tname]["port"] = mapping[n["idx"]]
        np.atomic_write_text(np.NODES_YAML, yaml.dump(ny, allow_unicode=True, sort_keys=False, width=200))
        log(f"[realize] ⚠️ 非恒等映射, realized_map 已回写 {len(mapping)} 条")
    group["import_audit"] = {
        "realized_at": time.strftime("%Y-%m-%d %H:%M"),
        "alive": len(alive), "dead_ports": dead, "unknown_exits": [list(u) for u in unknown[:10]],
    }
    np.save_manifest(manifest)
    log("[realize] import_audit 已写入 manifest")
    return 0 if len(mapping) + len(dead) == count else 2


def cmd_relay(args):
    """在桥核配置内给全部 proxies 接线/摘除 dialer-proxy 中转 (教训 #6 杠杆).

    --relay-node 需已存在于桥核配置; 不存在时从本地凭据文件
    config/relay-nodes.local.yaml (gitignored, 格式见 docs/NODE_ONBOARDING.md §六)
    读取同名节点定义注入。改完杀核, keeper 以新配置重生。
    """
    cfg_path = REPO / f"config/{args.group}-bridge.yaml"
    cfg = yaml.safe_load(open(cfg_path, encoding="utf-8"))
    relay_name = args.relay_node
    if args.mode == "off":
        for p in cfg.get("proxies", []):
            p.pop("dialer-proxy", None)
        np.atomic_write_text(cfg_path, yaml.dump(cfg, allow_unicode=True, sort_keys=False))
        log(f"[relay] dialer-proxy 已全部摘除 ({cfg_path})")
        return 0

    names = {p.get("name") for p in cfg.get("proxies", [])}
    if relay_name not in names:
        relay = load_relay_definition(relay_name)
        if relay is None:
            log(f"[-] 中转节点 {relay_name} 不在桥核配置, 且 config/relay-nodes.local.yaml "
                f"无同名定义 — 请按 NODE_ONBOARDING.md §六 手工补该文件后重试")
            return 2
        cfg.setdefault("proxies", []).insert(0, relay)
        log(f"[relay] 从 relay-nodes.local.yaml 注入中转节点 {relay_name}")
    wired = 0
    for p in cfg.get("proxies", []):
        if p.get("name") != relay_name:
            p["dialer-proxy"] = relay_name
            wired += 1
    np.atomic_write_text(cfg_path, yaml.dump(cfg, allow_unicode=True, sort_keys=False))
    log(f"[relay] dialer-proxy 已接线 {wired} 节点 → {relay_name} ({cfg_path})")
    return 0


def main():
    ap = argparse.ArgumentParser(description="standalone 桥核组导入器 (仅入 TetherPool)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_imp = sub.add_parser("import", help="解析订阅并写入 manifest/nodes.yaml/桥核配置")
    p_imp.add_argument("source", help="订阅 url 或本地 clash yaml")
    p_imp.add_argument("--group", required=True)
    p_imp.add_argument("--region", default="us-west")
    p_imp.add_argument("--tier", default="standard", choices=["standard", "premium"])
    p_imp.add_argument("--prefix", default=None)
    p_imp.add_argument("--filter-type", default="http", help="按 type 过滤 (空串=不过滤)")
    p_imp.add_argument("--filter-server", default="", help="按 server 子串过滤")
    p_imp.add_argument("--port-base", type=int, default=None, help="桥接起始端口 (默认取 manifest 之上空闲段)")
    p_imp.add_argument("--fault-domain", default=None, help="显式故障域 (默认 mac-<group>)")
    p_imp.add_argument("--replace", action="store_true")
    p_imp.add_argument("--dry-run", action="store_true")

    p_rl = sub.add_parser("realize", help="探活桥接端口并回写 realized_map/import_audit")
    p_rl.add_argument("--group", required=True)
    p_rl.add_argument("--workers", type=int, default=10)

    p_relay = sub.add_parser("relay", help="桥核中转杠杆: 直拨劣化时接线 dialer-proxy (教训 #6)")
    p_relay.add_argument("--group", required=True)
    p_relay.add_argument("--mode", default="on", choices=["on", "off"])
    p_relay.add_argument("--relay-node", default="[自建] 🇺🇸VLESS-AWS-Oregon-Reality")

    args = ap.parse_args()
    if args.cmd == "import":
        if args.fault_domain is None:
            args.fault_domain = f"mac-{args.group}"
        sys.exit(cmd_import(args))
    if args.cmd == "relay":
        sys.exit(cmd_relay(args))
    sys.exit(cmd_realize(args))


if __name__ == "__main__":
    main()
