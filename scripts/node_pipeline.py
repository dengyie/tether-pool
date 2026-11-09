#!/usr/bin/env python3
"""
scripts/node_pipeline.py - TetherPool × 3x-ui(Clash Verge) 节点接入流水线

单一入口，保证新增节点同时正确落入两个项目，并持续校验五方一致：

    manifest(config/node_groups.yaml, gitignore, 仅本地)  ← 唯一事实源
      ↔ Script.js 托管块(节点注入/中转组/url-test 组)
      ↔ nodes.yaml(TetherPool 注册)
      ↔ mihomo 活体(节点/dialer-proxy/组/listener)
      ↔ TetherPool 运行时(/v1/nodes)

子命令:
    import <url|file> --group X --region R   新增节点全流程(含 --dry-run)
    check                                    五方一致性审计(健康度仅 WARN)
    verify --group X [--load N]              数据面实测(出口IP/目标矩阵/网关/压测)
    heal   --group X [--yes] [--restart-verge] [--force-js]  漂移修复(含凭据轮换)

实证教训(2026-11-10 深测, 详见 docs/NODE_ONBOARDING.md):
  - Verge 以 root 侧冻结底座为 listener 事实现, Merge listener 按目标代理去重合并
  - 网关无热重载, nodes.yaml 变更后必须 start.sh restart
  - 环回桥必须 relay_mode=direct_only, 否则中继级联语义错乱
  - 凭据经 curl -U 传递, 勿把含 ?& 的凭据塞进 curl 代理 URL; urllib 不支持 https CONNECT
  - vendor 凭据仅存 gitignore 后的 manifest, 严禁入库 (模板: node_groups.example.yaml)
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import yaml

PROJECT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = PROJECT / "config" / "node_groups.yaml"
NODES_YAML = PROJECT / "config" / "nodes.yaml"
START_SH = PROJECT / "start.sh"
CONTROL = "http://127.0.0.1:9100"
GATEWAY = "http://127.0.0.1:9101"
VERGE_DIR = Path.home() / "Library/Application Support/io.github.clash-verge-rev.clash-verge-rev"
SCRIPT_JS = VERGE_DIR / "profiles" / "Script.js"
MERGE_YAML = VERGE_DIR / "profiles" / "Merge.yaml"
MIHOMO_SOCK = "/var/run/clash-verge-service/users/501/verge-mihomo.sock"
BLOCK_BEGIN = "// >>> NODE-PIPELINE:{group}:v1"
BLOCK_END = "// <<< NODE-PIPELINE:{group}:v1"
MAIN_BEGIN = "// >>> NODE-PIPELINE:main-call:v1"
MAIN_END = "// <<< NODE-PIPELINE:main-call:v1"
PROBE_URL = "https://api.ipify.org"
# 注入 mihomo 的白名单字段(其余 manifest 内部字段如 idx/exit_ip 不得外泄到运行时配置)
PROXY_FIELDS = ("name", "type", "server", "port", "username", "password", "udp")
TARGETS = [
    ("gstatic", "https://www.gstatic.com/generate_204", {"204"}),
    ("google", "https://www.google.com/generate_204", {"204"}),
    ("x.com", "https://x.com/robots.txt", {"200"}),
    ("claude", "https://claude.ai/robots.txt", {"200", "403", "405"}),
]


# ---------------------------------------------------------------- utilities

def log(msg: str):
    print(msg, flush=True)


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        raise SystemExit(
            f"[-] manifest 不存在: {MANIFEST_PATH}\n"
            f"    从模板初始化: cp config/node_groups.example.yaml config/node_groups.yaml 后填入真实凭据")
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def atomic_write_text(path: Path, content: str):
    """同目录 tmp + os.replace, 避免并发读者(Verge)读到半文件."""
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def save_manifest(m: dict):
    atomic_write_text(MANIFEST_PATH, yaml.dump(m, allow_unicode=True, sort_keys=False, width=200))


def read_script() -> str:
    """Script.js 内容 (缺失返回空串 — Verge 重装/首次初始化时 heal 需能从零重建)."""
    return SCRIPT_JS.read_text(encoding="utf-8") if SCRIPT_JS.exists() else ""


def pmap(func, items, workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(func, items))


def mihomo_get(path: str, timeout: float = 12):
    """mihomo unix socket GET. 独立短连接, 重试覆盖核心重启窗口(~10s)."""
    import http.client
    import socket as _socket

    class _UnixConn(http.client.HTTPConnection):
        def connect(self):
            self.sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            self.sock.connect(MIHOMO_SOCK)

    last = None
    for _ in range(5):
        try:
            conn = _UnixConn("localhost", timeout=timeout)
            conn.request("GET", path)
            resp = conn.getresponse()
            body = resp.read().decode()
            conn.close()
            return resp.status, (json.loads(body) if body.strip() else {})
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2)
    raise RuntimeError(f"mihomo API {path} failed: {last}")


def fetch_subscription(source: str) -> dict:
    """拉取订阅 yaml (url 或本地文件)."""
    if source.startswith("http"):
        req = urllib.request.Request(source, headers={"User-Agent": "clash-verge/1.7"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
        return yaml.safe_load(data)
    with open(source, encoding="utf-8") as f:
        return yaml.safe_load(f)


def curl_proxy_exit(proxy_args: list[str], timeout: int = 18) -> str:
    """经指定代理参数测出口 IP (返回空串=失败)."""
    try:
        r = subprocess.run(["curl", "-s", PROBE_URL, "--max-time", str(timeout)] + proxy_args,
                           capture_output=True, text=True, timeout=timeout + 5)
        return r.stdout.strip() if r.returncode == 0 else ""
    except Exception:  # noqa: BLE001
        return ""


def probe_vendor_exit(node: dict, timeout: int = 18) -> str:
    """直连 vendor HTTP 代理测出口 IP — curl -x + -U (凭据不进 URL, 规避 ?& 解析).

    无认证代理 (IP 白名单型) 自动省略 -U.
    """
    proxy_args = ["-x", f"{node['server']}:{node['port']}"]
    if node.get("username") or node.get("password"):
        proxy_args += ["-U", f"{node.get('username', '')}:{node.get('password', '')}"]
    return curl_proxy_exit(proxy_args, timeout)


def probe_bridge_exit(port: int, timeout: int = 18) -> str:
    """经本地 mihomo socks 桥测出口 IP."""
    return curl_proxy_exit(["--socks5-hostname", f"127.0.0.1:{port}"], timeout)


def curl_code(url: str, proxy_args: list[str], timeout: int = 18) -> str:
    try:
        r = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}"]
                           + proxy_args + [url, "--max-time", str(timeout)],
                           capture_output=True, text=True, timeout=timeout + 5)
        return r.stdout.strip() if r.returncode == 0 else f"F{r.returncode}"
    except Exception:  # noqa: BLE001
        return "T"


def listening_ports(low: int = 11000, high: int = 11999) -> set[int]:
    out = subprocess.run(["netstat", "-an", "-p", "tcp"], capture_output=True, text=True).stdout
    pat = re.compile(r"127\.0\.0\.1\.(\d+)\s+.*?\s+LISTEN")
    return {int(m.group(1)) for m in pat.finditer(out) if low <= int(m.group(1)) <= high}


def pool_nodes() -> dict:
    with urllib.request.urlopen(f"{CONTROL}/v1/nodes", timeout=15) as r:
        d = json.load(r)
    items = d if isinstance(d, list) else d.get("nodes", [])
    return {n["name"]: n for n in items}


def tether_names(group: dict) -> list[str]:
    """tether 注册名. seq 起始与补零均由 manifest 声明 (tether_seq_start/tether_name_template)."""
    tpl = group["tether_name_template"]
    start = group.get("tether_seq_start")
    if start is None:
        raise SystemExit(f"[-] manifest 组缺少 tether_seq_start (模板: {tpl})")
    return [tpl.format(seq=start + i, idx=n["idx"]) for i, n in enumerate(group["nodes"])]


def declared_ports(group: dict) -> list[int]:
    lo, hi = group["bridge_ports"]["declared"].split("-")
    return list(range(int(lo), int(hi) + 1))


def realized_ports(group: dict) -> list[int]:
    """桥接端口列表: 优先 per-node realized_map, 否则 realized 区间(按节点顺序)."""
    rmap = group.get("bridge_ports", {}).get("realized_map") or {}
    if rmap:
        return [int(rmap[str(n["idx"])]) for n in group["nodes"]]
    lo, hi = group["bridge_ports"]["realized"].split("-")
    return list(range(int(lo), int(hi) + 1))


def backup(path: Path, tag: str = "pipeline"):
    """复制备份(非移动), 原文件保持原位."""
    import shutil
    if path.exists():
        bak = path.with_name(f"{path.name}.bak-{tag}-{time.strftime('%Y%m%d-%H%M%S')}")
        shutil.copy2(path, bak)
        log(f"  [backup] {path.name} -> {bak.name}")
        return bak
    return None


def js_check_or_rollback(path: Path, bak: Path | None):
    """node --check 失败则从备份回滚并抛错 (防止坏 JS 部署到 Verge)."""
    r = subprocess.run(["node", "--check", str(path)], capture_output=True, text=True)
    if r.returncode != 0:
        if bak and bak.exists():
            bak.replace(path)  # 回滚(备份文件转移回原位)
        raise RuntimeError(f"node --check 失败, 已回滚: {r.stderr[:300]}")


def restart_tetherpool() -> bool:
    r = subprocess.run([str(START_SH), "restart"], capture_output=True, text=True)
    time.sleep(3)
    ok = "Running Successfully" in (r.stdout or "")
    log(f"  [tetherpool] restart {'✓' if ok else '⚠️ 未见成功标志: ' + (r.stdout or r.stderr)[-200:]}")
    return ok


def restart_verge_app(wait: float = 16.0):
    subprocess.run(["osascript", "-e", 'tell application "Clash Verge" to quit'],
                   capture_output=True)
    time.sleep(3)
    subprocess.run(["open", "-a", "Clash Verge"], capture_output=True)
    log(f"  [verge] 应用已重启, 等待重组 {wait:.0f}s...")
    time.sleep(wait)


# ---------------------------------------------------------------- JS generation

def clean_nodes(group: dict) -> list[dict]:
    """manifest 节点 → 注入 mihomo 的白名单字段对象 (gen 与 check 共用, 保证一致)."""
    out = []
    for n in group["nodes"]:
        obj = {k: n[k] for k in PROXY_FIELDS if k in n}
        obj["type"] = n.get("type", group.get("protocol", "http"))
        obj["udp"] = False
        out.append(obj)
    return out


def gen_group_js(group_name: str, group: dict) -> str:
    nodes_js = ",\n      ".join(json.dumps(n, ensure_ascii=False) for n in clean_nodes(group))
    relay = group.get("relay") or {}
    relay_candidates = json.dumps(relay.get("candidates", []), ensure_ascii=False)
    utg = group.get("url_test_group", {})
    fn = f"ensureNPGroup_{group_name}"
    realized = group["bridge_ports"].get("realized", "")
    return f"""// >>> NODE-PIPELINE:{group_name}:v1
// 由 scripts/node_pipeline.py 管理(managed block), 手改会被 import/heal 覆盖。
// 职责: 静态节点注入 + 中转 fallback 组(订阅缺候选时自动退直连) + url-test 组。
// TetherPool 桥接 listener 驻留在 Verge 冻结底座({realized}
// → 本数组节点), Merge.yaml 内为同目标重建镜像(按代理去重, 勿重复加)。
const NP_{group_name.upper()} = {{
  clashPrefix: {json.dumps(group['clash_node_prefix'], ensure_ascii=False)},
  urlTestGroup: {json.dumps(utg.get('name', group_name), ensure_ascii=False)},
  relayGroup: {json.dumps(relay.get('group'), ensure_ascii=False)},
  relayCandidates: {relay_candidates},
  nodes: [
      {nodes_js}
  ],
}};

function {fn}(config) {{
  const spec = NP_{group_name.upper()};
  const proxies = asArray(config.proxies);
  const names = new Set(
    proxies.map((proxy) => proxy?.name).filter(Boolean)
  );

  let relayName = null;
  if (spec.relayGroup && spec.relayCandidates.length > 0) {{
    const members = spec.relayCandidates.filter((n) => names.has(n));
    if (members.length > 0) {{
      const relayGroups =
        config["proxy-groups"] ||
        (config["proxy-groups"] = []);
      upsertGroup(relayGroups, {{
        name: spec.relayGroup,
        type: "fallback",
        url: "https://www.gstatic.com/generate_204",
        interval: 120,
        lazy: false,
        proxies: members,
      }});
      relayName = spec.relayGroup;
    }}
  }}

  for (const base of spec.nodes) {{
    const node = relayName
      ? {{ ...base, "dialer-proxy": relayName }}
      : {{ ...base }};
    if (names.has(node.name)) {{
      const idx = proxies.findIndex((p) => p?.name === node.name);
      proxies[idx] = {{ ...proxies[idx], ...node }};
    }} else {{
      proxies.push(node);
      names.add(node.name);
    }}
  }}
  config.proxies = proxies;

  const urlTestGroups =
    config["proxy-groups"] ||
    (config["proxy-groups"] = []);
  upsertGroup(urlTestGroups, {{
    name: spec.urlTestGroup,
    type: "url-test",
    url: "https://www.gstatic.com/generate_204",
    interval: 300,
    tolerance: 50,
    proxies: spec.nodes.map((n) => n.name),
  }});
}}
// <<< NODE-PIPELINE:{group_name}:v1"""


def gen_main_call_js(manifest: dict) -> str:
    calls = "\n".join(
        f"  ensureNPGroup_{gname}(config);" for gname in manifest["groups"])
    return f"""{MAIN_BEGIN.format()}
  // 节点流水线: 所有受管组(节点注入/中转/url-test)统一入口。
{calls}
{MAIN_END.format()}"""


def strip_managed(script: str, begin: str, end: str) -> str:
    """剥离全部受管区域(防御历史嵌套/孤儿标记), 返回无该区域的脚本."""
    pat = re.compile(re.escape(begin) + r"\n?.*?" + re.escape(end) + r"\n*", re.S)
    prev = None
    while pat.search(script) and prev != script:
        prev = script
        script = pat.sub("", script, count=1)
    orphan = {begin.strip(), end.strip()}
    script = "\n".join(l for l in script.splitlines() if l.strip() not in orphan)
    return re.sub(r"\n{3,}", "\n\n", script)


def upsert_js_block(script: str, group_name: str, block: str) -> str:
    """幂等替换托管块(先剥离全部旧区域再插入); 不存在时插到 Mihomo 节点兼容 段之前."""
    script = strip_managed(script, BLOCK_BEGIN.format(group=group_name),
                           BLOCK_END.format(group=group_name))
    anchor = "// ==================== Mihomo 节点兼容 ===================="
    if anchor in script:
        return script.replace(anchor, block + "\n\n" + anchor, 1)
    return script.rstrip() + "\n\n" + block + "\n"


def upsert_main_call(script: str, manifest: dict) -> str:
    script = strip_managed(script, MAIN_BEGIN, MAIN_END)
    block = gen_main_call_js(manifest)
    anchor = "  ensureHomeMeshNode(config);"
    if anchor in script:
        return script.replace(anchor, block + "\n" + anchor, 1)
    raise RuntimeError("main() 注入锚点缺失(ensureHomeMeshNode)")


# ---------------------------------------------------------------- 托管块解析 (check/heal 共用)

def parse_managed_nodes(script: str, group_name: str) -> list[dict] | None:
    """解析托管块 nodes 数组 (每行一个 json.dumps 对象). 缺块返回 None, 解析失败抛错."""
    begin = BLOCK_BEGIN.format(group=group_name)
    end = BLOCK_END.format(group=group_name)
    if begin not in script or end not in script:
        return None
    body = script.split(begin, 1)[1].split(end, 1)[0]
    objs = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if line.startswith("{") and line.endswith("}"):
            objs.append(json.loads(line))
    if not objs:
        raise RuntimeError(f"managed block {group_name} nodes 解析为空")
    return objs


def js_block_drift(group_name: str, group: dict, script: str) -> list[str]:
    """托管块与 manifest 期望(clean_nodes)的全字段对比. 返回漂移节点名列表(空=一致)."""
    deployed = parse_managed_nodes(script, group_name)
    if deployed is None:
        return [f"<block-missing:{group_name}>"]
    expected = clean_nodes(group)
    dep_by = {o.get("name"): o for o in deployed}
    drift = []
    for exp in expected:
        dep = dep_by.get(exp["name"])
        if dep is None or {k: v for k, v in dep.items()} != exp:
            drift.append(exp["name"])
    return drift


# ---------------------------------------------------------------- check

def layer(name: str, ok: bool, detail: str = "") -> bool:
    log(f"  [{'PASS ' if ok else 'DRIFT'}] {name}" + (f" — {detail}" if detail else ""))
    return ok


def warn(name: str, detail: str = ""):
    log(f"  [WARN ] {name}" + (f" — {detail}" if detail else ""))


def cmd_check(args):
    manifest = load_manifest()
    all_ok = True
    script = read_script()
    nodes_yaml = yaml.safe_load(open(NODES_YAML, encoding="utf-8")) or {}
    pool_registry = pool_nodes()
    live_listening = listening_ports()
    _, body = mihomo_get("/proxies")
    px = body.get("proxies", {})
    live_names = set(px.keys())

    for gname, group in manifest["groups"].items():
        log(f"\n== 组 {gname} ==")
        tnames = tether_names(group)
        ports = realized_ports(group)

        # L1 manifest 内部完备性 (server 必填; username/password 成对出现 — 兼容无认证代理)
        complete = all(
            n.get("server") and bool(n.get("username")) == bool(n.get("password"))
            for n in group["nodes"])
        all_ok &= layer("L1 manifest 完备性", complete,
                        f"{len(group['nodes'])} nodes" if complete else "缺 server 或凭据不成对")

        # L2 manifest ↔ Script.js 托管块 (全字段)
        drift = js_block_drift(gname, group, script)
        all_ok &= layer("L2 Script.js 托管块", not drift,
                        "节点全字段一致" if not drift else f"漂移: {drift[:4]}")

        # L3 manifest ↔ nodes.yaml
        by_name = {n.get("name"): n for n in nodes_yaml.get("nodes", [])}
        drift = []
        for tname, port in zip(tnames, ports):
            entry = by_name.get(tname)
            if not entry:
                drift.append(f"{tname}(缺失)")
                continue
            if entry.get("port") != port:
                drift.append(f"{tname}(port {entry.get('port')}≠{port})")
            if entry.get("relay_mode") != group.get("relay_mode", "direct_only"):
                drift.append(f"{tname}(relay_mode)")
            if entry.get("region") != group["region"]:
                drift.append(f"{tname}(region)")
        all_ok &= layer("L3 nodes.yaml 注册", not drift, "一致" if not drift else str(drift[:4]))

        # L4 manifest ↔ mihomo 活体 (中转组仅在候选存活时才要求 dialer-proxy)
        missing = [n["name"] for n in group["nodes"] if n["name"] not in live_names]
        relay = group.get("relay", {}) or {}
        relay_active = bool(relay.get("group")) and any(
            c in live_names for c in relay.get("candidates", []))
        dp_bad = [n["name"] for n in group["nodes"]
                  if n["name"] in px and px[n["name"]].get("dialer-proxy") != (relay.get("group") if relay_active else None)]
        utg = group.get("url_test_group", {}).get("name")
        grp_ok = utg in live_names and (not relay_active or relay.get("group") in live_names)
        ports_up = [p for p in ports if p in live_listening]
        all_ok &= layer("L4 mihomo 活体", not missing and not dp_bad and grp_ok and len(ports_up) == len(ports),
                        f"节点缺{len(missing)} dialer偏移{len(dp_bad)} 组{'缺' if not grp_ok else '✓'} "
                        f"listener {len(ports_up)}/{len(ports)}")

        # L5a manifest ↔ TetherPool 注册一致性 (计入 DRIFT)
        pool_drift = []
        for tname, port in zip(tnames, ports):
            entry = pool_registry.get(tname)
            if not entry:
                pool_drift.append(f"{tname}(未注册)")
            elif entry.get("port") != port:
                pool_drift.append(f"{tname}(port {entry.get('port')}≠{port})")
        all_ok &= layer("L5 TetherPool 注册", not pool_drift,
                        "一致" if not pool_drift else str(pool_drift[:4]))

        # L5b 探活健康度 (仅 WARN — vendor 瞬时抖动属自愈范畴, 不改判一致性)
        unhealthy = [f"{tname}={pool_registry[tname]['status']}"
                     for tname in tnames
                     if tname in pool_registry and pool_registry[tname].get("status") != "healthy"]
        warn("L5 探活健康度", f"{len(tnames) - len(unhealthy)}/{len(tnames)} healthy"
             + (f" | 非healthy: {unhealthy[:4]}" if unhealthy else ""))

    log("\n" + ("✅ 五方一致: 全部 PASS" if all_ok else "⚠️ 存在 DRIFT — 运行 heal 修复"))
    return 0 if all_ok else 2


# ---------------------------------------------------------------- verify

def cmd_verify(args):
    manifest = load_manifest()
    group = manifest["groups"][args.group]
    ports = realized_ports(group)
    servers = {n["exit_ip"] for n in group["nodes"]}
    ok = True

    log(f"== 数据面实测: {args.group} ({ports[0]}–{ports[-1]}) ==")
    exits = pmap(probe_bridge_exit, ports, workers=10)
    matched = sum(1 for e in exits if e in servers)
    log(f"  出口IP ∈ vendor 集合: {matched}/{len(ports)}")
    if matched != len(ports):
        log(f"  → 修复指引: python3 scripts/node_pipeline.py heal --group {args.group} --realize --yes")
    ok &= matched == len(ports)

    for tname, url, okc in TARGETS:
        codes = pmap(lambda p: curl_code(url, ["--socks5-hostname", f"127.0.0.1:{p}"]), ports, workers=10)
        good = sum(1 for c in codes if c in okc)
        log(f"  {tname:8s}: {good}/{len(ports)}")
        ok &= good == len(ports)

    route = group.get("gateway_route", "")
    if route:
        def gw(user):
            try:
                r = subprocess.run(
                    ["curl", "-s", "-x", GATEWAY, "-U", f"{user}:x", PROBE_URL, "--max-time", "35"],
                    capture_output=True, text=True, timeout=40)
                return r.stdout.strip() if r.returncode == 0 else ""
            except Exception:  # noqa: BLE001
                return ""

        def binding(acc):
            try:
                with urllib.request.urlopen(f"{CONTROL}/v1/acquire?account_id={acc}", timeout=10) as r:
                    d = json.load(r)
                return d.get("node_name", ""), d.get("exit_ip", "")
            except Exception:  # noqa: BLE001
                return "", ""

        e1, e2, s1 = gw(f"pipe_v1?{route}"), gw(f"pipe_v2?{route}"), gw(f"pipe_v1?{route}")
        n1, x1 = binding("pipe_v1")
        n2, x2 = binding("pipe_v2")
        consistent = (e1 == x1 and bool(x1)) and (e2 == x2 and bool(x2))
        sticky = s1 == e1 and bool(e1)
        log(f"  网关路由: {n1 or '?'}→{e1} | {n2 or '?'}→{e2} "
            f"{'✅ 出口与绑定一致' if consistent else '⚠️ 不一致'}")
        log(f"  粘性: {'✅ ' + s1 if sticky else '⚠️ 漂移 ' + str([e1, s1])}")
        ok &= consistent and sticky

    if args.load:
        burst_ports = (ports * args.load)[: max(args.load, len(ports))]
        burst = pmap(lambda p: curl_code("https://www.google.com/generate_204",
                                         ["--socks5-hostname", f"127.0.0.1:{p}"]),
                     burst_ports, workers=min(args.load, 60))
        good = sum(1 for c in burst if c == "204")
        log(f"  压测 {len(burst)} 并发连接: {good}/{len(burst)}")
        ok &= good == len(burst)

    log("\n" + (f"✅ verify {args.group}: 全绿" if ok else "⚠️ verify 存在失败项"))
    return 0 if ok else 2


# ---------------------------------------------------------------- import

def next_free_ports(manifest: dict, count: int) -> list[int]:
    used = []
    for g in manifest["groups"].values():
        bp = g.get("bridge_ports", {})
        for key in ("declared", "realized"):
            if bp.get(key):
                lo, hi = bp[key].split("-")
                used.extend(range(int(lo), int(hi) + 1))
    start = (max(used) + 1) if used else 11053
    return list(range(start, start + count))


def next_free_seq(nodes_yaml: dict, manifest: dict) -> int:
    """tether 注册名 seq 起始 = 已用最大 seq + 1 (扫描 nodes.yaml 的 clash-NNN-*)."""
    seqs = [int(m.group(1)) for n in nodes_yaml.get("nodes", [])
            if (m := re.match(r"clash-(\d+)-", str(n.get("name", ""))))]
    for g in manifest["groups"].values():
        seqs.append(int(g.get("tether_seq_start", 0)))
    return (max(seqs) + 1) if seqs else 52


def cmd_import(args):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.group):
        log("[-] --group 必须匹配 ^[a-z][a-z0-9_]*$ (同时用作 JS 标识符 / YAML key / 文件名片段)")
        return 1
    manifest = load_manifest()
    if args.group in manifest["groups"] and not args.replace:
        log(f"组 {args.group} 已存在于 manifest; 加 --replace 覆盖")
        return 1
    if args.relay_group and not args.relay_candidates:
        log("[-] --relay-group 必须与 --relay-candidates 成对给出 (否则会制造永久 DRIFT)")
        return 1

    log(f"[1/7] 拉取订阅: {args.source}")
    sub = fetch_subscription(args.source)
    proxies = [p for p in sub.get("proxies", []) if p.get("server") and p.get("port")]
    log(f"      解析到 {len(proxies)} 个节点")

    log("[2/7] 并发预测活 (直连 vendor, 记录实测出口 IP)")
    exits = pmap(probe_vendor_exit, proxies, workers=10)
    pairs = [(p, ip) for p, ip in zip(proxies, exits) if ip]
    log(f"      存活 {len(pairs)}/{len(proxies)}")
    if not pairs:
        log("      ❌ 全灭, 终止")
        return 2
    if len(pairs) < len(proxies):
        log("      ⚠️ 存在死亡节点, 仅导入存活部分")
    proxies = [p for p, _ in pairs]

    count = len(proxies)
    declared = next_free_ports(manifest, count)
    ny_now = yaml.safe_load(open(NODES_YAML, encoding="utf-8")) or {}
    seq_start = next_free_seq(ny_now, manifest)
    prefix = args.prefix or f"{args.group}-"
    clash_names = [f"{prefix}{i+1:02d}" for i in range(count)]
    relay = {"group": args.relay_group, "candidates": args.relay_candidates} if args.relay_group else None
    group = {
        "source": args.source,
        "protocol": str(proxies[0].get("type", "http")).lower(),
        "clash_node_prefix": prefix,
        "tether_name_template": f"clash-{{seq:03d}}-{args.group}-{{idx:02d}}",
        "tether_seq_start": seq_start,
        "region": args.region,
        "tier": args.tier,
        "egress_type": "clash_proxy",
        "tags": ["clash", "bridge", str(proxies[0].get("type", "http")).lower(), args.group],
        "relay_mode": "direct_only",
        "relay": relay,
        "url_test_group": {"name": args.url_test_group or f"{args.group}-auto", "interval": 300, "tolerance": 50},
        "bridge_ports": {"declared": f"{declared[0]}-{declared[-1]}", "realized": ""},
        "gateway_route": f"region={args.region}&type=clash_proxy",
        "nodes": [
            {"idx": i + 1, "name": clash_names[i], "type": group["protocol"],
             "server": p["server"], "port": p["port"],
             "username": p.get("username", ""), "password": p.get("password", ""),
             "exit_ip": exits_i}
            for i, (p, exits_i) in enumerate(pairs)
        ],
    }

    if args.dry_run:
        log(f"[dry-run] 将新增组 {args.group}: {count} 节点, seq_start={seq_start}, "
            f"声明桥接段 {declared[0]}-{declared[-1]}")
        log("[dry-run] 将写入: Script.js 托管块 / Merge.yaml listeners / nodes.yaml / manifest")
        return 0

    log("[3/7] 写 Script.js 托管块 + main() 调用")
    bak = backup(SCRIPT_JS, f"import-{args.group}")
    script = read_script()
    try:
        script = upsert_js_block(script, args.group, gen_group_js(args.group, group))
        script = upsert_main_call(script, {**manifest, "groups": {**manifest["groups"], args.group: group}})
    except RuntimeError as e:
        log(f"[-] Script.js 注入失败: {e} — 请先恢复 Clash Verge 宿主 Script.js (含 main/ensureHomeMeshNode 锚点)")
        return 1
    atomic_write_text(SCRIPT_JS, script)
    js_check_or_rollback(SCRIPT_JS, bak)

    log("[4/7] 写 Merge.yaml listeners (底座重建保险)")
    backup(MERGE_YAML, f"import-{args.group}")
    merge = yaml.safe_load(open(MERGE_YAML, encoding="utf-8")) or {}
    listeners = merge.get("listeners") or []
    existing_proxies = {str(l.get("proxy")) for l in listeners}
    new_ls = [{"name": f"pipe-{args.group}-{i+1:03d}", "type": "socks",
               "port": declared[i], "listen": "127.0.0.1", "proxy": clash_names[i]}
              for i in range(count) if clash_names[i] not in existing_proxies]
    merge["listeners"] = listeners + new_ls
    atomic_write_text(MERGE_YAML, yaml.dump(merge, allow_unicode=True, sort_keys=False))

    log("[5/7] 写 nodes.yaml + manifest")
    backup(NODES_YAML, f"import-{args.group}")
    have = {n.get("name") for n in ny_now.get("nodes", [])}
    for i, cname in enumerate(clash_names):
        tname = group["tether_name_template"].format(seq=seq_start + i, idx=i + 1)
        if tname in have:
            continue
        ny_now["nodes"].append({
            "name": tname, "host": "127.0.0.1", "port": declared[i],
            "protocol": "socks5h", "region": group["region"],
            "egress_type": "clash_proxy", "tags": group["tags"] + ["pipeline"],
            "weight": 100, "tier": group["tier"], "relay_mode": "direct_only",
        })
    atomic_write_text(NODES_YAML, yaml.dump(ny_now, allow_unicode=True, sort_keys=False, width=200))

    group["bridge_ports"]["realized"] = group["bridge_ports"]["declared"]
    manifest["groups"][args.group] = group
    save_manifest(manifest)

    log("[6/7] 重载: Verge 重新激活 → TetherPool 重启")
    if args.restart_verge:
        restart_verge_app()
    else:
        try:
            log("      ⏸ 请在 Clash Verge 里重新激活当前配置, 然后回车继续...")
            input()
        except EOFError:
            log("      [-] 非交互环境且未指定 --restart-verge, 中止 (重载未完成)")
            return 1
    restart_tetherpool()

    rc, _ = realize_group(manifest, args.group, declared)
    if rc != 0:
        return rc
    return cmd_verify(argparse.Namespace(group=args.group, load=10))


def realize_group(manifest: dict, gname: str, declared: list[int]) -> tuple[int, bool]:
    """实现端口映射: 探测声明端口出口 IP ↔ 节点 exit_ip 匹配, 漂移时回写 manifest+nodes.yaml.

    返回 (rc, changed): rc 0=一致或已校正, 2=无法唯一匹配(需人工); changed=是否发生回写.
    """
    group = manifest["groups"][gname]
    count = len(group["nodes"])
    time.sleep(5)
    live = listening_ports()
    up = [p for p in declared if p in live]
    if len(up) != count:
        log(f"      ⚠️ listener 在位 {len(up)}/{count} — 缺失部分需 Verge 重新激活后重跑 heal")
        return 2, False

    exits = pmap(probe_bridge_exit, declared, workers=10)
    ip_to_idx: dict[str, int] = {}
    ambiguous = set()
    for n in group["nodes"]:
        eip = n.get("exit_ip", "")
        if eip in ip_to_idx:
            ambiguous.add(eip)
        ip_to_idx[eip] = n["idx"]

    mapping: dict[int, int] = {}
    unknown = []
    for port, eip in zip(declared, exits):
        if not eip or eip not in ip_to_idx or eip in ambiguous:
            unknown.append((port, eip))
            continue
        mapping[ip_to_idx[eip]] = port
    if unknown or len(mapping) != count:
        log(f"      ❌ 端口↔节点无法唯一匹配 (未识别: {unknown[:3]}...), 需人工核对订阅出口")
        return 2, False

    order_changed = any(mapping[n["idx"]] != declared[i] for i, n in enumerate(group["nodes"]))
    if not order_changed:
        log(f"      ✓ 桥接段 {declared[0]}-{declared[-1]} 全部在位且映射与声明一致")
        return 0, False

    # 漂移: 回写 realized_map + nodes.yaml 端口, 重启 TetherPool
    log(f"      ⚠️ 端口映射漂移, 回写 realized_map: {mapping}")
    group["bridge_ports"]["realized_map"] = {str(idx): port for idx, port in mapping.items()}
    save_manifest(manifest)
    ny = yaml.safe_load(open(NODES_YAML, encoding="utf-8")) or {}
    tnames = tether_names(group)
    by_name = {n.get("name"): n for n in ny.get("nodes", [])}
    for tname, n in zip(tnames, group["nodes"]):
        entry = by_name.get(tname)
        if entry:
            entry["port"] = mapping[n["idx"]]
    atomic_write_text(NODES_YAML, yaml.dump(ny, allow_unicode=True, sort_keys=False, width=200))
    restart_tetherpool()
    log("      ✓ realized_map + nodes.yaml 已校正, TetherPool 已重启")
    return 0, True


# ---------------------------------------------------------------- heal

def cmd_heal(args):
    manifest = load_manifest()
    gname = args.group
    group = manifest["groups"][gname]
    fixed = []

    js_skip_reason = None
    if not SCRIPT_JS.exists():
        baks = sorted(VERGE_DIR.joinpath("profiles").glob("Script.js.bak-*"))
        if baks:
            import shutil
            log(f"[heal] Script.js 缺失 — 从最新备份恢复宿主脚本: {baks[-1].name}")
            shutil.copy2(baks[-1], SCRIPT_JS)
            fixed.append("Script.js restore")
        else:
            js_skip_reason = ("Script.js 缺失且无备份 (profiles/Script.js.bak-*) — "
                              "请先恢复宿主脚本(含 main/ensureHomeMeshNode 锚点)再重跑 heal")

    script = read_script()
    drift = js_block_drift(gname, group, script)
    if js_skip_reason is None and (drift or args.force_js):
        log(f"[heal] 重建 Script.js 托管块 (漂移: {drift[:4] if drift else 'force'})")
        bak = backup(SCRIPT_JS, f"heal-{gname}")
        try:
            script = upsert_js_block(script, gname, gen_group_js(gname, group))
            script = upsert_main_call(script, manifest)
        except RuntimeError as e:
            log(f"[heal] ❌ Script.js 重建失败: {e} — 请恢复宿主脚本后重跑 heal")
            js_skip_reason = str(e)
        else:
            atomic_write_text(SCRIPT_JS, script)
            js_check_or_rollback(SCRIPT_JS, bak)
            fixed.append("Script.js")
    if js_skip_reason:
        log(f"[heal] ⚠️ 跳过 JS 重建: {js_skip_reason}")

    ny = yaml.safe_load(open(NODES_YAML, encoding="utf-8")) or {}
    by_name = {n.get("name"): n for n in ny.get("nodes", [])}
    tnames = tether_names(group)
    ports = realized_ports(group)
    need_node_fix = any(
        (n := by_name.get(t)) is None or n.get("port") != p or n.get("relay_mode") != "direct_only"
        for t, p in zip(tnames, ports))
    if need_node_fix:
        log("[heal] 对齐 nodes.yaml (端口/relay_mode)")
        backup(NODES_YAML, f"heal-{gname}")
        for t, p in zip(tnames, ports):
            entry = by_name.get(t)
            if entry is None:
                ny["nodes"].append({"name": t, "host": "127.0.0.1", "port": p,
                                    "protocol": "socks5h", "region": group["region"],
                                    "egress_type": "clash_proxy", "tags": group["tags"] + ["pipeline"],
                                    "weight": 100, "tier": group["tier"], "relay_mode": "direct_only"})
            else:
                entry["port"] = p
                entry["relay_mode"] = "direct_only"
        atomic_write_text(NODES_YAML, yaml.dump(ny, allow_unicode=True, sort_keys=False, width=200))
        fixed.append("nodes.yaml")

    merge = yaml.safe_load(open(MERGE_YAML, encoding="utf-8")) or {}
    have_insurance = {str(l.get("proxy")) for l in merge.get("listeners", [])}
    missing_insurance = [n["name"] for n in group["nodes"] if n["name"] not in have_insurance]
    if missing_insurance:
        log("[heal] 补 Merge.yaml 重建镜像 listeners")
        backup(MERGE_YAML, f"heal-{gname}")
        ports_decl = declared_ports(group)
        new_ls = [{"name": f"pipe-{gname}-{i+1:03d}", "type": "socks",
                   "port": ports_decl[i], "listen": "127.0.0.1", "proxy": n["name"]}
                  for i, n in enumerate(group["nodes"]) if n["name"] not in have_insurance]
        merge["listeners"] = (merge.get("listeners") or []) + new_ls
        atomic_write_text(MERGE_YAML, yaml.dump(merge, allow_unicode=True, sort_keys=False))
        fixed.append("Merge.yaml")

    if fixed:
        if args.yes or (sys.stdin.isatty() and input("应用 TetherPool 重启? [y/N] ").lower() == "y"):
            restart_tetherpool()
            fixed.append("TetherPool restart")
        else:
            log("[heal] ⚠️ 未重启 TetherPool — 网关注册表仍是旧快照, 需手动 start.sh restart")

    if args.realize:
        rc_r, changed = realize_group(manifest, gname, declared_ports(group))
        if changed:
            fixed.append("realize(端口映射回写)")
        if rc_r != 0:
            log("[heal] ❌ realize 未能唯一匹配端口映射, 需人工核对")
            return 2

    live = listening_ports()
    ports_up = [p for p in ports if p in live]
    if len(ports_up) != len(ports):
        log(f"[heal] ⚠️ Verge 底座 listener 缺失 {len(ports) - len(ports_up)} 个 — "
            f"需在 Clash Verge 重新激活配置后重跑 heal")
        if args.restart_verge:
            restart_verge_app()
    rc = cmd_check(argparse.Namespace())
    log(("[heal] 已修复: " + ", ".join(fixed)) if fixed else "[heal] 无需修复")
    return rc


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="TetherPool × 3x-ui 节点接入流水线")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_imp = sub.add_parser("import", help="新增节点组全流程")
    p_imp.add_argument("source", help="订阅 url 或本地 yaml")
    p_imp.add_argument("--group", required=True)
    p_imp.add_argument("--region", required=True)
    p_imp.add_argument("--tier", default="standard", choices=["standard", "premium"])
    p_imp.add_argument("--prefix", default=None, help="mihomo 节点名前缀")
    p_imp.add_argument("--url-test-group", default=None)
    p_imp.add_argument("--relay-group", default=None, help="中转 fallback 组名")
    p_imp.add_argument("--relay-candidates", nargs="*", default=[])
    p_imp.add_argument("--replace", action="store_true")
    p_imp.add_argument("--dry-run", action="store_true")
    p_imp.add_argument("--restart-verge", action="store_true")
    p_imp.set_defaults(func=cmd_import)

    p_chk = sub.add_parser("check", help="五方一致性审计")
    p_chk.set_defaults(func=cmd_check)

    p_ver = sub.add_parser("verify", help="数据面实测")
    p_ver.add_argument("--group", required=True)
    p_ver.add_argument("--load", type=int, default=0)
    p_ver.set_defaults(func=cmd_verify)

    p_heal = sub.add_parser("heal", help="漂移修复(含凭据轮换)")
    p_heal.add_argument("--group", required=True)
    p_heal.add_argument("--yes", action="store_true")
    p_heal.add_argument("--force-js", action="store_true")
    p_heal.add_argument("--realize", action="store_true",
                        help="探测实际端口↔节点映射并回写 (出口 IP 漂移时使用)")
    p_heal.add_argument("--restart-verge", action="store_true")
    p_heal.set_defaults(func=cmd_heal)

    args = ap.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
