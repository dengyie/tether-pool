#!/usr/bin/env python3
"""sub 订阅池每日刷新: 拉取 -> 探活 -> 全量替换 sub-xxx 节点 (桥核 25001+ 端口段复用).

加固 (2026-11-14 深度 review):
- flock 单实例锁 (cron 与手动运行互斥, 探活临时 mihomo 不与主探活/其他刷新打架)
- 换绑悬空清理: 替换后 sub 节点名集变化, 旧节点名上的 account 绑定直接释放
  (sqlite delete), 消费端下次 acquire 自然重绑, 不会指到已不存在的节点
- 失败非零退出 + 明确 stderr 标记 [SUB_REFRESH_FAIL], 日志可 grep 告警
- 备份带时间戳并保留最近 7 份, 不再覆盖固定名
"""
import fcntl, json, re, socket, subprocess, sys, time, urllib.request, os, glob
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# bearer-token 型订阅 URL 即凭据, 仓库不落盘: 从 /etc/ip-pool-secrets.env 读。
def _sub_url() -> str:
    url = os.environ.get("SUB1_URL", "").strip()
    if not url:
        print("[SUB_REFRESH_FAIL] SUB1_URL not set (see /etc/ip-pool-secrets.env)", file=sys.stderr)
        sys.exit(1)
    return url

CFG = Path("/opt/ip-pool/config/huawei-bridge.yaml")
NODES = Path("/opt/ip-pool/config/nodes.yaml")
DB = Path("/opt/ip-pool/data/ip_pool.db")
LOCK = Path("/tmp/sub_pool_refresh.lock")
BACKUP_DIR = Path("/opt/ip-pool/config/backups")
BASE_PORT = 25001
PROBE_POOL = 39001
KEEP_BACKUPS = 7

def backup_with_rotation():
    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    for src in (CFG, NODES):
        data = src.read_bytes()
        (BACKUP_DIR / f"{src.name}.{stamp}").write_bytes(data)
    # 只轮转本脚本的备份 (曾误删 pigeonw 脚本的时间戳备份 - glob 跨脚本匹配)
    olds = sorted(glob.glob(str(BACKUP_DIR / "huawei-bridge.yaml.*")) +
                  glob.glob(str(BACKUP_DIR / "nodes.yaml.*")))
    for old in olds[:-KEEP_BACKUPS * 2]:
        os.unlink(old)

def _unbool_proxies(proxies):
    """YAML 1.1 陷阱: 凭据恰为 no/yes/on/off/y/n 时被 load 成 bool, dump 后变
    true/false 损坏凭据。回退为字符串原值。"""
    for p in proxies:
        for k in ("username", "password", "server"):
            v = p.get(k)
            if isinstance(v, bool):
                p[k] = "true" if v else "false"
    return proxies


def fetch_and_parse():
    import yaml
    raw = urllib.request.urlopen(_sub_url(), timeout=30).read().decode("utf-8", "replace")
    d = yaml.safe_load(raw)
    return _unbool_proxies(d.get("proxies", []))

def probe_via_listeners(proxies):
    import yaml
    listeners = [{"name": f"probe-{i:03d}", "type": "socks", "port": PROBE_POOL + i,
                  "listen": "127.0.0.1", "proxy": p["name"]} for i, p in enumerate(proxies)]
    cfg = {"proxies": proxies, "listeners": listeners, "mode": "direct",
           "log-level": "warning", "ipv6": False,
           "external-controller": "127.0.0.1:39998"}
    probe_dir = Path("/tmp/mihomo-probe-sub")
    probe_dir.mkdir(exist_ok=True)
    tmp = probe_dir / "probe.yaml"
    fd = os.open(str(tmp), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False))
    proc = subprocess.run(["/opt/ip-pool/bin/mihomo", "-t", "-d", str(probe_dir), "-f", str(tmp)],
                          capture_output=True, text=True)
    if "successful" not in proc.stdout + proc.stderr:
        print("probe config invalid:", proc.stdout[-300:], proc.stderr[-300:], file=sys.stderr)
        return []
    core = subprocess.Popen(["/opt/ip-pool/bin/mihomo", "-d", str(probe_dir), "-f", str(tmp)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)
    def probe(i_p):
        i, p = i_p
        try:
            s = socket.create_connection(("127.0.0.1", PROBE_POOL + i), timeout=10)
            s.sendall(b"\x05\x01\x00")
            if s.recv(2) != b"\x05\x00": return None
            ipb = bytes(int(x) for x in "208.95.112.1".split("."))
            s.sendall(b"\x05\x01\x00\x01" + ipb + b"\x00\x50")
            if s.recv(10)[1] != 0: return None
            s.sendall(b"GET /json HTTP/1.0\r\nHost: ip-api.com\r\n\r\n")
            s.settimeout(6)
            data = b""
            while True:
                c = s.recv(4096)
                if not c: break
                data += c
                if len(data) > 8192: break
            body = data.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in data else b""
            j = json.loads(body)
            if j.get("query"): return (i, p, j["query"])
        except Exception:
            return None
        return None
    try:
        with ThreadPoolExecutor(max_workers=50) as ex:
            return [r for r in ex.map(probe, enumerate(proxies)) if r]
    finally:
        core.terminate()
        try:
            core.wait(timeout=5)
        except Exception:
            core.kill()

def region_of(name):
    m = re.search(r"(?:s\d-)?([A-Z]{2})[_-]", name)
    cc = m.group(1).lower() if m else None
    return "us-west" if cc == "us" else (cc or "misc")

def main():
    import yaml
    lock_fd = open(LOCK, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[SUB_REFRESH_SKIP] another refresh is running", file=sys.stderr)
        return 0
    try:
        proxies = fetch_and_parse()
        # 强制 s1- 前缀 (is_sub 识别依赖; 裸名事故教训) + 订阅内部重名去重
        seen = set()
        for p in proxies:
            if not re.match(r"^s\d-", p["name"]):
                p["name"] = "s1-" + p["name"]
            base, n = p["name"], p["name"]
            i = 2
            while n in seen:
                n = f"{base}-{i}"; i += 1
            seen.add(n)
            p["name"] = n
        print(f"fetched {len(proxies)} proxies from sub")
        alive = probe_via_listeners(proxies)
        print(f"alive {len(alive)}/{len(proxies)}")
        if len(alive) < 5:
            print("[SUB_REFRESH_FAIL] alive too few, keep current state", file=sys.stderr)
            return 1
        backup_with_rotation()

        # 替换桥核 (sub 前缀条目)
        main = yaml.safe_load(CFG.read_text())
        is_sub = lambda name: bool(re.match(r"^s\d-", name or "") or "【机房】" in (name or "") or "【商宽】" in (name or ""))
        main["proxies"] = [p for p in main["proxies"] if not is_sub(p["name"])]
        main["listeners"] = [l for l in main["listeners"] if not str(l.get("name", "")).startswith("sub-bridge-")]
        new_proxies, new_listeners, new_nodes = [], [], []
        for i, (_, p, ip) in enumerate(alive):
            p["udp"] = False
            new_proxies.append(p)
            new_listeners.append({"name": f"sub-bridge-{i:03d}", "type": "socks",
                                  "port": BASE_PORT + i, "listen": "127.0.0.1", "proxy": p["name"]})
            reg = region_of(p["name"])
            kind = "residential" if "商宽" in p["name"] else ("datacenter" if "机房" in p["name"] else "")
            tags = ["clash", "bridge", "sub", str(p.get("type", "http"))]
            if kind: tags.append(kind)
            if reg == "us-west" and kind == "residential": tags.append("us-residential")
            new_nodes.append({"name": f"clash-{500+i:03d}-sub-{i:03d}", "host": "127.0.0.1",
                              "port": BASE_PORT + i, "protocol": "socks5h", "region": reg,
                              "egress_type": "clash_proxy", "tags": tags, "tier": "standard",
                              "relay_mode": "direct_only", "fault_domain": "huawei-sub-bridge",
                              "exit_ip": ip, "vendor_name": p["name"]})
        main["proxies"] += new_proxies
        main["listeners"] += new_listeners

        # nodes.yaml 替换 (clash-5xx-sub- 前缀)
        nodes_d = yaml.safe_load(NODES.read_text())
        nl = nodes_d if isinstance(nodes_d, list) else nodes_d.get("nodes")
        old_node_names = {n["name"] for n in nl if "-sub-" in str(n["name"]) and re.match(r"^clash-5", str(n["name"]))}
        nl[:] = [n for n in nl if n["name"] not in old_node_names]
        nl.extend(new_nodes)
        if isinstance(nodes_d, dict): nodes_d["nodes"] = nl
        new_names = {n["name"] for n in new_nodes}

        # 落盘前先校验
        CFG.write_text(yaml.safe_dump(main, allow_unicode=True, sort_keys=False))
        t = subprocess.run(["/opt/ip-pool/bin/mihomo", "-t", "-d", "/opt/ip-pool/config/huawei-bridge",
                            "-f", str(CFG)], capture_output=True, text=True)
        if "successful" not in t.stdout + t.stderr:
            print("[SUB_REFRESH_FAIL] mihomo -t failed: rc=%s out=%r err=%r" % (t.returncode, t.stdout[-300:], t.stderr[-300:]), file=sys.stderr)
            open("/tmp/sub-refresh-failed-cfg.yaml","w").write(Path(str(CFG)).read_text())
            latest_cfg = sorted(glob.glob(str(BACKUP_DIR / "huawei-bridge.yaml.*")))[-1]
            subprocess.run(["cp", latest_cfg, str(CFG)])
            return 1
        NODES.write_text(yaml.safe_dump(nodes_d, allow_unicode=True, sort_keys=False))

        subprocess.run(["systemctl", "restart", "ip-pool-huawei-bridge.service"], check=False)
        subprocess.run(["systemctl", "restart", "ip-pool-api.service", "ip-pool-gateway.service"], check=False)
        # 部署后验证: 桥核 listener 实际在位数 (mihomo 静默跳过冲突端口教训)
        time.sleep(6)
        r = subprocess.run(["ss", "-tln"], capture_output=True, text=True)
        listening = sum(1 for l in new_listeners if str(":%d " % l["port"]) in r.stdout)
        if listening < len(new_listeners) - 2:
            # 新状态已生效, 报 FAIL 会误导 —— 真回滚到部署前备份再重启。
            print("[SUB_REFRESH_FAIL] listeners missing: %d/%d up -> rolling back" % (
                listening, len(new_listeners)), file=sys.stderr)
            for suffix, dst in (("huawei-bridge.yaml.*", CFG), ("nodes.yaml.*", NODES)):
                baks = sorted(glob.glob(str(BACKUP_DIR / suffix)))
                if baks:
                    subprocess.run(["cp", baks[-1], str(dst)], check=False)
            subprocess.run(["systemctl", "restart", "ip-pool-huawei-bridge.service",
                            "ip-pool-api.service", "ip-pool-gateway.service"], check=False)
            time.sleep(6)
            r2 = subprocess.run(["ss", "-tln"], capture_output=True, text=True)
            listening2 = sum(1 for l in new_listeners if str(":%d " % l["port"]) in r2.stdout)
            print("[SUB_REFRESH_ROLLBACK] restored pre-refresh state, listeners=%d/%d" % (
                listening2, len(new_listeners)), file=sys.stderr)
            return 1

        # 悬空绑定清理 (移到部署验证通过后: 旧桥核活着时先清理, 请求可能重新绑到已退役节点)
        dangling = old_node_names - new_names
        if dangling:
            import sqlite3
            conn = sqlite3.connect(str(DB))
            cur = conn.cursor()
            ph = ",".join("?" * len(dangling))
            cur.execute(f"DELETE FROM account_allocations WHERE node_name IN ({ph})", tuple(dangling))
            n_alloc = cur.rowcount
            cur.execute(f"DELETE FROM binding_changes WHERE to_node IN ({ph}) OR from_node IN ({ph})", tuple(dangling) * 2)
            conn.commit()
            conn.close()
            print(f"dangling bindings cleaned: {n_alloc} allocations over {len(dangling)} retired nodes")

        print("deployed: %d sub nodes, bridge listeners=%d, nodes=%d, verified listening=%d" % (len(alive), len(main["listeners"]), len(nl), listening))
        return 0
    except Exception as e:
        import traceback
        print("[SUB_REFRESH_FAIL] unexpected:", e, file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)

if __name__ == "__main__":
    sys.exit(main())
