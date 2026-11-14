#!/usr/bin/env python3
"""pigeonw 订阅池每日刷新: 拉取 -> 探活 -> 槽位保号更新 pigeonw-bridge 核.

与 sub_pool_refresh.py 的关键差异: pigeonw 的消费者 (zcode-hub agent) 按节点编号
钉节点 (clash-301~320-pigeonw-XX -> 监听 12053~12072), 因此本脚本绝不重建节点,
只在固定槽位上原位更新 server/port/凭据/exit_ip, 编号与监听端口永不变。

槽位匹配算法 (保号优先):
  1. 新条目 (server+port+user+pass) 与旧槽位完全一致 -> 原槽位, 无变化
  2. 新条目 server+port 与旧槽位一致但凭据变了 -> 原槽位原地换凭据 (同出口轮换)
  3. 剩余新条目按顺序填剩余空闲槽位; 旧槽位无新条目匹配 -> 淘汰 (悬空绑定清理)
  4. 上游条目多于槽位容量 -> 扩容, 监听端口顺延 (12053 起最大到 12099)

安全性: flock 互斥 / 探活门 (全死不部署) / mihomo -t 失败回滚 / 时间戳备份轮转 7 份 /
部署后 ss -tln 实听校验 / 失败 [PIGEONW_REFRESH_FAIL] 可 grep 告警。
"""
import fcntl, glob, json, os, re, socket, subprocess, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SUB_URL = "https://nodes.pigeonw.com/s/jmQZPf7wiq1qXoET1K9irGu-5PFLp39seTefwDAz-kc/clash.yaml"
CFG = Path("/opt/ip-pool/config/pigeonw-bridge.yaml")
CFG_DIR = Path("/opt/ip-pool/config/pigeonw-bridge")
NODES = Path("/opt/ip-pool/config/nodes.yaml")
DB = Path("/opt/ip-pool/data/ip_pool.db")
LOCK = Path("/tmp/pigeonw_pool_refresh.lock")
BACKUP_DIR = Path("/opt/ip-pool/config/backups")
BASE_PORT = 12053
MAX_PORT = 12099          # 槽位扩容上限
PROBE_POOL = 39101        # 临时探活监听段 (避开 sub 刷新的 39001)
KEEP_BACKUPS = 7
PROXY_PREFIX = "PIGEONW-"
LISTENER_PREFIX = "pigeonw-bridge-"
NODE_RE = re.compile(r"^clash-(3\d\d)-pigeonw-(\d+)$")


def backup_with_rotation():
    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    for src in (CFG, NODES):
        (BACKUP_DIR / f"{src.name}.{stamp}-pigeonw").write_bytes(src.read_bytes())
    olds = sorted(glob.glob(str(BACKUP_DIR / "*-pigeonw")))
    for old in olds[:-KEEP_BACKUPS * 2]:
        os.unlink(old)


def fetch_and_parse():
    """curl 子进程拉取 (python urllib 直连该域 TLS 握手易超时, curl 复用系统 TLS 稳定)."""
    import yaml
    last_err = None
    for attempt in range(3):
        r = subprocess.run(["curl", "-sS", "--max-time", "40", "-A", "clash-verge/1.6.6",
                            SUB_URL, "-o", "/tmp/pigeonw-fetch.yaml"],
                           capture_output=True, text=True)
        if r.returncode == 0 and os.path.getsize("/tmp/pigeonw-fetch.yaml") > 100:
            break
        last_err = (r.returncode, r.stderr[-200:])
        time.sleep(3)
    else:
        raise RuntimeError(f"fetch failed after 3 attempts: {last_err}")
    d = yaml.safe_load(Path("/tmp/pigeonw-fetch.yaml").read_text())
    return d.get("proxies", [])


def probe_via_listeners(proxies):
    """临时 mihomo 核 + 明文 ip-api 探活, 返回 [(idx, proxy, exit_ip)] (仅活节点)."""
    import yaml
    listeners = [{"name": f"probe-{i:03d}", "type": "socks", "port": PROBE_POOL + i,
                  "listen": "127.0.0.1", "proxy": p["name"]} for i, p in enumerate(proxies)]
    cfg = {"proxies": proxies, "listeners": listeners, "mode": "global",
           "log-level": "warning", "ipv6": False, "allow-lan": False,
           "external-controller": ""}
    tmp = Path("/tmp/pigeonw-refresh-probe.yaml")
    tmp.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False))
    proc = subprocess.run(["/opt/ip-pool/bin/mihomo", "-t", "-d", "/tmp/mihomo-probe", "-f", str(tmp)],
                          capture_output=True, text=True)
    if "successful" not in proc.stdout + proc.stderr:
        print("probe config invalid:", proc.stdout[-300:], proc.stderr[-300:], file=sys.stderr)
        return []
    core = subprocess.Popen(["/opt/ip-pool/bin/mihomo", "-d", "/tmp/mihomo-probe", "-f", str(tmp)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)

    def probe(i_p):
        i, p = i_p
        try:
            s = socket.create_connection(("127.0.0.1", PROBE_POOL + i), timeout=10)
            s.sendall(b"\x05\x01\x00")
            if s.recv(2) != b"\x05\x00":
                return None
            ipb = bytes(int(x) for x in "208.95.112.1".split("."))
            s.sendall(b"\x05\x01\x00\x01" + ipb + b"\x00\x50")
            if s.recv(10)[1] != 0:
                return None
            s.sendall(b"GET /json HTTP/1.0\r\nHost: ip-api.com\r\n\r\n")
            s.settimeout(6)
            data = b""
            while True:
                c = s.recv(4096)
                if not c:
                    break
                data += c
                if len(data) > 8192:
                    break
            body = data.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in data else b""
            j = json.loads(body)
            if j.get("query"):
                return (i, p, j["query"])
        except Exception:
            return None
        return None

    try:
        with ThreadPoolExecutor(max_workers=20) as ex:
            return [r for r in ex.map(probe, enumerate(proxies)) if r]
    finally:
        core.terminate()


def slot_key(idx):
    return {"name": f"{PROXY_PREFIX}{idx:03d}", "listener": f"{LISTENER_PREFIX}{idx:03d}",
            "port": BASE_PORT + idx - 1, "node": f"clash-{300 + idx}-pigeonw-{idx:02d}"}


def main():
    import yaml
    lock_fd = open(LOCK, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[PIGEONW_REFRESH_SKIP] another refresh is running", file=sys.stderr)
        return 0
    try:
        fetched = fetch_and_parse()
        if not fetched:
            print("[PIGEONW_REFRESH_FAIL] fetch empty, keep current state", file=sys.stderr)
            return 1
        # 订阅条目统一改名 PIGEONW-NNN 在匹配后再定; 先去重 (重名会硬失败 mihomo -t)
        seen = set()
        for p in fetched:
            base, n, i = p["name"], p["name"], 2
            while n in seen:
                n = f"{base}-{i}"; i += 1
            seen.add(n)
            p["name"] = n
        print(f"fetched {len(fetched)} proxies from pigeonw")
        alive = probe_via_listeners(fetched)
        print(f"alive {len(alive)}/{len(fetched)}")
        if len(alive) < 3:
            print("[PIGEONW_REFRESH_FAIL] alive too few (%d/%d), keep current state" % (
                len(alive), len(fetched)), file=sys.stderr)
            return 1
        exit_ip_by_entry = {(p["server"], int(p.get("port", 0)), p.get("username"), p.get("password")): ip
                            for (_, p, ip) in alive}

        # ---- 读取现状 ----
        main_cfg = yaml.safe_load(CFG.read_text())
        old_proxies = {p["name"]: p for p in main_cfg["proxies"] if p["name"].startswith(PROXY_PREFIX)}
        old_listeners = {l["name"]: l for l in main_cfg["listeners"] if l["name"].startswith(LISTENER_PREFIX)}
        # listener pigeonw-bridge-NNN -> 槽位号; proxy PIGEONW-NNN -> 槽位号
        def idx_of_listener(lname):
            m = re.match(r"^pigeonw-bridge-(\d+)$", lname)
            return int(m.group(1)) if m else None
        slot_by_idx = {}   # idx -> {"proxy": {...}, "listener_port": int}
        for lname, l in old_listeners.items():
            idx = idx_of_listener(lname)
            if idx is None:
                continue
            target = l.get("proxy", "")
            slot_by_idx[idx] = {"proxy": old_proxies.get(target), "port": l["port"]}
        capacity = MAX_PORT - BASE_PORT + 1

        # ---- 匹配: 保号优先 ----
        # 用全部上游条目匹配 (探活瞬断不淘汰槽位); 探活结果只用于刷新 exit_ip。
        # quality gate: 存活 < 3 已在上游门拦截, 这里即便个别槽位探活失败也保留条目。
        entries = [{"p": p, "exit_ip": exit_ip_by_entry.get(
            (p["server"], int(p.get("port", 0)), p.get("username"), p.get("password")))}
            for p in fetched]
        assign = {}      # idx -> entry
        used = set()
        # 1) 完全一致
        for e in entries:
            p = e["p"]
            for idx, s in slot_by_idx.items():
                o = s["proxy"]
                if o and o["server"] == p["server"] and int(o.get("port", 0)) == int(p.get("port", 0)) \
                   and o.get("username") == p.get("username") and o.get("password") == p.get("password"):
                    if idx not in assign:
                        assign[idx] = e; used.add(id(e))
                        break
        # 2) 同出口 (server+port) 换凭据
        for e in entries:
            if id(e) in used:
                continue
            p = e["p"]
            for idx, s in slot_by_idx.items():
                if idx in assign:
                    continue
                o = s["proxy"]
                if o and o["server"] == p["server"] and int(o.get("port", 0)) == int(p.get("port", 0)):
                    assign[idx] = e; used.add(id(e))
                    break
        # 3) 剩余新条目按顺序填空闲槽位
        leftover = [e for e in entries if id(e) not in used]
        free = [idx for idx in range(1, capacity + 1) if idx not in assign]
        for idx, e in zip(free, leftover):
            assign[idx] = e; used.add(id(e))
        unplaced = [e for e in entries if id(e) not in used]
        retired = [idx for idx in slot_by_idx if idx not in assign]
        changed = [idx for idx, e in assign.items()
                   if not slot_by_idx.get(idx, {}).get("proxy") or
                   slot_by_idx[idx]["proxy"]["server"] != e["p"]["server"] or
                   int(slot_by_idx[idx]["proxy"].get("port", 0)) != int(e["p"].get("port", 0)) or
                   slot_by_idx[idx]["proxy"].get("username") != e["p"].get("username") or
                   slot_by_idx[idx]["proxy"].get("password") != e["p"].get("password")]
        print(f"slots: kept={len(assign) - len(changed)} changed={len(changed)} "
              f"retired={retired} expanded={sorted(set(assign) - set(slot_by_idx))} unplaced={len(unplaced)}")
        if unplaced:
            print(f"[PIGEONW_REFRESH_WARN] {len(unplaced)} alive entries beyond capacity {capacity}, dropped", file=sys.stderr)

        backup_with_rotation()

        # ---- 重建桥核配置 (编号/端口不变) ----
        new_proxies, new_listeners = [], []
        for idx in sorted(assign):
            e = assign[idx]
            k = slot_key(idx)
            p = dict(e["p"])
            p["name"] = k["name"]
            p["udp"] = False
            new_proxies.append(p)
            new_listeners.append({"name": k["listener"], "type": "socks", "port": k["port"],
                                  "listen": "127.0.0.1", "proxy": k["name"]})
        main_cfg["proxies"] = [p for p in main_cfg["proxies"] if not p["name"].startswith(PROXY_PREFIX)]
        main_cfg["listeners"] = [l for l in main_cfg["listeners"] if not l["name"].startswith(LISTENER_PREFIX)]
        main_cfg["proxies"] += new_proxies
        main_cfg["listeners"] += new_listeners

        # ---- nodes.yaml 原位更新 ----
        nodes_d = yaml.safe_load(NODES.read_text())
        nl = nodes_d if isinstance(nodes_d, list) else nodes_d["nodes"]
        old_node_names = {n["name"] for n in nl if NODE_RE.match(str(n["name"]))}
        keep = [n for n in nl if n["name"] not in old_node_names]
        new_nodes = []
        for idx in sorted(assign):
            e = assign[idx]
            k = slot_key(idx)
            ex = [n for n in nl if n["name"] == k["node"]]
            node = dict(ex[0]) if ex else {
                "name": k["node"], "host": "127.0.0.1", "protocol": "socks5h",
                "region": "us-west", "egress_type": "clash_proxy",
                "tags": ["clash", "bridge", "http", "pigeonw", "residential", "us-residential"],
                "weight": 100, "tier": "premium", "relay_mode": "direct_only",
                "fault_domain": "huawei-pigeonw-bridge", "provider_region": "us-west"}
            node["port"] = k["port"]
            if e["exit_ip"]:
                node["exit_ip"] = e["exit_ip"]      # 探活成功才刷新 (瞬断不覆盖旧值)
            node["vendor_name"] = f"{PROXY_PREFIX}{idx:03d}"
            new_nodes.append(node)
        keep.extend(new_nodes)
        if isinstance(nodes_d, dict):
            nodes_d["nodes"] = keep
        new_names = {n["name"] for n in new_nodes}

        # ---- 落盘 + 校验门 ----
        CFG.write_text(yaml.safe_dump(main_cfg, allow_unicode=True, sort_keys=False))
        t = subprocess.run(["/opt/ip-pool/bin/mihomo", "-t", "-d", str(CFG_DIR), "-f", str(CFG)],
                           capture_output=True, text=True)
        if "successful" not in t.stdout + t.stderr:
            print("[PIGEONW_REFRESH_FAIL] mihomo -t failed: rc=%s out=%r err=%r" % (
                t.returncode, t.stdout[-300:], t.stderr[-300:]), file=sys.stderr)
            latest = sorted(glob.glob(str(BACKUP_DIR / "pigeonw-bridge.yaml.*-pigeonw")))[-1]
            subprocess.run(["cp", latest, str(CFG)])
            return 1
        NODES.write_text(yaml.safe_dump(nodes_d, allow_unicode=True, sort_keys=False))

        # ---- 悬空绑定清理 (淘汰槽位) ----
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

        subprocess.run(["systemctl", "restart", "pigeonw-bridge.service"])
        subprocess.run(["systemctl", "restart", "ip-pool-api.service", "ip-pool-gateway.service"])
        time.sleep(6)
        r = subprocess.run(["ss", "-tln"], capture_output=True, text=True)
        listening = sum(1 for idx in assign if str(":%d " % slot_key(idx)["port"]) in r.stdout)
        if listening < len(assign) - 2:
            print("[PIGEONW_REFRESH_FAIL] listeners missing: %d/%d up" % (listening, len(assign)), file=sys.stderr)
            return 1
        print("deployed: %d slots (kept=%d changed=%d retired=%d), verified listening=%d, nodes=%d" % (
            len(assign), len(assign) - len(changed), len(changed), len(retired), listening, len(keep)))
        return 0
    except Exception as e:
        import traceback
        print("[PIGEONW_REFRESH_FAIL] unexpected:", e, file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)


if __name__ == "__main__":
    sys.exit(main())
