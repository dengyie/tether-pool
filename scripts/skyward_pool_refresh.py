#!/usr/bin/env python3
"""skyward 订阅池每日刷新 (2026-11-15 接入): 拉取 -> 探活 -> 槽位保号更新 huawei 桥核.

上游: skywardbase 订阅 (~36 节点, anytls 为主, 流量充足; 用户指定优先级稍高)。
用户须知: 该订阅节点连不上 google —— 不参与 target-health 的 google 类目标池,
靠 tags 里不带 premium/不进 google 目标集自然实现 (gateway target-health 按节点
名前缀+tags 分池, 本池 tags 用 skyward, 与 pigeonw/teleapi 不混)。

与 pigeonw 同款槽位保号机制 (消费者可按编号钉节点):
  clash-{3300+idx}-skyward-{idx:02d} -> 监听 {BASE_PORT+idx-1}
  1) 完全一致 -> 原槽位  2) 同出口换凭据 -> 原槽位  3) 顺序填空闲  4) 超容量淘汰

安全性: flock 互斥 / 探活门 (全死不部署) / mihomo -t 失败回滚 / 时间戳备份轮转 /
部署后 ss -tln 实听校验 / 失败 [SKYWARD_REFRESH_FAIL] 可 grep 告警。
"""
import fcntl, glob, json, os, re, socket, subprocess, sys, time
from pathlib import Path

# bearer-token 型订阅 URL 即凭据, 仓库不落盘: 从 /etc/ip-pool-secrets.env 读。
def _sub_url() -> str:
    url = os.environ.get("SKYWARD_SUB_URL", "").strip()
    if not url:
        print("[SKYWARD_REFRESH_FAIL] SKYWARD_SUB_URL not set (see /etc/ip-pool-secrets.env)", file=sys.stderr)
        sys.exit(1)
    return url

CFG = Path("/opt/ip-pool/config/huawei-bridge.yaml")
NODES = Path("/opt/ip-pool/config/nodes.yaml")
DB = Path("/opt/ip-pool/data/ip_pool.db")
LOCK = Path("/tmp/skyward_pool_refresh.lock")
BACKUP_DIR = Path("/opt/ip-pool/config/backups")
BASE_PORT = 25501        # skyward 端段 25501+ (sub 25001-25099 / gen 21001-24873 之外;
MAX_PORT = 25699         # watchdog listener 正则 250xx 已覆盖, 255xx 需并入 -> 部署时同步 ha_watchdog)
PROBE_POOL = 39301       # 临时探活监听段 (sub 39001 / pigeonw 39101 / genfix 39201 之外)
PROBE_DIR = "/tmp/mihomo-probe-skyward"
KEEP_BACKUPS = 7
PROXY_PREFIX = "SKYWARD-"
# 2026-11-19 可用性根修: 死链接计数 (连续 3 轮 fetch 失败/空订阅 → 淘汰本源全部节点)
from refresh_death_state import bump_failure, clear_failure
DEATH_STATE = "/opt/ip-pool/data/refresh_death_fails.json"
DEATH_KEY = "skyward"
DEATH_AFTER = int(os.environ.get("SKYWARD_DEATH_AFTER", "3"))
LISTENER_PREFIX = "skyward-bridge-"
NODE_RE = re.compile(r"^clash-(3\d\d)-skyward-(\d+)$")

# 订阅节点名 -> 池内 region 约定 (小写 ISO / 区域桶名, 与 sub 池 region_of 同语义)
NAME_REGION = {
    "台湾": "tw", "新加坡": "sg", "日本": "jp", "澳大利亚": "au",
    "美国": "us-west", "英国": "gb", "韩国": "kr", "香港": "hk",
}


def backup_with_rotation():
    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    for src in (CFG, NODES):
        (BACKUP_DIR / f"{src.name}.{stamp}-skyward").write_bytes(src.read_bytes())
    olds = sorted(glob.glob(str(BACKUP_DIR / "*-skyward")))
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


def region_of(name):
    for zh, reg in NAME_REGION.items():
        if str(name).startswith(zh):
            return reg
    return "misc"


def fetch_and_parse():
    """curl 子进程拉取 (python urllib 直连部分订阅域 TLS 握手易超时, curl 稳定);
    凭据文件落在 0700 私有临时目录, 解析后即删。"""
    import shutil, tempfile, yaml
    work = Path(tempfile.mkdtemp(prefix="skyward-refresh-"))
    try:
        os.chmod(work, 0o700)
        fetch_file = work / "fetch.yaml"
        last_err = None
        for attempt in range(3):
            r = subprocess.run(["curl", "-sS", "--max-time", "40", "-A", "clash-verge/1.6.6",
                                _sub_url(), "-o", str(fetch_file)],
                               capture_output=True, text=True)
            if r.returncode == 0 and fetch_file.exists() and fetch_file.stat().st_size > 100:
                break
            last_err = (r.returncode, r.stderr[-200:])
            time.sleep(3)
        else:
            raise RuntimeError(f"fetch failed after 3 attempts: {last_err}")
        d = yaml.safe_load(fetch_file.read_text())
        # 续费/备用等非出口条目一律剔除 (trojan 备用节点无流量价值)
        return _unbool_proxies([p for p in d.get("proxies", []) if p.get("type") in ("anytls",)])
    finally:
        shutil.rmtree(work, ignore_errors=True)


def probe_via_listeners(proxies):
    """临时 mihomo 核 + 明文 ip-api 探活, 返回 [(idx, proxy, exit_ip, cc)] (仅活节点).
    实现与 sub/pigeonw 共享 (scripts/bridge_probe.py); datadir 独立防 geo 缓存并发写坏。"""
    from bridge_probe import probe_via_listeners as _shared
    return _shared(proxies, probe_pool=PROBE_POOL, probe_dir=PROBE_DIR,
                   mode="global", external_controller="", allow_lan=False, max_workers=20)


def slot_key(idx):
    return {"name": f"{PROXY_PREFIX}{idx:02d}", "listener": f"{LISTENER_PREFIX}{idx:02d}",
            "port": BASE_PORT + idx - 1, "node": f"clash-{3300 + idx}-skyward-{idx:02d}"}


def main():
    import yaml
    lock_fd = open(LOCK, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[SKYWARD_REFRESH_SKIP] another refresh is running", file=sys.stderr)
        return 0
    # 跨脚本写锁 (2026-11-23): sub/skyward/teleapi 都 read-modify-write 同一份
    # huawei-bridge.yaml, 独立 flock 防不了互相丢更新。读 CFG 前获取, finally 释放。
    from atomic_yaml import acquire_bridge_writer_lock, release_bridge_writer_lock
    bridge_wlock = acquire_bridge_writer_lock()
    if bridge_wlock is None:
        print("[SKYWARD_REFRESH_SKIP] bridge writer lock busy >600s", file=sys.stderr)
        return 0
    try:
        force_drop = False
        try:
            fetched = fetch_and_parse()
            clear_failure(DEATH_STATE, DEATH_KEY)
        except Exception as e:
            fails = bump_failure(DEATH_STATE, DEATH_KEY)
            if fails >= DEATH_AFTER:
                fetched = []
                force_drop = True
                print(f"[SKYWARD_SOURCE_DEAD] {e}; 连续 {fails}/{DEATH_AFTER} 轮失效 "
                      f"→ 淘汰本源全部节点 (listeners/nodes.yaml/绑定随重建链路逐级回收)",
                      file=sys.stderr)
            else:
                print(f"[SKYWARD_REFRESH_FAIL] {e}; 连续失效 {fails}/{DEATH_AFTER} 轮, "
                      f"keep current state", file=sys.stderr)
                return 1
        if not fetched:
            # 2026-11-19 review 自查根修: 上游清空订阅与 fetch 失败同为死亡形态 ——
            # 旧版在此恒 return 1, 计数器无限累积但永不淘汰 (死代码分支)
            fails = bump_failure(DEATH_STATE, DEATH_KEY)
            if fails >= DEATH_AFTER:
                force_drop = True
                print(f"[SKYWARD_SOURCE_DEAD] fetch empty 连续 {fails}/{DEATH_AFTER} 轮 "
                      f"→ 淘汰本源全部节点 (listeners/nodes.yaml/绑定随重建链路逐级回收)",
                      file=sys.stderr)
                # 不 return: 带空 fetched 走重建链, force_drop 已绕过 alive 守卫
            else:
                print(f"[SKYWARD_REFRESH_FAIL] fetch empty (连续 {fails}/{DEATH_AFTER} 轮), "
                      f"keep current state", file=sys.stderr)
                return 1
        seen = set()
        for p in fetched:
            base, n, i = p["name"], p["name"], 2
            while n in seen:
                n = f"{base}-{i}"; i += 2
            seen.add(n)
            p["name"] = n
        print(f"fetched {len(fetched)} anytls proxies from skyward")
        alive = probe_via_listeners(fetched)
        print(f"alive {len(alive)}/{len(fetched)}")
        if len(alive) < 3 and not force_drop:
            print("[SKYWARD_REFRESH_FAIL] alive too few (%d/%d), keep current state" % (
                len(alive), len(fetched)), file=sys.stderr)
            return 1
        exit_ip_by_entry = {(p["server"], int(p.get("port", 0)), p.get("username"), p.get("password")): (ip, cc)
                            for (_, p, ip, cc) in alive}

        # ---- 读取现状 (huawei-bridge 主核; skyward 条目按前缀隔离) ----
        main_cfg = yaml.safe_load(CFG.read_text())
        old_proxies = {p["name"]: p for p in main_cfg["proxies"] if p["name"].startswith(PROXY_PREFIX)}
        old_listeners = {l["name"]: l for l in main_cfg["listeners"] if l["name"].startswith(LISTENER_PREFIX)}
        slot_by_idx = {}
        for lname, l in old_listeners.items():
            m = re.match(r"^skyward-bridge-(\d+)$", lname)
            if not m:
                continue
            slot_by_idx[int(m.group(1))] = {"proxy": old_proxies.get(l.get("proxy", "")), "port": l["port"]}
        capacity = MAX_PORT - BASE_PORT + 1

        # ---- 槽位匹配 (保号优先, 与 pigeonw 同算法) ----
        entries = [{"p": p, "geo": exit_ip_by_entry.get(
            (p["server"], int(p.get("port", 0)), p.get("username"), p.get("password")))}
            for p in fetched]
        assign = {}
        used = set()
        for e in entries:  # 1) 完全一致
            p = e["p"]
            for idx, s in slot_by_idx.items():
                o = s["proxy"]
                if o and o["server"] == p["server"] and int(o.get("port", 0)) == int(p.get("port", 0)) \
                   and o.get("username") == p.get("username") and o.get("password") == p.get("password"):
                    if idx not in assign:
                        assign[idx] = e; used.add(id(e)); break
        for e in entries:  # 2) 同出口换凭据
            if id(e) in used:
                continue
            p = e["p"]
            for idx, s in slot_by_idx.items():
                if idx in assign:
                    continue
                o = s["proxy"]
                if o and o["server"] == p["server"] and int(o.get("port", 0)) == int(p.get("port", 0)):
                    assign[idx] = e; used.add(id(e)); break
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

        backup_with_rotation()

        # ---- 重建桥核 skyward 条目 (编号/端口不变) ----
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

        # ---- nodes.yaml 原位更新 (tier=standard 但 weight 高一档: 用户指定优先级稍高) ----
        nodes_d = yaml.safe_load(NODES.read_text())
        nl = nodes_d if isinstance(nodes_d, list) else nodes_d["nodes"]
        old_node_names = {n["name"] for n in nl if NODE_RE.match(str(n["name"]))}
        keep = [n for n in nl if n["name"] not in old_node_names]
        new_nodes = []
        for idx in sorted(assign):
            e = assign[idx]
            k = slot_key(idx)
            region = region_of(e["p"]["name"])
            ex = [n for n in nl if n["name"] == k["node"]]
            node = dict(ex[0]) if ex else {
                "name": k["node"], "host": "127.0.0.1", "protocol": "socks5h",
                "region": region, "egress_type": "clash_proxy",
                "tags": ["clash", "bridge", "http", "skyward"],
                "weight": 150, "tier": "standard", "relay_mode": "direct_only",
                "fault_domain": "huawei-skyward-bridge", "provider_region": region}
            node["port"] = k["port"]
            # 换宿槽位: 旧 exit_ip 属于上一任上游必须丢弃; geo (ip-api countryCode) 只
            # 修正明显不符的 region (与 gen R2 同语义: us->us-west 其余小写 ISO)。
            if e["geo"]:
                ip, cc = e["geo"]
                if idx in changed or k["node"] not in old_node_names:
                    if ip:
                        node["exit_ip"] = ip
                    else:
                        node.pop("exit_ip", None)
                elif ip:
                    node["exit_ip"] = ip
            if cc:
                # region 映射 (us->us-west 其余小写 ISO): 内联实现而非 import core.storage
                # —— cron 用系统 python3, 无 pydantic, 拖 core 包会 ModuleNotFoundError。
                c = str(cc).strip().lower()
                true_region = ("us-west" if c == "us" else c) if len(c) == 2 and c.isalpha() else None
                # 上游名字只精确到国家/地区, ip-api 是出口实测 —— 名字与实测都可信
                # 时以实测为准 (香港例外: 名字 hk 而 exit 可能被识别为 cn, 保持 hk)
                if true_region and true_region != node.get("region") and node.get("region") != "hk":
                    node["region"] = true_region
                    node["provider_region"] = true_region
            node["vendor_name"] = f"{PROXY_PREFIX}{idx:02d}"
            new_nodes.append(node)
        keep.extend(new_nodes)
        if isinstance(nodes_d, dict):
            nodes_d["nodes"] = keep
        new_names = {n["name"] for n in new_nodes}

        # ---- 落盘 + 校验门 (2026-11-15 review P2: 原子写) ----
        from atomic_yaml import atomic_write_text
        atomic_write_text(CFG, yaml.safe_dump(main_cfg, allow_unicode=True, sort_keys=False))
        t = subprocess.run(["/opt/ip-pool/bin/mihomo", "-t", "-d", "/opt/ip-pool/config/huawei-bridge",
                            "-f", str(CFG)], capture_output=True, text=True)
        if "successful" not in t.stdout + t.stderr:
            print("[SKYWARD_REFRESH_FAIL] mihomo -t failed: rc=%s out=%r err=%r" % (
                t.returncode, t.stdout[-300:], t.stderr[-300:]), file=sys.stderr)
            latest = sorted(glob.glob(str(BACKUP_DIR / "huawei-bridge.yaml.*-skyward")))[-1]
            subprocess.run(["cp", latest, str(CFG)])
            return 1
        atomic_write_text(NODES, yaml.safe_dump(nodes_d, allow_unicode=True, sort_keys=False))

        subprocess.run(["systemctl", "restart", "ip-pool-huawei-bridge.service"], check=False)
        subprocess.run(["systemctl", "restart", "ip-pool-api.service", "ip-pool-gateway.service"], check=False)
        time.sleep(6)
        r = subprocess.run(["ss", "-tln"], capture_output=True, text=True)
        listening = sum(1 for idx in assign if str(":%d " % slot_key(idx)["port"]) in r.stdout)
        if listening < len(assign) - 2:
            print("[SKYWARD_REFRESH_FAIL] listeners missing: %d/%d up -> rolling back" % (
                listening, len(assign)), file=sys.stderr)
            for latest, dst in ((sorted(glob.glob(str(BACKUP_DIR / "huawei-bridge.yaml.*-skyward")))[-1], CFG),
                                (sorted(glob.glob(str(BACKUP_DIR / "nodes.yaml.*-skyward")))[-1], NODES)):
                subprocess.run(["cp", latest, str(dst)], check=False)
            subprocess.run(["systemctl", "restart", "ip-pool-huawei-bridge.service",
                            "ip-pool-api.service", "ip-pool-gateway.service"], check=False)
            time.sleep(6)
            print("[SKYWARD_REFRESH_ROLLBACK] restored pre-refresh state", file=sys.stderr)
            return 1

        # ---- 悬空绑定清理 (部署验证通过后) ----
        dangling = old_node_names - new_names
        if dangling:
            import sqlite3
            from datetime import datetime as _dt
            conn = sqlite3.connect(str(DB))
            cur = conn.cursor()
            ph = ",".join("?" * len(dangling))
            # 2026-11-15 review P1: epoch 推进 + change 行 (to_node=NULL),
            # 不删 binding_changes 历史 (保留窗由 prune_binding_changes 管)。
            orphaned = cur.execute(
                f"SELECT service, account_id, node_name FROM account_allocations "
                f"WHERE node_name IN ({ph})", tuple(dangling)).fetchall()
            cur.execute(f"DELETE FROM account_allocations WHERE node_name IN ({ph})", tuple(dangling))
            n_alloc = cur.rowcount
            now_str = _dt.now().isoformat()
            services = sorted({r[0] for r in orphaned})
            for svc in services:
                cur.execute("""
                    INSERT INTO binding_epoch_state (service, epoch, changed_at)
                    VALUES (?, 1, ?)
                    ON CONFLICT(service) DO UPDATE SET
                        epoch = binding_epoch_state.epoch + 1, changed_at = excluded.changed_at
                """, (svc, now_str))
                epoch = cur.execute(
                    "SELECT epoch FROM binding_epoch_state WHERE service = ?", (svc,)).fetchone()[0]
                for s, acct, node in orphaned:
                    if s != svc:
                        continue
                    cur.execute("""
                        INSERT INTO binding_changes (service, epoch, account_id, from_node, to_node, reason, created_at)
                        VALUES (?, ?, ?, ?, NULL, 'node_retired_dangling_cleanup', ?)
                    """, (svc, epoch, acct, node, now_str))
            conn.commit()
            conn.close()
            print(f"dangling bindings cleaned: {n_alloc} allocations over {len(dangling)} retired nodes "
                  f"(epoch advanced for {services or 'no services'})")

        print("deployed: %d slots (kept=%d changed=%d retired=%d), verified listening=%d, nodes=%d" % (
            len(assign), len(assign) - len(changed), len(changed), len(retired), listening, len(keep)))
        return 0
    except Exception as e:
        import traceback
        print("[SKYWARD_REFRESH_FAIL] unexpected:", e, file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        release_bridge_writer_lock(bridge_wlock)


if __name__ == "__main__":
    sys.exit(main())
