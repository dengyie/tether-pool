#!/usr/bin/env python3
"""teleapi_sub 订阅池每日刷新 (2026-11-15 接入): 拉取 -> 探活 -> 全量替换 teleapi-xxx 节点.

上游: ai01.teleapi.top 订阅 (~727 节点, 9 协议混装, 家宽 21 + 普通 ~706)。
用户定位: 质量一般, 进普通池 (tier=standard, weight=100, 无 premium 标记)。

与 sub_pool_refresh 同款全量替换机制 (消费者按节点名钉的不多, 替换成本可接受;
727 规模做槽位保号意义不大 —— 上游整列表重排, 保号匹配开销大于收益):
  clash-{6000+idx}-teleapisub-{idx:03d} -> 监听 {BASE_PORT+idx}
  1) 探活门: 活节点 < 30 视为上游故障, 保持现状不部署
  2) mihomo -t 校验失败回滚 / 时间戳备份轮转 7 份 / 部署后 ss -tln 实听校验
  3) 失败 [TELEAPISUB_REFRESH_FAIL] 可 grep 告警
  4) 悬空绑定清理 (旧节点名上的 allocation 直接释放, 消费端下次 acquire 重绑)

安全性: flock 互斥; 探活独立段 39401 (sub 39001 / pigeonw 39101 / genfix 39201 /
skyward 39301 之外); mihomo 一次起 727 listener 探活 (~40s), 不分批。
"""
import fcntl, glob, os, re, subprocess, sys, time
from pathlib import Path

sys.path.insert(0, "/opt/ip-pool")

# bearer-token 型订阅 URL 即凭据, 仓库不落盘: 从 /etc/ip-pool-secrets.env 读。
def _sub_url() -> str:
    url = os.environ.get("TELEAPISUB_URL", "").strip()
    if not url:
        print("[TELEAPISUB_REFRESH_FAIL] TELEAPISUB_URL not set (see /etc/ip-pool-secrets.env)", file=sys.stderr)
        sys.exit(1)
    return url

CFG = Path("/opt/ip-pool/config/huawei-bridge.yaml")
NODES = Path("/opt/ip-pool/config/nodes.yaml")
DB = Path("/opt/ip-pool/data/ip_pool.db")
LOCK = Path("/tmp/teleapisub_pool_refresh.lock")
BACKUP_DIR = Path("/opt/ip-pool/config/backups")
BASE_PORT = 25701         # teleapisub 端段 25701+ (sub 25001-25099 / skyward 25501-25699 之外;
MAX_PORT = 26699          # watchdog 正则 25[05]xx 不含 257xx+, 部署时同步扩 25[5-7]xx -> 用 2[56]xx 通配)
PROBE_POOL = 39401        # 临时探活监听段 (四个既有段之外)
PROBE_DIR = "/tmp/mihomo-probe-teleapisub"
KEEP_BACKUPS = 7
PROXY_PREFIX = "TELEAPISUB-"
# 2026-11-19 可用性根修: 死链接计数 (连续 3 轮 fetch 失败/空订阅 → 淘汰本源全部节点)
from refresh_death_state import bump_failure, clear_failure
DEATH_STATE = "/opt/ip-pool/data/refresh_death_fails.json"
DEATH_KEY = "TELEAPISUB"
DEATH_AFTER = int(os.environ.get("TELEAPISUB_DEATH_AFTER", "3"))
LISTENER_PREFIX = "teleapisub-bridge-"
NODE_RE = re.compile(r"^clash-6\d{3}-teleapisub-\d+$")

# 订阅节点名中文国家前缀 -> 池内 region 约定 (小写 ISO / us-west 桶名)
CN_REGION = {
    "美国": "us-west", "荷兰": "nl", "德国": "de", "法国": "fr", "加拿大": "ca",
    "新加坡": "sg", "罗马尼亚": "ro", "英国": "gb", "意大利": "it", "韩国": "kr",
    "香港": "hk", "瑞士": "ch", "西班牙": "es", "日本": "jp", "台湾": "tw",
    "印度": "in", "土耳其": "tr", "墨西哥": "mx", "保加利亚": "bg", "以色列": "il",
    "澳大利亚": "au", "巴西": "br", "阿根廷": "ar", "波兰": "pl", "瑞典": "se",
    "捷克": "cz", "奥地利": "at", "爱尔兰": "ie", "丹麦": "dk", "挪威": "no",
    "芬兰": "fi", "比利时": "be", "葡萄牙": "pt", "乌克兰": "ua", "俄罗斯": "ru",
    "泰国": "th", "越南": "vn", "菲律宾": "ph", "马来西亚": "my", "印尼": "id",
    "印度尼西亚": "id", "南非": "za", "阿联酋": "ae", "沙特": "sa", "埃及": "eg",
    "智利": "cl", "哥伦比亚": "co", "秘鲁": "pe", "拉脱维亚": "lv", "立陶宛": "lt",
    "爱沙尼亚": "ee", "白俄罗斯": "by", "阿尔巴尼亚": "al", "喀麦隆": "cm",
    "库拉索": "cw", "中国": "cn",
}


def backup_with_rotation():
    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    for src in (CFG, NODES):
        (BACKUP_DIR / f"{src.name}.{stamp}-teleapisub").write_bytes(src.read_bytes())
    olds = sorted(glob.glob(str(BACKUP_DIR / "*-teleapisub")))
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
    n = re.sub(r"^TELEAPISUB-", "", str(name))
    m = re.match(r"^([^-【]+)", n)
    return CN_REGION.get(m.group(1) if m else "", "misc")


def fetch_and_parse():
    """curl 子进程拉取 (python urllib 对部分订阅域 TLS 握手易超时); 0700 临时目录。"""
    import shutil, tempfile, yaml
    work = Path(tempfile.mkdtemp(prefix="teleapisub-refresh-"))
    try:
        os.chmod(work, 0o700)
        fetch_file = work / "fetch.yaml"
        last_err = None
        for attempt in range(3):
            r = subprocess.run(["curl", "-sS", "--max-time", "60", "-A", "clash-verge/1.6.6",
                                _sub_url(), "-o", str(fetch_file)],
                               capture_output=True, text=True)
            if r.returncode == 0 and fetch_file.exists() and fetch_file.stat().st_size > 1000:
                break
            last_err = (r.returncode, r.stderr[-200:])
            time.sleep(3)
        else:
            raise RuntimeError(f"fetch failed after 3 attempts: {last_err}")
        d = yaml.safe_load(fetch_file.read_text())
        return _unbool_proxies(d.get("proxies", []))
    finally:
        shutil.rmtree(work, ignore_errors=True)


def probe_via_listeners(proxies):
    """临时 mihomo 核 + 明文 ip-api 探活, 返回 [(idx, proxy, exit_ip, cc)] (仅活节点).
    实现与 sub/pigeonw/skyward 共享 (scripts/bridge_probe.py)。"""
    from bridge_probe import probe_via_listeners as _shared
    return _shared(proxies, probe_pool=PROBE_POOL, probe_dir=PROBE_DIR,
                   mode="global", external_controller="", allow_lan=False, max_workers=50)


def main():
    import yaml
    lock_fd = open(LOCK, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[TELEAPISUB_REFRESH_SKIP] another refresh is running", file=sys.stderr)
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
                print(f"[TELEAPISUB_SOURCE_DEAD] {e}; 连续 {fails}/{DEATH_AFTER} 轮失效 "
                      f"→ 淘汰本源全部节点 (listeners/nodes.yaml/绑定随重建链路逐级回收)",
                      file=sys.stderr)
            else:
                print(f"[TELEAPISUB_REFRESH_FAIL] {e}; 连续失效 {fails}/{DEATH_AFTER} 轮, "
                      f"keep current state", file=sys.stderr)
                return 1
        if not fetched:
            fails = bump_failure(DEATH_STATE, DEATH_KEY)
            print(f"[TELEAPISUB_REFRESH_FAIL] fetch empty (连续 {fails}/{DEATH_AFTER} 轮), "
                  f"keep current state", file=sys.stderr)
            return 1
        # 订阅内部重名去重 (重名会硬失败 mihomo -t)
        seen = set()
        for p in fetched:
            base, n, i = p["name"], p["name"], 2
            while n in seen:
                n = f"{base}-{i}"; i += 2
            seen.add(n)
            p["name"] = n
        # 命名空间隔离: 原始订阅名 (如 美国-Los Angeles-01) 在共享桥核配置里
        # 无法与其它来源条目区分, 下次刷新就替换不掉旧条目 -> 统一加前缀
        # (PROXY_PREFIX), region_of/kind 判断时再剥掉。
        for p in fetched:
            p["name"] = PROXY_PREFIX + p["name"]
        # 容量门: 超出端段容量的部分丢弃 (按原顺序保前 capacity 个)
        capacity = MAX_PORT - BASE_PORT + 1
        if len(fetched) > capacity:
            print(f"[TELEAPISUB_REFRESH_WARN] {len(fetched)} entries > capacity {capacity}, truncating", file=sys.stderr)
            fetched = fetched[:capacity]
        print(f"fetched {len(fetched)} proxies from teleapisub")
        alive = probe_via_listeners(fetched)
        print(f"alive {len(alive)}/{len(fetched)}")
        if len(alive) < 30 and not force_drop:
            print("[TELEAPISUB_REFRESH_FAIL] alive too few (%d/%d), keep current state" % (
                len(alive), len(fetched)), file=sys.stderr)
            return 1
        backup_with_rotation()

        # ---- 替换桥核 (teleapisub 前缀条目) ----
        main_cfg = yaml.safe_load(CFG.read_text())
        is_teleapisub = lambda name: str(name or "").startswith(PROXY_PREFIX)
        main_cfg["proxies"] = [p for p in main_cfg["proxies"] if not is_teleapisub(p["name"])]
        main_cfg["listeners"] = [l for l in main_cfg["listeners"] if not str(l.get("name", "")).startswith(LISTENER_PREFIX)]
        new_proxies, new_listeners, new_nodes = [], [], []
        for i, (_, p, ip, cc) in enumerate(alive):
            p["udp"] = False
            new_proxies.append(p)
            new_listeners.append({"name": f"{LISTENER_PREFIX}{i:03d}", "type": "socks",
                                  "port": BASE_PORT + i, "listen": "127.0.0.1", "proxy": p["name"]})
            reg = region_of(p["name"])
            # 探活拿到的真实出口国优先 (与 skyward 同款): 名字前缀只是兜底。
            # hk 名字保留 (ip-api 对部分 HK 出口返回 CN 的地理库偏差, skyward 实测教训)。
            c = str(cc or "").strip().lower()
            true_region = ("us-west" if c == "us" else c) if len(c) == 2 and c.isalpha() else None
            if true_region and reg != "hk":
                reg = true_region
            kind = "residential" if "【家宽】" in p["name"] else ""
            tags = ["clash", "bridge", "http", "teleapisub"]
            if kind:
                tags.append(kind)
            new_nodes.append({"name": f"clash-{6000 + i}-teleapisub-{i:03d}", "host": "127.0.0.1",
                              "port": BASE_PORT + i, "protocol": "socks5h", "region": reg,
                              "egress_type": "clash_proxy", "tags": tags, "weight": 100,
                              "tier": "standard", "relay_mode": "direct_only",
                              "fault_domain": "huawei-teleapisub-bridge",
                              "exit_ip": ip, "vendor_name": p["name"]})
        main_cfg["proxies"] += new_proxies
        main_cfg["listeners"] += new_listeners

        # ---- nodes.yaml 替换 (clash-6xxx-teleapisub- 前缀) ----
        nodes_d = yaml.safe_load(NODES.read_text())
        nl = nodes_d if isinstance(nodes_d, list) else nodes_d.get("nodes")
        old_node_names = {n["name"] for n in nl if NODE_RE.match(str(n["name"]))}
        nl[:] = [n for n in nl if n["name"] not in old_node_names]
        nl.extend(new_nodes)
        if isinstance(nodes_d, dict):
            nodes_d["nodes"] = nl
        new_names = {n["name"] for n in new_nodes}

        # ---- 落盘前校验 ----
        CFG.write_text(yaml.safe_dump(main_cfg, allow_unicode=True, sort_keys=False))
        t = subprocess.run(["/opt/ip-pool/bin/mihomo", "-t", "-d", "/opt/ip-pool/config/huawei-bridge",
                            "-f", str(CFG)], capture_output=True, text=True)
        if "successful" not in t.stdout + t.stderr:
            print("[TELEAPISUB_REFRESH_FAIL] mihomo -t failed: rc=%s out=%r err=%r" % (
                t.returncode, t.stdout[-300:], t.stderr[-300:]), file=sys.stderr)
            latest_cfg = sorted(glob.glob(str(BACKUP_DIR / "huawei-bridge.yaml.*-teleapisub")))[-1]
            subprocess.run(["cp", latest_cfg, str(CFG)])
            return 1
        NODES.write_text(yaml.safe_dump(nodes_d, allow_unicode=True, sort_keys=False))

        subprocess.run(["systemctl", "restart", "ip-pool-huawei-bridge.service"], check=False)
        subprocess.run(["systemctl", "restart", "ip-pool-api.service", "ip-pool-gateway.service"], check=False)
        time.sleep(8)
        r = subprocess.run(["ss", "-tln"], capture_output=True, text=True)
        listening = sum(1 for l in new_listeners if str(":%d " % l["port"]) in r.stdout)
        if listening < len(new_listeners) - 5:
            print("[TELEAPISUB_REFRESH_FAIL] listeners missing: %d/%d up -> rolling back" % (
                listening, len(new_listeners)), file=sys.stderr)
            for suffix, dst in (("huawei-bridge.yaml.*-teleapisub", CFG), ("nodes.yaml.*-teleapisub", NODES)):
                baks = sorted(glob.glob(str(BACKUP_DIR / suffix)))
                if baks:
                    subprocess.run(["cp", baks[-1], str(dst)], check=False)
            subprocess.run(["systemctl", "restart", "ip-pool-huawei-bridge.service",
                            "ip-pool-api.service", "ip-pool-gateway.service"], check=False)
            time.sleep(8)
            print("[TELEAPISUB_REFRESH_ROLLBACK] restored pre-refresh state", file=sys.stderr)
            return 1

        # ---- 悬空绑定清理 (部署验证通过后) ----
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

        print("deployed: %d teleapisub nodes, bridge listeners=%d, nodes=%d, verified listening=%d" % (
            len(alive), len(main_cfg["listeners"]), len(nl), listening))
        return 0
    except Exception as e:
        import traceback
        print("[TELEAPISUB_REFRESH_FAIL] unexpected:", e, file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)


if __name__ == "__main__":
    sys.exit(main())
