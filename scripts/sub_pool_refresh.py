#!/usr/bin/env python3
"""sub 订阅池每日刷新: 拉取 -> 探活 -> 全量替换 sub-xxx 节点 (桥核 25001+ 端口段复用).

多源 (2026-11-18): 支持 SUB1_URL / SUB2_URL / ... 任意多个订阅源, 按源序号加
s<n>- 前缀 (is_sub 识别依赖); 某源拉取失败时沿用该源上一轮条目, 只有全部源都
失败才保留现状 —— 单源挂掉不再冻结整个池子.

加固 (2026-11-14 深度 review):
- flock 单实例锁 (cron 与手动运行互斥, 探活临时 mihomo 不与主探活/其他刷新打架)
- 换绑悬空清理: 替换后 sub 节点名集变化, 旧节点名上的 account 绑定直接释放
  (sqlite delete), 消费端下次 acquire 自然重绑, 不会指到已不存在的节点
- 失败非零退出 + 明确 stderr 标记 [SUB_REFRESH_FAIL], 日志可 grep 告警
- 备份带时间戳并保留最近 7 份, 不再覆盖固定名
"""
import fcntl, json, re, socket, subprocess, sys, time, urllib.request, urllib.error, os, glob
from pathlib import Path

# 订阅侧拒绝的分层 (2026-11-19 review): 410/404 = 链接冻结/失效, 重试只会
# 加重风控或纯浪费; 401/403 = 鉴权/风控拒绝, 重试有害; 429 = 限速, 语义是
# "退避后可再试" —— 读 Retry-After 等一次, 无头时仅重试 1 次 (而非 3 次)。
PERMANENT_HTTP = {401, 403, 404, 410}
RATE_LIMITED_HTTP = {429}


class PermanentSubError(Exception):
    """源侧永久性拒绝 (冻结/失效/鉴权), 不做重试, 直接 carry-over。"""


class RateLimitedSubError(Exception):
    """429 限速: 语义与永久拒绝不同 —— Retry-After 内退避一次, 超时交回主流程
    (走 [SUB_SOURCE_FAIL] carry-over, 次日 cron 自然重试)。"""

    def __init__(self, code, retry_after=None):
        self.retry_after = retry_after
        super().__init__(f"HTTP {code}" + (f" (Retry-After {retry_after}s)" if retry_after else ""))

# bearer-token 型订阅 URL 即凭据, 仓库不落盘: 从 /etc/ip-pool-secrets.env 读。
def _sub_sources():
    """发现所有 SUB<n>_URL 订阅源, 返回 [(序号, url)] (按序号排序)。"""
    found = []
    for k, v in os.environ.items():
        m = re.fullmatch(r"SUB(\d+)_URL", k)
        if m and v.strip():
            found.append((int(m.group(1)), v.strip()))
    if not found:
        print("[SUB_REFRESH_FAIL] no SUB<n>_URL set (see /etc/ip-pool-secrets.env)", file=sys.stderr)
        sys.exit(1)
    return sorted(found)

CFG = Path("/opt/ip-pool/config/huawei-bridge.yaml")
NODES = Path("/opt/ip-pool/config/nodes.yaml")
DB = Path("/opt/ip-pool/data/ip_pool.db")
LOCK = Path("/tmp/sub_pool_refresh.lock")
BACKUP_DIR = Path("/opt/ip-pool/config/backups")
BASE_PORT = 25947
MAX_PORT = 26999          # 25947-27000 为 huawei-bridge 空闲连续段 (2026-11-22 扩容 499→1053 slots);
                          # 25501-25946 被本桥 skyward(35)/teleapisub(246) listener 占用, sub 段必须跳过
PROBE_POOL = 39001
KEEP_BACKUPS = 7
JUNK_SERVERS = {"127.0.0.1", "localhost", "::1", "0.0.0.0"}

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


def _classify_http_error(e):
    """HTTPError -> (异常实例, 剩余重试次数)。分类是唯一的, fetch 只按它循环。"""
    detail = ""
    try:
        detail = e.read()[:200].decode("utf-8", "replace").strip()
    except Exception:
        pass
    if e.code in PERMANENT_HTTP:
        return PermanentSubError(f"HTTP {e.code} {detail}".strip()), 0
    if e.code in RATE_LIMITED_HTTP:
        ra = e.headers.get("Retry-After") if e.headers else None
        try:
            wait = int(ra) if ra else None
        except (TypeError, ValueError):
            wait = None
        if wait is not None and wait <= 60:
            # Retry-After 可等且不长: 等这一次, 之后按预算耗尽处理 (不再连打)
            time.sleep(wait)
            return RateLimitedSubError(e.code, wait), 0
        # 无头或等待过长: 傻等不如低频再试, 给 1 次重试预算
        return RateLimitedSubError(e.code), 1
    return e, 2  # 瞬时错误: 保持原 3 次全量重试


def fetch_and_parse(url):
    import yaml
    # 部分机场订阅拒默认 python-urllib UA (403): 与 pigeonw 刷新同款 curl 身份;
    # VPS 直连偶发握手超时, 3 次重试后才交回主流程 (触发该源 carry-over)。
    # 但 4xx 明确拒绝不重试: 机场风控把连续拉取算作"异常拉取", 越重试越封。
    remaining = 2  # 首次之外的重试预算
    while True:
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "clash-verge/1.6.6", "Accept": "*/*"})
            raw = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
            d = yaml.safe_load(raw) or {}
            return _unbool_proxies(d.get("proxies") or [])
        except urllib.error.HTTPError as e:
            err, retries = _classify_http_error(e)
            if retries == 0:
                raise err from e
            remaining = min(remaining, retries)
        except Exception as e:
            err = e
        if remaining <= 0:
            raise err
        remaining -= 1
        time.sleep(2)


def normalize_source_proxies(proxies, src_idx):
    """源内规范: 统一 s<n>- 前缀 + 剔除伪节点 (到期提示 socks5 127.0.0.1:1 之类)。"""
    out = []
    for p in proxies:
        name = re.sub(r"^s\d+-", "", str(p.get("name") or ""))
        server = str(p.get("server") or "")
        try:
            port = int(p.get("port") or 0)
        except (TypeError, ValueError):
            port = 0
        if not name or not server or port <= 1 or server in JUNK_SERVERS:
            continue
        p["name"], p["server"], p["port"] = f"s{src_idx}-" + name, server, port
        out.append(p)
    return out


def upstream_key(p):
    """跨源上游去重键: 同一条线路在多个订阅里重复出现时只留一份。

    2026-11-22 SUB10 实证: yuetong 22 条 vless 同 server/port/uuid 只差
    reality SNI (servername: www.infineon.com / www.westerndigital.com / ...),
    是上游刻意分发的 22 条独立线路 (不同 SNI = 不同握手目标), 旧六元组键把
    21 条误当重复吞掉。SNI 入键后 SUB8 107 个重复组 sni 全相同、零影响。
    也纳入 reality public-key: 同 server 不同 pk 同理是不同线路。
    """
    ro = p.get("reality-opts") or {}
    return (str(p.get("type")), str(p.get("server")).lower(), p.get("port"),
            str(p.get("username") or ""), str(p.get("password") or ""), str(p.get("uuid") or ""),
            str(p.get("servername") or ""), str(ro.get("public-key") or ""))

def probe_via_listeners(proxies, extra_proxies=None):
    # F9 (二轮 review): 与 pigeonw 刷新共享的探活实现抽到 scripts/bridge_probe.py
    from bridge_probe import probe_via_listeners as _shared
    return _shared(proxies, probe_pool=PROBE_POOL, probe_dir="/tmp/mihomo-probe-sub",
                   mode="direct", external_controller="127.0.0.1:39998", max_workers=50,
                   extra_proxies=extra_proxies)

_CN_REGION = {
    "新加坡": "sg", "香港": "hk", "台湾": "tw", "日本": "jp", "韩国": "kr",
    "美国": "us", "加拿大": "ca", "英国": "gb", "德国": "de", "法国": "fr",
    "荷兰": "nl", "瑞士": "ch", "俄罗斯": "ru", "越南": "vn",
    "马来西亚": "my", "泰国": "th", "菲律宾": "ph",
    "澳大利亚": "au", "土耳其": "tr", "阿联酋": "ae", "巴西": "br",
    # 与 teleapi_pool_refresh.CN_REGION 对齐 (2026-11-22 SUB9 接入: 家宽源
    # 覆盖 mx/it/ee/cm/cn 等)。顺序约束: "印度尼西亚" 必须在 "印度" 之前
    # (region_of 按插入序子串匹配, 否则 in 前缀误标)。
    "印度尼西亚": "id", "印度": "in", "印尼": "id",
    "罗马尼亚": "ro", "意大利": "it", "西班牙": "es", "墨西哥": "mx",
    "保加利亚": "bg", "以色列": "il", "波兰": "pl", "瑞典": "se",
    "捷克": "cz", "奥地利": "at", "爱尔兰": "ie", "丹麦": "dk", "挪威": "no",
    "芬兰": "fi", "比利时": "be", "葡萄牙": "pt", "乌克兰": "ua",
    "南非": "za", "沙特": "sa", "埃及": "eg",
    "智利": "cl", "哥伦比亚": "co", "秘鲁": "pe", "拉脱维亚": "lv", "立陶宛": "lt",
    "爱沙尼亚": "ee", "白俄罗斯": "by", "阿尔巴尼亚": "al", "喀麦隆": "cm",
    "库拉索": "cw", "中国": "cn",
}

# 英文城市/国家名兜底: 机场常用 "[vless]Tokyo1 - B Group" 这类无国家码命名。
# 交集词 (india/indiana 类) 用词边界规避子串误报 (Indianapolis 不再命中 india);
# 边界右侧把数字也算进来 (Tokyo1/HongKong3 是机场高频命名), 左侧保持 \b 防前缀粘连。
_EN_REGION = {
    "tokyo": "jp", "osaka": "jp", "japan": "jp",
    "singapore": "sg",
    "hongkong": "hk", "hong kong": "hk",
    "taiwan": "tw", "taipei": "tw",
    "frankfurt": "de", "germany": "de", "berlin": "de",
    "california": "us-west", "los angeles": "us-west", "seattle": "us-west",
    "london": "gb", "united kingdom": "gb",
    "amsterdam": "nl", "netherlands": "nl",
    "paris": "fr", "france": "fr",
    "seoul": "kr", "korea": "kr",
    "sydney": "au", "australia": "au",
    "mumbai": "in", "india": "in",
}
_EN_REGION_PAT = re.compile(
    r"\b(?:" + "|".join(sorted(_EN_REGION, key=len, reverse=True)) + r")\d?\b",
    re.IGNORECASE)

def _flag_cc(name):
    """国旗 emoji (两个区域指示符) -> ISO 国家码小写; 非国旗开头的名字返回 None。"""
    ri = [ord(ch) - 0x1F1E6 for ch in name if 0x1F1E6 <= ord(ch) <= 0x1F1FF]
    if len(ri) >= 2:
        return chr(ord("a") + ri[0]) + chr(ord("a") + ri[1])
    return None

def region_of(name):
    # 优先级: 国旗 emoji > "US ..."/"s1-HK_11" 国家码 > 中文国名 > 英文城市名
    cc = _flag_cc(name)
    if not cc:
        m = re.search(r"(?:^|s\d-)([A-Z]{2})(?:[\s_-]|$)", name)
        cc = m.group(1).lower() if m else None
    if not cc:
        cc = next((v for k, v in _CN_REGION.items() if k in name), None)
    if not cc:
        m = _EN_REGION_PAT.search(name)
        cc = _EN_REGION[m.group(0).rstrip("0123456789").lower()] if m else None
    return "us-west" if cc == "us" else (cc or "misc")

def apply_source_failure(src_fails: dict, src_idx: int, error: Exception,
                         carried: list, death_after: int) -> tuple[list, str]:
    """失效计数与淘汰判定 (2026-11-19 review 根修, 纯函数便于测试)。

    PermanentSubError (410/404/401 = 链接冻结/失效) 累计计数; 连续 death_after 轮
    → 判源死亡: 返回空节点集 (该源全部节点随重建链路逐级回收: listeners/
    nodes.yaml/悬空绑定清理/PRUNE_MISSING_NODES)。RateLimitedSubError (429
    = 瞬时) 不计数, 沿用 carry-over。返回 (本轮节点集, 日志行)。"""
    if isinstance(error, PermanentSubError):
        n_fail = src_fails.get(str(src_idx), 0) + 1
        src_fails[str(src_idx)] = n_fail
        if n_fail >= death_after:
            return [], (f"[SUB_SOURCE_DEAD] SUB{src_idx}: {error}; 连续 {n_fail} 轮永久失效 "
                        f"→ 淘汰该源全部节点 (本轮移除 carried {len(carried)} 条, "
                        f"listeners/nodes.yaml/绑定随重建链路逐级回收)")
        return carried, (f"[SUB_SOURCE_FROZEN] SUB{src_idx}: {error}; 永久失效 {n_fail}/{death_after} 轮, "
                         f"暂 carry {len(carried)} previous nodes")
    return carried, (f"[SUB_SOURCE_THROTTLED] SUB{src_idx}: {error}; carrying {len(carried)} previous nodes")


def main():
    import yaml
    lock_fd = open(LOCK, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[SUB_REFRESH_SKIP] another refresh is running", file=sys.stderr)
        return 0
    # 跨脚本写锁 (2026-11-23): sub/skyward/teleapi 都 read-modify-write 同一份
    # huawei-bridge.yaml, 独立 flock 防不了互相丢更新。读 CFG 前获取, finally 释放。
    from atomic_yaml import acquire_bridge_writer_lock, release_bridge_writer_lock
    bridge_wlock = acquire_bridge_writer_lock()
    if bridge_wlock is None:
        print("[SUB_REFRESH_SKIP] bridge writer lock busy >600s", file=sys.stderr)
        return 0
    try:
        sources = _sub_sources()
        main = yaml.safe_load(CFG.read_text())
        old_sub = [p for p in main.get("proxies", []) if re.match(r"^s\d-", str(p.get("name") or ""))]

        # 源级中转: SUB<n>_RELAY=<桥核已有代理名>, 给该源每条代理接 dialer-proxy。
        # 用于上游屏蔽中国大陆直连的机场 (实测 SUB3 直拨 0 活, 经 SKYWARD 中转 2 活)。
        by_name = {str(p.get("name")): p for p in main.get("proxies", [])}
        relay_for, extra_relays = {}, {}
        for src_idx, _ in sources:
            relay = os.environ.get(f"SUB{src_idx}_RELAY", "").strip()
            if not relay:
                continue
            if relay in by_name:
                relay_for[src_idx] = relay
                extra_relays[relay] = dict(by_name[relay])
            else:
                print(f"[SUB_RELAY_FAIL] SUB{src_idx}_RELAY={relay} not in bridge config; "
                      f"this source stays direct", file=sys.stderr)

        # 2026-11-19 review 根修: 死链接节点淘汰。PermanentSubError (410/404/401
        # = 链接冻结/失效) 连续 SUB_SOURCE_DEATH_AFTER 轮后, 该源视为死亡:
        # 不再 carry-over, 其节点随本轮重建链路整体消失 (listeners/nodes.yaml/
        # 悬空绑定清理/PRUNE_MISSING_NODES 逐级回收)。瞬时失败 (超时/限速) 不计数。
        FAILS_STATE = Path("/opt/ip-pool/data/sub_source_fails.json")
        DEATH_AFTER = int(os.environ.get("SUB_SOURCE_DEATH_AFTER", "3"))
        try:
            src_fails = json.loads(FAILS_STATE.read_text()) if FAILS_STATE.exists() else {}
        except Exception:
            src_fails = {}

        proxies, seen_names, seen_up, ok_any, dropped = [], set(), set(), False, 0
        for src_idx, url in sources:
            try:
                got = fetch_and_parse(url)
                ok_any = True
                if src_fails.pop(str(src_idx), None) is not None:
                    print(f"[SUB_SOURCE_REVIVED] SUB{src_idx}: 重新可用, 失效计数清零",
                          file=sys.stderr)
                print(f"SUB{src_idx}: fetched {len(got)} proxies")
            except (PermanentSubError, RateLimitedSubError) as e:
                carried = [dict(p) for p in old_sub if str(p.get("name", "")).startswith(f"s{src_idx}-")]
                got, msg = apply_source_failure(src_fails, src_idx, e, carried, DEATH_AFTER)
                print(msg, file=sys.stderr)
            except Exception as e:
                # 单源失败不中止: 沿用该源上一轮条目, 其余源照常刷新
                got = [dict(p) for p in old_sub if str(p.get("name", "")).startswith(f"s{src_idx}-")]
                print(f"[SUB_SOURCE_FAIL] SUB{src_idx}: {e}; carrying {len(got)} previous nodes",
                      file=sys.stderr)
            for p in normalize_source_proxies(got, src_idx):
                if src_idx in relay_for:
                    p["dialer-proxy"] = relay_for[src_idx]
                up = upstream_key(p)
                if up in seen_up:
                    dropped += 1
                    continue
                seen_up.add(up)
                base, n, i = p["name"], p["name"], 2
                while n in seen_names:
                    n, i = f"{base}-{i}", i + 1
                seen_names.add(n)
                p["name"] = n
                proxies.append(p)

        # 失效计数持久化 (原子写; 成功/复活已在循环内清零) —— 跨轮累计是
        # 死亡判定的基础, 状态丢失会让死链接永不淘汰
        try:
            from atomic_yaml import atomic_write_text
            atomic_write_text(FAILS_STATE, json.dumps(src_fails, ensure_ascii=False, indent=1))
        except Exception as e:
            print(f"[SUB_SOURCE_STATE] 失效计数落盘失败: {e}", file=sys.stderr)

        if not ok_any:
            print("[SUB_REFRESH_FAIL] all sources failed, keep current state", file=sys.stderr)
            return 1
        if not proxies:
            print("[SUB_REFRESH_FAIL] no usable proxies from any source, keep current state",
                  file=sys.stderr)
            return 1
        print(f"merged {len(proxies)} proxies from {len(sources)} source(s), "
              f"upstream dups dropped={dropped}")
        alive = probe_via_listeners(proxies, extra_proxies=list(extra_relays.values()))
        # 主探活崩溃防御 (2026-11-22 SUB10 接入实证): probe config invalid 时
        # 返回 [] —— 若无此守卫, relay retry 会把"配置非法全灭"续命成"只活
        # 中继节点"的部分存活集 (285/881 实测), 部署残缺池踢掉 366 个健康节点。
        # 主探活 0 活 = 探活机制本身故障, 本轮立即放弃, 保持现状。
        if not alive:
            print("[SUB_REFRESH_FAIL] probe_via_listeners returned empty "
                  "(probe config invalid or all dead), keep current state", file=sys.stderr)
            return 1
        # 失败补探: 主探活 1300+ listener 一次全起, mihomo 资源竞争下尾部源
        # 单轮波动大 (2026-11-22 SUB10 实证: 主探活 0/17, 单独复测 5/17)。
        # 失败的全部节点 (不限 relay) 补探一次, 显著降低单轮误杀。
        # 补探分批 (≤200/批): 单轮 800 listener 的 retry 与主探活同样拥挤,
        # 尾部重握手型节点 (reality) 依旧被挤 — s10 混 802 条 retry 仍 0/17,
        # 200 批实测 6/17。批间天然错开 mihomo 资源高峰。
        alive_names = {p["name"] for _, p, _, _ in alive}
        retry = [p for p in proxies if p["name"] not in alive_names]
        RETRY_BATCH = 200
        for bi in range(0, len(retry), RETRY_BATCH):
            batch = retry[bi:bi + RETRY_BATCH]
            more = probe_via_listeners(batch, extra_proxies=list(extra_relays.values()))
            print(f"retry[{bi // RETRY_BATCH + 1}]: {len(more)}/{len(batch)} recovered")
            alive.extend(more)
        print(f"alive {len(alive)}/{len(proxies)}")
        if len(alive) < 5:
            print("[SUB_REFRESH_FAIL] alive too few, keep current state", file=sys.stderr)
            return 1
        slots = MAX_PORT - BASE_PORT + 1
        if len(alive) > slots:
            print(f"[SUB_REFRESH_WARN] {len(alive)} alive > {slots} slots ({BASE_PORT}-{MAX_PORT}), "
                  f"truncating", file=sys.stderr)
            alive = alive[:slots]
        backup_with_rotation()

        # 替换桥核 (sub 前缀条目)
        is_sub = lambda name: bool(re.match(r"^s\d-", name or "") or "【机房】" in (name or "") or "【商宽】" in (name or ""))
        main["proxies"] = [p for p in main["proxies"] if not is_sub(p["name"])]
        main["listeners"] = [l for l in main["listeners"] if not str(l.get("name", "")).startswith("sub-bridge-")]
        new_proxies, new_listeners, new_nodes = [], [], []
        for i, (_, p, ip, _cc) in enumerate(alive):
            p["udp"] = False
            new_proxies.append(p)
            new_listeners.append({"name": f"sub-bridge-{i:03d}", "type": "socks",
                                  "port": BASE_PORT + i, "listen": "127.0.0.1", "proxy": p["name"]})
            reg = region_of(p["name"])
            kind = "residential" if "商宽" in p["name"] else ("datacenter" if "机房" in p["name"] else "")
            src_tag = p["name"].split("-", 1)[0]
            tags = ["clash", "bridge", "sub", src_tag, str(p.get("type", "http"))]
            if kind: tags.append(kind)
            if reg == "us-west" and kind == "residential": tags.append("us-residential")
            # 名字嵌端口 (端口唯一 -> 名字唯一): 旧方案两段都嵌 alive 顺序号 i,
            # i 随每轮探活存活数变化, 同名不同实例在 DB/nodes.yaml 撞名
            # (重名 534 条实证)。端口天然唯一且与 listener 一一对应。
            new_nodes.append({"name": f"clash-{BASE_PORT + i}-sub-{i:03d}", "host": "127.0.0.1",
                              "port": BASE_PORT + i, "protocol": "socks5h", "region": reg,
                              "egress_type": "clash_proxy", "tags": tags, "tier": "standard",
                              "relay_mode": "direct_only", "fault_domain": "huawei-sub-bridge",
                              "exit_ip": ip, "vendor_name": p["name"]})
        main["proxies"] += new_proxies
        main["listeners"] += new_listeners

        # nodes.yaml 替换 (-sub- 是本脚本独占命名空间, teleapisub/skyward/pigeonw
        # 后缀不同不会误伤)。正则必须匹配全代次: 节点名 clash-{500+i}-sub-{i}
        # 的 i 上不封顶 (曾用 ^clash-5 只够到 i<100, 6xx/7xx/8xx 历史代次永不
        # 清理, nodes.yaml 积到 1557 条陈旧条目 2026-11-22 实证)。
        SUB_NODE_RE = re.compile(r"^clash-\d+-sub-\d+$")
        nodes_d = yaml.safe_load(NODES.read_text())
        nl = nodes_d if isinstance(nodes_d, list) else nodes_d.get("nodes")
        old_node_names = {n["name"] for n in nl if SUB_NODE_RE.match(str(n["name"]))}
        nl[:] = [n for n in nl if n["name"] not in old_node_names]
        nl.extend(new_nodes)
        if isinstance(nodes_d, dict): nodes_d["nodes"] = nl
        new_names = {n["name"] for n in new_nodes}

        # 落盘前先校验 (2026-11-15 review P2: 原子写, 崩溃不留半截 yaml)
        from atomic_yaml import atomic_write_text
        atomic_write_text(CFG, yaml.safe_dump(main, allow_unicode=True, sort_keys=False))
        try:
            t = subprocess.run(["/opt/ip-pool/bin/mihomo", "-t", "-d", "/opt/ip-pool/config/huawei-bridge",
                                "-f", str(CFG)], capture_output=True, text=True, timeout=180)
        except subprocess.TimeoutExpired:
            # 校验挂死 (mihomo -t 极端卡死): 新 CFG 已落盘但未验证, 必须回滚旧配置
            print("[SUB_REFRESH_FAIL] mihomo -t timed out (180s) -> rolling back", file=sys.stderr)
            latest_cfg = sorted(glob.glob(str(BACKUP_DIR / "huawei-bridge.yaml.*")))[-1]
            subprocess.run(["cp", latest_cfg, str(CFG)], timeout=30)
            raise
        if "successful" not in t.stdout + t.stderr:
            print("[SUB_REFRESH_FAIL] mihomo -t failed: rc=%s out=%r err=%r" % (t.returncode, t.stdout[-300:], t.stderr[-300:]), file=sys.stderr)
            # 失败配置含全部订阅凭据 (URL 即凭据铁律): 0600 落盘, 部署成功后清理
            _fc = "/tmp/sub-refresh-failed-cfg.yaml"
            _fd = os.open(_fc, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
            with os.fdopen(_fd, "w") as _ff:
                _ff.write(Path(str(CFG)).read_text())
            latest_cfg = sorted(glob.glob(str(BACKUP_DIR / "huawei-bridge.yaml.*")))[-1]
            subprocess.run(["cp", latest_cfg, str(CFG)], timeout=30)
            return 1
        atomic_write_text(NODES, yaml.safe_dump(nodes_d, allow_unicode=True, sort_keys=False))

        subprocess.run(["systemctl", "restart", "ip-pool-huawei-bridge.service"], check=False, timeout=120)
        subprocess.run(["systemctl", "restart", "ip-pool-api.service", "ip-pool-gateway.service"], check=False, timeout=120)
        # 部署后验证: 桥核 listener 实际在位数 (mihomo 静默跳过冲突端口教训)
        time.sleep(6)
        r = subprocess.run(["ss", "-tln"], capture_output=True, text=True, timeout=30)
        listening = sum(1 for l in new_listeners if str(":%d " % l["port"]) in r.stdout)
        if listening < len(new_listeners) - 2:
            # 新状态已生效, 报 FAIL 会误导 —— 真回滚到部署前备份再重启。
            print("[SUB_REFRESH_FAIL] listeners missing: %d/%d up -> rolling back" % (
                listening, len(new_listeners)), file=sys.stderr)
            for suffix, dst in (("huawei-bridge.yaml.*", CFG), ("nodes.yaml.*", NODES)):
                baks = sorted(glob.glob(str(BACKUP_DIR / suffix)))
                if baks:
                    subprocess.run(["cp", baks[-1], str(dst)], check=False, timeout=30)
            subprocess.run(["systemctl", "restart", "ip-pool-huawei-bridge.service",
                            "ip-pool-api.service", "ip-pool-gateway.service"], check=False, timeout=120)
            time.sleep(6)
            r2 = subprocess.run(["ss", "-tln"], capture_output=True, text=True, timeout=30)
            listening2 = sum(1 for l in new_listeners if str(":%d " % l["port"]) in r2.stdout)
            print("[SUB_REFRESH_ROLLBACK] restored pre-refresh state, listeners=%d/%d" % (
                listening2, len(new_listeners)), file=sys.stderr)
            return 1

        # 悬空绑定清理 (移到部署验证通过后: 旧桥核活着时先清理, 请求可能重新绑到已退役节点)
        dangling = old_node_names - new_names
        if dangling:
            import sqlite3
            from datetime import datetime as _dt
            conn = sqlite3.connect(str(DB))
            cur = conn.cursor()
            ph = ",".join("?" * len(dangling))
            # 2026-11-15 review P1: 悬空绑定也要推进 epoch + 落 change 行
            # (to_node=NULL), 下游 reconcile 才知道这批账户要重新拉绑定;
            # 同时绝不删 binding_changes 历史 (那是下游增量 diff 的账本,
            # 保留窗由 prune_binding_changes 统一管)。
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

        print("deployed: %d sub nodes, bridge listeners=%d, nodes=%d, verified listening=%d" % (len(alive), len(main["listeners"]), len(nl), listening))
        return 0
    except Exception as e:
        import traceback
        print("[SUB_REFRESH_FAIL] unexpected:", e, file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        release_bridge_writer_lock(bridge_wlock)

if __name__ == "__main__":
    sys.exit(main())
