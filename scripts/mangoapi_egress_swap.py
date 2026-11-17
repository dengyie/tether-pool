#!/usr/bin/env python3
"""mangoapi-egress-swap - MangoApi 渠道 EOF -> TetherPool 主动换线 agent.

监听 apihub 蓝绿双槽日志，按渠道聚合 EOF 事件（流中断级带渠道号 / 连接级按
base_url host 归属），达到阈值后对该渠道的 TetherPool 账号
(mangoapi-ch<N>, service=mangoapi) 执行 acquire(force_rebalance, avoid_banned)
强制换线。

作用域纪律（2026-11-18 review 根修）：只看 **已迁移** 渠道（channels.setting.proxy
含 `mangoapi-ch`）。CN 直连/内环/绑定线渠道的 EOF 是上游站自身问题，换线对它们
无意义，且会对从未走网关的账号产生垃圾绑定。

保守语义：只软换绑、不 ban 节点（EOF 可能是上游站自身质量差，误 ban 会伤及
共享节点的其他消费方）。换绑**失败不消耗**冷却/事件窗口/日限（失败只进入
60s 短退避，EOF 证据保留，连续失败升级告警）——否则控制面瞬断时会静默吞掉
最需要换线场景的换线能力。
"""
import collections
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

TP_BASE = os.environ.get("TP_BASE", "http://127.0.0.1:9100")
TOKEN_FILE = os.environ.get("TOKEN_FILE", "/root/.mangoapi-tp-token")
LOGS = ["/data/new-api/logs/apihub-blue.out.log", "/data/new-api/logs/apihub-green.out.log"]
DB = "/data/new-api/data/new-api.db"

MIGRATED_MARK = "mangoapi-ch"   # channels.setting.proxy 含此标记 = 已迁移到 :9101

POLL_SECONDS = 10
WINDOW_SECONDS = 300            # EOF 事件聚合窗口
THRESHOLD = 3                   # 窗口内 >=3 次 EOF 才触发换线
PER_CHANNEL_COOLDOWN = 600      # 成功换线后单渠道最小间隔
FAIL_RETRY_SECONDS = 60         # 失败后短退避（不丢事件）
FAIL_ERROR_STREAK = 3           # 连续失败 N 次升级 [ERROR]
DAILY_CAP = 60                  # 全局每日**成功**换线上限
DB_REFRESH_SECONDS = 600        # host -> channel 映射刷新周期

RE_STREAM_BROKEN = re.compile(r"渠道\s*#(\d+)")
RE_CONN_EOF = re.compile(r'do request failed: Post "(https?://[^"]+)":\s*(EOF|unexpected EOF|connection reset by peer)')
RE_STREAM_EOF = re.compile(r'end_error="(unexpected EOF|EOF)"|reason=(eof|scanner_error)')
# 渠道 typed URL 约束解析 (2026-11-18 特定区域站点: 渠道钉 region 时 agent
# 换绑必须透传同一约束, 否则换绑会把渠道甩到错误区域)
RE_C_TYPE = re.compile(r"__type_([a-z0-9_]+)")
RE_C_REGION = re.compile(r"__region_([a-z0-9_-]+)")
RE_C_TIER = re.compile(r"__tier_([a-z0-9_]+)")
RE_C_STRICT = re.compile(r"__region_strict_true|__strict_region_true")


def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def load_token():
    try:
        return Path(TOKEN_FILE).read_text().strip()
    except OSError:
        return ""


def _host_of(url: str):
    try:
        return (urllib.parse.urlparse((url or "").strip()).hostname or "").lower()
    except Exception:
        return None


def parse_channel_params(proxy_url: str) -> dict:
    """从渠道 typed URL 解析约束 (type/region/tier/region_strict) —— 换绑透传用。

    分词方式与网关 parse_proxy_credentials 同构 (username.split("__") 后逐段
    前缀匹配), 规避正则贪婪吞 `__` 边界; flag 段先于 `region_` 值段判定
    (与网关同坑: region_strict_true 会被 region_ 前缀吞成 region="strict_true")。"""
    u = proxy_url.split("@", 1)[0]
    if "//" in u:
        u = u.split("//", 1)[1]
    user = u.rsplit(":", 1)[0] if ":" in u else u  # 剥 scheme 与 :password
    user = user.strip("/")
    p = {"egress_preference": None, "region": None, "tier": None, "region_strict": False}
    for tok in user.split("__"):
        tok = tok.strip().lower()
        if tok in ("region_strict_true", "strict_region_true", "region_strict", "strict_region"):
            p["region_strict"] = True
        elif tok.startswith("type_"):
            p["egress_preference"] = tok[5:]
        elif tok.startswith("region_"):
            p["region"] = tok[7:]
        elif tok.startswith("tier_"):
            p["tier"] = tok[5:]
    return p


def build_watch_index(rows):
    """rows = [(id, base_url, setting)] -> (host_map, migrated_ids, channel_params)。

    只保留已迁移渠道（setting 含 MIGRATED_MARK）——review P1 根修：
    非迁移渠道的 EOF 换线无意义且会产生垃圾绑定。
    channel_params: 渠道 typed URL 里的约束 (region/type/tier/region_strict),
    换绑时透传给 acquire —— 特定区域站点 (如 mulink 钉 hongkong) 换绑不脱区。"""
    host_map = collections.defaultdict(list)
    ids = set()
    params = {}
    for cid, base, setting in rows:
        if MIGRATED_MARK not in (setting or ""):
            continue
        ids.add(cid)
        h = _host_of(base)
        if h:
            host_map[h].append(cid)
        proxy = ""
        try:
            proxy = (json.loads(setting) or {}).get("proxy", "")
        except Exception:
            pass
        p = parse_channel_params(proxy)
        if any(p.values()):
            params[cid] = p
    return dict(host_map), ids, params


def load_host_channels():
    import sqlite3
    try:
        c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
        rows = c.execute("SELECT id, base_url, setting FROM channels WHERE status=1").fetchall()
        c.close()
    except Exception as e:
        log(f"[warn] DB 读取失败: {e}")
        return {}, set()
    return build_watch_index(rows)


def extract_channel_ids(line: str, host_channels: dict, migrated_ids: set) -> set:
    """从单条日志行提取目标渠道 id 集合（流中断级渠道号 ∪ 连接级 host 归属）。

    作用域在函数内收窄到已迁移渠道 —— 即使调用方漏过滤也不会对 7897 线
    渠道产生换线动作（review P1 根修: 防垃圾绑定）。"""
    cids = set()
    m = RE_STREAM_BROKEN.search(line)
    if m and RE_STREAM_EOF.search(line):
        cids.add(int(m.group(1)))
    m2 = RE_CONN_EOF.search(line)
    if m2:
        h = _host_of(m2.group(1))
        if h:
            cids.update(host_channels.get(h, []))
    return cids & migrated_ids


def swap_due(cid, now, eof_events, last_swap, last_fail, swapped_count, cap_reached):
    """触发判定：阈值达到 且 不在成功冷却/失败退避内 且 未超日限。"""
    dq = eof_events.get(cid)
    if not dq or len(dq) < THRESHOLD:
        return False
    if now - last_swap.get(cid, 0) < PER_CHANNEL_COOLDOWN:
        return False
    if now - last_fail.get(cid, 0) < FAIL_RETRY_SECONDS:
        return False
    return not cap_reached


def tp_request(path, token, timeout=15, method="GET"):
    req = urllib.request.Request(TP_BASE + path, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def rebalance(cid, token, ch_params=None):
    """对该渠道账号强制换绑；返回 (ok, detail)。

    - acquire 是 POST-only 写端点 (GET 405, review 二次根修: urllib 默认 GET)。
    - 约束参数名是 **egress_preference** (/v1/allocation/acquire 端点签名;
      `type=` 会被 FastAPI 静默忽略并回落默认 warp_ipv6, review 三次根修)。
    - ch_params: 渠道 typed URL 解析出的约束 (region/type/tier/region_strict),
      透传给 acquire —— 特定区域站点换绑不脱区 (review 五次根修)。
    - force_rebalance 本身保证换新节点; /v1/allocation/status 只有全局摘要
      无 per-account 视图, exclude_node 不可用, 不做无用调用。"""
    acct = f"mangoapi-ch{cid}"
    q = (f"/v1/allocation/acquire?service=mangoapi&account_id={acct}"
         f"&force_rebalance=true&avoid_banned=true")
    p = ch_params or {}
    if p.get("egress_preference"):
        q += f"&egress_preference={urllib.parse.quote(p['egress_preference'])}"
    if p.get("region"):
        q += f"&region={urllib.parse.quote(p['region'])}"
    if p.get("tier"):
        q += f"&tier={urllib.parse.quote(p['tier'])}"
    if p.get("region_strict"):
        q += "&region_strict=true"
    try:
        r = tp_request(q, token, method="POST")
        new_node = (r.get("allocation") or r).get("node_name") if isinstance(r, dict) else None
        return True, f"-> {new_node}"
    except urllib.error.HTTPError as e:
        return False, f"acquire http {e.code}"
    except Exception as e:
        return False, f"acquire {e}"


class LogTail:
    """增量跟踪单文件, 自动处理截断/轮转。"""

    def __init__(self, path):
        self.path = path
        self.offset = 0
        self.inode = None
        self._init()

    def _init(self):
        try:
            st = os.stat(self.path)
            self.offset, self.inode = st.st_size, st.st_ino
        except OSError:
            self.offset, self.inode = 0, None

    def poll(self):
        """返回自上次以来的新行列表。"""
        try:
            st = os.stat(self.path)
        except OSError:
            return []
        if self.inode is None:
            self._init()
            return []
        if st.st_ino != self.inode:  # 轮转: 从新文件头重读
            self.offset, self.inode = 0, st.st_ino
        elif st.st_size < self.offset:  # 截断
            self.offset = 0
        if st.st_size == self.offset:
            return []
        with open(self.path, "r", errors="replace") as f:
            f.seek(self.offset)
            data = f.read()
            self.offset = f.tell()
        return [ln for ln in data.splitlines() if ln]


def main():
    token = load_token()
    log(f"mangoapi-egress-swap start: TP_BASE={TP_BASE} token={'loaded' if token else 'MISSING'} "
        f"threshold={THRESHOLD}/{WINDOW_SECONDS}s cooldown={PER_CHANNEL_COOLDOWN}s "
        f"fail_retry={FAIL_RETRY_SECONDS}s cap={DAILY_CAP}/day (success only)")
    tails = [LogTail(p) for p in LOGS]
    host_channels, migrated_ids, channel_params = load_host_channels()
    log(f"watch: {len(migrated_ids)} migrated channels, {len(host_channels)} hosts, "
        f"{len(channel_params)} with region/type constraints")
    eof_events = collections.defaultdict(collections.deque)  # cid -> deque[ts]
    last_swap, last_fail = {}, {}
    fail_streak = collections.Counter()
    last_db_refresh = 0.0
    swapped_today_day, swapped_count = time.strftime("%Y%m%d"), 0
    while True:
        now = time.time()
        if now - last_db_refresh > DB_REFRESH_SECONDS:
            host_channels, migrated_ids, channel_params = load_host_channels()
            last_db_refresh = now
        today = time.strftime("%Y%m%d")
        if today != swapped_today_day:
            swapped_today_day, swapped_count = today, 0
        cap_reached = swapped_count >= DAILY_CAP
        for tail in tails:
            for line in tail.poll():
                for cid in extract_channel_ids(line, host_channels, migrated_ids):
                    eof_events[cid].append(now)
        # 每轮统一做窗口修剪 (事件只在到达时入列; 失败不补列不虚增)
        for cid in list(eof_events):
            dq = eof_events[cid]
            while dq and now - dq[0] > WINDOW_SECONDS:
                dq.popleft()
            if not swap_due(cid, now, eof_events, last_swap, last_fail, swapped_count, cap_reached):
                continue
            ok, detail = rebalance(cid, token, channel_params.get(cid))
            if ok:
                last_swap[cid] = now
                eof_events[cid].clear()
                fail_streak.pop(cid, None)
                swapped_count += 1
                log(f"[SWAP] ch{cid} {detail} (eof {THRESHOLD}/{WINDOW_SECONDS}s, swapped_today={swapped_count})")
            else:
                fail_streak[cid] += 1
                last_fail[cid] = now
                level = "[ERROR]" if fail_streak[cid] >= FAIL_ERROR_STREAK else "[FAIL]"
                log(f"{level} ch{cid} {detail} (streak={fail_streak[cid]}, events kept, "
                    f"retry in {FAIL_RETRY_SECONDS}s)")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
